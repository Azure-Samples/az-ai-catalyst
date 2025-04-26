import inspect
import logging
import mimetypes
from pathlib import Path
from typing import (
    Annotated,
    Any,
    Callable,
    get_args,
    get_origin,
    get_type_hints,
)

from rich.console import Console

from az_ai.ingestion.repository import Repository
from az_ai.ingestion.schema import (
    CommandFunctionType,
    Document,
    Fragment,
    FragmentSpec,
    OperationSpec,
    OperationInputSpec,
    OperationOutputSpec,
    OperationsLogEntry,
)

logger = logging.getLogger(__name__)


class OperationError(Exception):
    pass


class Ingestion:
    def __init__(self, repository: Repository = None):
        self._operations: dict[str, OperationSpec] = {}
        self.repository = repository

    def operation(self) -> Callable[[CommandFunctionType], CommandFunctionType]:
        def decorator(func: CommandFunctionType) -> CommandFunctionType:
            logger.debug("Registering operation function %s...", func.__name__)
            self._operations[func.__name__] = self._parse_signature(func)
            return func

        return decorator

    def operations(self) -> dict[str, OperationSpec]:
        """
        Get the list of registered operations.
        """
        return self._operations

    def __call__(self, *args, **kwargs):
        console = Console()
        console.log(
            f"Run ingestion pipeline with args: {kwargs}",
        )
        with console.status("Running ingestion pipeline...") as status:
            try:
                for operation in self.operations().values():
                    status.update(
                        f"Running operation: {operation.name} ({len(self.repository.find())})...",
                        spinner="dots",
                    )
                    self._run_operation(operation, console)
            except Exception as e:
                console.log(f"Error running ingestion pipeline: {e}")
                raise e

    def _run_operation(self, operation: OperationSpec, console):
        specs = operation.input.specs()
        for spec in specs:
            fragments = self.repository.find(spec)
            for fragment in fragments:
                if len(self.repository.find_operations_log_entry(
                    operation_name=operation.name,
                    input_fragment_ref=fragment.id
                )) > 0:
                    console.log(f"Skipping operation {operation.name} on fragment {fragment.id}...")
                else:
                    self._run_operation_on_fragment(operation, fragment, console)

    def _run_operation_on_fragment(self, operation: OperationSpec, fragment: Fragment, console):
        console.log(
            f"Running operation {operation.name} on fragment {fragment.id}..."
        )
        result = operation.func(fragment)
    
        if not operation.output.multiple:
            result = [result]
        if result is None:
            raise OperationError(
                f"Operation {operation.name} returned None for fragment {fragment.id}"
            )
        for res in result:
            output_spec = operation.output.spec()
            if not output_spec.matches(res):
                console.log(
                    f"Result {res.id} does not match output spec {output_spec}"
                )
                raise OperationError(
                    f"Non compliant Fragment returned for operation {operation.name}: {fragment}"
                )
            console.log(
                f" -> Storing result {res.id}:{type(res).__name__}\\[{res.label}]: {res.human_name()}..."
            )
            self.repository.store(res)
        self.repository.add_operations_log_entry(
            OperationsLogEntry.create_from(
                operation=operation,
                input_fragments=[fragment],
                output_fragments=result,
            )
        )

    def mermaid(self) -> str:
        """
        Generate a mermaid diagram of the ingestion pipeline.
        """
        diagram = "flowchart TD\n"
        fragment_specs = set()
        for operation in self.operations().values():
            fragment_specs.add(operation.output.spec())
        for spec in fragment_specs:
            fragment_label = spec.fragment_type
            if spec.label:
                fragment_label += f"[{spec.label}]"
            shape = "doc"
            diagram += (
                f"""    {spec}@{{ shape: {shape}, label: "{fragment_label}" }}\n"""
            )

        for operation in self.operations().values():
            diagram += f"""    {operation.name}@{{ shape: rect, label: "{operation.name}" }}\n"""
            diagram += f"""    {operation.name} --> {operation.output.spec()}\n"""
            for spec in operation.input.specs():
                diagram += f"""    {spec} --> {operation.name}\n"""

        return diagram

    def add_document_from_file(
        self, file: str | Path, mime_type: str = None
    ) -> Document:
        """
        Create a Document fragment from a file.
        """
        logger.debug("Creating document from file %s...", file)
        if isinstance(file, str):
            file = Path(file)
        file = file.resolve()
        if not file.exists():
            raise OperationError(f"File {file} does not exist.")

        if self.repository.find(FragmentSpec(
                fragment_type="Document",
                label="start"
            )
        ):
            return

        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(str(file))
            if mime_type is None:
                mime_type = "application/octet-stream"
        document = Document(
            label="start",
            content_url=file.as_uri(),
            metadata={
                "file_name": file.name,
                "file_path": str(file),
                "file_size": file.stat().st_size,
                "mime_type": mime_type,
            },
        )

        self.repository.store(document)
        return document

    def _parse_signature(self, func: CommandFunctionType) -> OperationSpec:
        """
        Parse the signature of the function to extract its parameters and return type
        """
        logger.debug("Parsing function signature for %s...", func.__name__)

        type_hints = get_type_hints(func)
        signature = inspect.signature(func)

        input = self._parse_parameters(func, type_hints, signature)
        output = self._parse_return_type(func, type_hints, signature)

        return OperationSpec(
            name=func.__name__,
            func=func,
            input=input,
            output=output,
        )

    def _parse_parameters(
        self,
        func: CommandFunctionType,
        type_hints: dict[str, Any],
        signature: inspect.Signature,
    ):
        if len(signature.parameters) != 1:
            raise OperationError("Operation function must have exactly 1 parameter.")

        for param in signature.parameters.values():
            param_name = param.name
            param_type = param.annotation
            logger.debug("Parameter: %s, Type: %s", param_name, param_type)

            filter = {}
            if get_origin(param_type) is Annotated:
                param_type, filter = get_args(param_type)
                if not isinstance(filter, dict):
                    raise OperationError(
                        f"Operation function parameter {param_name} filter must be a dict not {filter}"
                    )

            if not issubclass(param_type, Fragment):
                raise OperationError(
                    f"Operation function parameter {param_name} must be of type Fragment not {param_type}"
                )
            input = OperationInputSpec(
                name=param_name,
                fragment_type=param_type.__name__,
                filter=filter,
            )
        return input

    def _parse_return_type(
        self,
        func: CommandFunctionType,
        type_hints: dict[str, Any],
        signature: inspect.Signature,
    ) -> OperationOutputSpec:
        """
        Parse the return type of the function to extract its parameters and return type
        """
        logger.debug("Parsing return type for %s...", func.__name__)
        label = None
        return_annotation = signature.return_annotation
        if return_annotation is inspect.Signature.empty:
            raise OperationError(
                f"Operation function {func.__name__} must have a return type annotation."
            )
        if get_origin(return_annotation) is Annotated:
            return_annotation, label = get_args(return_annotation)
            if not isinstance(label, str):
                raise OperationError(
                    f"Operation function return Fragment label must be a str not {label}"
                )

        base_type = self._get_base_type(return_annotation)
        multiple = False
        if hasattr(base_type, "__origin__"):
            if base_type.__origin__ is list:
                multiple = True
                base_type = get_args(base_type)[0]
            else:
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of list[Fragment] or Fragment not {base_type}"
                )
        else:
            if not issubclass(base_type, Fragment):
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of Fragment not {base_type}"
                )

        output = OperationOutputSpec(
            fragment_type=base_type.__name__,
            multiple=multiple,
            label=label,
        )
        return output

    def _get_base_type(self, type_hint):
        """Extract the base type from regular or Annotated types."""
        origin = get_origin(type_hint)
        if origin is Annotated:
            # For Annotated types, the first argument is the actual type
            return get_args(type_hint)[0]
        return type_hint
