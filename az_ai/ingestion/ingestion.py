import logging
import inspect
from typing import Callable, TypeVar, Any, get_args, get_type_hints, get_origin, List, Annotated

from rich.console import Console

from az_ai.ingestion.schema import Fragment, OperationInfo, CommandFunctionType, OperationInput, OperationOutput

logger = logging.getLogger(__name__)


class OperationError(Exception):
    pass



class Ingestion:
    def __init__(self):
        self._operations : dict[str, OperationInfo] = {}

    def operation(self) -> Callable[[CommandFunctionType], CommandFunctionType]:
        def decorator(func: CommandFunctionType) -> CommandFunctionType:
            logger.debug("Registering operation function %s...", func.__name__)
            self._operations[func.__name__] = self._parse_signature(func)
            return func

        return decorator
    
    def operations(self) -> dict[str, OperationInfo]:
        """
        Get the list of registered operations.
        """
        return self._operations
    
    def __call__(self, *args, **kwargs):
        console = Console()
        console.print(f"Run ingestion pipeline with args: {kwargs}", )
        for operation in self.operations().values():
            console.print(f"  Run [bold]{operation.name}[/]...", )
            operation(1)

    def _parse_signature(self, func: CommandFunctionType) -> OperationInfo:
        """
        Parse the signature of the function to extract its parameters and return type
        """
        logger.debug("Parsing function signature for %s...", func.__name__)

        type_hints = get_type_hints(func)
        signature = inspect.signature(func)
        

        input = self._parse_parameters(func, type_hints, signature)
        output = self._parse_return_type(func, type_hints, signature) 
            
        return OperationInfo(
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
            input =OperationInput(
                name=param_name,
                input_type=param_type,
                filter=filter,
            )
        return input


    def _parse_return_type(
        self,
        func: CommandFunctionType,
        type_hints: dict[str, Any],
        signature: inspect.Signature,
    ) -> OperationOutput:
        """
        Parse the return type of the function to extract its parameters and return type
        """
        logger.debug("Parsing return type for %s...", func.__name__)

        return_annotation = signature.return_annotation
        if return_annotation is inspect.Signature.empty:
            raise OperationError(
                f"Operation function {func.__name__} must have a return type annotation."
            )
        metadata = {}
        if get_origin(return_annotation) is Annotated:
            return_annotation, metadata = get_args(return_annotation)
            if not isinstance(metadata, dict):
                raise OperationError(
                    f"Operation function return metadata must be a dict not {metadata}"
                )

        base_type = self._get_base_type(return_annotation)
        multiple = False
        if hasattr(base_type, "__origin__"):
            if base_type.__origin__ is list:
                multiple = True
            else:
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of list[Fragment] or Fragment not {base_type}"
                )
        else:
            if not issubclass(base_type, Fragment):
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of Fragment not {base_type}"
                )

        output = OperationOutput(
            output_type=base_type,
            multiple=multiple,
            metadata=metadata,
        )
        return output
    
    def _get_base_type(self, type_hint):
        """Extract the base type from regular or Annotated types."""
        origin = get_origin(type_hint)
        if origin is Annotated:
            # For Annotated types, the first argument is the actual type
            return get_args(type_hint)[0]
        return type_hint