import time

from rich.console import Console
from rich.markup import escape

from az_ai.catalyst.helpers.rich import fragment_as_table
from az_ai.catalyst.repository import Repository
from az_ai.catalyst.schema import (
    Fragment,
    OperationsLogEntry,
    OperationSpec,
)


class OperationError(Exception):
    pass


class CatalystRunner:
    def __init__(self, catalyst, repository: Repository = None):
        self.catalyst = catalyst
        self.repository = catalyst.repository
        self._console = Console()

    def run(self, *args, **kwargs):
        self._console.log(
            f"Run catalyst pipeline with args: {kwargs}",
        )
        with self._console.status("Running catalyst pipeline...") as status:
            try:
                for operation in self.catalyst.operations().values():
                    status.update(
                        f"Running {escape(str(operation))}...",
                        spinner="dots",
                    )
                    self._run_operation(operation)
            except Exception as e:
                self._console.log(f"Error running catalyst pipeline: {e}")
                raise e

    def _run_operation(self, operation: OperationSpec):
        self._console.log(f"Running {escape(str(operation))}: ")

        inputs = [self.repository.find(input.selector()) for input in operation.input_specs]

        call_arguments = self._create_call_arguments(operation, inputs, operation.scope == "same")
        for arguments in call_arguments:
            input_fragment_ids = self._input_fragment_ids_set(arguments)
            if self._skip_operation(input_fragment_ids, operation):
                self._console.log(f"  Skip for {escape(str(input_fragment_ids))}...")
            else:
                self._console.log(f"  Execute with {escape(str(input_fragment_ids))}...")
                start_time = time.time_ns()
                results = operation.func(*arguments)
                end_time = time.time_ns()
                self._process_operation_result(operation, input_fragment_ids, results, end_time - start_time)

    def _skip_operation(self, input_fragment_ids: set[str], operation: OperationSpec) -> bool:
        return (
            len(
                self.repository.find_operations_log_entry(
                    operation_name=operation.name,
                    input_fragment_refs=input_fragment_ids,
                )
            )
            > 0
        )

    def _create_call_arguments(
        self, operation: OperationSpec, inputs: list[list[Fragment]], same_scope
    ) -> list[list[list[Fragment] | Fragment]]:
        """
        Create argument lists for operation function calls based on input fragments.

        When same_scope is True, arguments are grouped by source document reference,
        ensuring that operation functions only receive fragments from the same source.
        Otherwise, all input combinations are considered.

        For each input specification:
        - Single inputs: Each matching fragment generates a separate argument set.
        - Multiple inputs (input_spec.multiple=True): The entire list of matching fragments
          is passed as a single argument.

        Args:
            operation: The operation specification containing input requirements.
            inputs: Lists of fragments matching each input specification.
            same_scope: If True, only combine fragments from the same source document.

        Returns:
            A list of argument lists ready to be passed to the operation function.
        """
        source_refs = set()
        if same_scope:
            for input_list in inputs:
                fragments = input_list if isinstance(input_list, list) else [input_list]
                source_refs.update(fragment.source_document_ref() for fragment in fragments)
        else:
            source_refs = {"placeholder"}

        call_arguments_by_source = {source_ref: [[]] for source_ref in source_refs}

        for input_spec, fragments in zip(operation.input_specs, inputs, strict=True):
            for source_ref in source_refs:
                current_args = call_arguments_by_source[source_ref]
                if same_scope:
                    matching_fragments = [f for f in fragments if f.source_document_ref() == source_ref]
                else:
                    matching_fragments = fragments

                if input_spec.multiple:
                    # For multiple inputs, append the list of matching fragments to each argument set
                    for args in current_args:
                        args.append(matching_fragments)
                else:
                    # For single inputs, create a new argument set for each matching fragment
                    call_arguments_by_source[source_ref] = [
                        args + [fragment] for args in current_args for fragment in matching_fragments
                    ]
        return [arg for args_list in call_arguments_by_source.values() for arg in args_list]

    def _process_operation_result(
        self,
        operation: OperationSpec,
        input_fragment_ids: set[str],
        result: Fragment | list[Fragment],
        duration_ns: int,
    ):
        if result is None:
            raise OperationError(
                f"Operation {operation.name} returned None"  # TODO: better document for which run
            )
        results = result if operation.output_spec.multiple else [result]

        for result in results:
            output_spec = operation.output_spec.selector()
            if not output_spec.matches(result):
                self._console.log(f"Result {result} does not match output spec {escape(str(output_spec))}")
                raise OperationError(
                    f"Non compliant Fragment returned for operation {operation.name}"  # TODO: better document wich run
                )
            self._console.log(f"    -> Storing {escape(str(result))}...")
            self.repository.store(result)
            self._console.log(fragment_as_table(result))

        self.repository.add_operations_log_entry(
            OperationsLogEntry(
                operation_name=operation.name,
                input_refs=input_fragment_ids,
                output_refs=[fragment.id for fragment in results],
                duration_ns=duration_ns,
            )
        )

    def _input_fragment_ids_set(self, arguments: list[list[Fragment] | Fragment]) -> set[str]:
        """
        Flatten the input arguments for the operation.
        """
        input_fragments = []
        for arg in arguments:
            if isinstance(arg, list):
                input_fragments.extend(arg)
            else:
                input_fragments.append(arg)

        return set(fragment.id for fragment in input_fragments)
