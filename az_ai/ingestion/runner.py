import time

from rich.console import Console
from rich.markup import escape

from az_ai.ingestion.repository import Repository
from az_ai.ingestion.schema import (
    Fragment,
    OperationsLogEntry,
    OperationSpec,
)
from az_ai.ingestion.tools.rich import fragment_as_table


class OperationError(Exception):
    pass


class IngestionRunner:
    def __init__(self, ingestion, repository: Repository = None):
        self.ingestion = ingestion
        self.repository = ingestion.repository
        self._console = Console()

    def run(self, *args, **kwargs):
        self._console.log(
            f"Run ingestion pipeline with args: {kwargs}",
        )
        with self._console.status("Running ingestion pipeline...") as status:
            try:
                for operation in self.ingestion.operations().values():
                    status.update(
                        f"Running {escape(str(operation))}...",
                        spinner="dots",
                    )
                    self._run_operation(operation)
            except Exception as e:
                self._console.log(f"Error running ingestion pipeline: {e}")
                raise e

    def _run_operation(self, operation: OperationSpec):
        self._console.log(f"Running {escape(str(operation))}: ")
        inputs = [self.repository.find(input.selector()) for input in operation.input_specs]

        call_arguments = self._create_call_arguments(operation, inputs)

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
        return len(
                self.repository.find_operations_log_entry(
                    operation_name=operation.name,
                    input_fragment_refs=input_fragment_ids,
                )
            ) > 0

    def _create_call_arguments(self, operation: OperationSpec, inputs: list[list[Fragment]]) -> list[list[Fragment] | Fragment]:
        """
        Create the call arguments for calling the operation's function. Take into account if
        input parameters are multiple or not. Use inputs as a list of filtered argument
        values for each argument. This function will return a list of arguments to be passed
        to the operation function for each call of the function.
        """
        call_arguments = [[]]
        for input_spec, fragments in zip(operation.input_specs, inputs, strict=True):
            if input_spec.multiple:
                # current input is multiple: we append the fragments for that input to every existing call in the list
                for arguments in call_arguments:
                    arguments.append(fragments)
            else:
                # current input is single: we need to create a new call for each fragment filtered for this input
                call_arguments = [
                    previous_args.copy() + [fragment] for previous_args in call_arguments for fragment in fragments
                ]
        return call_arguments

    def _process_operation_result(
        self, operation: OperationSpec, input_fragment_ids: set[str], result: Fragment | list[Fragment], duration_ns: int
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
                    f"Non compliant Fragment returned for operation {operation.name}"  # TODO: better document for which run
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