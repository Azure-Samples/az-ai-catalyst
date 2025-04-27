import time
from pathlib import Path

from rich.console import Console
from rich.markup import escape

from az_ai.ingestion.repository import Repository
from az_ai.ingestion.schema import (
    Document,
    Fragment,
    FragmentSpec,
    OperationInputSpec,
    OperationOutputSpec,
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
        specs = operation.input.specs()
        self._console.log(f"Running {escape(str(operation))}: ")
        for spec in specs:
            fragments = self.repository.find(spec)
            for fragment in fragments:
                if (
                    len(
                        self.repository.find_operations_log_entry(
                            operation_name=operation.name,
                            input_fragment_ref=fragment.id,
                        )
                    )
                    > 0
                ):
                    self._console.log(
                        f"  Skip {escape(fragment.human_file_name())} ({fragment.id})..."
                    )
                else:
                    self._console.log(
                        f"  Apply on {escape(fragment.human_file_name())} ({fragment.id})..."
                    )
                    self._run_operation_on_fragment(operation, fragment)


    def _run_operation_on_fragment(self, operation: OperationSpec, fragment: Fragment):
        result = operation.func(fragment)
        if result is None:
            raise OperationError(
                f"Operation {operation.name} returned None for fragment {fragment.id}"
            )
        results = result if operation.output.multiple else [result]

        for result in results:
            output_spec = operation.output.spec()
            if not output_spec.matches(result):
                self._console.log(
                    f"Result {result} does not match output spec {escape(str(output_spec))}"
                )
                raise OperationError(
                    f"Non compliant Fragment returned for operation {operation.name}: {escape(str(fragment))}"
                )
            self._console.log(
                f"    -> Storing {escape(str(result))}..."
            )
            self.repository.store(result)
            self._console.log(fragment_as_table(result))

        self.repository.add_operations_log_entry(
            OperationsLogEntry.create_from(
                operation=operation,
                input_fragments=[fragment],
                output_fragments=results,
            )
        )

