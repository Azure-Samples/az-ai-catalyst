from collections import OrderedDict
from inspect import getsource
from itertools import chain
from textwrap import dedent

from az_ai.ingestion import Ingestion


def markdown(ingestion: Ingestion, title:str) -> str:
    return "\n".join(
        [
            f"# {title}",
            "## Diagram",       
            "```mermaid",
            "---",
            f"title: {title}",
            "---",
            mermaid(ingestion),
            "```",
            "## Operations documentation",
        ]

        + [f"### {operation.name}\n\n{dedent(operation.func.__doc__ or "")}" 
           "<details>\n<summary>Code</summary>\n\n"
           f"```python\n{getsource(operation.func)}\n```\n\n"
           "</details>\n"
           for operation in ingestion.operations().values()]
    )


def mermaid(ingestion: Ingestion) -> str:
    """
    Generate a mermaid diagram of the ingestion pipeline.
    """
    input_boxes = OrderedDict()
    operation_boxes = OrderedDict()
    output_boxes = OrderedDict()

    # generate boxes for operations and outputs
    for operation in ingestion.operations().values():
        selector = operation.output_spec.selector()
        for label in selector.labels:
            output_boxes[
                (
                    f"{selector.fragment_type}_{label}",
                    f"""@{{ shape: doc, label: "{selector.fragment_type}[{label}]" }}""",
                )
            ] = selector

        operation_boxes[f"""    {operation.name}@{{ shape: rect, label: "{operation.name}" }}"""] = operation

    # generate boxes for inputs (based on existing outputs)
    for operation in ingestion.operations().values():
        for input in operation.input_specs:
            selector = input.selector()
            for output_selector in chain(output_boxes.values(), input_boxes.values()):
                if selector.matches(output_selector):
                    # we have an input box that matches an output box or existing input box: do nothing
                    break
            else:
                if len(selector.labels) == 0:
                    input_boxes[
                        (f"{selector.fragment_type}", f"""@{{ shape: doc, label: "{selector.fragment_type}[]" }}""")
                    ] = selector
                else:
                    for label in selector.labels:
                        input_boxes[
                            (
                                f"{selector.fragment_type}_{label}",
                                f"""@{{ shape: doc, label: "{selector.fragment_type}[{label}]" }}""",
                            )
                        ] = selector

    diagram = ["flowchart TD"]
    diagram += [f"    {box[0]}{box[1]}" for box in input_boxes]
    diagram.append("")
    diagram += [box for box in operation_boxes]
    diagram.append("")
    diagram += [f"    {box[0]}{box[1]}" for box in output_boxes]
    diagram.append("")

    for operation in ingestion.operations().values():
        for input in operation.input_specs:
            selector = input.selector()
            multiple = "- \\* -" if input.multiple else ""
            # we don't have a selector match all outputs
            for box, sel in chain(output_boxes.items(), input_boxes.items()):
                if selector.matches(sel):
                    diagram.append(f"""    {box[0]} -{multiple}-> {operation.name}""")

        selector = operation.output_spec.selector()
        labels = [""] if not selector.labels else selector.labels
        multiple = "- \\* -" if operation.output_spec.multiple else ""
        for label in labels:
            diagram.append(f"""    {operation.name} -{multiple}-> {selector.fragment_type}_{label}""")
        diagram.append("")

    return "\n".join(diagram)
