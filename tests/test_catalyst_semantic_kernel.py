from pathlib import Path
from typing import Annotated

from pydantic import Field
import pytest
from semantic_kernel.connectors.ai.function_choice_behavior import FunctionChoiceBehavior
from semantic_kernel.connectors.ai.open_ai.services.azure_chat_completion import AzureChatCompletion
from semantic_kernel.contents import ChatHistory
from semantic_kernel.kernel_pydantic import KernelBaseModel

from az_ai.catalyst import Catalyst, Document, Fragment, OperationError
from az_ai.catalyst.repository import LocalRepository
from az_ai.catalyst.schema import FragmentSelector


@pytest.fixture(scope="function")
def catalyst(tmpdir):
    return Catalyst(repository_url=str(tmpdir))


class Step(KernelBaseModel):
    explanation: str = Field(description="The explanation of the step")
    output: str


class Reasoning(KernelBaseModel):
    steps: list[Step]
    final_answer: str


@pytest.mark.asyncio
@pytest.mark.skip(reason="TBD")
async def test_kernel_call(catalyst):
    kernel = catalyst.kernel
    assert kernel is not None

    service_id = "structured-output"
    kernel.add_service(
        AzureChatCompletion(
            service_id=service_id,
            endpoint=catalyst.settings.azure_ai_endpoint,
            api_version=catalyst.settings.azure_openai_api_version,
            # deployment_name="gpt-4.1-2025-04-14",
            deployment_name="o1-2024-12-17",
        )
    )

    req_settings = kernel.get_prompt_execution_settings_from_service_id(service_id=service_id)
    # req_settings.max_tokens = 2000
    # req_settings.temperature = 0.7
    # req_settings.top_p = 0.8
    req_settings.function_choice_behavior = FunctionChoiceBehavior.Auto(filters={"excluded_plugins": ["chat"]})

    # NOTE: This is the key setting in this example that tells the OpenAI service
    # to return structured output based on the Pydantic model Reasoning.
    req_settings.response_format = Reasoning

    chat_function = kernel.add_function(
        prompt="You are a helpful math tutor. Guide the user through the solution step by step.\n\n"
        + """{{$chat_history}}""",
        function_name="chat",
        plugin_name="chat",
        prompt_execution_settings=req_settings,
    )

    history = ChatHistory()
    history.add_user_message("how can I solve 8x + 7y = -23, and 4x=12?")

    result = await kernel.invoke(chat_function, chat_history=history)

    reasoning = Reasoning.model_validate_json(result.value[0].content)

    print("Reasoning:", reasoning.model_dump_json(indent=2))
