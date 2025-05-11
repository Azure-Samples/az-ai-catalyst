import functools
import inspect
import logging
import mimetypes
from collections.abc import Callable
from pathlib import Path
from typing import (
    Annotated,
    Any,
    get_args,
    get_origin,
    get_type_hints,
)
from urllib.parse import urlparse

from azure.ai.documentintelligence import DocumentIntelligenceClient
from azure.ai.projects import AIProjectClient
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from azure.search.documents import SearchClient
from azure.search.documents.indexes import SearchIndexClient
from pydantic import ValidationError
from semantic_kernel import Kernel

from az_ai.catalyst.azure_repository import AzureRepository
from az_ai.catalyst.helpers.content_understanding_client import AzureContentUnderstandingClient
from az_ai.catalyst.repository import LocalRepository, Repository
from az_ai.catalyst.runner import CatalystRunner, OperationError
from az_ai.catalyst.schema import (
    CommandFunctionType,
    Document,
    Fragment,
    FragmentSelector,
    OperationInputSpec,
    OperationOutputSpec,
    OperationSpec,
)
from az_ai.catalyst.settings import CatalystSettings

logger = logging.getLogger(__name__)


class CatalystInitializationError(RuntimeError):
    def __init__(self, e: ValidationError):
        message = ["Error initializing Catalyst:"]
        for error in e.errors():
            message.append(f"  - {error['loc'][-1].upper()}: {error['msg']}")
        message.append("You can use environment variables like AZURE_AI_ENDPOINT.")
        message.append("Alternatively you can also set them in .env, pyproject.toml or in the code.")
        message.append("For more information, please refer to https://aka.ms/az-ai-catalyst/docs/SETTINGS.md")
        super().__init__("\n".join(message))


class Catalyst:
    def __init__(
        self,
        repository: Repository = None,
        settings_cls: CatalystSettings = CatalystSettings,
        repository_url: str = None,
    ):
        override_settings = {}
        if repository_url:  # if repository_url is provided, it will override the settings
            override_settings["repository_url"] = repository_url

        try:
            self.settings = settings_cls(**override_settings)
        except ValidationError as e:
            raise CatalystInitializationError(e) from None

        if repository:
            self.repository = repository
        else:
            parsed_url = urlparse(self.settings.repository_url)
            match parsed_url.scheme:
                case "" | "file":
                    self.repository = LocalRepository(path=parsed_url.path)
                case "https":
                    if not self.settings.repository_container_name:
                        raise ValueError(
                            "repository_container_name setting is mandatory for an Azure Storage repository"
                        )
                    self.repository = AzureRepository(
                        url=self.settings.repository_url,
                        container_name=self.settings.repository_container_name,
                        credential=self.credential,
                    )
                case _:
                    raise OperationError(f"Unsupported repository URL : '{repository_url}'")
        self._operations: dict[str, OperationSpec] = {}

    def __call__(self, *args, **kwargs):
        CatalystRunner(self, self.repository).run(*args, **kwargs)

    def operation(self, scope="same") -> Callable[[CommandFunctionType], CommandFunctionType]:
        """
        Decorator to register an operation function.

        Args:
            scope (str): The scope of the operation. Can be "same" or "all". Default is "same".
                "same" means the operation will be executed once for each batch of fragments with
                the same source document.
        """

        def decorator(func: CommandFunctionType) -> CommandFunctionType:
            logger.debug("Registering operation function %s...", func.__name__)
            operation_spec = self._parse_operator_function_signature(func, scope)
            operation_spec.scope = scope
            self._operations[func.__name__] = operation_spec

            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)

            return wrapper  # type: ignore

        return decorator

    def operations(self) -> dict[str, OperationSpec]:
        """
        Get the list of registered operations.
        """
        return self._operations

    def update_index(self):
        """
        Update the index with the new fragments.
        """
        documents = []
        for fragment in self.repository.find(FragmentSelector(fragment_type="Chunk")):
            document = {
                "id": fragment.id,
                "content": fragment.content_as_str(),
                "vector": fragment.vector,
            }
            for key, value in fragment.metadata.items():
                if value:
                    document[key] = str(value)
            documents.append(document)

        self.search_client.upload_documents(documents)

    def add_document_from_file(self, file: str | Path, mime_type: str = None) -> Document:
        """
        Create a Document fragment from a file.
        """
        logger.debug("Creating document from file %s...", file)
        if isinstance(file, str):
            file = Path(file)
        file = file.resolve()
        if not file.exists():
            raise OperationError(f"File {file} does not exist.")

        for document in self.repository.find(FragmentSelector(fragment_type="Document"), with_content=False):
            if document.metadata.get("file_name") == file.name:
                print(f"File {file.name} already added. Ignoring.")
                return

        if mime_type is None:
            mime_type, _ = mimetypes.guess_type(str(file))
            if mime_type is None:
                mime_type = "application/octet-stream"
        document = Document(
            label="start",
            content_url=file.as_uri(),
            mime_type=mime_type,  # this is the fragment mime type
            parent_names=[file.stem],
            metadata={
                "file_name": file.name,
                "file_path": str(file),
                "file_size": file.stat().st_size,
                "file_type": mime_type,  # this is the original file mime type
            },
        )
        self.repository.store(document)

        return document

    @property
    def credential(self):
        """
        Get the Azure credential.
        """
        if not hasattr(self, "_credential"):
            self._credential = DefaultAzureCredential()
        return self._credential

    @property
    def ai_project_client(self):
        """
        Get the AI project client.
        """
        if not hasattr(self, "_ai_project_client"):
            self._ai_project_client = AIProjectClient.from_connection_string(
                conn_str=self.settings.azure_ai_project_connection_string, credential=self.credential
            )
        return self._ai_project_client

    @property
    def document_intelligence_client(self):
        """
        Get the document intelligence client.
        """
        if not hasattr(self, "_document_intelligence_client"):
            if not self.settings.azure_ai_document_intelligence_endpoint:
                raise OperationError("Azure AI Document Intelligence endpoint is not set.")
            if not self.settings.azure_ai_document_intelligence_api_version:
                raise OperationError("Azure AI Document Intelligence API version is not set.")
            self._document_intelligence_client = DocumentIntelligenceClient(
                self.settings.azure_ai_document_intelligence_endpoint,
                api_version=self.settings.azure_ai_document_intelligence_api_version,
                credential=self.credential,
            )
        return self._document_intelligence_client

    @property
    def azure_openai_client(self):
        """
        Get the Azure OpenAI client.
        """
        if not hasattr(self, "_azure_openai_client"):
            self._azure_openai_client = self.ai_project_client.inference.get_azure_openai_client(
                api_version=self.settings.azure_openai_api_version
            )
        return self._azure_openai_client

    @property
    def search_index_client(self):
        """
        Get the Azure AI Search Index client.
        """
        if not hasattr(self, "_search_index_client"):
            self._search_index_client = SearchIndexClient(
                endpoint=self.settings.azure_ai_search_endpoint,
                credential=self.credential,
            )
        return self._search_index_client

    @property
    def search_client(self):
        """
        Get the Azure AI Search client.
        """
        if not hasattr(self, "_search_client"):
            self._search_client = SearchClient(
                endpoint=self.settings.azure_ai_search_endpoint,
                credential=self.credential,
                index_name=self.settings.index_name,
            )
        return self._search_client

    @property
    def content_understanding_client(self) -> AzureContentUnderstandingClient:
        """
        Get the Azure AI Content Understanding client.
        """
        if not hasattr(self, "_content_understanding_client"):
            self._content_understanding_client = AzureContentUnderstandingClient(
                endpoint=self.settings.azure_content_understanding_endpoint,
                api_version=self.settings.azure_content_understanding_api_version,
                # Hack to get around analyzer creation not working with RBAC "Cognitive Services Contributor" role
                subscription_key=self.azure_openai_client.api_key,
                token_provider=get_bearer_token_provider(
                    self.credential, "https://cognitiveservices.azure.com/.default"
                ),
            )
        return self._content_understanding_client


    @property
    def kernel(self) -> Kernel:
        """
        Get the Semantic Kernel instance.
        """
        if not hasattr(self, "_kernel"):
            self._kernel = Kernel()
        return self._kernel
    def _parse_operator_function_signature(self, func: CommandFunctionType, scope: str) -> OperationSpec:
        logger.debug("Parsing function signature for %s...", func.__name__)
        type_hints = get_type_hints(func)
        signature = inspect.signature(func)

        return OperationSpec(
            name=func.__name__,
            func=func,
            input_specs=self._parse_operator_function_parameters(func, type_hints, signature),
            output_spec=self._parse_operator_function_return_type(func, type_hints, signature),
        )

    def _parse_operator_function_parameters(
        self,
        func: CommandFunctionType,
        type_hints: dict[str, Any],
        signature: inspect.Signature,
    ) -> list[OperationInputSpec]:
        """
        Parse the parameters of the function to extract their types and names
        """
        logger.debug("Parsing parameters for %s...", func.__name__)
        input_specs = []
        for param in signature.parameters.values():
            param_name = param.name
            param_type = param.annotation
            if param_name == "self":
                continue

            input = self._parse_operator_function_parameter(func, param_name, param_type)
            input_specs.append(input)

        if len(input_specs) == 0:
            raise OperationError(f"Operation function {func.__name__} must have at least one 'input' parameter.")

        return input_specs

    def _parse_operator_function_parameter(
        self,
        func: CommandFunctionType,
        param_name: str,
        param_type: Any,
    ) -> OperationInputSpec:
        logger.debug("Parameter: %s, Type: %s", param_name, param_type)
        filter = {}

        if get_origin(param_type) is Annotated:
            param_type, filter = get_args(param_type)
            if not isinstance(filter, dict):
                raise OperationError(f"Operation function parameter {param_name} filter must be a dict not {filter}")

        base_type = self._get_base_type(param_type)
        multiple = False
        if hasattr(base_type, "__origin__"):
            if base_type.__origin__ is list:
                multiple = True
                base_type = get_args(base_type)[0]
            else:
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of list[Fragment] or "
                    f"Fragment not {base_type}"
                )
        else:
            if not issubclass(base_type, Fragment):
                raise OperationError(
                    f"Operation function parameter {param_name} must be of type Fragment not {param_type}"
                )

        return OperationInputSpec(
            name=param_name,
            fragment_type=base_type.__name__,
            multiple=multiple,
            filter=filter,
        )

    def _parse_operator_function_return_type(
        self,
        func: CommandFunctionType,
        type_hints: dict[str, Any],
        signature: inspect.Signature,
    ) -> OperationOutputSpec:
        logger.debug("Parsing return type for %s...", func.__name__)
        label = None
        return_annotation = signature.return_annotation
        if return_annotation is inspect.Signature.empty:
            raise OperationError(f"Operation function {func.__name__} must have a return type annotation.")
        if get_origin(return_annotation) is Annotated:
            return_annotation, label = get_args(return_annotation)
            if not isinstance(label, str):
                raise OperationError(f"Operation function return Fragment label must be a str not {label}")
        else:
            raise OperationError(
                f"""Operation {func.__name__} must have a return type of Annotated[Fragment, "label"] """
                f"not {return_annotation}"
            )

        base_type = self._get_base_type(return_annotation)
        multiple = False
        if hasattr(base_type, "__origin__"):
            if base_type.__origin__ is list:
                multiple = True
                base_type = get_args(base_type)[0]
            else:
                raise OperationError(
                    f"Operation function {func.__name__} must have a return type of list[Fragment] or "
                    f"Fragment not {base_type}"
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
