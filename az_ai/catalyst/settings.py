import urllib.parse
from pathlib import Path

from pydantic import (
    AliasChoices,
    Field,
    field_validator,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)

# TODO: implement load_azd_env


class CatalystSettings(BaseSettings):
    repository_url: Path | str = Field(
        description="URL of the repository, which can be a local path or remote Azure Storage Account URL"
    )
    repository_container_name: str | None = Field(
        default=None, description="Name of the blob container name within the Azure storage"
    )

    @field_validator("repository_url")
    def validate_repository_url(cls, v: Path | str) -> str:
        try:
            v = v.resolve().as_uri() if isinstance(v, Path) else v
            parsed = urllib.parse.urlparse(v)
            if not parsed.scheme and not parsed.netloc and not parsed.path:
                raise ValueError("Repository URL cannot be empty")
            return v
        except Exception as e:
            raise ValueError(f"Invalid repository URL: '{v}'") from e

    index_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("index_name", "Name of the index to use (and create if it does not exist)"),
    )

    azure_ai_project_connection_string: str
    azure_ai_endpoint: str = Field(validation_alias=AliasChoices("azure_ai_endpoint", "azure_openai_endpoint"))
    azure_content_understanding_endpoint: str = Field(
        description="The endpoint to use for the Content Understanding endpoint",
    )
    azure_content_understanding_api_version: str = Field(
        default="2024-12-01-preview",
        description="The API version to use for the Content Understanding endpoint",
    )
    azure_ai_document_intelligence_endpoint: str = Field(
        default_factory=lambda data: data["azure_ai_endpoint"],
        description="The endpoint to use for the Document Intelligence endpoint",
    )
    azure_ai_document_intelligence_api_version: str = Field(
        default="2024-11-30",
        description="The API version to use for the Document Intelligence endpoint",
    )
    azure_openai_api_version: str = Field(
        validation_alias=AliasChoices("openai_api_version", "azure_openai_api_version")
    )
    azure_ai_search_endpoint: str = Field(validation_alias=AliasChoices("search_endpoint", "azure_ai_search_endpoint"))
    azure_ai_endpoint: str = Field(validation_alias=AliasChoices("azure_ai_endpoint", "azure_openai_endpoint"))

    model_config = SettingsConfigDict(
        env_file=".env",
        pyproject_toml_depth=5,
        pyproject_toml_table_header=("tool", "az_ai", "catalyst"),
        extra="ignore",
    )

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        return (
            init_settings,
            PyprojectTomlConfigSettingsSource(settings_cls),
            env_settings,
            dotenv_settings,
            file_secret_settings,
        )
