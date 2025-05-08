from pathlib import Path

from pydantic import (
    AliasChoices,
    Field,
)
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    PyprojectTomlConfigSettingsSource,
    SettingsConfigDict,
)

# TODO: implement load_azd_env


class CatalystSettings(BaseSettings):
    repository_path: Path
    index_name: str | None = Field(
        default=None,
        validation_alias=AliasChoices("index_name", "Name of the index to use (and create if it does not exist)"),
    )

    azure_ai_project_connection_string: str
    azure_ai_endpoint: str = Field(validation_alias=AliasChoices("azure_ai_endpoint", "azure_openai_endpoint"))
    azure_ai_content_understanding_endpoint: str = Field(
        description="The endpoint to use for the Content Understanding endpoint",
    )
    azure_ai_content_understanding_api_version: str = Field(
        default="2024-12-01-preview",
        description="The API version to use for the Content Understanding endpoint",
    )
    azure_ai_document_intelligence_endpoint: str = Field(
        default_factory=lambda data: data['azure_ai_endpoint'],
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
