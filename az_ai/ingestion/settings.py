
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

class IngestionSettings(BaseSettings):
    azure_ai_project_connection_string: str
    azure_ai_endpoint: str = Field(validation_alias=AliasChoices("azure_ai_endpoint", "azure_openai_endpoint"))
    azure_openai_api_version: str = Field(validation_alias=AliasChoices("openai_api_version", "azure_openai_api_version"))
    azure_ai_search_endpoint: str = Field(validation_alias=AliasChoices("search_endpoint", "azure_ai_search_endpoint"))

    model_config = SettingsConfigDict(
        env_file='.env', 
        pyproject_toml_depth=5,
        pyproject_toml_table_header=('tool', 'az_ai', 'ingestion'),
        extra='ignore')

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

