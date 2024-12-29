import os

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """App settings from env file."""
    model_config = SettingsConfigDict(
        env_file=os.environ.get('ENV_FILE', '.env.local'),
        extra='allow',
    )
    CONFIG_PATH: str


settings = Settings()
