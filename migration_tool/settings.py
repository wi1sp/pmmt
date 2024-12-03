from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """App settings from env file."""

    DB_HOST: str
    DB_PORT: int
    DB_USER: str
    DB_USER_PASSWORD: str
    GH_PAT: str
