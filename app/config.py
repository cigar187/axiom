"""
Central configuration loaded from environment variables / .env file.
"""
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    DATABASE_URL: str
    RUNDOWN_API_KEY: str
    RUNDOWN_BASE_URL: str = "https://therundown-therundown-v1.p.rapidapi.com"
    AXIOM_INTERNAL_TOKEN: str
    APP_ENV: str = "production"
    LOG_LEVEL: str = "INFO"
    PORT: int = 8080


settings = Settings()
