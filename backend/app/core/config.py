from pydantic_settings import BaseSettings
from typing import Optional

class Settings(BaseSettings):
    # Database - default to SQLite for easy local development
    database_url: str = "sqlite:///./moderation.db"
    
    # Claude API
    anthropic_api_key: str = ""
    
    # JWT
    secret_key: str = "change-this-in-production-secret-key-12345"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 480  # 8 hours
    
    # API Settings
    api_v1_str: str = "/api/v1"
    
    # Snowflake (optional - used for pulling messages from DWH)
    snowflake_user: Optional[str] = None
    snowflake_password: Optional[str] = None
    snowflake_account: Optional[str] = None
    snowflake_warehouse: Optional[str] = None
    snowflake_role: Optional[str] = None
    
    class Config:
        env_file = ".env"

settings = Settings()
