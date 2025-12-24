from pydantic import ConfigDict, SecretStr
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """
    Application configuration loaded from environment variables
    or defaults.

    Attributes:
        redis_host (str): Hostname for the Redis server.
        redis_port (int): Port number for Redis.
        redis_db (int): Redis database index.
        redis_password (SecretStr): Password for Redis authentication. Optional.
        redis_decode_responses (bool): Whether Redis should decode bytes to str. Defaults to True.
        log_level (str): Logging level for the application. Defaults to 'INFO'.
    """

    redis_host: str = "localhost"
    redis_port: int
    redis_db: int
    redis_password: SecretStr
    redis_decode_responses: bool = True
    log_level: str = "INFO"

    model_config = ConfigDict(
        env_file=".env",
        env_prefix="APP_",
    )


# Singleton settings instance to be imported throughout the project
settings = Settings()
