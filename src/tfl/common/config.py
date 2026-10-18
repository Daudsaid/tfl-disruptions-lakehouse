from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """All config comes from environment variables or .env. Nothing is hard-coded."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    kafka_bootstrap_servers: str = "localhost:29092"
    topic_line_status: str = "tfl.line_status"

    tfl_url: str = "https://api.tfl.gov.uk/Line/Mode/tube/Status"
    tfl_app_key: str | None = None
    poll_interval_seconds: int = 60
    request_timeout_seconds: int = 10

    lake_root: str = "/app/lake"
    checkpoint_root: str = "/app/lake/_checkpoints"
    topic_dlq: str = "tfl.dlq"
    topic_status_changes: str = "tfl.status_changes"
    episode_gap_minutes: int = 5

    postgres_host: str = "postgres"
    postgres_port: int = 5432
    postgres_db: str = "tfl"
    postgres_user: str = "tfl"
    postgres_password: str = ""

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
