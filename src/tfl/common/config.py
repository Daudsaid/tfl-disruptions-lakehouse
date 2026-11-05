from urllib.parse import parse_qs, unquote, urlsplit

from pydantic import model_validator
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
    postgres_sslmode: str = "prefer"  # set to "require" for a hosted database such as Neon
    # Optional: a full connection string (e.g. Neon's "postgresql://user:pass@host/db?sslmode=require").
    # When set, it overrides the separate POSTGRES_* values. A connection string pasted into
    # POSTGRES_HOST by mistake is handled the same way.
    database_url: str | None = None

    @model_validator(mode="after")
    def _apply_connection_string(self) -> "Settings":
        self.postgres_host = self.postgres_host.strip()
        url = (self.database_url or "").strip()
        if not url and "://" in self.postgres_host:
            url = self.postgres_host
        if not url:
            return self
        parts = urlsplit(url)
        if not parts.hostname:
            raise ValueError("DATABASE_URL / POSTGRES_HOST connection string has no host")
        self.postgres_host = parts.hostname
        self.postgres_port = parts.port or 5432
        if parts.username:
            self.postgres_user = unquote(parts.username)
        if parts.password:
            self.postgres_password = unquote(parts.password)
        if parts.path.strip("/"):
            self.postgres_db = parts.path.strip("/")
        sslmode = parse_qs(parts.query).get("sslmode")
        if sslmode:
            self.postgres_sslmode = sslmode[0]
        return self

    @property
    def jdbc_url(self) -> str:
        return (
            f"jdbc:postgresql://{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
            f"?sslmode={self.postgres_sslmode}"
        )
