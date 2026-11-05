import pytest

from tfl.common.config import Settings

NEON = "postgresql://neondb_owner:npg_secret@ep-cool-name-a1b2.eu-west-2.aws.neon.tech/neondb?sslmode=require"


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for key in ["DATABASE_URL", "POSTGRES_HOST", "POSTGRES_PORT", "POSTGRES_DB", "POSTGRES_USER",
                "POSTGRES_PASSWORD", "POSTGRES_SSLMODE"]:
        monkeypatch.delenv(key, raising=False)


def assert_neon(s: Settings) -> None:
    assert s.postgres_host == "ep-cool-name-a1b2.eu-west-2.aws.neon.tech"
    assert s.postgres_port == 5432
    assert s.postgres_db == "neondb"
    assert s.postgres_user == "neondb_owner"
    assert s.postgres_password == "npg_secret"
    assert s.postgres_sslmode == "require"


def test_separate_values_unchanged(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", "  postgres ")
    s = Settings(_env_file=None)
    assert s.postgres_host == "postgres"
    assert s.postgres_db == "tfl"


def test_database_url(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", NEON)
    monkeypatch.setenv("POSTGRES_USER", "tfl")  # stale local value is overridden
    assert_neon(Settings(_env_file=None))


def test_connection_string_pasted_into_host(monkeypatch):
    monkeypatch.setenv("POSTGRES_HOST", NEON)
    assert_neon(Settings(_env_file=None))


def test_jdbc_url_from_connection_string(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", NEON)
    assert Settings(_env_file=None).jdbc_url == (
        "jdbc:postgresql://ep-cool-name-a1b2.eu-west-2.aws.neon.tech:5432/neondb?sslmode=require"
    )
