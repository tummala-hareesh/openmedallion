"""tests/test_credentials.py — credential file loading and connection string building."""
import pytest
import yaml

from openmedallion.pipeline.bronze import _load_credentials_file, _build_conn_str, BronzeLoader


# ---------------------------------------------------------------------------
# _build_conn_str
# ---------------------------------------------------------------------------

class TestBuildConnStr:

    def test_sqlite(self):
        s = _build_conn_str("sqlite", {"path": "data/mydb.db"})
        assert s == "sqlite:///data/mydb.db"

    def test_oracle(self):
        s = _build_conn_str("oracle", {
            "host": "dbhost", "port": 1521, "service": "XE",
            "username": "hr", "password": "secret",
        })
        assert s == "oracle+oracledb://hr:secret@dbhost:1521/?service_name=XE"

    def test_oracle_default_port(self):
        s = _build_conn_str("oracle", {
            "host": "dbhost", "service": "XE",
            "username": "hr", "password": "secret",
        })
        assert "1521" in s

    def test_postgres(self):
        s = _build_conn_str("postgres", {
            "host": "pghost", "port": 5432, "database": "mydb",
            "username": "user", "password": "pass",
        })
        assert s.startswith("postgresql+psycopg2://")
        assert "pghost:5432/mydb" in s

    def test_postgres_default_port(self):
        s = _build_conn_str("postgres", {
            "host": "pghost", "database": "mydb",
            "username": "user", "password": "pass",
        })
        assert "5432" in s

    def test_mysql(self):
        s = _build_conn_str("mysql", {
            "host": "myhost", "port": 3306, "database": "db",
            "username": "u", "password": "p",
        })
        assert s.startswith("mysql+pymysql://")

    def test_mssql(self):
        s = _build_conn_str("mssql", {
            "host": "mshost", "port": 1433, "database": "db",
            "username": "u", "password": "p",
        })
        assert s.startswith("mssql+pyodbc://")
        assert "driver=" in s

    def test_unsupported_dialect_raises(self):
        with pytest.raises(ValueError, match="unsupported dialect"):
            _build_conn_str("db2", {"host": "h", "username": "u", "password": "p"})


# ---------------------------------------------------------------------------
# _load_credentials_file
# ---------------------------------------------------------------------------

class TestLoadCredentialsFile:

    def test_loads_correct_dialect(self, tmp_path):
        f = tmp_path / "secrets.yaml"
        f.write_text(yaml.dump({"sqlite": {"path": "db.db"}, "oracle": {"host": "h"}}))
        creds = _load_credentials_file(str(f), "sqlite")
        assert creds == {"path": "db.db"}

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="not found"):
            _load_credentials_file(str(tmp_path / "missing.yaml"), "sqlite")

    def test_missing_dialect_raises(self, tmp_path):
        f = tmp_path / "secrets.yaml"
        f.write_text(yaml.dump({"oracle": {"host": "h"}}))
        with pytest.raises(KeyError, match="postgres"):
            _load_credentials_file(str(f), "postgres")

    def test_full_roundtrip_sqlite(self, tmp_path):
        f = tmp_path / "secrets.yaml"
        f.write_text(yaml.dump({"sqlite": {"path": "data/test.db"}}))
        creds = _load_credentials_file(str(f), "sqlite")
        conn  = _build_conn_str("sqlite", creds)
        assert conn == "sqlite:///data/test.db"

    def test_full_roundtrip_oracle(self, tmp_path):
        f = tmp_path / "secrets.yaml"
        f.write_text(yaml.dump({"oracle": {
            "host": "localhost", "port": 1521, "service": "XE",
            "username": "hr", "password": "pwd",
        }}))
        creds = _load_credentials_file(str(f), "oracle")
        conn  = _build_conn_str("oracle", creds)
        assert "oracle+oracledb://" in conn
        assert "localhost:1521/?service_name=XE" in conn


# ---------------------------------------------------------------------------
# BronzeLoader._resolve_conn_str
# ---------------------------------------------------------------------------

class TestResolveConnStr:

    def _cfg(self, source: dict) -> dict:
        return {
            "pipeline": {"name": "test"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "source": source,
        }

    def test_credentials_file_path_resolved(self, tmp_path):
        f = tmp_path / "secrets.yaml"
        f.write_text(yaml.dump({"sqlite": {"path": "mydb.db"}}))
        loader = BronzeLoader(self._cfg({
            "type": "sql_database",
            "dialect": "sqlite",
            "credentials_file": str(f),
        }))
        assert loader._resolve_conn_str() == "sqlite:///mydb.db"

    def test_connection_string_fallback(self):
        loader = BronzeLoader(self._cfg({
            "type": "sql_database",
            "connection_string": "sqlite:///fallback.db",
        }))
        assert loader._resolve_conn_str() == "sqlite:///fallback.db"


# ---------------------------------------------------------------------------
# Validator — credentials_file + dialect
# ---------------------------------------------------------------------------

class TestValidatorCredentials:

    def _valid_cfg(self, **source_overrides):
        cfg = {
            "pipeline": {"name": "t"},
            "paths": {"bronze": "b", "silver": "s", "gold": "g", "export": "e"},
            "source": {"type": "sql_database", **source_overrides},
        }
        return cfg

    def test_credentials_file_and_dialect_accepted(self):
        from openmedallion.config.validator import _validate_config
        _validate_config(self._valid_cfg(
            credentials_file="/workspace/secrets.yaml",
            dialect="oracle",
        ))

    def test_credentials_file_without_dialect_raises(self):
        from openmedallion.config.validator import _validate_config
        with pytest.raises(ValueError, match="dialect"):
            _validate_config(self._valid_cfg(credentials_file="/workspace/secrets.yaml"))

    def test_invalid_dialect_raises(self):
        from openmedallion.config.validator import _validate_config
        with pytest.raises(ValueError, match="dialect"):
            _validate_config(self._valid_cfg(
                credentials_file="/workspace/secrets.yaml",
                dialect="db2",
            ))

    def test_all_valid_dialects_accepted(self):
        from openmedallion.config.validator import _validate_config
        for dialect in ("oracle", "postgres", "mysql", "mssql", "sqlite"):
            _validate_config(self._valid_cfg(
                credentials_file="/workspace/secrets.yaml",
                dialect=dialect,
            ))

    def test_connection_string_still_accepted(self):
        from openmedallion.config.validator import _validate_config
        _validate_config(self._valid_cfg(connection_string="sqlite:///db.db"))
