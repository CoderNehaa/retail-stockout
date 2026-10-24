"""Shared config, secrets and Azure clients for the Section 2 simulator.

Secrets come from Key Vault through your `az login` (DefaultAzureCredential).
The .env file holds only names and URLs, never passwords or connection strings.
"""
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _load_env() -> None:
    env = ROOT / ".env"
    if not env.exists():
        return
    for line in env.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, v = line.split("=", 1)
        os.environ.setdefault(k.strip(), v.strip())


_load_env()


@dataclass(frozen=True)
class Config:
    key_vault_url: str = os.getenv("KEY_VAULT_URL", "https://kv-retail-nde.vault.azure.net/")
    storage_account: str = os.getenv("STORAGE_ACCOUNT", "retailstoragelakende")
    landing_container: str = os.getenv("LANDING_CONTAINER", "landing")
    sql_server: str = os.getenv("SQL_SERVER", "sql-retail-server-nde.database.windows.net")
    sql_database: str = os.getenv("SQL_DATABASE", "sqldb-erp")
    sql_user: str = os.getenv("SQL_USER", "sim_writer")
    sql_driver: str = os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
    eh_name: str = os.getenv("EVENT_HUB_NAME", "pos-sales")
    m5_dir: Path = Path(os.getenv("M5_DIR", str(ROOT / "data" / "m5")))
    state_dir: Path = Path(os.getenv("STATE_DIR", str(ROOT / "sim_state")))
    start_day: str = os.getenv("START_DAY", "d_1700")
    minutes_per_day: float = float(os.getenv("SIM_MINUTES_PER_DAY", "10"))
    seed: int = int(os.getenv("SIM_SEED", "42"))


CFG = Config()

# 5 suppliers with different reliability (share of deliveries on time) and lead time.
SUPPLIERS = {
    "SUP_1": {"on_time": 0.95, "lead_days": 2},
    "SUP_2": {"on_time": 0.90, "lead_days": 3},
    "SUP_3": {"on_time": 0.85, "lead_days": 3},
    "SUP_4": {"on_time": 0.70, "lead_days": 4},
    "SUP_5": {"on_time": 0.80, "lead_days": 3},
}
STORE_CITY = {"CA": "Los Angeles", "TX": "Dallas", "WI": "Milwaukee"}


def day_num(d: str) -> int:
    return int(d.split("_")[1])


# --------------------------------------------------------------------------- secrets

@lru_cache(maxsize=1)
def _credential():
    from azure.identity import DefaultAzureCredential
    # Uses your `az login` on the laptop. Interactive browser login is a fallback.
    return DefaultAzureCredential(exclude_interactive_browser_credential=False)


@lru_cache(maxsize=None)
def secret(name: str) -> str:
    from azure.keyvault.secrets import SecretClient
    return SecretClient(CFG.key_vault_url, _credential()).get_secret(name).value


# --------------------------------------------------------------------------- SQL

def sql_connect(retries: int = 5):
    """Connect to sqldb-erp. A paused serverless DB takes up to ~1 min to resume."""
    import pyodbc
    conn_str = (
        f"Driver={{{CFG.sql_driver}}};Server=tcp:{CFG.sql_server},1433;"
        f"Database={CFG.sql_database};Uid={CFG.sql_user};Pwd={secret('sql-sim-password')};"
        "Encrypt=yes;TrustServerCertificate=no;Connection Timeout=60;"
    )
    for attempt in range(1, retries + 1):
        try:
            return pyodbc.connect(conn_str, autocommit=False)
        except pyodbc.Error as e:  # 40613 = database resuming / not available yet
            if attempt == retries:
                raise
            print(f"  SQL not ready ({e.args[0]}), retrying in 20s ({attempt}/{retries})")
            time.sleep(20)


# --------------------------------------------------------------------------- ADLS landing

class Landing:
    """Writes files to the landing container (or a local folder in --dry-run)."""

    def __init__(self, dry_run: bool):
        self.dry_run = dry_run
        self.local_root = ROOT / "out" / "landing"
        if not dry_run:
            from azure.storage.filedatalake import DataLakeServiceClient
            svc = DataLakeServiceClient(
                f"https://{CFG.storage_account}.dfs.core.windows.net", credential=_credential())
            self.fs = svc.get_file_system_client(CFG.landing_container)

    def write_bytes(self, path: str, data: bytes) -> None:
        if self.dry_run:
            p = self.local_root / path
            p.parent.mkdir(parents=True, exist_ok=True)
            p.write_bytes(data)
        else:
            self.fs.get_file_client(path).upload_data(data, overwrite=True)

    def write_jsonl(self, path: str, rows: list[dict]) -> None:
        body = "\n".join(json.dumps(r, default=str) for r in rows) + ("\n" if rows else "")
        self.write_bytes(path, body.encode("utf-8"))
