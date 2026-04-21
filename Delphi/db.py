"""
Connection helpers and a small SQL-file runner for the unmmgdss → Delphi OMOP ETL.

Loaded by the orchestrator and by every per-step module.
"""

from __future__ import annotations

import pathlib
from typing import Any

import pyodbc
import yaml


def load_config(path: str | pathlib.Path) -> dict[str, Any]:
    with open(path, "r") as f:
        return yaml.safe_load(f)


def make_conn_str(server_cfg: dict[str, str]) -> str:
    return (
        f"DRIVER={{{server_cfg['driver']}}};"
        f"SERVER={server_cfg['server']};"
        f"DATABASE={server_cfg['database']};"
        "Trusted_Connection=yes;"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )


def connect_source(cfg: dict[str, Any]) -> pyodbc.Connection:
    return pyodbc.connect(make_conn_str(cfg["source"]), timeout=15)


def connect_destination(cfg: dict[str, Any]) -> pyodbc.Connection:
    return pyodbc.connect(make_conn_str(cfg["destination"]), timeout=15)


def run_sql_file(
    conn: pyodbc.Connection,
    path: str | pathlib.Path,
    params: dict[str, Any] | None = None,
) -> int:
    """Read a .sql file, substitute @key placeholders from `params`, execute it.

    Returns cursor.rowcount of the LAST statement in the batch (pyodbc limitation).
    """
    sql = pathlib.Path(path).read_text()
    for key, value in (params or {}).items():
        sql = sql.replace(f"@{key}", str(value))
    cursor = conn.cursor()
    cursor.execute(sql)
    rowcount = cursor.rowcount
    conn.commit()
    cursor.close()
    return rowcount
