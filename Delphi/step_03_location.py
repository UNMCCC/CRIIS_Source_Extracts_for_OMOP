"""
Step 3 — Location: stage patient addresses from unmmgdss.dss.pt_dim_v and
care-site addresses from OMOP_CARESITE_LOCATION.csv into STG_LOCATION.

Location is a full refresh; no lookback filter is applied (see ADR-003).
Care-site LOCATION_IDs are taken from the CSV as-is; they must continue to
equal step_02_care_site's CHECKSUM(CAST(care_site_id AS VARCHAR(MAX)) + street_addr)
so CARE_SITE.LOCATION_ID resolves against LOCATION.LOCATION_ID.
"""

from __future__ import annotations

import csv
import datetime
import decimal
import logging
import pathlib
from typing import Any

import pyodbc

log = logging.getLogger("etl.step_03_location")

EXTRACT_SQL_PATH = (
    pathlib.Path(__file__).parent
    / "MSSQL_UNMMGDSS_Extract_SQL"
    / "OMOP_Location_Extract.sql"
)
# UTF-16, pipe-delimited; one column per STG_LOCATION field (minus pt_corrected_mrn).
CARESITE_CSV_PATH = pathlib.Path(__file__).parent / "OMOP_CARESITE_LOCATION.csv"

INSERT_SQL = """
INSERT INTO {schema}.STG_LOCATION (
    identity_context, location_id, address_1, address_2, city, state, zip,
    county, location_source_value, country_concept_id, country_source_value,
    latitude, longitude, updt_dt_tm, pt_corrected_mrn
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
"""

# Pin parameter types/widths so fast_executemany does not infer buffer sizes
# from the first row (which triggers HY090 on all-NULL or empty-string columns).
# Order and sizes must match STG_LOCATION / INSERT_SQL.  `None` defers to pyodbc's
# default for updt_dt_tm (DATETIMEOFFSET).
INPUT_SIZES = [
    (pyodbc.SQL_VARCHAR, 255, 0),   # identity_context
    (pyodbc.SQL_INTEGER, 0, 0),     # location_id
    (pyodbc.SQL_VARCHAR, 50, 0),    # address_1
    (pyodbc.SQL_VARCHAR, 100, 0),   # address_2
    (pyodbc.SQL_VARCHAR, 50, 0),    # city
    (pyodbc.SQL_VARCHAR, 2, 0),     # state
    (pyodbc.SQL_VARCHAR, 9, 0),     # zip
    (pyodbc.SQL_VARCHAR, 20, 0),    # county
    (pyodbc.SQL_VARCHAR, 50, 0),    # location_source_value
    (pyodbc.SQL_INTEGER, 0, 0),     # country_concept_id
    (pyodbc.SQL_VARCHAR, 80, 0),    # country_source_value
    (pyodbc.SQL_DOUBLE, 0, 0),      # latitude
    (pyodbc.SQL_DOUBLE, 0, 0),      # longitude
    (pyodbc.SQL_VARCHAR, 34, 0),    # updt_dt_tm as string; SQL Server coerces to DATETIMEOFFSET
    (pyodbc.SQL_VARCHAR, 20, 0),    # pt_corrected_mrn
]


def _normalize(v: Any) -> Any:
    """fast_executemany-friendly coercion.

    - '' -> None so pyodbc doesn't bind a zero-length buffer.
    - datetime -> ISO string (Driver 18 + fast_executemany mis-binds DATETIMEOFFSET).
    - Decimal -> float (fast_executemany can't bind Decimal to SQL_DOUBLE; HY090).
    """
    if v == "":
        return None
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ", timespec="seconds")
    if isinstance(v, decimal.Decimal):
        return float(v)
    return v


def _clean(val: str | None) -> str | None:
    """Empty or whitespace-only strings -> None (SQL NULL)."""
    s = (val or "").strip()
    return s or None


def run(src_conn: pyodbc.Connection, dst_conn: pyodbc.Connection, cfg: dict[str, Any]) -> int:
    schema = cfg["omop_database_schema"]

    extract_sql = EXTRACT_SQL_PATH.read_text()
    src_cursor = src_conn.cursor()
    src_cursor.execute(extract_sql)
    person_rows = [
        tuple(_normalize(v) for v in r)
        for r in src_cursor.fetchall()
    ]
    src_cursor.close()
    log.info("Fetched %d person-address rows from pt_dim_v", len(person_rows))

    caresite_rows = []
    with open(CARESITE_CSV_PATH, encoding="utf-16", newline="") as f:
        for r in csv.DictReader(f, delimiter="|"):
            country_cid = _clean(r["country_concept_id"])
            caresite_rows.append((
                _clean(r["identity_context"]),
                int(r["location_id"]),
                _clean(r["address_1"]),
                _clean(r["address_2"]),
                _clean(r["city"]),
                _clean(r["state"]),
                _clean(r["zip"]),
                _clean(r["county"]),
                _clean(r["location_source_value"]),
                int(country_cid) if country_cid else None,
                _clean(r["country_source_value"]),
                float(r["latitude"]),
                float(r["longitude"]),
                _clean(r["updt_dt_tm"]),
                None,  # pt_corrected_mrn for person locations
            ))
    log.info("Read %d care-site rows from %s", len(caresite_rows), CARESITE_CSV_PATH.name)

    cursor = dst_conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.STG_LOCATION")
    cursor.fast_executemany = True
    insert = INSERT_SQL.format(schema=schema)
    cursor.setinputsizes(INPUT_SIZES)
    cursor.executemany(insert, person_rows)
    cursor.setinputsizes(INPUT_SIZES)
    cursor.executemany(insert, caresite_rows)
    dst_conn.commit()
    cursor.close()

    total = len(person_rows) + len(caresite_rows)
    log.info(
        "Inserted %d rows into STG_LOCATION (%d person, %d care-site)",
        total, len(person_rows), len(caresite_rows),
    )
    return total
