"""
Step 2 — Care Site: load OMOP_CARESITE_FROM_EDW.csv into STG_CARE_SITE.

LOCATION_ID is recomputed using SQL Server CHECKSUM() to replace the
original Vertica HASH() values.  The CHECKSUM expression must match the
one used in step_03_location so the IDs resolve across tables.
"""

from __future__ import annotations

import csv
import logging
import pathlib
from typing import Any

import pyodbc

log = logging.getLogger("etl.step_02_care_site")

CSV_PATH = pathlib.Path(__file__).parent / "OMOP_CARESITE_FROM_EDW.csv"

INSERT_SQL = """
INSERT INTO {schema}.STG_CARE_SITE (
    identity_context, care_site_id, care_site_name,
    place_of_service_concept_id, street_addr, location_id,
    care_site_source_value, place_of_service_source_value, updt_dt_tm
) VALUES (
    ?, ?, ?, ?, ?,
    CHECKSUM(CAST(? AS VARCHAR(MAX)) + ?),
    ?, ?, ?
)
"""


def run(src_conn: pyodbc.Connection, dst_conn: pyodbc.Connection, cfg: dict[str, Any]) -> int:
    schema = cfg["omop_database_schema"]

    rows = []
    with open(CSV_PATH, newline="") as f:
        for r in csv.DictReader(f):
            rows.append((
                r["Identity_Context"],
                int(r["CARE_SITE_ID"]),
                r["care_site_name"],
                int(r["place_of_service_concept_id"]),
                r["STREET_ADDR"],
                # CHECKSUM params: CAST(care_site_id AS VARCHAR) + street_addr
                r["CARE_SITE_ID"],
                r["STREET_ADDR"],
                r["care_site_source_value"],
                r["place_of_service_source_value"] or None,
                r["UPDT_DT_TM"],
            ))

    log.info("Read %d rows from %s", len(rows), CSV_PATH.name)

    cursor = dst_conn.cursor()
    cursor.execute(f"TRUNCATE TABLE {schema}.STG_CARE_SITE")
    cursor.executemany(INSERT_SQL.format(schema=schema), rows)
    dst_conn.commit()
    cursor.close()

    log.info("Inserted %d rows into STG_CARE_SITE", len(rows))
    return len(rows)
