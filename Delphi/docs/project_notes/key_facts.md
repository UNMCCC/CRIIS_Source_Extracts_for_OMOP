# Key Facts

Non-sensitive project configuration and reference information for the Delphi OMOP incremental ETL.

> ⚠️ **Never** put passwords, connection strings with secrets, API keys, or service-account JSON in this file. This file is checked into git. Store secrets in a password manager or in `.env` files excluded via `.gitignore`.

## Servers and Databases

**Source (extract from):**
- Server: `MGBBRPSQLDBS1\UNMMGSQLDWPROD`
- Database: `unmmgdss`

**Destination (stage + transform + load into):**
- Server: `unmmg-sql-ccc\unmmgsqlunmccc`
- Database: `Delphi`

**Authentication:**
- Kerberos / Trusted Connection via ODBC Driver 18 for SQL Server.

## ETL Configuration

- Lookback window: **14 days** (seeded in `OMOP_INCR_CONFIG` by step 1 of the workflow).
- Target trigger cadence: **daily at 22:00 America/Denver**.
- Row filter on extracts: `DATEDIFF(DAY, updt_dt_tm, GETDATE()) <= <lookback>`.

## Python Environment

- Conda env name: `mssql`
- Python version: 3.12
- Installed packages of record: `pyodbc`, `pyyaml`
- Activate before running anything: `conda activate mssql`
- Do **not** `pip install` into `base`.

## Directory Layout (reference)

- `ArchitecturePlan.md` — design of record for orchestrator and staging strategy
- `Delphi_OMOP_STG_DDL.sql` — `STG_*` staging tables (destination)
- `Delphi_OMOP_INCR_DDL.sql` — `OMOP_INCR_*` incremental target tables
- `MSSQL_Vertica_Translations/` — T-SQL transforms translated from Vertica (see its `README.md` for the conversion cheatsheet)
- `MSSQL_UNMMGDSS_Extract_SQL/` — extract-side SQL against the new `unmmgdss` source
- `OMOP-Workflows-Py/` — **reference only**, earlier file-drop prototypes; do not extend
- `Notes.org` — per-step source-mapping scratchpad (HealtheIntent vs unmmgdss columns)
- `test_connection.py` — source + destination connection probe driven by `config.yaml`

## Workflow Step Order

1. Config → 2. Care Site → 3. Location → 4. Provider → 5. Person → 6. Visit Occurrence → 7. Visit Detail → (8. Condition, 9. Procedure, 10. Drug Exposure, 11. Observation → 12. Observation Final, 13. Specimen, 14. Measurement). Code Value runs standalone.
