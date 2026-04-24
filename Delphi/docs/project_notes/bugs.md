# Bug Log

Chronological log of bugs encountered in the Delphi OMOP incremental ETL migration and their fixes. Keep entries brief (1-3 lines for descriptions) and include prevention notes when useful.

## Format

```
### YYYY-MM-DD - Brief Bug Description
- **Issue**: What went wrong
- **Root Cause**: Why it happened
- **Solution**: How it was fixed
- **Prevention**: How to avoid it in the future
```

## Entries

### 2026-04-23 - STG_LOCATION.address_2 truncation (SQL error 8152)
- **Issue**: Step 3 insert into `STG_LOCATION` failed with `String or binary data would be truncated` on `address_2`. The source (`pt_dim_v.pt_addr2`) had values up to 53 chars; the staging column was `VARCHAR(50)`.
- **Root Cause**: `STG_LOCATION` column widths were seeded without profiling source data — they did not all match the true `MAX(LEN)` on `unmmgdss`.
- **Solution**: Widened `STG_LOCATION.address_2` to `VARCHAR(100)` in `Delphi_OMOP_STG_DDL.sql` (and applied the matching `ALTER TABLE` on the destination). Profiled the other varchar columns at the same time (`max_addr1=46, max_city=28, max_state=2, max_county=18, max_mrn=9`) — all fit.
- **Prevention**: For every new `STG_*` table, run a `SELECT MAX(LEN(col))` probe over each VARCHAR column on the source against live data before the first load. Keep widths at the source-observed max with headroom, not at whatever the Vertica translation happened to use.

### 2026-04-23 - pyodbc fast_executemany HY090 on Decimal / datetime / DATETIMEOFFSET
- **Issue**: Step 3 failed with `HY090 Invalid string or buffer length (0)` during `cursor.executemany` against `STG_LOCATION`. Plain `executemany` on the same row worked; only `fast_executemany = True` failed.
- **Root Cause**: ODBC Driver 18 + pyodbc `fast_executemany` mis-binds two Python types returned by the source driver:
  - `decimal.Decimal` values bound to `SQL_DOUBLE` params (SQL Server `DECIMAL`/`NUMERIC` columns return as `Decimal`, not `float`).
  - `datetime.datetime` values bound to `DATETIMEOFFSET` params.
  Empty strings `""` in nullable VARCHARs also cause zero-length buffers under fast_executemany.
- **Solution**: Normalize extract rows before `executemany`: `Decimal → float`, `datetime → ISO string`, `"" → None`. Pin parameter sizes with `cursor.setinputsizes([...])`. Bind DATETIMEOFFSET columns as `SQL_VARCHAR`; SQL Server coerces on insert. See `step_03_location.py::_normalize` and `INPUT_SIZES`.
- **Prevention**: Every step that does `fast_executemany` into STG_* must apply the same three normalizations — persist any new conversions in `_normalize` and reuse the pattern for steps 4+. Diagnose with a small "insert 1 row in three binding modes" probe (fast=OFF, fast=ON, fast=ON+setinputsizes) — it isolates the issue in seconds.
