# Architectural Decisions

Architectural Decision Records (ADRs) for the Delphi OMOP incremental ETL project. The design of record lives in `../../ArchitecturePlan.md`; this file captures the decisions behind it and any subsequent revisions.

## Format

```
### ADR-XXX: Decision Title (YYYY-MM-DD)

**Context:**
- Why the decision was needed / problem it solves

**Decision:**
- What was chosen

**Alternatives Considered:**
- Option → Why rejected

**Consequences:**
- Trade-offs (good and bad)
```

## Decisions

### ADR-001: Extract → Stage → Transform on destination server (seeded from ArchitecturePlan.md)

**Context:**
- Replacing a Vertica-based OMOP incremental ETL with an MS SQL Server implementation.
- Source (`unmmgdss` on `MGBBRPSQLDBS1\UNMMGSQLDWPROD`) and destination (`Delphi` on `unmmg-sql-ccc\unmmgsqlunmccc`) are separate servers; cross-server joins are undesirable for volume/perf reasons.

**Decision:**
- Each step Extracts from source with a 14-day lookback on `updt_dt_tm`, Stages into `STG_*` tables on the destination via `fast_executemany`, and Transforms natively on the destination reading `STG_*` JOIN existing `OMOP_*` tables.

**Alternatives Considered:**
- Linked-server cross-joins → Rejected: perf and permissions complexity.
- File drop (pipe-delimited) like the earlier `OMOP-Workflows-Py` prototypes → Rejected: extra I/O, no staging benefit.

**Consequences:**
- ✅ Native joins on the destination for the transform step.
- ✅ Staging tables give a debuggable intermediate.
- ❌ Extra DDL surface (`STG_*` + `OMOP_INCR_*`) and TRUNCATE/INSERT discipline per run.

### ADR-003: Location is a full refresh — no lookback filter (2026-04-22)

**Context:**
- The default per-step pattern filters extracts by `DATEDIFF(DAY, updt_dt_tm, GETDATE()) <= lookback_days`.
- Step 3 (Location) sources person addresses from `unmmgdss.dss.pt_dim_v` (`pt_edit_date`) and care-site addresses from a static CSV (`OMOP_CARESITE_LOCATION.csv`, UTF-16, pipe-delimited). Location volume is low and addresses can shift for historical patients outside the 14-day window.

**Decision:**
- Step 3 does **not** apply the lookback window. Every run pulls all eligible person addresses (`pt_addr1 <> '?'`) plus every care-site row from the CSV, `TRUNCATE`s `STG_LOCATION`, and reloads.
- Care-site `location_id` values are read from the CSV as-is (no CHECKSUM recomputation). They must continue to equal step_02_care_site's `CHECKSUM(CAST(care_site_id AS VARCHAR(MAX)) + street_addr)` so `CARE_SITE.LOCATION_ID` resolves against `LOCATION.LOCATION_ID` — this is now a data invariant of the CSV rather than a code invariant.

**Alternatives Considered:**
- Incremental by `pt_edit_date` like the default pattern → Rejected: would miss address corrections applied outside the 14-day window.

**Consequences:**
- ✅ Simple and always-correct for this low-volume table.
- ❌ Every run rehydrates the full table; fine given row counts but worth revisiting if person-address volume grows.

### ADR-002: Use `CHECKSUM()` as the HealtheIntent `HASH()` substitute (seeded from ArchitecturePlan.md)

**Context:**
- Vertica transforms used `HASH()` to derive synthetic keys (e.g., `LOCATION_ID` joined from `CARE_SITE.LOCATION_ID`).
- MS SQL Server has no equivalent `HASH()` returning a stable integer; `HASHBYTES` returns varbinary.

**Decision:**
- Use `CHECKSUM()` as the substitute across CARE_SITE and LOCATION. The CARE_SITE and LOCATION expressions must remain identical so that `CARE_SITE.LOCATION_ID` resolves against `LOCATION.LOCATION_ID`.

**Alternatives Considered:**
- `HASHBYTES('SHA2_256', ...)` cast to bigint → Rejected: lossy cast, still need identical expressions on both sides.
- Sequence-based surrogate keys → Rejected: would break cross-table joins already in the transform SQL.

**Consequences:**
- ✅ Minimal change to existing transform SQL.
- ❌ `CHECKSUM()` has known collision risk; acceptable here because the input columns are small and deterministic.
