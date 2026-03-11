# Migrating Cerner ETLs away from EDW/HealtheIntent

  ## Proposed Architecture: Python-Orchestrated ETL with Destination-Side Staging

  ## The Core Problem

  The Omop_*.sql files in MSSQL_Vertica_Translations are MS SQL Server ETL queries translated from Vertica workflow 
  files that ran on HealtheIntent. We need to migrate the the workflow to extract data from the Med Group data warehouse: 
  **MGBBRPSQLDBS1\\UNMMGSQLDWPROD,unmmgdss** then transform and load it into OMOP destination tables at MS SQL Server 
  database: **unmmg-sql-ccc\\unmmgsqlunmccc,Delphi**. Several queries join source tables with destination OMOP tables (e.g., Measurement UPDATE joins OMOP_INCREMENTAL_VISIT_DETAIL, Visit Detail joins OMOP_INCR_VISIT_OCCURRENCE). This means we can't simply run each query against one server — some queries need data from both servers simultaneously.

  ## Approach: Stage Source Data on the Destination Server

  Principle: Pull the needed source data to temporary staging tables on the destination server, then run the
  transformation SQL on the destination server where it has access to both staged source data and existing OMOP tables.

  ## Components

  ### 1. Python Orchestrator (omop_etl.py)

  - Uses pyodbc with two connection objects — one per server
  - Reads the lookback config value first, then passes it to all steps
  - Executes the 14-step workflow in dependency order
  - Handles logging, error handling, and retry logic

  ### 2. Per-Step Pattern (for each SQL file)

  Each step follows a three-phase pattern:

  Phase A — Extract: Run a SELECT on the source server to pull only the rows needed (filtered by the 14-day lookback
  window). Fetch results into Python (raw cursor batches). The source table and column name will have to be updated to 
  correspond to the new source (SERVER = "MGBBRPSQLDBS1\\UNMMGSQLDWPROD" DATABASE = "unmmgdss")

  Phase B — Stage: Write those rows into a staging table on the destination server (e.g.,
  STG_MD_F_ENCOUNTER) using fast_executemany with pyodbc.

  Phase C — Transform & Load: Run the existing transformation SQL on the destination server, but rewritten to read from
  the staging tables instead of the original source schemas. The JOINs to other OMOP tables work natively since they're
  on the same server.

  ### 3. Source Table Inventory

  Build a manifest that maps each workflow step to:
  - Which source tables it needs (e.g., UNMHSC_EDW_MILL_CDS.MD_F_ENCOUNTER)
  - What filter to apply during extraction (the lookback DATEDIFF clause)
  - What columns are needed (to minimize data transfer)

  This keeps the extract queries lean and targeted.

  ### 4. Dependency & Ordering
```
  Config (1)
    ├── Care Site (2) ─┐
    ├── Location (3)   ├── [no cross-deps]
    ├── Provider (4)   │
    └── Person (5)     │
                       │
  Visit Occurrence (6) ←┘
    └── Visit Detail (7)
          ├── Condition (8)
          ├── Procedure (9)
          ├── Drug Exposure (10)
          ├── Observation (11) → Observation Final (12)
          ├── Specimen (13)
          └── Measurement (14)

  Code Value (standalone)
```

  ## Key Design Decisions:

  1. Staging table lifetime — Use persistent STG_* tables with TRUNCATE at the start of each run. Temp tables would be cleaner but can't be inspected for debugging.
  2. Data transfer method — Use fast_executemany with pyodbc. 
  3. Idempotency — The current scripts do INSERT without checking for duplicates. We may want to add NOT EXISTS guards or use MERGE statements so reruns don't create duplicates. NOT SURE ABOUT THIS ONE, YET.
  4. Config management — Store both connection strings and the lookback window in a single config file (config.yaml).
  5. Parallelism — Steps 2–5 and 8–14 are independent and could be parallelized. However, in the fisrt instance, keep it sequential for simplicity.
