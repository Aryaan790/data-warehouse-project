# SQL Data Warehouse & Analytics

A SQL Server data warehouse built on the **medallion architecture** (bronze → silver → gold),
ingesting two source systems — CRM and ERP — from CSV extracts into a cleansed, conformed
data model. ~1,400 lines of T-SQL.

Built following a structured data-warehouse course (Baraa Khatib Salkini's *SQL Data
Warehouse Project*); all scripts in this repository were written and debugged by hand
in SSMS.

## Status

| Layer | Purpose | Status |
|---|---|---|
| **Bronze** | Raw landing — tables mirror the source CSVs exactly | ✅ Complete |
| **Silver** | Cleansed & conformed — deduplicated, typed, standardized | ✅ Complete |
| **Gold** | Star schema — dimensions, facts, business views | 🚧 In progress |

## Architecture

```
CSV extracts (CRM + ERP)
        │  BULK INSERT via stored procedure (truncate-and-reload)
        ▼
   ┌─────────┐     cleanse, dedupe,      ┌─────────┐    dimensional     ┌────────┐
   │ bronze  │ ──  type-cast, derive  ─► │ silver  │ ──   modeling   ─► │  gold  │
   └─────────┘     SCD date ranges       └─────────┘    (planned)       └────────┘
    6 tables                              6 tables
```

## What's inside

### `scripts/`
- `init_database.sql` — creates the `DataWarehouse` database and the three schemas
- `bronze/` — DDL for the six landing tables and `bronze.load_bronze`, a stored
  procedure that truncates and bulk-loads every table with `TRY…CATCH` error handling
  and per-table load-duration logging
- `silver/` — DDL plus the transformation and load scripts:
  - **Deduplication** — `ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC)`
    keeps only the latest record per customer
  - **Standardization** — `TRIM` on text fields; coded values (marital status, gender,
    product line) mapped to descriptive values with `CASE`
  - **Date-range derivation** — each product record's end date is computed from the *next*
    record's start date:
    `CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS DATE)`
  - **Sales integrity** — recomputes `sales = quantity × price` where the source value is
    missing, zero, or inconsistent

### `tests/quality_checks/`
Standalone validation queries run against each layer before and after loading:
primary-key uniqueness, NULL/duplicate detection, invalid and out-of-range dates,
and cross-layer row-count reconciliation between bronze and silver.

## Running it

1. SQL Server + SSMS (any recent version)
2. Run `scripts/init_database.sql`
3. Run the bronze DDL, then `EXEC bronze.load_bronze;` (adjust the CSV paths in the
   procedure to your local `datasets/` folder)
4. Run the silver DDL and load scripts
5. Run anything in `tests/quality_checks/` to verify the result

## License

MIT — see [LICENSE](LICENSE).
