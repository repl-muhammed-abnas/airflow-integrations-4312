# Technology Stack — Xero Migration Slice

## Source (Workato `014-501 PSA`)
- **Platform:** Workato (iPaaS), package export format.
- **Artifacts:** `*.recipe.json` (workflow definitions), `*.lookup_table.json` (`{name, schema[], data?}`), `*.connection.json`, `package.manifest`.
- **Connectors:** Deltek Vantagepoint (v5.5.4), Xero (v1.0.0).
- **CI/CD:** Azure DevOps pipeline (`integrations_014-501 Vantagepoint and Xero.yml`) — zips `/code/*.*`, bumps `package.manifest` version, publishes `integrations_vantagepoint_xero_014_501.zip`. Uses 7-Zip. Pool: `IntegrationPlatform`.

## Target (Airflow Mapping Framework)
- **Language:** Python 3.
- **Datastore:** PostgreSQL (JSONB used for metadata/conditions; `gen_random_uuid()` for tenant_id; triggers for audit + updated_at).
- **DB driver:** `psycopg2` (root scripts); SQLAlchemy (framework UI).
- **Orchestration:** Apache Airflow (custom operators).
- **Caching:** in-memory / Redis / database / hybrid (`cache_manager.py`).
- **Web UI:** Flask + SQLAlchemy + WTForms.
- **Dependencies:** see `airflow_mapping_framework/config/requirements.txt`.

## Migration tooling (repo root)
- **Language:** Python 3 (stdlib `csv`, `json`; `psycopg2` for DB writes).
- **`validate_workato_simple.py`:** stdlib-only (no third-party deps) by design.
- **`validate_workato_basic.py` / `validate_workato_migration.py`:** richer reporting (may reference pandas-style analysis; confirm before running).

## Environment notes
- **OS:** Windows 11; shell is PowerShell (Bash also available).
- **DB config in `migrate_workato_data.py` is hardcoded** (`localhost:5432`, db `mapping_db`, user `postgres`) and prompts interactively before running.
