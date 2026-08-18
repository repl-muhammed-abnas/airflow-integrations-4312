# V2.2 Splitter Test CSV — row-by-row catalog

**File:** [v2_2_splitter_test.csv](v2_2_splitter_test.csv)
**Upload to:** `/moodys/User Sync/Input` on your trial SFTP
**Test target:** http://localhost:9010/

16 rows: 6 non-France smoke rows (one per allowed country except France), 9 France rows (full V2.2 derivation matrix + new mapper entries), 1 non-permitted country (`IN`).

## Pre-conditions

- The splitter's PGP gate (`is_pgp` at [main_dag.py:32](../main_dag.py#L32)) requires a `.pgp` extension. If you're uploading a plain CSV, name it `v2_2_splitter_test.csv.pgp` or rely on whatever bypass your local trial has wired.
- The empty `task_id=""` at [main_dag.py:52](../main_dag.py#L52) must be patched before the splitter DAG will load — Airflow rejects empty task IDs.
- All seven `_v1` country DAGs (plus `germany`) must be loaded without import errors in `http://localhost:9010/dags`.

## Row catalog

### Routing smoke tests (1 row per non-France allowed country)

| # | Country ID | Login Name | What it tests |
|---|---|---|---|
| 1 | `US` | 29200001 (Alex Smith) | US row routes to `united_states_v1`. Job Title (`Senior Analyst`) is propagated but ignored by US DAG. |
| 2 | `CA` | 29200002 (Marie Tremblay) | Canada row routes to `canada_v1`. Job Title intentionally empty to verify the empty-column path. |
| 3 | `LT` | 29200003 (Jonas Petrauskas) | Lithuania row routes to `lithuania_v1`. Job Title (`Software Engineer`) populated; LT DAG ignores it. |
| 4 | `CR` | 29200004 (Diego Rojas) | Costa Rica row routes to `costa_rica_v1`. Job Title empty. **Note:** `CR` is both a country code and a France employee category — don't confuse them. |
| 5 | `JP` | 29200005 (Hiroshi Tanaka) | Japan row routes to `japan_v1`. Job Title (`Manager`) populated; JP DAG ignores it. |
| 6 | `DE` | 29200006 (Anna Schmidt) | Germany row routes to `germany`. Trial only — `is_instance_trial` gates this; in UAT/prod, Germany is skipped. Job Title (`Risk Analyst`) populated; Germany DAG ignores it (no V2.2 Job Title behaviour for Germany). |

**Expected splitter behaviour for rows 1–6:**
- `create_input_data_collection` accepts all 6 records (countryid IN ALLOWED_COUNTRIES).
- `move_to_processing_<country>` task group writes one file per country to its configured `Processing/<country>/` SFTP path.
- Each per-country file's header row ends with `|Job Title` and each data row carries the value (or empty).

### France V2.2 derivation matrix

All FR rows use the same base setup (Paris boulevard Haussmann location, Moodys Analytics SAS department code `1094`, supervisor Christoph Gerz) so the only varying inputs are `Employee Category`, `FTE%`, and `Job Title`. This isolates the new `get_employee_type_name` logic.

| # | Login Name | Employee Category | FTE% | Job Title | Expected derived type | What it tests |
|---|---|---|---|---|---|---|
| 7 | 29200007 (Lucas Bernard) | `ETAM` | 1.0 | `Intern` | `Intern` | Job Title overrides Employee Category — Intern wins even when category=ETAM and FTE=1.0. |
| 8 | 29200008 (Camille Dubois) | `CR` | 1.0 | `Co-op Student` | `Intern` | Co-op Student also maps to Intern, overriding the CR category. Exercises both INTERN_JOB_TITLES values. |
| 9 | 29200009 (Sophie Lefevre) | `ETAM` | 0.5 | `Analyst` | `Admin` | ETAM + FTE<1.0 = Admin. The Admin branch added in V2.2. |
| 10 | 29200010 (Julien Moreau) | `ETAM` | 1.0 | `Senior Analyst` | `ETAM` | ETAM + FTE≥1.0 = ETAM passthrough (the "plain ETAM" case). |
| 11 | 29200011 (Marc Laurent) | `CR` | 1.0 | `Manager` | `CR` | CR passthrough. Also uses `actualworkinghrs=38.30` to match the CR-specific `France - 38H30/WEEK` schedule. |
| 12 | 29200012 (Isabelle Girard) | `CA` | 1.0 | `Senior Director` | `CA` | CA passthrough. Schedule should be `France - 1 for each day&nbsp;&nbsp;&nbsp;&nbsp;Monday to Friday` (4 spaces before "Monday" — verify mapper spelling matches Replicon Sandbox). Pay Rule: `Moodys - France - Cadre Autonome`. |
| 13 | 29200013 (Antoine Rousseau) | `NA` | 1.0 | (empty) | `NA` | Edge passthrough — non-matched categories return as-is so NA/CA213/CA217 still flow. Validator should still pass if mapper has the `NA` employeetype entry (it does). |

### France V2.2 new mapper-entry rows

| # | Login Name | Tests | What it tests |
|---|---|---|---|
| 14 | 29200014 (Nathalie Faure) | Remote-worker location `32021` + MDY028 department | Location `Remote Worker- Montbonnot Saint Martin - 20 Rue Lavoisier` (code `32021`) and department `MDY028 BvD Editions Electroniques` (code `MDY028`) — both added in V2.2 mapper §5.2 and §5.3. ETAM + FTE=1.0 = derived `ETAM`. |
| 15 | 29200015 (Vincent Mercier) | Remote-worker location `32221` + MDY029 department + Admin derivation | Location `Remote Worker- Paris - 21 Blvd Haussmann` (code `32221`) and department `MDY029 Moodys Analytics`. Cross-cuts: ETAM + FTE=0.5 should derive `Admin`. |

### Non-permitted country (skip-log path)

| # | Country ID | Login Name | What it tests |
|---|---|---|---|
| 16 | `IN` | 29200016 (Raj Patel) | India is NOT in `ALLOWED_COUNTRIES` (`config.py:8`). The `query_non_permitted_country_records` task should pick this row up, `log_non_permitted_country_records` writes it to the master log with severity `Skipped`, `create_skip_logs_csv` builds a CSV, and `upload_skip_log_to_sftp` writes the skip log to the configured log filepath. |

## What QA should observe

### Splitter DAG (`moodys_user_sync_split_input_data_based_on_country_master_trial`)

- `new_file_sensor` picks up the file.
- `is_pgp = Yes` (if you named the file with `.pgp`) → `download_file` → `decrypt_file` (or `dummy_load_data` if your local env has `can_decrypt_file=false`).
- `load_data` parses the CSV with `|` delimiter.
- `create_input_data_collection` shows length=16 in logs.
- `query_non_permitted_country_records` returns 1 row (the `IN` row); `log_non_permitted_country_records` writes one skipped entry; a skip-log CSV is uploaded.
- `query_permitted_country_records` returns 15 rows.
- Each `move_to_processing_<country>` task group fires `create_csv_<country>` + `upload_log_to_sftp_<country>`. Verify on SFTP that 7 country files exist in the right processing folders, each with the new `|Job Title` column in the header.
- Germany routing only fires when `is_instance_trial == True`.

### Per-country DAGs (after splitter completes)

Each of `canada_v1`, `costa_rica_v1`, `france_v1`, `germany`, `japan_v1`, `lithuania_v1`, `united_states_v1` should pick up its respective file from `Processing/<country>/` and process its row(s).

**Non-France DAGs:** `dag_run.conf['jobtitle']` should be set on each user record (verify in the child DAG's `process_users` task XCom or logs). No mapper/payload/task changes mean behaviour should otherwise match the pre-V2.2 stack.

**France DAG (`moodys_user_sync_france_master_trial_v1`):** 9 rows. The interesting XCom/log values to verify per row:

| Login | Expected derivedemployeetype | Expected timesheettemplatename | Expected schedule | Expected payrulename | Expected OT template | Expected activities |
|---|---|---|---|---|---|---|
| 29200007 (Bernard) | `Intern` | `France - In/Out Timesheet` | `France - 35H/WEEK` | `Moodys - France - Intern` | (none) | Home, Office |
| 29200008 (Dubois) | `Intern` | `France - In/Out Timesheet` | `France - 35H/WEEK` | `Moodys - France - Intern` | (none) | Home, Office |
| 29200009 (Lefevre) | `Admin` | `France - In/Out Timesheet` | `France - 35H/WEEK` | `Moodys - France - Admin` | `France OT approval` | Home, Office, Complimentary Hours |
| 29200010 (Moreau) | `ETAM` | `France - In/Out Timesheet` | `France - 37H/WEEK` | `Moodys - France - ETAM` | `France OT approval` | Home, Office, Overtime |
| 29200011 (Laurent) | `CR` | `France - In/Out Timesheet` | `France - 38H30/WEEK` | `Moodys - France - Cadre En Realisation` | `France OT approval` | Home, Office, Overtime |
| 29200012 (Girard) | `CA` | `France - Timesheet - Forfait Jours` | `France - 1 for each day    Monday to Friday` | `Moodys - France - Cadre Autonome` | `France OT approval` | Home, Office, Overtime |
| 29200013 (Rousseau) | `NA` | `France - In/Out Timesheet Admin` | `France - 35H/WEEK` (fallback) | (None — dict miss) | (none — NA not in OVERTIME_ELIGIBLE_TYPES) | (none — no `NA` activity entries in mapper) |
| 29200014 (Faure) | `ETAM` | `France - In/Out Timesheet` | `France - 37H/WEEK` | `Moodys - France - ETAM` | `France OT approval` | Home, Office, Overtime |
| 29200015 (Mercier) | `Admin` | `France - In/Out Timesheet` | `France - 35H/WEEK` | `Moodys - France - Admin` | `France OT approval` | Home, Office, Complimentary Hours |

**All France rows:**
- `timesheetperiod` = `Monthly` (V2.2 §4.3 — single value for all categories).
- `timesheetapprovalpathname` = `Supervisor` (V2.2 §4.4 — single value).
- `holidayCalendar.name` = `France` (unchanged from pre-V2.2).

**Job Title UDF (rows 7-12, 14-15):**
- `add_new_user` payload's `customFieldValues` must include a Job Title UDF entry pointing to `jobtitledefinitionuri`.
- If Replicon tenant doesn't have a `Job Title` UDF defined on the user object, `jobtitledefinitionuri=None` and the write silently skips — Job Title classification still works (it reads from feed → conf, not from Replicon), but the UDF won't be set. **Confirm the UDF exists in trial Replicon before treating any user as "failed."**

### Validator (negative-path) check

Row 13 (Rousseau, NA category) will derive `NA` and:
- `employeetypename` value in conf is `NA`.
- `is_employeetype_available_in_mapper` checks `derivedemployeetype == 'NA'` against the mapper — `NA` is in the mapper at line 581 of `user_sync_mapper.py`, so this passes.
- `payrulename` = None → `payrulescripturi` null-short-circuit; PayRule slot in payload emits `null`. Should not cause validator failure.

If you want to manually trigger a validator failure: edit row 13 to use an unknown location code like `99000` — that should cause `is_location_available_in_mapper` to return `False`, `test_valid_fields_add` to fail, and a log entry `Location not available in mapper` to appear.

## Quick reference — schedule / pay rule / activity expected values

Derived from [france_v1/utils/request_payload.py](../../../france_v1/utils/request_payload.py) and [france_v1/mapper/user_sync_mapper.py](../../../france_v1/mapper/user_sync_mapper.py):

- **Schedules** ([request_payload.py:548-558](../../../france_v1/utils/request_payload.py#L548-L558))
- **Pay Rules** ([request_payload.py:44-50](../../../france_v1/utils/request_payload.py#L44-L50))
- **Overtime constants** ([request_payload.py:52-54](../../../france_v1/utils/request_payload.py#L52-L54))
- **Activities** ([user_sync_mapper.py](../../france_v1/mapper/user_sync_mapper.py) — 14 entries under `type == 'activity'`)
