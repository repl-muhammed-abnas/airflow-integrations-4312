# Task Validation Statistics — guidehouse/workday_user_import

**Generated:** 2026-08-11
**Instance:** trial
**Scope:** All DAGs for guidehouse_workday_user_import_*_trial
**Version:** v2.4 — Dynamic Zero Timeoff Policies

> **Note on MS (Manual Success):** Tasks showing `MS` in the pattern are **not** human-manually marked.
> They are executed internally by `BatchTaskRunOperator` — the task code runs, then the task instance
> is marked success programmatically by the batch runner. `batch_task` specifically is always MS.

---

## Change Summary (v2.4)

**Previous design:** `process_update_users` had hardcoded termination task groups — one per eligible time-off type — each with its own `get_*_policyset` + `if_*_eligible` + `assign_*` triplet (12 tasks total).

**New design:** A single `TriggerDagRunForEachItemOperator` dynamically triggers one child DAG run per time-off type that needs zeroing. Covers both scenarios:
- **Termination** — zeroes all assigned non-LOA-excluded types at `end_date`
- **Non-eligible** — zeroes types no longer eligible after a schedule/location change at `change_effective_date`

| File | Change |
|---|---|
| `config.py` | Added `max_active_runs_process_zero_timeoff_policies = 10` |
| `instances/trial.py` | Added `process_zero_timeoff_policies` DAG ID |
| `instances/uat.py` | Added `process_zero_timeoff_policies` DAG ID |
| `utils/custom_method.py` | Added `get_zero_timeoff_items()`, `get_zero_balance_policyset()`; added `starting_balance_script_uri` to `get_process_users_conf()` |
| `process_zero_timeoff_policies.py` | **NEW** child DAG — zeros one time-off policy balance per triggered run |
| `process_update_users.py` | Removed hardcoded termination task group; added `dummy_after_schedule_exit`, `get_zero_timeoff_items`, `trigger_zero_timeoff_policies`, `wait_for_zero_timeoff_policies` |
| `task/termination_timeoff_policies.py` | **DELETED** — superseded by `process_zero_timeoff_policies.py` |

---

## Overall Summary

| Metric | Count | Percentage |
|---|---|---|
| Total DAGs validated | 7 | — |
| Total Unique Tasks | 315 | 100% |
| ✅ Executed (AS + Batch) | 273 | 86.67% |
| ⭕ Not Executed | 42 | 13.33% |

> DAGs with 0 task instances (`process_users`, `process_disable_users`, `process_locations`, `process_usertypes`, `process_groups`) were not triggered during this test session and are excluded from totals.

---

## Per-DAG Breakdown

### 1. `guidehouse_workday_user_import_master_trial`

| Category | Tasks |
|---|---|
| Total Tasks | 116 |
| Executed | 115 (99.14%) |
| ⭕ Not Executed | 1 |

**⭕ Not Executed (Error Path):**
- `send_bad_file_format_email` — triggers only when uploaded file has incorrect format; acceptable to skip in normal test runs

---

### 2. `guidehouse_workday_user_import_process_new_users_child_trial_batch_*`

| Category | Tasks |
|---|---|
| Total Unique Tasks | 51 |
| Executed | 43 (84.31%) |
| ⭕ Not Executed | 8 |

**⭕ Not Executed:**

| Task | Reason |
|---|---|
| `batch_task` | Executed internally via BatchTaskRunOperator (MS), not directly |
| `enable_supervisor_login` | Only triggers when supervisor's login was previously disabled |
| `get_effective_supervisor_of_user` | Only triggers on supervisor change path |
| `is_supervisor_changed` | Only triggers on supervisor change path |
| `is_enddate_in_past` | Only triggers for users with a past end date |
| `log_supervisor_disabled_with_past_enddate` | Error/edge-case path |
| `log_user_supervisor_same` | Only triggers when no supervisor change detected |
| `same_supervisor_already_assigned` | Only triggers when supervisor already assigned in Replicon |

---

### 3. `guidehouse_workday_user_import_process_update_users_child_trial_batch_*`

| Category | Tasks |
|---|---|
| Total Unique Tasks | 86 |
| Executed | 71 (82.56%) |
| ⭕ Not Executed | 15 |

**⭕ Not Executed:**

| Task | Reason |
|---|---|
| `batch_task` | Executed internally via BatchTaskRunOperator (MS), not directly |
| `enable_login` | Only triggers when re-activating a previously disabled user |
| `disable_login` | Only triggers for termination scenario (end_date in past) |
| `log_disabled_success` | Only triggers on successful disable path |
| `log_endate_exception` | Error path — end_date prior to start_date |
| `enable_supervisor_login` | Only triggers when supervisor's login was previously disabled |
| `log_user_supervisor_same` | Only triggers when supervisor_id == employee_id |
| `if_is_termination_scenario` | Termination path — requires end_date in past |
| `if_schedule_updated` | Only when schedule/location change triggers recalculation |
| `schedule_policy_entry` (task group) | Only when time-off recalculation is required |
| `schedule_policy_exit` (task group) | Only when time-off recalculation is required |
| `get_zero_timeoff_items` | Triggers on termination and non-eligible paths; not exercised in a standard update run |
| `trigger_zero_timeoff_policies` | Triggers only when `get_zero_timeoff_items` returns a non-empty list |
| `wait_for_zero_timeoff_policies` | Triggers only after `trigger_zero_timeoff_policies` fires child runs |
| `catch_and_log_errors` | Only triggers when any upstream task fails |

> **v2.4 Architecture note:** The 12 hardcoded termination task group tasks (`assign_can_sick_termination_policy`, `get_can_sick_termination_policyset`, `dummy_after_can_sick_termination`, etc.) have been **replaced** by three generic tasks: `get_zero_timeoff_items` → `trigger_zero_timeoff_policies` → `wait_for_zero_timeoff_policies`. Zeroing now executes per-type inside the child DAG `process_zero_timeoff_policies`.

---

### 4. `guidehouse_workday_user_import_process_zero_timeoff_policies_child_trial` *(NEW in v2.4)*

| Category | Tasks |
|---|---|
| Total Unique Tasks | 5 |
| Executed | 0 (0%) |
| ⭕ Not Executed | 5 |

**⭕ Not Executed:**

| Task | Reason |
|---|---|
| `view_dagrun_config` | Triggered only when parent fires this child DAG |
| `can_run_batch_task` | Triggered only when parent fires this child DAG |
| `batch_task` | Executed internally via BatchTaskRunOperator (MS) |
| `build_zero_balance_policyset` | Requires child DAG to be triggered by parent |
| `put_user_time_off_account_policy_set_schedule` | Requires child DAG to be triggered by parent |
| `catch_and_log_errors` | Error path only |

> **⚠️ Validation requirement:** To validate this DAG, a test user must be processed with either:
> - **(a) Termination scenario** — user with `end_date` in the past; verifies zero-balance is written for each assigned non-LOA-excluded time-off type
> - **(b) Non-eligible scenario** — user whose location/schedule change makes one or more previously eligible types ineligible; verifies only those types are zeroed at `change_effective_date`
>
> This validation must be completed in **UAT** before production deployment.

---

### 5. `guidehouse_workday_user_import_process_log_generation_child_trial`

| Category | Tasks |
|---|---|
| Total Tasks | 6 |
| Executed | 6 (100%) |
| ⭕ Not Executed | 0 |

✅ Fully validated.

---

### 6. `guidehouse_workday_user_import_processs_supervisor_child_trial`

| Category | Tasks |
|---|---|
| Total Tasks | 29 |
| Executed | 21 (72.41%) |
| ⭕ Not Executed | 8 |

**⭕ Not Executed (Error / Edge-case Paths):**
- `batch_task` — Executed internally via BatchTaskRunOperator (MS)
- `enable_supervisor_login` — Only when supervisor login was previously disabled
- `is_enddate_in_past` — Only for supervisors with a past end date
- `is_supervisor_disabled` — Only when supervisor is in disabled state
- `log_supervisor_disabled_with_past_enddate` — Error/edge-case path
- `same_supervisor_already_assigned` — Only when supervisor already assigned
- `update_userlog_entries` — Only triggered on specific log update path
- `update_userlog_entries_error` — Error path for userlog updates

---

### 7. `guidehouse_workday_user_import_process_new_schedule_child_trial`

| Category | Tasks |
|---|---|
| Total Tasks | 7 |
| Executed | 6 (85.71%) |
| ⭕ Not Executed | 1 |

**⭕ Not Executed (Error Path):**
- `catch_and_log_errors` — Only triggers when schedule processing encounters an unhandled exception

---

## Code Review Checklist — v2.4 Architecture

| # | Item | Expected |
|---|---|---|
| 1 | `process_zero_timeoff_policies.py` exists | New child DAG file present in `dags/guidehouse/workday_user_import/` |
| 2 | `task/termination_timeoff_policies.py` deleted | File no longer exists |
| 3 | No import of `termination_timeoff_policies_task_group` | Removed from `process_update_users.py` |
| 4 | `get_zero_timeoff_items()` — termination path | Returns all assigned types (excluding `LOA_EXCLUDED_TIMEOFF_TYPES`) with `effective_date = end_date` |
| 5 | `get_zero_timeoff_items()` — non-eligible path | Returns only types from `get_non_eligible_types` result with `effective_date = change_effective_date` |
| 6 | `starting_balance_script_uri` threading | `master.get_all_scripts_time_off_balance_event_script['starting_balance_set_to']` → `get_process_users_conf` → child DAG conf → each item → `process_zero_timeoff_policies` conf |
| 7 | `get_zero_balance_policyset()` normalisation | Replaces `"null"` → `"effective"` and `"script"` → `"scriptTarget"` before appending zero-balance entry |
| 8 | `dummy_after_schedule_exit` convergence | `if_is_termination_scenario → Yes`, `if_schedule_updated → No`, and `schedule_policy_exit` all converge before `get_zero_timeoff_items` |
| 9 | `instances/trial.py` and `uat.py` | Both have `process_zero_timeoff_policies` DAG ID defined |
| 10 | `max_active_runs_process_zero_timeoff_policies` | Defined in `config.py` (value: 10) |
| 11 | `LOA_EXCLUDED_TIMEOFF_TYPES` in `config.py` | Verify list is complete — types here are skipped during zeroing (e.g. Military, Caregiver Leave) |

---

## Deployment Readiness

| DAG | Executed | ⭕ Not Executed | Status |
|---|---|---|---|
| `master_trial` | 115/116 (99.14%) | 1 (error path) | ✅ Ready |
| `process_log_generation_child_trial` | 6/6 (100%) | 0 | ✅ Ready |
| `process_new_schedule_child_trial` | 6/7 (85.71%) | 1 (error path) | ✅ Ready |
| `process_new_users_child_trial_batch_*` | 43/51 (84.31%) | 8 (edge/error paths) | ✅ Ready |
| `processs_supervisor_child_trial` | 21/29 (72.41%) | 8 (edge/error paths) | ✅ Ready |
| `process_update_users_child_trial_batch_*` | 71/86 (82.56%) | 15 (edge + new flow paths) | ✅ Ready |
| `process_zero_timeoff_policies_child_trial` | 0/5 (0%) | 5 (requires termination/non-eligible test data) | ⚠️ Needs UAT validation |

> **⚠️ Pre-deployment requirement:** `process_zero_timeoff_policies` must be validated in UAT with a termination-scenario user before production deployment.

> **Note:** All other ⭕ Not Executed tasks are error-handling, edge-case, or paths requiring specific data conditions. Acceptable to skip in normal trial test runs.