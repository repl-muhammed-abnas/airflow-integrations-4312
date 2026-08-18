# Source Opportunities → Polaris Project Sync

Creates Polaris projects from Salesforce opportunities staged in
`sf_replicon_opportunities` (populated out-of-band by an upstream ETL),
exposed via two `integration_gateway` REST endpoints.

## Architecture: three-tier master → page-child → op-create / op-update-execution

```
main.py                   (master)              — 1 run, fans out by page
child_dag.py              (page-child)          — 1 run per page, classifies + fans out by opportunity
op_create_dag.py          (op-create)           — 1 run per opportunity, create + transition to Initiate
op_update_execution_dag.py (op-update-execution) — 1 run per opportunity, update fields + transition to Execution
```

Modeled on `confirmed_bookings_export`, not `deltek_internal/project_sync`'s
flatter 2-tier shape — template selection depends on each opportunity's
`engagementContractType`, so each opportunity needs its own individually
replayable DAG run.

### Routing table (stage × probability → action)

| Stage | Probability | Action |
|---|---|---|
| `Closing` | ≥ 70 | op-create → **Initiate** |
| `Closed Won` | = 100 | op-update-execution → **Execution** |
| `Closed Lost` / `Closed/No Decision` / `Sales Rejected` | = 0 | op-close-out → **Closed** |
| anything else | any | skipped |

### Master (`main.py`)
`max_active_runs=1` (cursor read-modify-write must not race).
Calls `POST /sourceOpportunities/batches` with stored watermark and
`minProbability: 70` (gateway pre-filter — no opportunity below 70 qualifies
for any action), fans out one page-child per page, emails a failure report
if any page/op failed, and advances the cursor **only on full success** —
a partial failure leaves the watermark untouched so the next scheduled run
retries the whole window.

### Page-child (`child_dag.py`)
Fetches one page via `POST /sourceOpportunities`, then runs
`classify_opportunities` to bucket rows into `creates`, `update_executions`,
`close_outs`, and `skipped` using stage+probability together. Fans out three
**sequential** branches — `has_creates` → `join_creates` → `has_update_executions`
→ `join_updates` → `has_close_outs` → `join_close_outs` — before `log_failure`
(sequential per `BatchTaskRunOperator` constraint; no parallel fan-out is used
and there is no `join_all_work` task).

### Op-create (`op_create_dag.py`)
One run per Closing opportunity: resolve/create client → **`check_project_exists`
idempotency guard** (skip if project already exists) → resolve template by
`engagementContractType` → duplicate project → `RepliconBatchExecutionSensor`
poll (no `tasks_to_retry` — copy call has no idempotency key) → apply
opportunity data → attach client → **`update_project_workflow_state`**
(`putProjectWorkflowState3` GraphQL mutation, `projectWorkflowStateId="INITIATE"`
via `config.POLARIS_INITIATE_STATE_ID`).

### Op-update-execution (`op_update_execution_dag.py`)
One run per Closed Won opportunity: validate → `find_existing_project` →
**`project_found`** (`IfOperator` — branches, does NOT raise):
- **Yes** (project exists — the normal case): update `startDate`/`servicesRevenue`
  → `update_project_workflow_state` (`projectWorkflowStateId="EXECUTION"`).
- **No** (project missing — e.g. the earlier "Closing" create step was
  skipped/failed/never ran for this opportunity): run the same create flow as
  `op_create_dag.py` (resolve/create client → resolve template by
  `engagementContractType` → duplicate project → poll batch → apply
  opportunity data → attach client), then transition straight to
  **`update_project_workflow_state_after_create`**
  (`projectWorkflowStateId="EXECUTION"` — no intermediate "Initiate" stop,
  since the opportunity is already at 100% Closed Won). This guarantees the
  Polaris project always ends up existing and correctly staged, instead of
  failing the run and requiring a manual op-create replay.

Both branches converge at `join_result` before the shared `log_failure`/`end_task`.

### Op-close-out (`op_close_out_dag.py`)
One run per Closed Lost / Closed/No Decision / Sales Rejected opportunity
(probability == 0). Finds the project by name; if found: **`update_project_workflow_state`**
(`projectWorkflowStateId="CLOSEOUT"` via `config.POLARIS_CLOSEOUT_STATE_ID`);
if not found: `log_not_found_in_polaris` (structured warning, op-DAG run
succeeds — missing project is expected when the opportunity was lost before
ever reaching Closing).

## Key decisions
- **Templates** (confirmed 2026-08-04): `engagementContractType` values are
  `"Statement of Work (SOW)"`, `"Change Order/APC"`, `"Work Order (WO)"`.
  `"Statement of Work (SOW)"` → `2026 - Project Template (DO NOT MODIFY)`;
  `"Change Order/APC"` and `"Work Order (WO)"` both →
  `2026 - Work Order Template (DO NOT MODIFY)`.
- **Stage-based routing** (2026-08-08): `classify_opportunities` uses both
  `stage` and `probability` together; the old single `MIN_PROBABILITY=50`
  threshold is replaced by explicit stage conditions (see routing table above).
  `MIN_PROBABILITY=70` is still sent to the gateway as a server-side pre-filter.
- **Polaris workflow state transitions** (updated 2026-08-12): all three op-DAGs
  use `putProjectWorkflowState3` GraphQL mutation (`endpoint='/graphql'`,
  `app='polaris'`) via `request_payload.build_workflow_state_mutation_payload`.
  State IDs: `config.POLARIS_INITIATE_STATE_ID="INITIATE"` (create path),
  `config.POLARIS_EXECUTION_STATE_ID="EXECUTION"` (update-execution path),
  `config.POLARIS_CLOSEOUT_STATE_ID="CLOSEOUT"` (close-out path). Replaces the
  old 3-step REST pattern (GetProjectWorkflowStateActions → guard → PerformProjectWorkflowAction).
- **Tenant/company_key**: standard `resource_planner` convention, same as
  `confirmed_bookings_export` (not `project_sync`'s trial tenant).
- **Utils split**: `utils/request_payload.py` (pure payload builders, no
  `rail.result()` calls) + `utils/custom_methods.py` (pure response/callback
  functions) — op-DAG files (`op_create_dag.py`, `op_update_execution_dag.py`,
  `op_close_out_dag.py`) own all XCom wiring via `lambda: fn(result(...))`.
- **`customFieldsToApply` ships empty** in the modify-project payload —
  `OpportunityItem`'s fields don't map onto `project_sync`'s
  `Sol_Sales_*`-keyed custom fields; wire this in once real template custom
  fields are confirmed.
- **`destinationProjectInfo.dateRange.startDate`** (in `create_duplicate_project`)
  uses the opportunity's `startDate` when present, else today (the run
  date) — `resolve_project_start_date` in `utils/request_payload.py`.

## Open risks
- `destinationProjectInfo.code = opportunity['opportunityNumber']` is an
  unconfirmed guess (no Case-lookup analog).
- Staged dev rollout + Polaris-admin sign-off still pending before any
  non-dev instance is unpaused.
- If a "Closed Won" opportunity's project was never created (the "Closing"
  step was skipped or failed), the op-update-execution DAG's `project_found`
  No-branch now creates the project itself and transitions it straight to
  Execution (2026-08-12) — this is no longer a hard failure requiring a
  manual op-create replay. Unconfirmed: whether Polaris accepts a direct
  draft→EXECUTION transition without first passing through INITIATE; if it
  rejects this, `update_project_workflow_state_after_create` will fail
  loudly with a Polaris API error, which is testable on first real run.
- Close-out (Closed Lost / Closed/No Decision / Sales Rejected → Closeout stage)
  is handled by `op_close_out_dag.py` — see the Op-close-out section above.
