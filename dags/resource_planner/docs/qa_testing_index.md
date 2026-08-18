# QA Testing — Resource Planner Integrations

**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0

This is the entry point. Start here, then drill into the per-integration guide for the DAG you're testing.

---

## 1. Read first

Before running any test:

1. **[Shared reference](qa_testing_reference.md)** — tools, login URLs, how to trigger DAGs, how to set Airflow Variables, how to query the database, common verification patterns.

You only need to read this once per project. The per-integration guides reference back to it instead of repeating.

---

## 2. Pick your integration

Integrations are grouped by **direction** (Polaris → RP or RP → Polaris) and ordered by **complexity / risk** (simplest first, most complex last). Test in this order during a fresh QA cycle.

### Polaris → RP (forward — pulls data from Polaris into RP)

| # | Integration | DAG ID prefix | Trigger | Guide |
|---|---|---|---|---|
| 1 | User export | `resource_planner_user_export_*` | Daily `0 0 * * *` | [qa_testing_guide_users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) (Section: User Export) |
| 2 | TimeOff Type export | `resource_planner_timeoff_type_export_*` | Daily `0 0 * * *` | [qa_testing_guide_users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) (Section: TimeOff Type) |
| 3 | TimeOff Booking export | `resource_planner_timeoff_export_report_*` | Hourly `0 * * * *` | [qa_testing_guide_timeoff_bookings.md](qa_testing_guide_timeoff_bookings.md) |
| 4 | Project Tasks — delta | `resource_planner_project_task_export_delta_*` | Hourly `0 * * * *` | [qa_testing_guide_project_tasks_delta.md](qa_testing_guide_project_tasks_delta.md) |
| 5 | Project Tasks — bulk | `resource_planner_project_task_export_bulk_*` | Manual | [qa_testing_guide_project_tasks_bulk.md](qa_testing_guide_project_tasks_bulk.md) |
| 6 | Task Resource Allocation — bulk | `resource_planner_task_resource_allocation_export_*` | Manual / configured | [qa_testing_guide_task_resource_allocation_bulk.md](qa_testing_guide_task_resource_allocation_bulk.md) |
| 7 | Task Resource Allocation — webhooks | `resource_planner_task_alloc_webhook_*` (3 DAGs) | Event-driven | [qa_testing_guide_task_resource_allocation_webhooks.md](qa_testing_guide_task_resource_allocation_webhooks.md) |
| 8 | Ensure Project Tasks (JIT) | `resource_planner_ensure_project_tasks_*` | Triggered (fire-and-forget) | [qa_testing_guide_ensure_project_tasks.md](qa_testing_guide_ensure_project_tasks.md) |

### RP → Polaris (reverse — pushes RP changes to Polaris)

| # | Integration | DAG ID prefix | Trigger | Guide |
|---|---|---|---|---|
| 9 | Confirmed Bookings Export | `resource_planner_confirmed_bookings_export_*` + page-children + op-DAGs + sync-failure-retry | Manual / configurable | [qa_testing_guide_confirmed_bookings_export.md](qa_testing_guide_confirmed_bookings_export.md) |

---

## 3. Smoke test plan (full-system shakedown)

Run this sequence on a fresh dev environment to verify the whole system is healthy. ~30 min start to finish if everything passes.

| Order | Test | Why this first | Guide reference |
|---|---|---|---|
| 1 | Connectivity test | If the gateway is down, every other test fails. Run the `integration_gateway_connectivity` DAG with a known-good payload. | See `dags/resource_planner/integration_gateway_connectivity/` |
| 2 | User export — TC-USR-01 | Tests gateway insert, Polaris report read | [users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) |
| 3 | TimeOff Type export — TC-TOT-01 | Tests LWAPI / SOAP path | [users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) |
| 4 | Project Tasks Bulk — TC-PTB-01 | First populates `rp_source_time_codes` (foundation for allocations) | [project_tasks_bulk.md](qa_testing_guide_project_tasks_bulk.md) |
| 5 | Task Resource Allocation Bulk — TC-TRA-01 | Tests allocation write + JIT | [task_resource_allocation_bulk.md](qa_testing_guide_task_resource_allocation_bulk.md) |
| 6 | TC-TRA-XGEN-01 (no orphans) | Critical post-step check | [task_resource_allocation_bulk.md](qa_testing_guide_task_resource_allocation_bulk.md) |
| 7 | TimeOff Bookings — TC-TOB-01 | Tests another path through `rp_source` writes | [timeoff_bookings.md](qa_testing_guide_timeoff_bookings.md) |
| 8 | Allocation webhook — TC-WHK-02 (manual injection) | Tests the event-driven path | [task_resource_allocation_webhooks.md](qa_testing_guide_task_resource_allocation_webhooks.md) |
| 9 | Confirmed Bookings Export — TC-CBE-01 | Tests the **reverse** flow (RP → Polaris) | [confirmed_bookings_export.md](qa_testing_guide_confirmed_bookings_export.md) |
| 10 | Confirmed Bookings — TC-CBE-XGEN-01 (no orphans) | Sanity-check the reverse flow's cleanup | [confirmed_bookings_export.md](qa_testing_guide_confirmed_bookings_export.md) |
| 11 | Project Tasks Delta — TC-PTD-08 (JIT + delta no-duplicates) | Format-alignment check between two writers | [project_tasks_delta.md](qa_testing_guide_project_tasks_delta.md) |

If any of #1–11 fails, stop and fix before continuing — downstream tests depend on earlier ones.

---

## 4. Sign-off bundle per release

For a release candidate, run the **Sign-off criteria** section at the bottom of every integration guide. A release passes if all of them pass (or known failures are documented and accepted).

| Guide | Sign-off criteria |
|---|---|
| [users_timeoff_types.md](qa_testing_guide_users_timeoff_types.md) | Section 6 |
| [timeoff_bookings.md](qa_testing_guide_timeoff_bookings.md) | Section 5 |
| [project_tasks_delta.md](qa_testing_guide_project_tasks_delta.md) | Section 5 |
| [project_tasks_bulk.md](qa_testing_guide_project_tasks_bulk.md) | Section 5 |
| [task_resource_allocation_bulk.md](qa_testing_guide_task_resource_allocation_bulk.md) | Section 5 |
| [task_resource_allocation_webhooks.md](qa_testing_guide_task_resource_allocation_webhooks.md) | Section 5 |
| [confirmed_bookings_export.md](qa_testing_guide_confirmed_bookings_export.md) | Section 5 |
| [ensure_project_tasks.md](qa_testing_guide_ensure_project_tasks.md) | Section 5 |

---

## 5. Critical cross-integration tests

These tests span multiple DAGs and catch regressions at the boundaries. **Don't skip these** — they're the ones that caught real bugs during development.

| Test | What it catches |
|---|---|
| TC-PTD-08 (JIT + delta same row format) | Format drift between `ensure_project_tasks` and `project_task_export_delta` — would create phantom duplicates |
| TC-TRA-XGEN-01 (no orphan allocations) | Allocations referencing missing time-codes — the whole reason JIT exists |
| TC-WHK-XGEN-02 (no orphans after webhook) | Same as above for event-driven path |
| TC-CBE-XGEN-01 (no `outbound_pending_op` left after RP→Polaris run) | Bookings stuck in "pending push" state forever |
| TC-CBE-XGEN-02 (every `time_code` resolves) | Outbound bookings referencing non-existent Polaris tasks (would fail mid-push) |
| TC-EPT-XGEN-01 (delta no-op after JIT) | Confirms the JIT format is canonical |

---

## 6. Bug-reporting checklist

When a test fails, file a bug with these details so engineering can reproduce:

- [ ] DAG ID + run ID + task ID that failed
- [ ] DAG run conf (visible in the `view_dag_run_conf` task log)
- [ ] Full traceback from the failed task's log
- [ ] Time the failure happened (so we can correlate with gateway / Polaris logs)
- [ ] DB state immediately after: relevant `rp_source*` rows; row counts vs baseline
- [ ] Any Airflow Variables you changed during the test session
- [ ] Output of the test's "Verify" SQL — both your actual output and what you expected

---

## 7. Where the docs live

All test guides are markdown files under [`dags/resource_planner/docs/`](.):

```
docs/
├─ qa_testing_index.md                              ← you are here
├─ qa_testing_reference.md                          ← shared mechanics
├─ qa_testing_guide_users_timeoff_types.md
├─ qa_testing_guide_timeoff_bookings.md
├─ qa_testing_guide_project_tasks_delta.md
├─ qa_testing_guide_project_tasks_bulk.md
├─ qa_testing_guide_task_resource_allocation_bulk.md
├─ qa_testing_guide_task_resource_allocation_webhooks.md
├─ qa_testing_guide_confirmed_bookings_export.md
└─ qa_testing_guide_ensure_project_tasks.md
```

Markdown renders nicely in VS Code (Ctrl+Shift+V to preview), GitHub, and most IDEs. If your team uses Confluence or another wiki, export to HTML/PDF as needed.

Engineering owns updates: when a DAG's behavior changes, the corresponding guide gets updated in the same PR.
