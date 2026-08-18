# QA Testing Guide — Task Resource Allocation Webhooks

**Scope:** `resource_planner_task_alloc_webhook_added_*`, `..._deleted_*`, `..._modified_child_*` (3 DAGs)
**Direction:** Polaris → Resource Planner (event-driven, webhook-pushed)
**Audience:** QA engineers
**Last updated:** 2026-05-12 · v1.0
**Common mechanics:** see [qa_testing_reference.md](qa_testing_reference.md)

---

## 0. Context

| | Allocation Webhooks |
|---|---|
| What it does | Real-time sync of allocation changes from Polaris to RP. Polaris fires a webhook on every `Created` / `Modified` / `Deleted` event; a separate DAG processes each event type. |
| Polaris events | `ProjectPolarisTaskAllocationCreated`, `ProjectPolarisTaskAllocationModified`, `ProjectPolarisTaskAllocationDeleted` |
| RP write — added | `PUT /api/v1/rp/sourceAllocations` (replacement payload) |
| RP write — modified | `PUT /api/v1/rp/sourceAllocations` (same, partitioned by hash for serialization) |
| RP write — deleted | `PATCH /api/v1/rp/sourceAllocations` (mark hours=0) |
| Side trigger ⭐ | `trigger_ensure_project_tasks` on added/modified |
| Target table | `dbo.rp_source` |
| Schedule | **None** — purely event-driven |
| Concurrency model | Added: `max_active_runs=N` (default). Modified: N partitioned children for serialization per `md5(allocation_uuid) % N`. Deleted: simple, fast. |

### Why 3 DAGs?

| Event | DAG | Why it's separate |
|---|---|---|
| Created | `..._added_{instance}` | Full flow: lookups, GraphQL fetch, write |
| Modified | `..._modified_child_{instance}_{1..N}` | **N partitioned children**, hash-routed by `allocation_uuid`. Each child runs `max_active_runs=1`, so back-to-back modifications of the same allocation serialize correctly (last writer wins). |
| Deleted | `..._deleted_{instance}` | Minimal — just a PATCH marking hours=0. No Polaris fetch needed. |

The webhook receiver layer (`dags/resource_planner/webhooks/...`) routes incoming events to the correct DAG by reading the `X-Replicon-Webhook-Event-Type` header.

---

## 1. Pre-requisites

1. **Webhook configuration in Polaris**: a webhook must be registered pointing at the receiver URL with the correct bearer token. (Done at integration setup; QA usually doesn't reconfigure this.)
2. **Bearer token Variable** for each event type — see `instances/dev.py`:
   ```python
   webhook_added_bearer_token_var = "<some variable name>"
   webhook_modified_bearer_token_var = "..."
   webhook_deleted_bearer_token_var = "..."
   ```
3. **All 3 receiver DAGs unpaused** in Airflow.
4. **`ensure_project_tasks_{instance}` unpaused** (for the JIT trigger from added/modified).
5. **Lookup data in place**: at least one user in `rp_resources` (for `usersUserId` lookup). If not, the webhook DAG will write with empty `users_user_id`.

---

## 2. Test Cases

### TC-WHK-01 · Added webhook: end-to-end happy path

**How to test**
1. **Polaris** UI:
   - Open a project, navigate to **Allocations**.
   - Allocate a user to a task for **3 working days, 8h each**.
   - Submit.
2. Polaris fires the `Created` webhook to the Airflow receiver within seconds.
3. **Airflow** UI: open `resource_planner_task_alloc_webhook_added_dev`. A new DAG run should appear.
4. Click the run. Verify all tasks succeeded.

**Expected**
- Receiver routes to `_added` DAG.
- Conf includes `allocation_id`, `allocation_uuid`, `project_uri`, `task_uri`, `user_uri`.
- DAG fetches the allocation via GraphQL → expands → writes 3 rows via PUT.
- JIT trigger fires for the project.

**Verify**
```sql
SELECT source_booking_id, work_date, hours FROM dbo.rp_source
 WHERE source_booking_id = '<UUID-FROM-WEBHOOK>'
 ORDER BY work_date;
```
Expect 3 rows.

### TC-WHK-02 · Manual webhook injection (faster than UI)

**How to test (faster repeatable):** see [qa_testing_reference.md §7.2](qa_testing_reference.md#72-direct-post-to-the-webhook-receiver-faster-for-repeatable-tests).

```bash
curl -k -X POST "<airflow-webhook-receiver-url>" \
  -H "Content-Type: application/json" \
  -H "X-Replicon-Webhook-Event-Type: ProjectPolarisTaskAllocationCreated" \
  -H "Authorization: Bearer <token-from-Variable>" \
  -d @sample-added-event.json
```

`sample-added-event.json`:
```json
{
  "webhook": {
    "data": {
      "id":     "urn:replicon-tenant:abc:psa-task-allocation:<UUID>",
      "project":{ "uri": "urn:replicon-tenant:abc:project:<PID>" },
      "task":   { "uri": "urn:replicon-tenant:abc:task:<TID>" },
      "user":   { "uri": "urn:replicon-tenant:abc:user:<UID>" },
      "actingUser": { "uri": "urn:replicon-tenant:abc:user:U-1" }
    }
  }
}
```

**Expected:** same as TC-WHK-01 but no Polaris UI involvement. Use this for repeated runs.

---

### TC-WHK-03 · Modified webhook: hash-partitioned serialization

**How to test**
1. **Polaris**: take an existing allocation, modify its hours (8 → 6 for one day). Submit.
2. Polaris fires `Modified` event.
3. **Airflow**: the receiver computes `md5(allocation_uuid) % modified_child_count` to pick a child DAG (e.g. `..._modified_child_dev_2`). A new run appears under that specific child.

**Expected**
- Specific partitioned child DAG runs.
- DAG re-fetches the allocation, re-expands, writes new rows (overwriting old via PUT).

**Verify** the modified day shows the new hours.

---

### TC-WHK-04 · Same allocation modified twice quickly → both runs serialize on the same child

**Goal:** confirm the `max_active_runs=1` per partitioned child serializes back-to-back updates correctly.

**How to test**
1. Modify the same allocation twice within 1 second.
2. Both webhook events fire.

**Expected**
- Both go to the **same** partitioned child (deterministic hash route).
- Run #2 queues until run #1 finishes (because `max_active_runs=1`).
- Last-write-wins behaviour preserved.

**Verify**: the final DB state matches the **second** modification, not the first.

---

### TC-WHK-05 · Deleted webhook: rows marked hours=0

**How to test**
1. **Polaris**: delete an existing allocation.
2. Polaris fires `Deleted` event.

**Expected**
- `..._deleted_dev` DAG run appears.
- Conf has `allocation_uuid`.
- DAG fires `mark_allocation_deleted` PATCH.

**Verify**
```sql
SELECT source_booking_id, work_date, hours FROM dbo.rp_source
 WHERE source_booking_id = '<DELETED_UUID>';
```
Expect rows still present with `hours = 0`.

---

### TC-WHK-06 · JIT trigger fires for brand-new project (added)

**Same as TC-TRA-05** but driven by a webhook event instead of bulk DAG. Allocate against a project not yet in `rp_source_time_codes` → JIT should fire and fill.

**Verify** the project rows appear in `rp_source_time_codes` within seconds.

---

### TC-WHK-07 · Out-of-order webhooks (network jitter)

**Goal:** when delete arrives before create (e.g. immediate cancel), behaviour stays consistent.

**How to test**
1. Manually inject (curl) a `Deleted` event for a brand-new UUID that doesn't exist in RP yet.
2. Then inject a `Created` event for the same UUID.

**Expected**
- Delete fires PATCH against rows that don't exist → 0 rows affected. DAG still succeeds.
- Create fires PUT → inserts rows.
- Final state: allocation exists (last write wins).

**Verify**: rows present after the second event.

---

### TC-WHK-08 · Authorization required

**How to test**
1. POST a webhook event with **no** Authorization header or a wrong bearer token.

**Expected**
- Webhook receiver returns 401/403.
- No DAG triggered.

---

### TC-WHK-09 · Malformed webhook payload

**How to test**
1. POST a webhook event with missing `data.id` or other required fields.

**Expected**
- Receiver either rejects (4xx) or triggers a DAG that fails fast at `prepare_*_request`.
- No rows written.

---

## 3. Cross-DAG / regression

### TC-WHK-XGEN-01 · Webhook + bulk DAG harmonious

**Goal:** if the bulk allocation DAG runs while webhooks are flowing, no conflicts.

Both write to `rp_source` via the same gateway endpoints. PUT is a "replacement" — both will reach the same final state. Concurrency only matters if both touch the same allocation_uuid in the same millisecond — extremely rare; gateway transactions handle it.

---

### TC-WHK-XGEN-02 · No orphan allocations

Same query as TC-TRA-XGEN-01. After webhook activity + JIT, expect 0 orphans within ~30s of the webhook landing.

---

## 4. Cleanup / Reset

```sql
-- DEV ONLY
DELETE FROM dbo.rp_source
 WHERE source_system = 'Polaris'
   AND source_booking_id IN ('<TEST_UUID_1>', '<TEST_UUID_2>');
```

In Polaris: delete or archive the test allocations + project.

---

## 5. Sign-off criteria

- [ ] TC-WHK-01 (added end-to-end) — pass
- [ ] TC-WHK-03 (modified routes to partitioned child) — pass
- [ ] TC-WHK-04 (serialization of same UUID) — pass (**critical** — wrong concurrency = lost updates)
- [ ] TC-WHK-05 (deleted PATCHes) — pass
- [ ] TC-WHK-06 (JIT for new project) — pass
- [ ] TC-WHK-08 (auth required) — pass
- [ ] TC-WHK-XGEN-02 (no orphans) — pass
- [ ] Average webhook→row latency < 30 seconds in dev
- [ ] No `WARN`/`ERROR` lines unexpected

---

## 6. Known limitations / out of scope

- **Webhook receiver scaling**: a flood of events (e.g. bulk import in Polaris that fires 1000 webhook events in a minute) may queue up against `max_active_runs`. The receiver itself returns 200 immediately; DAG runs queue.
- **At-least-once delivery**: Polaris may fire the same event twice. DAG operations are idempotent (PUT is replacement, PATCH is set-to-zero), so duplicate events produce the same final state.
- **Event ordering across types**: if `Modified` and `Deleted` for the same UUID arrive out of order, the **later-completing** DAG wins. Polaris doesn't include timestamps in the event body — out of scope to fix.
- **Webhook token rotation**: out of scope here. Coordinate with DevOps; they update the Airflow Variable.
