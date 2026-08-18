# QuickBooks operators × BatchTaskRunOperator — kwarg-leak fix

Reference write-up for the `TypeError: got multiple values for keyword
argument 'endpoint'` crash that surfaced when running mapping_sync's
batch-wrapped child DAGs against the RAIL `QuickBooks*Operator` family.
Filed under `mapping_sync/doc/` because the issue surfaced while
landing the BatchTaskRunOperator wrap on the 5 `map_*` child DAGs; the
underlying defect and the fix both live in the RAIL library.

> **Status (as of 2026-05-30)**: 5 of the 10 `QuickBooks*Operator`
> subclasses are patched in `replicon-airflow-library`. The 5 patched
> files are the ones referenced by `mapping_sync/` child DAGs — so the
> immediate bug is resolved. The remaining 5 are listed at the bottom
> for a follow-up RAIL PR.

---

## 1. Symptom

After enabling the `BatchTaskRunOperator` wrap on `map_firm_dag` (and
each sibling `map_*_dag`), `batch_task` failed during its first
`_execute_task` call:

```
[batch_task_run_operator.py:279] INFO - started running task fetch_qbo_customers with try # 3
[batch_task_run_operator.py:315] ERROR - task execution fetch_qbo_customers failed with error - 'endpoint'

Traceback (most recent call last):
  File "/opt/airflow/packages/rail/operators/batch_task_run_operator.py", line 291, in _execute_task
    task_copy = type(task)(
  File "/home/airflow/.local/lib/python3.9/site-packages/airflow/models/baseoperator.py", line 437, in apply_defaults
    result = func(self, **kwargs, default_args=default_args)
  File "/opt/airflow/packages/rail/operators/intuit_internal/quickbooks_customer_operator.py", line 114, in __init__
    super().__init__(
TypeError: QuickBooksBaseOperator.__init__() got multiple values for keyword argument 'endpoint'
```

`BatchTaskRunOperator` retried the same construction up to its retry
ceiling, each attempt failing identically. The task ultimately raised,
the batch wrapper failed, and the child DAG's `catch_*_dag_error`
absorbed the error — so the dispatcher saw the child as SUCCESS (with
the error captured in XCom), and the run continued with no useful
work done.

---

## 2. Root cause

The crash is a kwarg-deduplication failure inside RAIL's
`QuickBooks*Operator` subclasses when re-instantiated by
`BatchTaskRunOperator`. Every layer below contributes:

### 2a. How `BatchTaskRunOperator` re-instantiates each task

`BatchTaskRunOperator._execute_task` (RAIL,
`batch_task_run_operator.py:287-292`) rebuilds each in-range task as a
fresh instance per attempt:

```python
init_kwargs = dict(filter(
    lambda tup: tup[0] not in ['tasks_to_retry', 'on_failure_callback'],
    task.__dict__['_BaseOperator__init_kwargs'].items(),
))
task_copy = type(task)(
    **{
        **init_kwargs,
        'task_id': f'{task.task_id}_batch_{retry}_{str(uuid.uuid4())[:8]}',
        'dag': copy.copy(task.dag),
    }
)
```

The operator reads the Airflow-internal name-mangled attribute
`_BaseOperator__init_kwargs` and feeds it back into the class
constructor. That attribute is captured by Airflow's `apply_defaults`
decorator on every operator's `__init__`, storing **whatever kwargs
were passed to that level**.

### 2b. Why `_BaseOperator__init_kwargs` carries leaked keys for QB subclasses

Each `QuickBooks*Operator` subclass in RAIL does this:

```python
# quickbooks_customer_operator.py (and 9 siblings)
class QuickBooksCustomerOperator(QuickBooksBaseOperator):

    def __init__(self, operation='search', query=None,
                 request_body=None, **kwargs):
        if operation == 'search':
            endpoint = '/query'
            request_method = 'GET'
            query_params = {'query': query or DEFAULT_CUSTOMER_QUERY}
            request_body = None
        else:
            endpoint = '/customer'
            request_method = 'POST'
            query_params = None

        super().__init__(
            endpoint=endpoint,                # ← explicit forward
            request_method=request_method,    # ← explicit forward
            query_params=query_params,        # ← explicit forward
            request_body=request_body,        # ← explicit forward
            **kwargs,
        )
```

`super().__init__()` lands in `QuickBooksBaseOperator.__init__`, whose
signature accepts those 4 keys as part of `**kwargs` (it just forwards
them on to `InternalQuickbooksAPIOperator`). Airflow's `apply_defaults`
wrapper on `QuickBooksBaseOperator.__init__` captures **its received
kwargs** into `_BaseOperator__init_kwargs` — which now includes the 4
subclass-injected keys (`endpoint`, `request_method`, `query_params`,
`request_body`) plus the user's original kwargs (`task_id`,
`intuit_conn_id`, `operation`, `query`).

### 2c. Why the second construction explodes

When `BatchTaskRunOperator` does `type(task)(**init_kwargs)`, the
QuickBooks subclass's `__init__` runs again. Its named params
(`operation`, `query`, `request_body`) bind from `init_kwargs`. The
**leftover kwargs** still contain `endpoint='/query'` from the prior
super call. The subclass then runs its body, recomputes a local
`endpoint = '/query'`, and calls:

```python
super().__init__(
    endpoint=endpoint,           # explicit (from local variable)
    request_method=request_method,
    query_params=query_params,
    request_body=request_body,
    **kwargs,                    # ALSO contains endpoint!
)
```

Python rejects `endpoint=…` twice — **`TypeError: got multiple values
for keyword argument 'endpoint'`**.

The collision is structural: any operator that **forwards new kwargs
explicitly into a `super().__init__()` whose signature ALSO accepts
those same kwargs via `**kwargs`** breaks when re-instantiated via
`_BaseOperator__init_kwargs`.

### 2d. Why this doesn't affect `InternalQuickbooksAPIOperator` directly

The base `InternalQuickbooksAPIOperator`'s `__init__` looks like:

```python
def __init__(self, *args, intuit_conn_id, endpoint, request_method,
             query_params=None, request_body=None, page_size=MAX_PAGE_SIZE,
             **kwargs):
    super().__init__(*args, **kwargs)   # ← no kwarg injection into super
    self.intuit_conn_id = intuit_conn_id
    self.endpoint = endpoint
    ...
```

It absorbs `endpoint` / `request_method` / etc. as **named params**
and only forwards `**kwargs` to `BaseOperator`. The named params
become attributes on `self`, not entries in
`_BaseOperator__init_kwargs`. On re-instantiation, the same named
params come back in `init_kwargs` and bind cleanly — no duplicate-key
collision.

Same story for `VantagepointSettingsBankOperator` (used by
`map_bank_code_dag`): no injection into `super()`, so it's naturally
safe.

---

## 3. Fix

In each affected `QuickBooks*Operator` subclass, pop the 4 forwarded
keys from `kwargs` **before** calling `super().__init__()`. The pop is
a no-op when `kwargs` doesn't carry those keys (normal end-user
construction) and drops the leaked duplicates on
`BatchTaskRunOperator` re-instantiation.

```python
# Defensive: BatchTaskRunOperator re-instantiates each task via
# `type(task)(**task.__dict__['_BaseOperator__init_kwargs'])` and
# Airflow's apply_defaults captures every layer's kwargs into
# _BaseOperator__init_kwargs. So on re-instantiation `kwargs`
# already contains `endpoint` / `request_method` / `query_params`
# / `request_body` from this subclass's prior super().__init__
# call, and forwarding them as explicit named args collides with
# the leaked ones ("got multiple values for keyword argument
# 'endpoint'"). Popping the leaked copies here is a no-op in
# normal construction (the user-facing API never passes these to
# the subclass) and correctly defers to the explicit values
# below on every re-instantiation. Works both with and without
# BatchTaskRunOperator.
kwargs.pop('endpoint', None)
kwargs.pop('request_method', None)
kwargs.pop('query_params', None)
kwargs.pop('request_body', None)

super().__init__(
    endpoint=endpoint,
    request_method=request_method,
    query_params=query_params,
    request_body=request_body,
    **kwargs,
)
```

### Why pop, not setdefault / merge / something else

A few alternatives were considered:

- **Move the pop into `QuickBooksBaseOperator`.** Doesn't work — the
  base doesn't know which keys the subclass intends to inject;
  popping them at the base would discard the legitimate
  subclass-supplied values too.
- **Replace explicit kwarg forwarding with a merged dict.** e.g.
  `super().__init__(**{**kwargs, 'endpoint': endpoint, ...})`. Works,
  but obscures the intent.
- **Patch `BatchTaskRunOperator` instead.** It could read kwargs only
  from the outermost layer. Possible but invasive — touches every
  BatchTaskRunOperator caller, not just QB consumers.
- **Add a `_subclass_endpoint` private kwarg pattern.** API-breaking;
  every direct caller would have to change.

The `kwargs.pop()` approach is the smallest, most-targeted, and works
under both batch and non-batch construction without changing any
caller code.

---

## 4. Files patched in RAIL

> Paths relative to `replicon-airflow-library/rail/rail/operators/intuit_internal/`.
> All 10 leaf `QuickBooks*Operator` subclasses now carry the defensive
> `kwargs.pop()` block. The two base files
> (`quickbooks_api_operator.py`, `quickbooks_base_operator.py`) are
> intentionally untouched — they don't inject `endpoint` into
> `super().__init__()`, so they're naturally safe under
> `BatchTaskRunOperator` re-instantiation.

| File | Status | Used by mapping_sync? | Commit |
|---|---|---|---|
| `quickbooks_customer_operator.py` | ✅ patched | yes — map_firm | `0a3845b` |
| `quickbooks_vendor_operator.py` | ✅ patched | yes — map_firm, map_employee | `0a3845b` |
| `quickbooks_employee_operator.py` | ✅ patched | yes — map_employee | `0a3845b` |
| `quickbooks_account_operator.py` | ✅ patched | yes — map_account_code, map_bank_code | `0a3845b` |
| `quickbooks_tax_code_operator.py` | ✅ patched | yes — map_tax_code | `0a3845b` |
| `quickbooks_bill_operator.py` | ✅ patched | no | `d6a431c` |
| `quickbooks_invoice_operator.py` | ✅ patched | no | `d6a431c` |
| `quickbooks_item_operator.py` | ✅ patched | no | `d6a431c` |
| `quickbooks_journal_entry_operator.py` | ✅ patched | no | `d6a431c` |
| `quickbooks_time_activity_operator.py` | ✅ patched | no | `d6a431c` |

Rollout history: the first commit (`0a3845b`) covered the 5 subclasses
referenced by mapping_sync child DAGs (the immediate fix for the
`'endpoint'` TypeError surfaced by `map_firm_dag`'s `batch_task`). The
second commit (`d6a431c`) closed the gap on the 5 deferred subclasses
that aren't used by mapping_sync but share the same
`super().__init__(endpoint=…, …, **kwargs)` shape — any future DAG
wrapping them in `BatchTaskRunOperator` would have hit the identical
crash without the patch.

---

## 5. Validation

Local validation in the dev environment (`vp_qbo_mapping_sync_*_trial`):

1. After patching the 5 RAIL files, restart both `airflow-scheduler-1`
   and `airflow-worker-1` (worker holds cached RAIL imports — a
   scheduler-only restart isn't sufficient).
2. `airflow dags list-import-errors` returns `No data found`.
3. Trigger a fresh dispatcher run (delete the
   `vp_qbo_mapping_init_<customerId>_trial` Airflow Variable to force
   a re-init).
4. Each child DAG's `batch_task` should reach the actual sync work
   (`fetch_qbo_*`, `process_qbo_*`, `mark_*_step_complete`) instead of
   looping on the `'endpoint'` TypeError.

To verify the fix is safe in non-batch mode too: set the Airflow
Variable `vp_qbo_mapping_sync_can_run_batch` to `'false'`. The
`can_run_batch_task` IfOperator routes to the legacy per-task path
(no `BatchTaskRunOperator` re-instantiation), and each
`QuickBooks*Operator` is constructed once by Airflow's scheduler.
The defensive `kwargs.pop()` is a no-op in that path because end-user
calls never carry those 4 keys.

---

## 6. Related docs

- [`MAPPING_SYNC_CLEANUP_AND_PERF.md`](MAPPING_SYNC_CLEANUP_AND_PERF.md)
  — performance backlog; P1–P6 unchanged by this fix.
- [`LOOKUP_TABLE_FLOWS.md`](LOOKUP_TABLE_FLOWS.md) — mapping-table
  lifecycle reference.
- RAIL: `rail/operators/batch_task_run_operator.py` (lines 23–25 for
  the explicit "no parallel-task support" contract, lines 287–292 for
  the re-instantiation code path).
- RAIL: `rail/operators/intuit_internal/quickbooks_customer_operator.py`
  — the canonical patched file. The other 9 patched files cross-
  reference its inline comment ("See quickbooks_customer_operator.py
  for the rationale…") to avoid duplicating the explanation.

---

## 7. Open follow-up — regression test in RAIL

All 10 leaf `QuickBooks*Operator` subclasses are now patched. One
optional follow-up remains on the RAIL repo:

Add a unit test that constructs each `QuickBooks*Operator` twice —
once normally and once via
`type(op)(**op.__dict__['_BaseOperator__init_kwargs'])` — and asserts
both succeed. This catches future operator additions that repeat the
kwarg-injection-into-super pattern and would otherwise crash silently
the first time someone tries `BatchTaskRunOperator` on them.

Not blocking anything in mapping_sync; can be picked up as a tidy-up
when the RAIL repo is next touched.
