# Mapping-Sync S3 Collection Locking

**Status:** active · **Scope:** `mapping_sync/utils/*_sync.py` + `_shared.py` writes to the
customer S3 collection (`collections.db.gz`).

This note documents how the four child-DAG sync helpers are protected against
concurrent writers, why they do **not** need to be rewritten as RAIL
`S3*CollectionOperator` instances, and the two operational prerequisites that
must be satisfied for the locking to actually hold.

---

## 1. The locking model (where the lock lives)

The same customer collection file
(`{integration}/{integration_type}/{customer}/collections/collections.db.gz`)
is mutated from **two systems**:

- **integration-platform-api** (TypeScript) on API calls, via
  `withS3Lock(...)` in `src/lib/clients/s3-lock.ts`.
- **airflow-integrations** RAIL operators / mapping-sync helpers (Python) on
  DAG runs.

To make the two mutually exclusive, RAIL has a faithful Python port of the
TypeScript lock at `replicon-airflow-library/rail/rail/lib/s3_lock.py`, and the
lock is taken **inside** the shared chokepoint
`rail.lib.s3_collection.get_or_create_s3_collection_artifact(...)`:

- **Lock object:** an S3 object whose key is `<collection-key>.lock` — the
  *same* key the API derives (`buildLockKey` = collection key + `.lock`) on the
  *same* bucket. So an API mutation and an Airflow mutation of the same
  collection contend on the same lock object.
- **Primitive:** create-only `PutObject(IfNoneMatch='*')` to acquire,
  `DeleteObject(IfMatch=<etag>)` to release / break stale locks (fenced).
- **Default:** `get_or_create_s3_collection_artifact(..., use_lock=True)`. The
  whole **download → mutate → upload** cycle runs inside the lock. The
  pre-existing optimistic ETag check (`S3CollectionConcurrencyError`) stays as a
  backstop underneath the mutex.

> The lock is a **pessimistic mutex** layered over the optimistic ETag check.
> With the lock held, the ETag check should essentially never fire; it remains
> only to catch the stale-break edge case described in §4.

---

## 2. Are the four sync helpers covered? — Yes, automatically

All four helpers (and `_shared.py`) call `get_or_create_s3_collection_artifact`
**without** passing `use_lock`, so they inherit the `use_lock=True` default.
Each helper opens one artifact, performs **many** row mutations on
`artifact.local_filename`, and commits — all inside the lock:

| Helper | child DAG | opens `get_or_create` | `conn.commit()` | locked? |
|---|---|---|---|---|
| `_account_sync.py`  | `sync_qbo_accounts_to_vp`  | yes | yes | ✅ (default `use_lock=True`) |
| `_employee_sync.py` | `sync_qbo_employees_to_vp` | yes | yes | ✅ |
| `_firm_sync.py`     | `sync_qbo_firms_to_vp`     | yes | yes | ✅ |
| `_tax_code_sync.py` | `sync_qbo_tax_codes_to_vp` | yes | yes | ✅ |

`_shared.py` routes some writes through
`S3UpdateCollectionOperator`, which funnels through the same
`get_or_create_s3_collection_artifact` chokepoint internally — so those writes
are locked too. No action required for any of them.

**Conclusion:** locking is taken care of. No code change is needed in the
helpers to *get* locking.

---

## 3. Why NOT migrate these to `S3*CollectionOperator`

These helpers are **batch** syncs: one download, many row mutations, one upload.
`_firm_sync.py` states the design intent explicitly:

> *"Single download → modify all → single upload. Doing per-record
> `S3UpsertCollectionOperator.execute` would round-trip the entire
> collections.db.gz N times."*

With the lock living at the `get_or_create` level, the whole batch is a **single
locked download/modify/upload unit** — which is exactly what you want.
Rewriting them as one `S3UpsertCollectionOperator`/`S3UpdateCollectionOperator`
call per record would acquire+release the lock (and re-download+re-upload the
full file) **once per row** — slower, and it churns the ETag for no benefit.

> Historical note: earlier guidance said "the operator is the canonical lock
> surface." That is now **superseded** — `get_or_create_s3_collection_artifact`
> is the lock surface, so raw batch callers are first-class and correctly
> locked. Keep the raw batch pattern for these helpers.

Do **not** "fix" anything by passing `use_lock=False` in these helpers — they
are writes and must stay locked. (`use_lock=False` exists only for genuinely
read-only paths, e.g. `S3QueryCollectionOperator` in `single-row` mode.)

---

## 4. Caveat 1 — botocore prerequisite (hard gate)

The lock uses **native conditional** `PutObject(IfNoneMatch)` /
`DeleteObject(IfMatch)`. A HeadObject-then-Put *precheck* fallback would be
non-atomic and would silently break the cross-system mutex, so
`rail.lib.s3_lock.acquire_s3_lock` **refuses to degrade**: it raises
`S3LockUnsupportedError` if the loaded botocore can't do the native conditional
ops.

**Implication:** because the helpers default to `use_lock=True`, they will
**fail loudly with `S3LockUnsupportedError`** on any worker whose botocore lacks
native conditional `PutObject`/`DeleteObject` (the AWS feature shipped ~botocore
**1.35.7**, Aug 2024).

### Where the floor is actually enforced
This is **not** governed by a `boto3>=…` line in the consuming repo's
`requirements*.txt` (there is none). RAIL ships as a wheel, and
`replicon-airflow-library/requirements.txt` pins **`boto3==1.37.0`**, which
becomes the wheel's `install_requires`. So every image that installs the RAIL
wheel pulls `boto3==1.37.0` (botocore ≈1.37) — comfortably above the ~1.35.7
conditional-write floor.

> Do not add the floor as a comment in `replicon-airflow-library/requirements.txt`:
> `rail/setup.py` reads that file with `f.read().splitlines()` straight into
> `install_requires`, so any `#`-comment line becomes an invalid requirement and
> breaks the wheel build. The exact pin is the enforcement; keep it ≥ 1.35.7.

### Confirmed state
| Deployment path | airflow | amazon provider | boto3/botocore | conditional ops |
|---|---|---|---|---|
| 2.11 / fips (current target) | 2.11.0 | 9.7.0 (pinned) | boto3 1.37.0 / botocore 1.37.38 | ✅ probe-confirmed |
| legacy 2.7.3 (`C:\Unionpoint\replicon-airflow-library`, `boto3>=1.21.2`) | 2.7.3 | resolves to 8.27.0 | resolves to **botocore 1.43.22** | ✅ resolve-confirmed |

Both paths clear the ~1.35.7 floor. The 2.7.3 result was verified by a
`pip install --dry-run` of its floor set — pip backtracks the amazon provider to
the newest version compatible with airflow 2.7.3 (8.27.0), which permits a
current boto3, landing **botocore 1.43.22**. No Airflow constraints file is used
in the build (verified), so nothing pins botocore back below the floor. And
because the lock code ships in a **new rail wheel**, deploying it requires a
rebuild — and any rebuild today resolves a conditional-capable botocore.

> Residual hardening (optional): the 2.7.3 floor is `boto3>=1.21.2`, which is
> *technically* low enough to permit an incapable botocore if a constraints file
> were ever added or the provider cap changed. Bumping it to `boto3>=1.35.7`
> would make the dependency floor itself enforce the requirement. The 2.11 repo
> already pins `boto3==1.37.0`, so it is covered.

### Action items
- [ ] (optional hardening) Bump `boto3>=1.21.2` → `boto3>=1.35.7` in the 2.7.3
      `replicon-airflow-library/requirements.txt` so the floor enforces
      conditional-write support directly (current resolution already lands
      1.43.22, so this is belt-and-suspenders).
- [ ] Verify on any target worker image:
      ```python
      import boto3, botocore; print(botocore.__version__)
      m = boto3.client('s3').meta.service_model
      print('IfNoneMatch' in m.operation_model('PutObject').input_shape.members)
      print('IfMatch'    in m.operation_model('DeleteObject').input_shape.members)
      ```
      Both must print `True`. (Confirmed `True` on botocore 1.37.38.)
- [ ] If a target image resolves below the floor, gate the rollout on the image
      version (or pin `boto3`/`botocore` in that image's requirements). **Do
      not** work around it with `use_lock=False` — that loses cross-system
      safety on a write path.

---

## 5. Caveat 2 — TTL must exceed the batch hold time

The lock's stale-break uses the **acquirer's** configured TTL (default **60 s**),
not the value stored in the lock body. The mapping-sync helpers hold the lock
for the **entire** mutate loop — e.g. `_firm_sync` runs its full per-record loop
between the artifact open and `conn.commit()`.

If a batch holds the lock **longer than the TTL**, another writer (the API, or a
task retry) can legitimately **stale-break the lock mid-batch** and start its own
mutation. The `S3CollectionConcurrencyError` ETag backstop will then catch the
resulting conflict at upload time — but that surfaces as an avoidable task
failure / retry, **and mutual exclusion was silently lost for the overlap**.

> Release no longer false-fails on this. `release_s3_lock` treats a
> `412 PreconditionFailed` on its `IfMatch` delete (the signature of a lock
> that was stale-broken and re-acquired) the same as a missing object: it
> swallows and logs `s3_lock_release_stale_broken` instead of raising. So a run
> whose body completed won't be reported as failed just because its lock was
> reclaimed. That is damage-limitation only — it does **not** restore the lost
> mutual exclusion. The real fix is still to size the TTL correctly below.

Both systems compute staleness from the same TTL, so they **must agree on it**:

| System | Setting | Default |
|---|---|---|
| airflow-integrations (RAIL) | Airflow Variable `collections_lock_ttl_seconds` | 60 |
| integration-platform-api | env `COLLECTIONS_LOCK_TTL_SECONDS` | 60 |

### Action items
- [ ] Measure the worst-case batch runtime (largest customer's firm / employee
      sync — these have the biggest record counts).
- [ ] Set **both** `collections_lock_ttl_seconds` (Airflow Variable) **and**
      `COLLECTIONS_LOCK_TTL_SECONDS` (API env) to a value comfortably **above**
      that worst case (e.g. 2–3× the p99 batch duration). Keep the two values
      equal.
- [ ] Re-tune if record volumes grow materially.

> Trade-off: a larger TTL means a *crashed* holder's lock takes longer to be
> reclaimed (the lock is only auto-broken after the TTL). Pick the smallest
> value that safely exceeds the longest legitimate batch.

---

## 6. Lock granularity & contention (informational)

The lock key is per **collection file**:
`{integration}/{integration_type}/{customer}/collections/collections.db.gz.lock`.

- If the four helpers resolve to the **same** `integration/integration_type/customer`,
  they share one lock object and **serialize against each other** when run
  concurrently (correct for safety; reduces parallelism).
- If they use **distinct** `integration_type`s, they write different files and
  do **not** contend with each other — but each still serializes against the API
  and against its own retries.

Either regime is safe. Confirm which applies (trace the `s3_integration_type`
each helper passes) only if you need it for capacity / parallelism planning.

---

## 7. References

- RAIL lock implementation: `replicon-airflow-library/rail/rail/lib/s3_lock.py`
  (`acquire_s3_lock`, `release_s3_lock`, `with_s3_lock`, `build_lock_key`).
- RAIL lock tests: `replicon-airflow-library/rail/rail/lib/s3_lock_test.py`
  (ports the API's `s3-lock.test.ts` 1:1 + timestamp/key interop tests).
- Chokepoint wiring: `rail.lib.s3_collection.get_or_create_s3_collection_artifact`
  (`use_lock`, `lock_ttl_seconds` params).
- API counterpart (source of truth for the protocol):
  `integration-platform-api/src/lib/clients/s3-lock.ts` and
  `src/lib/features/collections/{service,s3}.ts`.
- Related: `MAPPING_SYNC_CLEANUP_AND_PERF.md`, `LOOKUP_TABLE_FLOWS.md`,
  the per-sync `MAP_*_SYNC_FIX_LOG.md` files.
