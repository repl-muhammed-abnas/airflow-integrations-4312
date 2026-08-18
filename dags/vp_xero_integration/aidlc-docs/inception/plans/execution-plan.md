# Execution Plan — Vantagepoint ↔ Xero Initial Mapping Sync

**Unit of work:** `airflow-integrations/dags/vp_xero_integration/mapping_sync` (+ shared `common/`), built by mirroring the QuickBooks `vp_quickbooks_integration/mapping_sync` reference. Plus an enabling dependency in `replicon-airflow-library` (RAIL Xero pagination).

**Inputs:** reverse-engineering parity docs [00–08](../reverse-engineering/xero-mapping-sync/README.md); user stories [stories.md](../stories.md) (US-0…US-6, US-8, US-9; US-7 descoped); decisions [user-stories-questions.md](../user-stories-questions.md).

---

## 1. Scope & risk summary

| Dimension | Assessment |
| --- | --- |
| **Type** | Brownfield port; greenfield package built from a **proven template** (QBO `mapping_sync`). |
| **Scope** | 3 mapping domains (firm, account, tax) + dispatcher/init + validation + docs; **employee descoped** (Q1). |
| **Complexity** | Medium-high in the engines (compile SQL, name-matching, tax component fan-out + compound linking, idempotent upserts); low in scaffolding (clone QBO). |
| **Key risks** | (R1) RAIL pagination gap (G1) blocks firm list reads — **external dependency on RAIL team**. (R2) Correctness of matching/compile logic vs Workato. (R3) Multi-tenant isolation + idempotent re-runs. (R4) Reproducing Workato bugs by accident (fix list in Q9). (R5) Xero API rate limits on large tenants. |
| **Mitigations** | Template reuse; parity docs as the spec; adversarial validation (US-6); fix-logs in `doc/`; sequence US-9/G1 first. |

**Risk level: MEDIUM.** Warrants functional design + light NFR work; heavy infra design not needed (reuses existing platform).

---

## 2. Default decisions applied (Q4–Q10 — user skipped; recommended options taken)

| Q | Decision applied |
| --- | --- |
| Q4 firm matching | **A** — match by Name with `MIN(ClientID)` (Workato parity). (Persisting ContactID into VP is a future enhancement.) |
| Q5 validation/self-heal | **A** — validation read-only/reporting; self-heal + archived-cleanup live in the sync engines; signal failures via `mapping_table_state.Status='Error'` + dispatcher hard-fail. |
| Q6 orphan deactivation | **A** — scope deactivation to previously-Xero-sourced VP accounts (don't deactivate manually-created VP accounts). |
| Q7 map_account_type | **A** — seeded S3 collection (data-driven, seeded by `init_mapping_collections`). |
| Q8 logging | **A** — RAIL log operators + `PostDagRunDetailsToMiddlewareApiOperator`; no `log` collection. |
| Q9 Workato bugs | **A** — fix all identified bugs in the port; document each in the per-table fix-log. |
| Q10 pagination | **A** — page all Xero list calls (depends on US-9/G1). |

> These can be revisited; they are recorded here as the working baseline for construction.

---

## 3. Units of work (proposed → confirmed in Units Generation)

| Unit | Stories | Description | Depends on |
| --- | --- | --- | --- |
| **U0 — RAIL Xero pagination** | US-9 (G1) | Add page-looping to `XeroAPIOperator` (RAIL). G2–G4 deferred. | — (RAIL team) |
| **U1 — Foundation + orchestration** | US-0, US-1, US-2 | Package scaffold, `common/tables.py`+config, `main_dag`, `dispatcher_dag`, init/state/premapping. | U0 (for later reads) |
| **U2 — Firm mapping** | US-3 | `map_firm_dag` + `_firm_sync.py` (seed-merged engine). | U1, **U0/G1** |
| **U3 — Account mapping** | US-4 | `map_account_code_dag` + `_account_sync.py`. | U1 |
| **U4 — Tax mapping** | US-5 | `map_tax_code_dag` + `_tax_code_sync.py` (fan-out + compound). | U1 |
| **U5 — Validation** | US-6 | `validate_mappings_dag` + `_validate.py`. | U2, U3, U4 |
| **U6 — Docs** | US-8 | `mapping_sync/doc/` parity. | U1 |

**Critical path:** U0 (G1) → U1 → U2 → U5. U3/U4 parallel to U2 after U1.

---

## 4. Stage execution plan

### Inception (remaining)

| Stage | Decision | Rationale |
| --- | --- | --- |
| **Application Design** (Stage 6) | **SKIP (light — folded in)** | Component/file/dependency design is already fully determined and documented in [00-architecture-parity.md](../reverse-engineering/xero-mapping-sync/00-architecture-parity.md) (folder layout, file responsibilities, RAIL operators, DAG topology). No new architecture to invent. Any residual component detail is captured per-unit in Functional Design. |
| **Units Generation** (Stage 7) | **EXECUTE** | The system decomposes into the 6+1 units above with real dependencies and a critical path; worth formalizing (`unit-of-work.md`, `unit-of-work-dependency.md`, `unit-of-work-story-map.md`). |

### Construction (per unit)

| Stage | U0 RAIL | U1 Found/Orch | U2 Firm | U3 Account | U4 Tax | U5 Validation | U6 Docs |
| --- | --- | --- | --- | --- | --- | --- | --- |
| **Functional Design** (COND) | SKIP¹ | EXECUTE (light) | **EXECUTE** | **EXECUTE** | **EXECUTE** | EXECUTE (light) | SKIP |
| **NFR Requirements** (COND) | EXECUTE (light)² | EXECUTE (light)³ | SKIP⁴ | SKIP⁴ | SKIP⁴ | SKIP⁴ | SKIP |
| **NFR Design** (COND) | SKIP | EXECUTE (light)³ | SKIP | SKIP | SKIP | SKIP | SKIP |
| **Infrastructure Design** (COND) | SKIP⁵ | SKIP⁵ | SKIP⁵ | SKIP⁵ | SKIP⁵ | SKIP⁵ | SKIP |
| **Code Generation** (ALWAYS) | EXECUTE | EXECUTE | EXECUTE | EXECUTE | EXECUTE | EXECUTE | EXECUTE |

Footnotes:
1. **U0 Functional Design SKIP** — small, well-defined change to one RAIL operator (add pagination loop); spec is in [08 §3 G1](../reverse-engineering/xero-mapping-sync/08-xero-api-inventory.md). Goes straight to code gen with tests.
2. **U0 NFR (light)** — pagination must handle rate limits (429 already handled) and large result sets; capture as acceptance criteria.
3. **U1 NFR (light)** — the cross-cutting NFRs live at the orchestration layer: multi-tenant isolation (per-customer S3 partition), idempotent re-runs (init gate + UNIQUE upserts), per-step state, schedule/concurrency (`max_active_runs`), Xero rate-limit handling. Documented once for U1 and inherited by U2–U5.
4. **U2–U5 NFR SKIP** — inherit U1's NFRs; no per-engine NFR work needed.
5. **Infrastructure Design SKIP (all)** — infra is **inherited from the existing platform**: Airflow, S3-backed SQLite collections, `vantagepoint_default`/`xero_default` connections, middleware OAuth, per-instance Variables. No new infra; the reused infra is listed in U1's functional design. (Override to EXECUTE only if a new Xero connection/secret provisioning workflow is required.)

### Build & Test (ALWAYS, whole unit)
**EXECUTE** — `build-instructions.md`, `unit-test-instructions.md`, `build-and-test-summary.md`. Emphasis on: engine unit tests (matching/compile SQL, tax fan-out + compound, idempotent upsert), pagination test (>1 page), and a dispatcher integration test (init → children → validation → ready).

---

## 5. Recommended construction sequence

```
1. U0  RAIL Xero pagination (G1)        [RAIL team — unblocks U2]
2. U1  Foundation + dispatcher           [scaffold from QBO]
3. U2  Firm   ─┐
   U3  Account ┤ (U3/U4 parallel after U1; U2 also needs U0/G1)
   U4  Tax    ─┘
4. U5  Validation                        [after U2–U4]
5. U6  Docs                              [alongside, finalize at end]
6. Build & Test                          [per unit + end-to-end]
```

**Hard external gate:** U2 (firm) cannot complete reads correctly until **U0/G1 (RAIL pagination)** lands. U3/U4 (account/tax lists are small) can proceed in parallel without it, but should still adopt pagination once available.

---

## 6. What's NOT in scope (explicit)
- Employee mapping (US-7, Q1).
- GL transaction flows (invoices, vouchers, payments, journals) and the **PUT** Xero calls (RAIL G2) — future efforts.
- Xero polling triggers (RAIL G4) — future.
- Currency mapping (`sync_currency_codes`) — GL, not in the firm/account/tax trio.
