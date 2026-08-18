"""
Employee mapping sync (sections L+M+N from the pre-split file).

QBO Employee → VP Employee + paired QBO Vendor for expense
processing. See section banner comments below and
MAP_EMPLOYEE_SYNC_FIX_LOG.md for the per-row fix history.

Public surface (re-exported via `python_callable_method.py`):
    sync_qbo_employees_to_vp
"""
import logging

import rail
from airflow.models import Variable

# Shared helpers still live in `python_callable_method.py` during the
# staged split.
from vp_quickbooks_integration.mapping_sync.utils._shared import (
    _extract_qbo_entity_id,
    _extract_qbo_records,
    _filter_none,
    _resolve_cfg_then_variable,
)
from vp_quickbooks_integration.common.tables import (
    MAP_EMPLOYEE_TABLE_NAME,
    MAP_EMPLOYEE_UNIQUE_COLUMNS,
)
from vp_quickbooks_integration.mapping_sync.config import IntegrationConfig

_log = logging.getLogger(__name__)


# ===========================================================================
# EMPLOYEE MAPPING — schema
# ===========================================================================
# MAP_EMPLOYEE_TABLE_NAME + MAP_EMPLOYEE_COLUMNS in utils/tables.py.


# ===========================================================================
# PER-TENANT LOOKUPS for employee mapping (Airflow Variables)
# ===========================================================================

def lookup_default_employee_labor_type(instance):
    """Default `Type` (labor type) for new VP employees (Workato
    `014_503_PSA_CFG_DefaultEmployeeLaborType`).

    Resolution: CFG_DefaultEmployeeLaborType → Variable → None.
    """
    return _resolve_cfg_then_variable(
        'CFG_DefaultEmployeeLaborType',
        f'vp_qbo_mapping_sync_default_employee_labor_type_{instance}',
    )


def lookup_default_organization(instance):
    """Default `Org` for new VP employees when QBO Department is empty or
    when department-to-org get-or-create is not configured (Workato
    `014_503_PSA_CFG_DefaultOrganization`).

    Resolution: CFG_DefaultOrganization → Variable → None. Middleware
    does not currently ship this CFG key; the VP-side fallback
    (`_fetch_first_vp_organization_org`) covers the None case for the
    employee create body. See CFG_MIGRATION.md.

    NOTE: The Workato recipe also supports a per-department org get-or-create
    against `/api/organizations`. v1 of this port uses only this Variable's
    value — every employee without a matching org in the Variable gets the
    default. Per-department get-or-create can be added later if a tenant
    needs it (TODO during real-data testing).
    """
    return _resolve_cfg_then_variable(
        'CFG_DefaultOrganization',
        f'vp_qbo_mapping_sync_default_organization_{instance}',
    )


def _fetch_first_vp_organization_org(vp_conn_id, context):
    """GET /api/organization, return the first row's `Org` value, or None.

    Mirrors recipe `014_503_psa_vantagepoint_upsert_employee.recipe.json`
    line 1010 / 2194 fallback expression
    `_('data.deltek_vantagepoint_connector.82b250d3.organizations.first.Org')`.
    Used when `lookup_default_organization(instance)` is unset for the
    tenant — VP otherwise rejects POST/PUT /employee with
    `Organization is required`.

    Caller is responsible for caching this. VP would otherwise be queried
    once per employee in the bulk sync.
    """
    from rail import VantagepointAPIOperator  # pylint: disable=import-outside-toplevel
    try:
        result = VantagepointAPIOperator(
            task_id='_fetch_vp_first_organization',
            vp_conn_id=vp_conn_id,
            endpoint='/organization',
            request_method='GET',
            pagination=False,
        ).execute(context)
    except Exception as exc:  # pylint: disable=broad-exception-caught
        import logging  # pylint: disable=import-outside-toplevel
        logging.getLogger(__name__).warning(
            "GET /api/organization failed: %s — Org fallback unavailable, "
            "POST /employee may fail with 'Organization is required'.",
            exc,
        )
        return None

    if isinstance(result, list) and result:
        first = result[0] if isinstance(result[0], dict) else {}
    elif isinstance(result, dict):
        first = result
    else:
        first = {}
    return first.get('Org') or None


# ===========================================================================
# EMPLOYEE MAPPING — body builders (QBO Employee → VP Employee + QBO Vendor)
# Recipe references:
#   014_503_psa_synch_employees.recipe.json
#   014_503_psa_vantagepoint_upsert_employee.recipe.json
# ===========================================================================

def _format_vp_date(date_string):
    """Format an ISO-ish date string for VP (YYYY-MM-DD). None on failure."""
    if not date_string:
        return None
    try:
        from datetime import datetime  # pylint: disable=import-outside-toplevel
        if 'T' in date_string:
            return datetime.fromisoformat(
                date_string.replace('Z', '+00:00')
            ).strftime('%Y-%m-%d')
        return datetime.strptime(date_string, '%Y-%m-%d').strftime('%Y-%m-%d')
    except (ValueError, TypeError):
        return None


def _employee_display_name(qbo_employee):
    """Best-effort display name for a QBO employee record."""
    if qbo_employee.get('DisplayName'):
        return qbo_employee['DisplayName']
    parts = [
        (qbo_employee.get('GivenName') or '').strip(),
        (qbo_employee.get('FamilyName') or '').strip(),
    ]
    return ' '.join(p for p in parts if p) or ''


def _employee_expense_vendor_name(qbo_employee):
    """Convention from the Phase-3 Step-2 doc: '<DisplayName> (Employee)'."""
    display = _employee_display_name(qbo_employee)
    if not display:
        return None
    return f"{display} (Employee)"


def build_qbo_expense_vendor_body(qbo_employee):
    """Build POST /vendor body — used to create the QBO vendor that
    represents an employee for expense-reimbursement processing.

    Mirrors `create_employee_vendor` in PHASE_3_STEP_2_EMPLOYEE_SYNC.
    Vendor1099 is hardcoded False (the doc's convention for employee vendors).
    """
    vendor_name = _employee_expense_vendor_name(qbo_employee)
    if not vendor_name:
        return None

    primary_addr = qbo_employee.get('PrimaryAddr') or {}
    primary_email = qbo_employee.get('PrimaryEmailAddr') or {}
    primary_phone = qbo_employee.get('PrimaryPhone') or {}

    body = {
        'DisplayName': vendor_name,
        'CompanyName': qbo_employee.get('CompanyName'),
        'GivenName': qbo_employee.get('GivenName'),
        'FamilyName': qbo_employee.get('FamilyName'),
        'Active': bool(qbo_employee.get('Active', True)),
        'Vendor1099': False,
    }
    if primary_email.get('Address'):
        body['PrimaryEmailAddr'] = {'Address': primary_email['Address']}
    if primary_phone.get('FreeFormNumber'):
        body['PrimaryPhone'] = {
            'FreeFormNumber': primary_phone['FreeFormNumber']}
    if any(primary_addr.get(f) for f in ('Line1', 'Line2', 'City')):
        body['BillAddr'] = _filter_none({
            'Line1': primary_addr.get('Line1'),
            'Line2': primary_addr.get('Line2'),
            'Line3': primary_addr.get('Line3'),
            'City': primary_addr.get('City'),
            'CountrySubDivisionCode': primary_addr.get('CountrySubDivisionCode'),
            'PostalCode': primary_addr.get('PostalCode'),
            'Country': primary_addr.get('Country'),
        })
    return _filter_none(body)


def build_vp_employee_create_body_from_qbo(qbo_employee, instance, vp_default_org):
    """POST /employee body. Strict parity with Workato `synch_employees` →
    `upsert_employee`:

      - `synch_employees` (recipe lines 823-828) passes only
        FirstName / MiddleName / LastName / EmplQBOID to upsert_employee.
        MiddleName is accepted as a parameter but never mapped to the VP
        POST body (recipe POST mapping at lines 1006-1034 has no
        MiddleName field), so we don't send it either.
      - `Org` falls back to the first VP organization when the Airflow
        Variable is unset (recipe expression at line 1010 — see
        `_fetch_first_vp_organization_org`).
      - `Type` reads from the Airflow Variable equivalent of Workato's
        account property `CFG_DefaultEmployeeLaborType` (recipe line 1018).
      - `ReadyForProcessing` and `ReadyForApproval` are hardcoded `"true"`
        (recipe lines 1008-1009).
      - `HomeCompany` and `EmployeeCompany` are sent as empty strings —
        recipe has no VP-side fallback for either (lines 1011-1012),
        defaulting to blank when no param is passed.
      - Everything else the recipe defines (HireDate, addresses, phones,
        EMail, Salutation, Suffix, OrganizationName, TerminationDate) is
        NOT sent because `synch_employees` doesn't pass them — they'd
        resolve to blank/null on the recipe side. Initial mapping sync
        intentionally creates VP employees with name + ids + defaults
        only; richer data flows via the per-employee polling recipe
        `014_503_psa_employee_upserted_in_vantagepoint` in Workato,
        which would be a separate trigger DAG here if/when needed.

    `vp_default_org` is the pre-fetched fallback from
    `_fetch_first_vp_organization_org`. Caller fetches once and passes in.
    """
    qbo_id = qbo_employee.get('Id')
    return {
        'ReadyForProcessing': 'true',
        'ReadyForApproval': 'true',
        'Org': lookup_default_organization(instance) or vp_default_org or '',
        'HomeCompany': '',
        'EmployeeCompany': '',
        'EMail': '',
        'LastName': qbo_employee.get('FamilyName'),
        'FirstName': qbo_employee.get('GivenName'),
        'Employee': qbo_id,
        'QBOID': qbo_id,
        'Type': lookup_default_employee_labor_type(instance) or '',
        'Status': 'A',
    }


def build_vp_employee_update_body_from_qbo(qbo_employee, instance, vp_default_org):
    """PUT /employee/{Employee} body. Strict parity with Workato
    `upsert_employee` PUT branch (recipe lines 2194-2215). Excludes
    `QBOID` and `Type` (create-time only).

    VP rejects PUT with empty `HomeCompany` / `EmployeeCompany` on an
    existing employee ('Please provide a Employee Company Name for
    table Employees'). The Workato recipe's `.presence || blank`
    expression at lines 2195-2197 OMITS these fields on PUT when no
    param is passed (the `blank` literal returns nil → Workato's HTTP
    layer drops the key), so VP preserves the existing values. POST
    on a fresh employee accepts `""` for the same fields. We mirror
    by dropping empty strings here. See MAP_EMPLOYEE_SYNC_FIX_LOG.md
    #8.
    """
    body = build_vp_employee_create_body_from_qbo(
        qbo_employee, instance, vp_default_org,
    )
    body.pop('QBOID', None)
    body.pop('Type', None)
    for field in ('HomeCompany', 'EmployeeCompany', 'EMail'):
        if body.get(field) == '':
            body.pop(field)
    return body


# ===========================================================================
# EMPLOYEE MAPPING — sync engine (called by map_employee_dag PythonOperator)
# ===========================================================================

def _index_qbo_vendors_by_display_name(qbo_vendors):
    """Build a lowercased-DisplayName → vendor dict for O(1) expense-vendor
    lookups inside the per-employee loop. Case-insensitive to tolerate any
    capitalization drift.
    """
    index = {}
    for vendor in qbo_vendors or []:
        name = (vendor.get('DisplayName') or '').strip().lower()
        if name:
            index[name] = vendor
    return index


def _load_vp_employees_by_qboid(vp_conn_id, context):
    """Bulk-load every VP employee and index by QBOID.

    Called once at the top of `sync_qbo_employees_to_vp`. Replaces what
    was a per-record `_find_vp_employee_by_qbo_id` GET — for a tenant
    with N QBO employees the old shape issued N sequential VP API
    calls; this one paginated call serves all N in-memory lookups.

    Why we need it: map_employee starts empty on first run, but the VP
    tenant may already have employees with QBOID populated (from prior
    Workato runs, manual imports, etc.). If we POST /employee blindly
    with the QBO Id as the VP `Employee` primary key, VP returns
    'Record 00000|<id> already exists and cannot be added'. Pre-
    resolving lets us route those records to the PUT update path with
    the existing VP `Employee` key.

    Mirrors `_load_vp_firms_by_qboid` for firms. The Workato equivalent
    is the bulk pre-population step in
    `014_503_psa_map_employees.recipe.json` (custom_action
    `GET vision/QuickBooks/Employees`); the Airflow port uses the
    generic VantagepointEmployeeOperator paginated GET to avoid a
    custom-endpoint operator dependency. See MAP_EMPLOYEE_SYNC_FIX_LOG.md #7.

    VP employees with no QBOID are skipped at index time (they can't
    contribute to QBOID-keyed lookups).

    Returns: `dict[qboid_str, employee_dict]`.
    """
    from rail import VantagepointEmployeeOperator  # pylint: disable=import-outside-toplevel

    result = VantagepointEmployeeOperator(
        task_id='_bulk_get_vp_employees_for_qboid_index',
        vp_conn_id=vp_conn_id,
        request_method='GET',
        pagination=True,
    ).execute(context)

    if isinstance(result, dict):
        records = [result]
    elif isinstance(result, list):
        records = [r for r in result if isinstance(r, dict)]
    else:
        return {}

    index = {}
    for record in records:
        qboid = record.get('QBOID')
        if not qboid:
            continue
        index[str(qboid)] = record
    return index


def _load_existing_map_employee_index(cur):
    """Read existing map_employee rows from the open sqlite cursor, indexed
    by QBOID.
    """
    cur.execute(
        f'SELECT Employee, QBOID, QBOVendorID, QBOVendorName, Name '
        f'FROM {MAP_EMPLOYEE_TABLE_NAME}'
    )
    index = {}
    for emp_id, qbo_id, qbo_vendor_id, qbo_vendor_name, name in cur.fetchall():
        if qbo_id:
            index[str(qbo_id)] = {
                'Employee': emp_id,
                'QBOID': qbo_id,
                'QBOVendorID': qbo_vendor_id,
                'QBOVendorName': qbo_vendor_name,
                'Name': name,
            }
    return index


def _build_map_employee_row(vp_employee, qbo_id, qbo_vendor_id,
                            qbo_vendor_name, display_name):
    """Assemble one map_employee row dict for the batched upsert.

    Keys cover every column in MAP_EMPLOYEE_COLUMNS (the upsert operator
    builds its ON CONFLICT statement from the first row's keys, so all rows
    must share this exact column set). The natural key is QBOID — see
    MAP_EMPLOYEE_UNIQUE_COLUMNS, declared as a UNIQUE index by
    `dispatcher_dag.init_mapping_collections` — so a re-sync of the same QBO
    employee replaces its row in place (Employee / vendor / Name refreshed via
    ON CONFLICT DO UPDATE) instead of stacking a duplicate. Mirrors the
    map_firm analog (`_build_map_firm_row`).
    """
    return {
        'Employee': vp_employee,
        'QBOID': qbo_id,
        'QBOVendorID': qbo_vendor_id,
        'QBOVendorName': qbo_vendor_name,
        'Name': display_name,
    }


def _extract_vp_employee_id(vp_employee_response):
    """Pull `Employee` (the VP primary key) from a VantagepointFirmOperator
    response (list or dict)."""
    if isinstance(vp_employee_response, list) and vp_employee_response:
        return (vp_employee_response[0] or {}).get('Employee')
    if isinstance(vp_employee_response, dict):
        return vp_employee_response.get('Employee')
    return None


def sync_qbo_employees_to_vp(instance):
    """Forward sync (QBO Employee → VP Employee).

    For each active QBO Employee:
      1. Find-or-create a QBO Vendor named "<DisplayName> (Employee)" used
         for expense reimbursement (Workato `vendor_association` step).
      2. Look up an existing map_employee row by QBO Employee Id.
         - If found → PUT /employee/{Employee} to refresh editable fields.
         - If not   → POST /employee, capture the returned `Employee` field,
                      record the cross-reference in map_employee.

    Reads:
      - rail.result('fetch_qbo_employees') — list of QBO Employee records
      - rail.result('fetch_qbo_vendors')   — list of QBO Vendor records
        (used as the case-insensitive name index for the expense-vendor
        find-or-create)

    Inactive QBO employees are skipped (recipe parity — VPA-only
    subcontractors and inactive records are excluded from sync).

    Returns a summary dict; raises at the end if any per-record failure
    occurred so the dag's catch_*_dag_error fires.
    """
    import sqlite3  # pylint: disable=import-outside-toplevel
    import rail.lib.s3_collection  # pylint: disable=import-outside-toplevel
    from rail import (  # pylint: disable=import-outside-toplevel
        S3UpsertCollectionOperator,
        VantagepointEmployeeOperator,
        QuickBooksVendorOperator,
    )

    context = rail.get_current_context()
    log = context['task_instance'].log

    employees = _extract_qbo_records(rail.result('fetch_qbo_employees'))
    vendor_index = _index_qbo_vendors_by_display_name(
        _extract_qbo_records(rail.result('fetch_qbo_vendors'))
    )
    log.info(
        "Processing %d QBO employees (%d existing QBO vendors indexed)",
        len(employees), len(vendor_index),
    )

    conn_ids = IntegrationConfig.get_conn_ids(context)
    vp_conn_id = conn_ids['vp_conn_id']
    intuit_conn_id = conn_ids['intuit_conn_id']

    s3_integration = IntegrationConfig.S3_INTEGRATION_NAME
    s3_customer = IntegrationConfig.get_s3_customer(context)
    s3_integration_type = IntegrationConfig.get_s3_integration_type(context)
    s3_artifact_name = rail.lib.s3_collection.get_s3_collection_artifact_name(
        context, s3_integration, s3_customer, s3_integration_type
    )

    # Org fallback: recipe `014_503_psa_vantagepoint_upsert_employee`
    # queries GET /api/organization and uses the first row's `Org` when
    # no Organization parameter is passed (always the case in
    # synch_employees → upsert_employee). Fetch once and cache locally
    # to avoid N queries in the bulk loop.
    vp_default_org = _fetch_first_vp_organization_org(vp_conn_id, context)
    log.info(
        "Org fallback for employee POST/PUT: %s",
        vp_default_org or "<unavailable — POST/PUT may fail if "
        "vp_qbo_mapping_sync_default_organization_<instance> is also unset>",
    )

    summary = {
        'created': 0,
        'updated': 0,
        'backfilled_from_vp': 0,
        'skipped_inactive': 0,
        'skipped_employee_id_too_long': 0,
        'errors': [],
    }

    # ---- Phase 0: load existing map_employee index (no S3 lock) ----
    # Read-only snapshot used purely for the find-or-create decision. Opened
    # with use_lock=False so the HTTP work in Phase 1 never holds the S3
    # collection lock (mirrors read_map_tax_code_for_staging / the firm sync).
    # The read is closed before any QBO/VP round-trip; the keyed upsert in
    # Phase 2 makes the final write idempotent even if map_employee changed in
    # the interim.
    #
    # Why the raw artifact read and NOT S3QueryCollectionOperator: we need
    # EVERY map_employee row materialized into an in-memory dict for branching
    # here. The operator's 'single-row' mode returns only 0/1 row, and its
    # 'dataset' mode opens the artifact with use_lock=True (it writes the
    # result back as a new collection table) and returns a collection-name
    # reference rather than the rows — i.e. it would re-introduce the very lock
    # this phase removes AND force a second read to materialize the rows. The
    # lock-free raw read is the correct tool (and the codebase convention for
    # "read existing map into Python"); writes still go through the canonical
    # S3UpsertCollectionOperator in Phase 2.
    existing_map = {}
    with rail.lib.s3_collection.get_or_create_s3_collection_artifact(
        s3_artifact_name, s3_integration, s3_customer, context,
        integration_type=s3_integration_type, use_lock=False,
    ) as artifact:
        with sqlite3.connect(artifact.local_filename) as conn:
            existing_map = _load_existing_map_employee_index(conn.cursor())

    # ---- Phase 1: all API work, accumulate map rows in memory ----
    # One bulk GET serves all per-record QBOID lookups for this run (replaces
    # N sequential VP GETs with one paginated call indexed in memory). Nothing
    # touches S3 here, so the collection lock is NOT held across the QBO/VP
    # POST/PUT round-trips.
    vp_employees_by_qboid = _load_vp_employees_by_qboid(vp_conn_id, context)
    log.info(
        "Loaded %d VP employees with QBOID for in-memory lookup",
        len(vp_employees_by_qboid),
    )

    map_rows = []

    for qbo_employee in employees:
        qbo_id = qbo_employee.get('Id')
        display_name = _employee_display_name(qbo_employee)

        if not qbo_id:
            summary['errors'].append({
                'qbo_id': None,
                'name': display_name,
                'error': 'QBO employee record has no Id field',
            })
            continue

        if not qbo_employee.get('Active', True):
            log.info("Skipping inactive QBO employee %s (%s)",
                     qbo_id, display_name)
            summary['skipped_inactive'] += 1
            continue

        # VP's `Employee` primary-key field has a tenant-enforced
        # max length (VP error: 'The correct format is XXXX').
        # When the QBO Id is longer, VP rejects the POST. We
        # could omit `Employee` and let VP autonumber, but that
        # assumes autonumber is enabled tenant-side. Conservative
        # choice: skip with a warning so the task succeeds and
        # the affected records can be triaged out-of-band.
        # See MAP_EMPLOYEE_SYNC_FIX_LOG.md #2 for alternatives.
        if len(str(qbo_id)) > 4:
            log.warning(
                "QBO employee %s (%s): Id length %d exceeds "
                "VP Employee field max (4 chars); skipping.",
                qbo_id, display_name, len(str(qbo_id)),
            )
            summary['skipped_employee_id_too_long'] += 1
            continue

        try:
            # ---- Step 1: find or create QBO Vendor for expenses ----
            expense_vendor_name = _employee_expense_vendor_name(
                qbo_employee)
            qbo_vendor_id = None
            qbo_vendor_name = expense_vendor_name

            existing_vendor = (
                vendor_index.get(expense_vendor_name.strip().lower())
                if expense_vendor_name else None
            )
            if existing_vendor:
                qbo_vendor_id = existing_vendor.get('Id')
            else:
                vendor_body = build_qbo_expense_vendor_body(
                    qbo_employee)
                if vendor_body:
                    vendor_result = QuickBooksVendorOperator(
                        task_id=f'_qbo_create_expense_vendor_{qbo_id}',
                        intuit_conn_id=intuit_conn_id,
                        operation='create',
                        request_body=vendor_body,
                    ).execute(context)
                    qbo_vendor_id = _extract_qbo_entity_id(
                        vendor_result, 'Vendor'
                    )
                    if qbo_vendor_id:
                        # Cache the new vendor in the in-memory index
                        # in case two employees share a vendor name.
                        vendor_index[expense_vendor_name.strip().lower()] = {
                            'Id': qbo_vendor_id,
                            'DisplayName': expense_vendor_name,
                        }

            # ---- Step 2: create or update VP Employee ----
            existing = existing_map.get(str(qbo_id))

            # map_employee may be empty on first run, but VP may
            # already have an employee with this QBOID (from prior
            # Workato runs, manual imports, etc.). Look in the
            # bulk VP-by-QBOID index; on a hit, route to PUT and
            # backfill the map. See MAP_EMPLOYEE_SYNC_FIX_LOG.md
            # #7 and the firm analog (`_load_vp_firms_by_qboid`).
            if not (existing and existing.get('Employee')):
                vp_existing = vp_employees_by_qboid.get(str(qbo_id))
                if vp_existing and vp_existing.get('Employee'):
                    existing = {
                        'Employee': vp_existing['Employee'],
                        'QBOID': str(qbo_id),
                    }
                    summary['backfilled_from_vp'] += 1

            if existing and existing.get('Employee'):
                vp_employee_id = existing['Employee']
                update_body = build_vp_employee_update_body_from_qbo(
                    qbo_employee, instance, vp_default_org,
                )
                VantagepointEmployeeOperator(
                    task_id=f'_put_employee_{qbo_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='PUT',
                    employee=vp_employee_id,
                    request_body=update_body,
                    pagination=False,
                ).execute(context)
                summary['updated'] += 1
            else:
                create_body = build_vp_employee_create_body_from_qbo(
                    qbo_employee, instance, vp_default_org,
                )
                create_result = VantagepointEmployeeOperator(
                    task_id=f'_post_employee_{qbo_id}',
                    vp_conn_id=vp_conn_id,
                    request_method='POST',
                    request_body=create_body,
                    pagination=False,
                ).execute(context)
                vp_employee_id = _extract_vp_employee_id(create_result)
                if not vp_employee_id:
                    # Fallback: if VP autonumbered the Employee field
                    # and we sent QBO Id, the response may not echo
                    # it back. Use the value we sent (matches doc
                    # convention `Employee = QBO Id`).
                    vp_employee_id = qbo_id
                summary['created'] += 1

            # ---- Step 3: accumulate map_employee row (written in Phase 2) ----
            map_rows.append(_build_map_employee_row(
                vp_employee_id, str(qbo_id),
                qbo_vendor_id, qbo_vendor_name, display_name,
            ))

        except Exception as exc:  # pylint: disable=broad-exception-caught
            log.error(
                "Failed to sync QBO employee %s (%s): %s",
                qbo_id, display_name, exc,
            )
            summary['errors'].append({
                'qbo_id': qbo_id,
                'name': display_name,
                'error': str(exc),
            })

    # ---- Phase 2: single batched upsert (one S3 lock cycle) ----
    # All accumulated rows go up in ONE download/modify/upload/lock cycle via
    # the canonical S3 collection operator, keyed on QBOID. The old shape held
    # the collection open and locked across every QBO/VP HTTP call; this
    # confines the lock to the batched write.
    if map_rows:
        S3UpsertCollectionOperator(
            task_id='_upsert_map_employee',
            integration=s3_integration,
            customer=s3_customer,
            integration_type=s3_integration_type,
            collection_name=MAP_EMPLOYEE_TABLE_NAME,
            key_columns=MAP_EMPLOYEE_UNIQUE_COLUMNS,
            rows=map_rows,
        ).execute(context)
        log.info("Upserted %d map_employee row(s) in one S3 cycle.",
                 len(map_rows))
    else:
        log.info("No map_employee rows to upsert.")

    log.info("map_employee sync summary: %s", summary)
    if summary['errors']:
        raise RuntimeError(
            f"map_employee sync had {len(summary['errors'])} failure(s); "
            f"first: {summary['errors'][0]}"
        )
    return summary

