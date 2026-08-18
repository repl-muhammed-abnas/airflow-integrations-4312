"""
Response filters for the Azenta Oracle -> Polaris project sync (F1013).

Parse Oracle Fusion REST responses and flatten planned-hours rows
(financialProjectPlans ResourceAssignments, or the projectBudgets fallback)
into a normalised assignment shape consumed by custom_methods + request_payload.
"""


def _items(response):
    """Return the 'items' list from an Oracle onlyData list response (tolerant)."""
    if not response:
        return []
    if isinstance(response, list):
        return response
    return response.get('items', []) or []


def pick_financial_plan_version_id(response):
    """Pick the financial project plan version id (prefer a 'Current' status)."""
    items = _items(response)
    if not items:
        return None
    for item in items:
        if 'CURRENT' in (item.get('PlanVersionStatus') or '').upper():
            return str(item['PlanVersionId'])
    return str(items[0]['PlanVersionId'])


def pick_budget_version_id(response):
    """Pick the 'Current Working' budget version id (fallback hours source)."""
    items = _items(response)
    return str(items[0]['PlanVersionId']) if items else None


def _planning_amounts(row):
    """Normalise the PlanningAmounts sub-collection to a list of dicts."""
    amounts = row.get('PlanningAmounts') or []
    if isinstance(amounts, dict):
        amounts = amounts.get('items', []) or []
    return amounts


def _planning_row_to_assignment(row):
    """Flatten one ResourceAssignment/PlanningResource row to a normalised assignment."""
    amounts = _planning_amounts(row)
    first = amounts[0] if amounts else {}

    # Project Plan uses PlannedQuantity; Budget uses Quantity.
    qty = first.get('PlannedQuantity')
    if qty is None:
        qty = first.get('Quantity')

    end_date = (
        row.get('PlanningFinishDate')
        or row.get('PlanningEndDate')
        or row.get('FinishDate')
    )

    return {
        'task_id': row.get('TaskId'),
        'task_number': row.get('TaskNumber'),
        'task_name': row.get('TaskName'),
        'resource_name': (row.get('ResourceName') or '').strip(),
        'rbs_element_id': row.get('RbsElementId'),
        'start_date': row.get('PlanningStartDate'),
        'end_date': end_date,
        'planned_qty': float(qty) if qty is not None else None,
    }


def flatten_planning_rows(response):
    """Flatten a planning-rows response into a list of normalised assignment dicts."""
    return [_planning_row_to_assignment(row) for row in _items(response)]
