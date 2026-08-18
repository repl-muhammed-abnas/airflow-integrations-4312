"""
Utilities for confirmed bookings export (RP → Polaris).
Handles collapsing daily rows into schedule rules and building GraphQL mutations.
"""
from datetime import datetime, timedelta


def build_uri(tenant_id, resource_type, resource_id):
    """Build a Replicon URN from tenant ID and resource type/id."""
    return f"urn:replicon-tenant:{tenant_id}:{resource_type}:{resource_id}"


def collapse_daily_rows_to_schedule_rules(daily_rows):
    """
    Collapse consecutive daily rows with the same hours into schedule rules (date ranges).
    Used for CREATE mutations which accept scheduleRules array.

    Input: list of dicts with 'workDate' (YYYY-MM-DD) and 'hoursPerDay' (float)
    Output: list of schedule rule dicts for GraphQL
    """
    if not daily_rows:
        return []

    # Sort by date
    sorted_rows = sorted(daily_rows, key=lambda r: r['workDate'])

    rules = []
    current_start = sorted_rows[0]['workDate']
    current_hours = sorted_rows[0]['hoursPerDay']
    current_end = current_start

    for i in range(1, len(sorted_rows)):
        row = sorted_rows[i]
        prev_date = datetime.strptime(current_end, '%Y-%m-%d')
        this_date = datetime.strptime(row['workDate'], '%Y-%m-%d')
        gap_days = (this_date - prev_date).days

        if row['hoursPerDay'] == current_hours and gap_days == 1:
            # Consecutive day with same hours — extend range
            current_end = row['workDate']
        else:
            # Break — save current rule and start new one
            rules.append({
                "dateRange": {
                    "startDate": f"{current_start}T00:00:00.000Z",
                    "endDate": f"{current_end}T00:00:00.000Z"
                },
                "do": {
                    "load": 100,
                    "setHours": current_hours,
                    "excludeWeekdays": []
                }
            })
            current_start = row['workDate']
            current_hours = row['hoursPerDay']
            current_end = row['workDate']

    # Save last rule
    rules.append({
        "dateRange": {
            "startDate": f"{current_start}T00:00:00.000Z",
            "endDate": f"{current_end}T00:00:00.000Z"
        },
        "do": {
            "load": 100,
            "setHours": current_hours,
            "excludeWeekdays": []
        }
    })

    return rules


def build_create_mutation(tenant_id, booking_guid, project_id, task_id, user_uri, schedule_rules):
    """Build a createTaskResourceUserAllocation GraphQL mutation."""
    allocation_id = build_uri(tenant_id, "psa-task-allocation", booking_guid)
    task_uri = build_uri(tenant_id, "task", task_id)
    project_uri = build_uri(tenant_id, "project", project_id)

    # Format schedule rules as GraphQL input
    rules_str = ""
    for rule in schedule_rules:
        rules_str += f"""
        {{
          dateRange: {{
            startDate: "{rule['dateRange']['startDate']}"
            endDate: "{rule['dateRange']['endDate']}"
          }}
          do: {{
            load: {rule['do']['load']}
            setHours: {rule['do']['setHours']}
            excludeWeekdays: []
          }}
        }}"""

    return f"""mutation {{
  createTaskResourceUserAllocation(
    input: {{
      taskAllocationId: "{allocation_id}"
      taskUri: "{task_uri}"
      projectUri: "{project_uri}"
      allocationUserUri: "{user_uri}"
      scheduleRules: [{rules_str}
      ]
    }}
  ) {{
    taskResourceUserAllocation {{
      id
      taskUri
      projectUri
      allocationUserUri
      totalHours
      startDate
      endDate
    }}
  }}
}}"""


def build_update_mutation(tenant_id, booking_guid, project_id, task_id, work_date, hours):
    """
    Build an updateTaskResourceUserAllocation GraphQL mutation for a single day (PARTIAL mode).
    Also used for DELETE by setting hours=0.
    """
    allocation_id = build_uri(tenant_id, "psa-task-allocation", booking_guid)
    task_uri = build_uri(tenant_id, "task", task_id)
    project_uri = build_uri(tenant_id, "project", project_id)

    dt = datetime.strptime(str(work_date)[:10], '%Y-%m-%d')

    return f"""mutation {{
  updateTaskResourceUserAllocation(
    input: {{
      allocationEditMode: PARTIAL
      allocationHours: {hours}
      projectUri: "{project_uri}"
      taskUri: "{task_uri}"
      taskAllocationId: "{allocation_id}"
      dateRange: {{
        startDate: {{day: {dt.day}, month: {dt.month}, year: {dt.year}}}
        endDate: {{day: {dt.day}, month: {dt.month}, year: {dt.year}}}
      }}
    }}
  ) {{
    taskResourceUserAllocation {{
      id
      taskUri
      projectUri
      allocationUserUri
      totalHours
      startDate
      endDate
    }}
  }}
}}"""
