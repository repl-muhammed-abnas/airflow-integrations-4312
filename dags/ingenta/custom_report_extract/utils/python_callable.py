from datetime import datetime
import rail

null = None


def build_initial_list():
    """
    Replaces 3 nested ForEachOperator loops that built 'Initial List':
      1. foreach allocation users × allocation rows  (insert_to_list_187)
      2. foreach 38 time users × time rows           (insert_to_list_191)
      3. foreach all time-off bookings               (insert_to_list_194)

    Uses O(1) dict lookups instead of O(n) linear scans per row.
    """
    time_data = rail.load_all_records(rail.result('query_list_get_allentriesfromtimedata_176'))
    allocation_data = rail.load_all_records(rail.result('query_list_get_allentriesfrom_allocationdata_179'))
    time_off_data = rail.load_all_records(rail.result('query_list_get_alltimeoffbookingsforeachuser_192'))
    contract_data = rail.load_all_records(rail.result('parse_csv_r_i_t_contract_daysreport_26'))
    client_project_data = rail.load_all_records(rail.result('parse_csv_122'))

    # Pre-build lookup dicts for O(1) access
    time_set = {
        (r['useruri'], r['projecturi'], r['monthactual'])
        for r in time_data
    }
    contract_lookup = {
        (r['useruri'], r['Month (Entry Date)']): r['contractdays']
        for r in contract_data
    }
    client_lookup = (
        {r['projecturi']: r['Client Name'] for r in client_project_data if r.get('Client Name')}
        if client_project_data and client_project_data[0].get('Client Name')
        else {}
    )
    alloc_lookup = {
        (r['useruri'], r['projecturi'], r['monthallocationdate']): r['projectallocateddays']
        for r in allocation_data
    }

    initial_list = []

    # 1. Allocation rows: only where no matching time entry exists for same user+project+month
    distinct_alloc_users = list(dict.fromkeys(r['useruri'] for r in allocation_data))
    for useruri in distinct_alloc_users:
        for alloc_row in (r for r in allocation_data if r['useruri'] == useruri):
            key = (alloc_row['useruri'], alloc_row['projecturi'], alloc_row['monthallocationdate'])
            if key not in time_set:
                initial_list.append({
                    "username": alloc_row['username'],
                    "userdepartmentname": alloc_row['department'],
                    "userdeptfortrans": alloc_row['userdeptfortrans'],
                    "clientname": client_lookup.get(alloc_row['projecturi']),
                    "projectname": alloc_row['projectname'],
                    "projectdeptfortrans": alloc_row['projectdeptfortrans'],
                    "timeofftype": "",
                    "month": alloc_row['monthallocationdate'],
                    "timeoffdays": 0,
                    "netcontractdays": contract_lookup.get((alloc_row['useruri'], alloc_row['monthallocationdate'])),
                    "actualdays": 0,
                    "allocateddays": alloc_row['projectallocateddays'],
                    "availabledays": null,
                    "actualvsplanned": null,
                    "useruri": alloc_row['useruri'],
                })

    # 2. Time data rows (38 users × N records each)
    distinct_time_users = list(dict.fromkeys(r['useruri'] for r in time_data))
    for useruri in distinct_time_users:
        for time_row in (r for r in time_data if r['useruri'] == useruri):
            if time_row.get('clientname') == '< None >':
                continue
            initial_list.append({
                "username": time_row['username'],
                "userdepartmentname": time_row['department'],
                "userdeptfortrans": time_row['userdeptfortrans'],
                "clientname": time_row['clientname'],
                "projectname": time_row['projectname'],
                "projectdeptfortrans": time_row['projectdeptfortrans'],
                "timeofftype": "",
                "month": time_row['monthactual'],
                "timeoffdays": null,
                "netcontractdays": contract_lookup.get((time_row['useruri'], time_row['monthactual'])),
                "actualdays": time_row['actualdays'] if time_row['actualdays'] else 0,
                "allocateddays": alloc_lookup.get((time_row['useruri'], time_row['projecturi'], time_row['monthactual'])),
                "useruri": time_row['useruri'],
                "actualvsplanned": null,
            })

    # 3. Time-off rows (column names are RAIL-sanitised: spaces/brackets → underscores)
    for timeoff_row in time_off_data:
        initial_list.append({
            "username": timeoff_row['User_Name'],
            "userdepartmentname": timeoff_row['Department__Current_'],
            "userdeptfortrans": timeoff_row['User_Dept_for_TRANS'],
            "clientname": null,
            "projectname": null,
            "projectdeptfortrans": "",
            "timeofftype": timeoff_row['Time_Off_Type'],
            "month": timeoff_row['Month__Time_Off_Date_'],
            "timeoffdays": timeoff_row['Time_Off_Days'] if timeoff_row['Time_Off_Days'] else 0,
            "netcontractdays": contract_lookup.get((timeoff_row['useruri'], timeoff_row['Month__Time_Off_Date_'])),
            "actualdays": 0,
            "allocateddays": 0,
            "availabledays": null,
            "useruri": timeoff_row['useruri'],
            "actualvsplanned": null,
        })

    return initial_list


def build_monthly_splits():
    """
    Replaces the nested foreach month × foreach row loop that built 'Monthlysplitlist'.
    Returns a dict keyed by month name with summary stats used for child dag conf:
      nettimeoffdays, netcontractdays, actualdays, allocateddays, availabledays, actualvsplanned.
    """
    initial_list = rail.result('build_initial_list')

    months = sorted(
        list({row['month'] for row in initial_list if row.get('month')}),
        key=lambda m: datetime.strptime(m, "%B").month
    )

    result = {}
    for month in months:
        month_rows = [r for r in initial_list if r['month'] == month]

        # Contract days: sum one value per unique user (matches original foreach_document_204 logic)
        seen_users = set()
        net_contract = 0.0
        for row in month_rows:
            if row['useruri'] not in seen_users:
                net_contract += float(row['netcontractdays']) if row.get('netcontractdays') else 0.0
                seen_users.add(row['useruri'])

        net_actual = round(
            sum(float(r['actualdays']) for r in month_rows if r.get('actualdays') is not None), 2
        )
        net_allocated = sum(
            float(r['allocateddays']) for r in month_rows if r.get('allocateddays') is not None
        )
        net_timeoff = sum(
            float(r['timeoffdays']) for r in month_rows if r.get('timeoffdays') is not None
        )

        result[month] = {
            "timeoffdays": net_timeoff,
            "netcontractdays": net_contract,
            "actualdays": net_actual,
            "allocateddays": net_allocated,
            "availabledays": round(net_contract - net_timeoff - net_allocated, 2),
            "actualvsplanned": round(net_allocated - net_actual, 2),
        }

    return result


def build_timeoff_filter_values(dag_run):
    """
    Replaces the ForEach chains (tasks 36-59) that built the time-off report filter list.
    Returns the filter list directly (not JSON-serialised).
    Logic mirrors: if_payload_userids_present_37 → userStatus branches → dept branch → date range appends.
    """
    user_filter_uri = rail.result('log_user_filter_uri_35')
    dept_filter_uri = rail.result('log_department_filter_uri_34')
    date_range_uri = rail.result('log_date_range_filter_uri_33')
    start_date = rail.result('log_daterangestart_3')
    end_date = rail.result('log_daterangeend_4')

    conf = dag_run.conf['webhook']['data']
    user_ids = conf.get('userIds')
    dept_ids = conf.get('departmentIds')
    user_status_ids = conf.get('userStatusIds')

    filter_list = []

    if user_ids and not dept_ids:
        for uid in user_ids.split(","):
            filter_list.append({"reportFilterUri": user_filter_uri, "value": uid})
    elif not user_ids and not dept_ids and user_status_ids:
        user_ref_result = rail.result('parse_csv_r_i_t_userreferencefile_30')
        user_reference = rail.load_all_records(user_ref_result) if user_ref_result else []
        if user_status_ids == 1:
            for user in user_reference:
                if user['User Status'] == 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": user['useruri'].split(":")[-1]})
        else:
            for user in user_reference:
                if user['User Status'] != 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": user['useruri'].split(":")[-1]})

    if dept_ids:
        for did in dept_ids.split(","):
            filter_list.append({"reportFilterUri": dept_filter_uri, "value": did})

    filter_list.append({"reportFilterUri": date_range_uri, "value": None})
    filter_list.append({"reportFilterUri": date_range_uri, "value": start_date})
    filter_list.append({"reportFilterUri": date_range_uri, "value": end_date})

    return filter_list


def build_allocation_filter_values(dag_run):
    """
    Replaces the ForEach chains (tasks 68/74-102) that built the allocation report filter list.
    Returns the filter list directly.
    Logic mirrors: userIds/userStatus/project/dept branches → date range appends.
    """
    user_filter_uri = rail.result('log_user_filter_uri_72')
    dept_filter_uri = rail.result('log_department_filteruri_70')
    date_range_uri = rail.result('log_date_range_filter_uri_73')
    project_filter_uri = rail.result('log_project_filter_uri_69')
    start_date = rail.result('log_daterangestart_3')
    end_date = rail.result('log_daterangeend_4')

    conf = dag_run.conf['webhook']['data']
    user_ids = conf.get('userIds')
    dept_ids = conf.get('departmentIds')
    user_status_ids = conf.get('userStatusIds')
    project_ids = conf.get('projectIds')

    filter_list = []

    if user_ids and not dept_ids:
        for uid in user_ids.split(","):
            filter_list.append({"reportFilterUri": user_filter_uri, "value": uid})
    elif not user_ids and not dept_ids and user_status_ids:
        user_ref_result = rail.result('parse_csv_r_i_t_userreferencefile_30')
        user_reference = rail.load_all_records(user_ref_result) if user_ref_result else []
        if user_status_ids == 1:
            for user in user_reference:
                if user['User Status'] == 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": user['useruri'].split(":")[-1]})
        else:
            for user in user_reference:
                if user['User Status'] != 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": user['useruri'].split(":")[-1]})

    if project_ids:
        for pid in project_ids.split(","):
            filter_list.append({"reportFilterUri": project_filter_uri, "value": pid})

    if dept_ids:
        if dept_filter_uri:  # mirrors if_log_department_filteruri_70_present_94 check
            for did in dept_ids.split(","):
                filter_list.append({"reportFilterUri": dept_filter_uri, "value": did})

    filter_list.append({"reportFilterUri": date_range_uri, "value": None})
    filter_list.append({"reportFilterUri": date_range_uri, "value": start_date})
    filter_list.append({"reportFilterUri": date_range_uri, "value": end_date})

    return filter_list


def build_timedata_filter_values(dag_run):
    """
    Replaces the ForEach chains (tasks 125/132-164) that built the time-data report filter list.
    Returns the filter list directly.
    Date range entries come first, then userStatus/project/client/dept/user branches.
    """
    user_filter_uri = rail.result('log_user_filter_uri_129')
    dept_filter_uri = rail.result('log_department_filteruri_130')
    date_range_uri = rail.result('log_date_range_filter_uri_128')
    project_filter_uri = rail.result('log_project_filter_uri_127')
    client_filter_uri = rail.result('log_client_filter_131')
    start_date = rail.result('log_daterangestart_3')
    end_date = rail.result('log_daterangeend_4')

    conf = dag_run.conf['webhook']['data']
    user_ids = conf.get('userIds')
    dept_ids = conf.get('departmentIds')
    user_status_ids = conf.get('userStatusIds')
    project_ids = conf.get('projectIds')
    client_ids = conf.get('clientIds')

    filter_list = []

    # Date range entries come first (mirrors tasks 132-134)
    filter_list.append({"reportFilterUri": date_range_uri, "value": None})
    filter_list.append({"reportFilterUri": date_range_uri, "value": start_date})
    filter_list.append({"reportFilterUri": date_range_uri, "value": end_date})

    if not user_ids and not dept_ids and user_status_ids:
        client_project_data = rail.load_all_records(rail.result('parse_csv_122')) or []
        if user_status_ids == 1:
            for project in client_project_data:
                if project['Project Name'] == 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": project['projecturi'].split(":")[-1]})
        else:
            for project in client_project_data:
                if project['Project Name'] != 'Enabled':
                    filter_list.append({"reportFilterUri": user_filter_uri, "value": project['projecturi'].split(":")[-1]})

    if project_ids:
        for pid in project_ids.split(","):
            filter_list.append({"reportFilterUri": project_filter_uri, "value": pid})

    if client_ids:
        for cid in client_ids.split(","):
            filter_list.append({"reportFilterUri": client_filter_uri, "value": cid})

    if dept_ids:
        for did in dept_ids.split(","):
            filter_list.append({"reportFilterUri": dept_filter_uri, "value": did})

    if user_ids and not dept_ids:
        for uid in user_ids.split(","):
            filter_list.append({"reportFilterUri": user_filter_uri, "value": uid})

    return filter_list


def build_user_split_list():
    """
    Replaces the nested ForEach chains (tasks 4-20) in child_dag.py.
    For each distinct user in the filtered month data:
    - Appends one detail row per source record (availabledays=None; useruri=None except the last row)
    - Appends one per-user summary row with accumulated totals
    Mirrors: declare_variable_8/9/10 accumulators + insert_to_list_17/19/summary_20 logic.
    """
    all_rows = rail.load_all_records(rail.result('filter_by_month')) or []

    # Preserve insertion order of distinct users (same as ForEach would process)
    seen = set()
    users = []
    for r in all_rows:
        if r['useruri'] not in seen:
            seen.add(r['useruri'])
            users.append(r['useruri'])

    result_list = []

    for useruri in users:
        user_rows = [r for r in all_rows if r['useruri'] == useruri]
        if not user_rows:
            continue

        # Accumulate totals (mirrors update_variable_12/13/14)
        net_actual = 0.0
        net_allocated = 0.0
        net_timeoff = 0.0
        for row in user_rows:
            net_allocated += float(row['allocateddays']) if row.get('allocateddays') is not None else 0.0
            net_actual += float(row['actualdays']) if row.get('actualdays') is not None else 0.0
            net_timeoff += float(row['timeoffdays']) if row.get('timeoffdays') is not None else 0.0

        # Detail rows (mirrors insert_to_list_17 for non-last, insert_to_list_19 for last)
        for i, row in enumerate(user_rows):
            is_last = (i == len(user_rows) - 1)
            allocated = float(row['allocateddays']) if row.get('allocateddays') is not None else 0.0
            actual = float(row['actualdays']) if row.get('actualdays') is not None else 0.0
            result_list.append({
                "username": row['username'],
                "userdepartmentname": row['userdepartmentname'],
                "userdeptfortrans": row['userdeptfortrans'],
                "projectdeptfortrans": row['projectdeptfortrans'],
                "clientname": row['clientname'],
                "projectname": row['projectname'],
                "timeofftype": row['timeofftype'],
                "month": row['month'],
                "timeoffdays": row['timeoffdays'],
                "netcontractdays": row['netcontractdays'],
                "actualdays": row['actualdays'],
                "allocateddays": row['allocateddays'],
                "availabledays": None,
                "actualvsplanned": allocated - actual,
                "useruri": row['useruri'] if is_last else None,
            })

        # Per-user summary row (mirrors insert_to_list_summary_20)
        last_row = user_rows[-1]
        net_contract = float(last_row['netcontractdays']) if last_row.get('netcontractdays') is not None else 0.0
        result_list.append({
            "username": str(last_row['username']) + " " + "Summary",
            "userdepartmentname": None,
            "userdeptfortrans": None,
            "projectdeptfortrans": "",
            "clientname": None,
            "projectname": None,
            "timeofftype": "",
            "month": last_row['month'],
            "timeoffdays": net_timeoff,
            "netcontractdays": last_row['netcontractdays'],
            "actualdays": round(net_actual, 2),
            "allocateddays": net_allocated,
            "availabledays": round(net_contract - net_timeoff - net_allocated, 2),
            "actualvsplanned": round(net_allocated - net_actual, 2),
            "useruri": last_row['useruri'],
        })

    return result_list
