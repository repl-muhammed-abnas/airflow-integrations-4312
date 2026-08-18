"""
Computerease-Replicon Time Data Sync Integration

This DAG syncs approved time entries from Replicon Time Workbench to Computerease:
1. Fetches available Time Data Export columns from Replicon
2. Validates required columns exist
3. Exports time data with filters (date range, approved status)
4. Groups data by employee
5. Triggers child DAG for each employee to process in batches
6. Handles errors with detailed CSV reports

Key Features:
- Time Workbench API
- Distributed Time Type (Pay Code) for Regular/Overtime/Double Time
- Pre-sync validation with error reports
- Configurable date range (90 days lookback, 30 days forward)
- Separate validation errors vs sync errors
"""
from datetime import datetime, timedelta
from itertools import groupby
from collections import defaultdict
import rail
import requests
import csv
from io import StringIO
from ce_replicon_integration.time_sync.utils import request_payload

def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=f'{config.main_dag_id}',
        description=f'{config.company_key} Replicon To Computerease Time Sync Main DAG.',
        company_key=config.company_key,
        max_active_runs=config.max_active_runs,
        multi_tenant=True
    ) as dag:
        
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_time_data_columns',
            end_task='should_log_history',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_column_uris(time_columns):
            # Standard column URIs
            standard_columns = {
                'login_name': "urn:replicon:time-data-export-column:user-login-name",
                'entry_date': "urn:replicon:time-data-export-column:entry-date",
                'comments': "urn:replicon:time-data-export-column:comments",
                'hours': "urn:replicon:time-data-export-totals-column:hours",
                'in_time': "urn:replicon:time-data-export-column:in-time",
                'out_time': "urn:replicon:time-data-export-column:out-time",
                'punch_in_time': "urn:replicon:time-data-export-column:punch-in-time",
                'punch_out_time': "urn:replicon:time-data-export-column:punch-out-time",
                'break_time_name': "urn:replicon:time-data-export-column:break-type-name",
                'project_code': "urn:replicon:time-data-export-column:project-code",
                'task_code': "urn:replicon:time-data-export-column:task-code",
                'task_code_full_path': "urn:replicon:time-data-export-column:task-hierarchy-code",
                'time_off_type': "urn:replicon:time-data-export-column:time-off-code-name",
                'pay_code_name': "urn:replicon:time-data-export-column:pay-code-name"
            }

            column_uris = list(standard_columns.values())

            oef_uris = {}
            timesheet_oefs = config.oefs if hasattr(config, 'oefs') else []

            for item in time_columns:
                display_text = item.get('displayText', '').strip()
                if display_text == 'Time Entry Object Extension Field':
                    for col in item.get('columns', []):
                        col_display = col.get('displayText', '')
                        for oef in timesheet_oefs:
                            if col_display == oef['name']:
                                oef_uris[oef['id']] = col.get('uri')
                    break

            column_uris.extend([uri for uri in oef_uris.values() if uri])

            return {
                'column_uris': column_uris,
                'pay_code_name_uri': standard_columns['pay_code_name'],
                'amount_uri': oef_uris.get('amount'),
                'work_payment_uri': oef_uris.get('workpayment'),
                'has_in_out_time': True
            }

        get_time_data_columns = rail.RepliconServiceOperator(
            task_id='get_time_data_columns',
            endpoint='/services/TimeDataExportService1.svc/GetAllColumns',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda resp: get_column_uris(resp)
        )

        has_required_columns = rail.IfOperator(
            task_id='has_required_columns',
            test=lambda: None not in rail.result('get_time_data_columns').get('column_uris', []),
            yes_task='get_time_export_download_script',
            no_task='should_log_history'
        )

        get_time_export_download_script = rail.RepliconServiceOperator(
            task_id='get_time_export_download_script',
            endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', config.replicon_export_file_format_name, 'uri')
        )

        create_time_export_batch = rail.RepliconServiceOperator(
            task_id="create_time_export_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataExportBatch",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: request_payload.get_create_time_export_batch_payload_request(dag_run, config)
        )

        execute_time_export_batch, wait_for_time_export_batch = rail.batch_execution(
            group_id='execute_time_export_batch',
            creation_task_id=create_time_export_batch.task_id,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_time_export_batch_result = rail.RepliconServiceOperator(
            task_id="get_time_export_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={"timeDataExportBatchUri": "{{ result('create_time_export_batch') }}"}
        )

        create_time_export_download_batch = rail.RepliconServiceOperator(
            task_id="create_time_export_download_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataDownloadBatch",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=request_payload.get_create_time_export_download_batch_payload
        )

        execute_time_export_download_batch, wait_for_time_export_download_batch = rail.batch_execution(
            group_id='execute_time_export_download_batch',
            creation_task_id=create_time_export_download_batch.task_id,
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}'
        )

        get_time_export_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_time_export_download_batch_result",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataDownloadBatchResults",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "timeDataDownloadBatchUri": "{{ result('create_time_export_download_batch') }}"}
        )

        get_my_identity = rail.RepliconServiceOperator(
            task_id="get_my_identity",
            endpoint="/services/UserAccessControlService1.svc/GetMyIdentity",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
        )

        get_date_format = rail.RepliconServiceOperator(
            task_id="get_date_format",
            endpoint="/services/InternationalizationService1.svc/GetDateFormatForUser",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data= lambda : {
                "userUri": rail.result("get_my_identity")["actualUser"]["uri"]
            }
        )

        get_all_pay_codes = rail.RepliconServiceOperator(
            task_id="get_all_pay_codes",
            endpoint="/services/PayCodeService1.svc/GetAllPayCodes",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data_handler=lambda resp: [item['name'].strip() for item in sorted(resp, key=lambda x: x['multiplier'])]
        )

        def parse_time(t_str):
            try:
                return datetime.strptime(t_str.strip(), config.TIME_FMT)
            except ValueError as e:
                raise ValueError(f"Unable to parse time '{t_str}': {e}")

        def format_time(d):
            return d.strftime('%I:%M:%S %p')

        def group_key(row):
            return (row['Login Name'], row['Entry Date'])

        def project_key(row):
            return (row.get('Project Code', '').strip(), row.get('Task Code (Full Path)', '').strip())

        def is_time_off(row):
            return row['Distributed Time Type Name'].strip() == 'Time Off' or row['Time Off Type Name'].strip() != ''

        def is_break(row):
            return row.get('Name', '').strip() != ''

        def split_in_out_rows(data):
            results = []
            time_type_order = rail.result('get_all_pay_codes')
            type_order_index = {t: i for i, t in enumerate(time_type_order)}

            sorted_data = sorted(data, key=group_key)
            for key, group in groupby(sorted_data, key=group_key):
                group = list(group)

                # Keep Time Off rows as-is, skip Break rows entirely
                time_off_rows = [r for r in group if is_time_off(r) and not is_break(r)]
                for row in time_off_rows:
                    results.append(dict(row))

                non_timeoff_rows = [r for r in group if not is_time_off(r) and not is_break(r)]

                # Determine punch_time_data from first non-timeoff row with empty Distributed Time Type Name
                punch_time_data = False
                for r in non_timeoff_rows:
                    if r['Distributed Time Type Name'].strip() == '':
                        has_punch = r.get('Punch In Time', '').strip() != '' and r.get('Punch Out Time', '').strip() != ''
                        has_in_out = r.get('In Time', '').strip() != '' and r.get('Out Time', '').strip() != ''
                        if has_punch:
                            punch_time_data = True
                        elif has_in_out:
                            punch_time_data = False
                        break

                # Separate In/Out rows and bucket rows
                if punch_time_data:
                    all_inout_rows = [r for r in non_timeoff_rows if r.get('Punch In Time', '').strip() != '' and r.get('Punch Out Time', '').strip() != '']
                else:
                    all_inout_rows = [r for r in non_timeoff_rows if r.get('In Time', '').strip() != '' and r.get('Out Time', '').strip() != '']

                bucket_rows = [r for r in non_timeoff_rows if r['Distributed Time Type Name'].strip() != '']

                if not all_inout_rows:
                    for row in bucket_rows:
                        passthrough_row = dict(row)
                        passthrough_row['In Time'] = ''
                        passthrough_row['Out Time'] = ''
                        passthrough_row['Punch In Time'] = ''
                        passthrough_row['Punch Out Time'] = ''
                        results.append(passthrough_row)
                    continue

                # Separate positive and negative In/Out rows
                positive_inout_rows = [r for r in all_inout_rows if float(r['Hours']) > 0]
                negative_inout_rows = [r for r in all_inout_rows if float(r['Hours']) < 0]

                # Separate positive and negative bucket rows
                positive_bucket_rows = [r for r in bucket_rows if float(r['Hours']) > 0]
                negative_bucket_rows = [r for r in bucket_rows if float(r['Hours']) < 0]

                # Helper to build timeline from rows
                def build_timeline(rows):
                    timeline = []
                    for row in rows:
                        if punch_time_data:
                            start = parse_time(row.get('Punch In Time', '').strip())
                            end = parse_time(row.get('Punch Out Time', '').strip())
                        else:
                            start = parse_time(row.get('In Time', '').strip())
                            end = parse_time(row.get('Out Time', '').strip())
                        if end <= start:
                            end = end + timedelta(hours=24)
                        timeline.append((start, end, row))
                    return timeline

                # Process negative hours first
                # Find overlap between negative and positive In/Out timelines
                # and compute remaining negative time slots
                if negative_inout_rows and positive_inout_rows:
                    neg_timeline = build_timeline(sorted(negative_inout_rows,
                        key=lambda r: parse_time(r['Punch In Time'] if punch_time_data else r['In Time'])))
                    pos_timeline = build_timeline(sorted(positive_inout_rows,
                        key=lambda r: parse_time(r['Punch In Time'] if punch_time_data else r['In Time'])))

                    # Compute remaining positive segments after cancellation
                    remaining_pos_segments = []
                    for pos_start, pos_end, pos_row in pos_timeline:
                        pos_remaining = [(pos_start, pos_end, pos_row)]
                        for neg_start, neg_end, _ in neg_timeline:
                            new_remaining = []
                            for seg_start, seg_end, seg_row in pos_remaining:
                                overlap_start = max(seg_start, neg_start)
                                overlap_end = min(seg_end, neg_end)
                                if overlap_start < overlap_end:
                                    # Split around overlap
                                    if seg_start < overlap_start:
                                        new_remaining.append((seg_start, overlap_start, seg_row))
                                    if overlap_end < seg_end:
                                        new_remaining.append((overlap_end, seg_end, seg_row))
                                else:
                                    new_remaining.append((seg_start, seg_end, seg_row))
                            pos_remaining = new_remaining
                        remaining_pos_segments.extend(pos_remaining)

                    # Compute remaining negative segments after cancellation
                    remaining_neg_segments = []
                    for neg_start, neg_end, neg_row in neg_timeline:
                        neg_remaining = [(neg_start, neg_end, neg_row)]
                        for pos_start, pos_end, _ in pos_timeline:
                            new_remaining = []
                            for seg_start, seg_end, seg_row in neg_remaining:
                                overlap_start = max(seg_start, pos_start)
                                overlap_end = min(seg_end, pos_end)
                                if overlap_start < overlap_end:
                                    if seg_start < overlap_start:
                                        new_remaining.append((seg_start, overlap_start, seg_row))
                                    if overlap_end < seg_end:
                                        new_remaining.append((overlap_end, seg_end, seg_row))
                                else:
                                    new_remaining.append((seg_start, seg_end, seg_row))
                            neg_remaining = new_remaining
                        remaining_neg_segments.extend(neg_remaining)

                elif negative_inout_rows:
                    remaining_neg_segments = build_timeline(sorted(negative_inout_rows,
                        key=lambda r: parse_time(r['Punch In Time'] if punch_time_data else r['In Time'])))
                    remaining_pos_segments = []
                elif positive_inout_rows:
                    remaining_pos_segments = build_timeline(sorted(positive_inout_rows,
                        key=lambda r: parse_time(r['Punch In Time'] if punch_time_data else r['In Time'])))
                    remaining_neg_segments = []
                else:
                    remaining_pos_segments = []
                    remaining_neg_segments = []

                # Process negative remaining segments with negative bucket rows
                if remaining_neg_segments and negative_bucket_rows:
                    neg_bucket_rows_sorted = sorted(
                        negative_bucket_rows,
                        key=lambda r: (
                            type_order_index.get(r['Distributed Time Type Name'].strip(), 999),
                            r.get('Project Code', '').strip(),
                            r.get('Task Code (Full Path)', '').strip()
                        )
                    )
                    neg_bucket_queue = {}
                    for row in neg_bucket_rows_sorted:
                        pk = project_key(row)
                        if pk not in neg_bucket_queue:
                            neg_bucket_queue[pk] = []
                        neg_bucket_queue[pk].append([row, abs(float(row['Hours']))])

                    for seg_start, seg_end, inout_row in remaining_neg_segments:
                        current_time = seg_start
                        seg_remaining = (seg_end - seg_start).total_seconds() / 3600
                        pk = project_key(inout_row)

                        def get_next_neg_bucket():
                            if pk in neg_bucket_queue:
                                for bucket in neg_bucket_queue[pk]:
                                    if bucket[1] > 0:
                                        return bucket
                            for p_key in sorted(neg_bucket_queue.keys()):
                                for bucket in neg_bucket_queue[p_key]:
                                    if bucket[1] > 0:
                                        return bucket
                            return None

                        while seg_remaining > 0:
                            bucket = get_next_neg_bucket()
                            if bucket is None:
                                break
                            bucket_row, bucket_hrs = bucket
                            alloc_hrs = min(seg_remaining, bucket_hrs)
                            split_end = current_time + timedelta(hours=alloc_hrs)

                            split_row = dict(bucket_row)
                            split_row['In Time'] = format_time(current_time)
                            split_row['Out Time'] = format_time(split_end)
                            split_row['Hours'] = str(-round(alloc_hrs, 2))
                            if punch_time_data:
                                split_row['Punch In Time'] = inout_row['Punch In Time']
                            results.append(split_row)

                            bucket[1] -= alloc_hrs
                            seg_remaining -= alloc_hrs
                            current_time = split_end

                # If no positive In/Out rows or no positive bucket rows, skip positive processing
                if not remaining_pos_segments or not positive_bucket_rows:
                    continue

                # Sort positive bucket rows
                bucket_rows_sorted = sorted(
                    positive_bucket_rows,
                    key=lambda r: (
                        type_order_index.get(r['Distributed Time Type Name'].strip(), 999),
                        r.get('Project Code', '').strip(),
                        r.get('Task Code (Full Path)', '').strip()
                    )
                )

                # Group positive bucket rows by Project Code + Task Code (Full Path)
                project_bucket_queues = {}
                for row in bucket_rows_sorted:
                    pk = project_key(row)
                    if pk not in project_bucket_queues:
                        project_bucket_queues[pk] = []
                    project_bucket_queues[pk].append([row, float(row['Hours'])])

                # Walk through remaining positive timeline segments
                for seg_start, seg_end, inout_row in remaining_pos_segments:
                    current_time = seg_start
                    seg_remaining = (seg_end - seg_start).total_seconds() / 3600
                    pk = project_key(inout_row)

                    def get_next_bucket():
                        if pk in project_bucket_queues:
                            for bucket in project_bucket_queues[pk]:
                                if bucket[1] > 0:
                                    return bucket
                        for p_key in sorted(project_bucket_queues.keys()):
                            for bucket in project_bucket_queues[p_key]:
                                if bucket[1] > 0:
                                    return bucket
                        return None

                    while seg_remaining > 0:
                        bucket = get_next_bucket()
                        if bucket is None:
                            break

                        bucket_row, bucket_hrs = bucket
                        alloc_hrs = min(seg_remaining, bucket_hrs)
                        split_end = current_time + timedelta(hours=alloc_hrs)

                        split_row = dict(bucket_row)
                        split_row['In Time'] = format_time(current_time)
                        split_row['Out Time'] = format_time(split_end)
                        split_row['Hours'] = str(round(alloc_hrs, 2))
                        if punch_time_data:
                            split_row['Punch In Time'] = inout_row['Punch In Time']
                        results.append(split_row)

                        bucket[1] -= alloc_hrs
                        seg_remaining -= alloc_hrs
                        current_time = split_end

            return results

        def parse_and_group_time_entries():
            response = requests.get(rail.result('get_time_export_download_batch_result')['downloadUrl'])
            response.raise_for_status()
            csv_reader = csv.DictReader(StringIO(response.text))
            data = list(csv_reader)
            
            valid_entries = split_in_out_rows(data)

            employee_groups = defaultdict(list)

            for entry in valid_entries:
                employee = entry.get('Login Name', '').strip()
                if employee:
                    employee_groups[employee].append(entry)

            grouped_data = []
            for employee, entries in employee_groups.items():
                grouped_data.append({
                    'employee': employee,
                    'entries': entries,
                    'entry_count': len(entries)
                })

            return grouped_data

        group_time_entries_by_employee = rail.PythonOperator(
            task_id="group_time_entries_by_employee",
            python_callable=parse_and_group_time_entries
        )

        has_data_to_export = rail.IfOperator(
            task_id='has_data_to_export',
            test=lambda: len(rail.result('group_time_entries_by_employee')) > 0,
            yes_task='process_employees_time_data',
            no_task='should_log_history'
        )

        process_employees_time_data = rail.TriggerDagRunForEachItemOperator(
            task_id='process_employees_time_data',
            items=lambda: rail.result('group_time_entries_by_employee'),
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'{config.child_dag_id}',
            conf=lambda dag_run, item: {
                'employee': item['employee'],
                'entries': item['entries'],
                'entry_count': item['entry_count'],
                'export_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                'date_format_uri': rail.result('get_date_format'),
                'computerease_conn_id': dag_run.conf['computerease_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id']
            }
        )

        wait_for_processing_employees = rail.WaitForDagRunsSensor(
            task_id='wait_for_processing_employees',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_employees_time_data") }}'
        )

        gather_time_entries_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_entries_errors',
            dag_runs="{{ result('process_employees_time_data') }}",
            dagrun_task_id='catch_time_entries_error',
            flatten=True
        )

        is_time_entry_error = rail.IfOperator(
            task_id='is_time_entry_error',
            test="{{ (get_task_state('gather_time_entries_errors') == 'success' and result('gather_time_entries_errors') | length > 0) }}",
            yes_task='fail_time_entry_error',
            no_task='mark_time_export_as_complete'
        )

        mark_time_export_as_complete = rail.RepliconServiceOperator(
            task_id='mark_time_export_as_complete',
            endpoint='/services/TimeDataExportService1.svc/MarkTimeDataExportAsComplete',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "target": {
                    "uri": "{{ result('get_time_export_batch_result')['timeDataExportUri'] }}"
                }
            }
        )

        fail_time_entry_error = rail.FailOperator(
            task_id='fail_time_entry_error',
            message="{{ result('gather_time_entries_errors') | map_to_attr('error') | join('|') }}"
        )

        should_log_history = rail.IfOperator(
            task_id='should_log_history',
            test="{{ not(get_task_state('has_data_to_export') == 'success' and \
                    result('has_data_to_export') != 'process_employees_time_data') }}",
            trigger_rule='all_done',
            yes_task='log_dagrun_details_to_table',
            no_task='delete_this_dagrun'
        )

        log_dagrun_details_to_table = rail.PostDagRunDetailsToRepliconOperator(
            task_id='log_dagrun_details_to_table',
            required_configs={
                'airflow_connector_ui_connid': config.airflow_connector_ui_connid,
                'hmac_secret_var': config.hmac_secret
            },
            company_key='{{ dag_run.conf.company_key }}',
            connector_name=config.provider,
            integration_type=config.workflow
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun'
        )

        batch_task >> get_time_data_columns >> has_required_columns
        batch_task >> should_log_history

        has_required_columns >> rail.Label('Yes') >> get_time_export_download_script >> create_time_export_batch >> execute_time_export_batch >> wait_for_time_export_batch >> get_time_export_batch_result \
        >> create_time_export_download_batch >> execute_time_export_download_batch >> wait_for_time_export_download_batch \
        >> get_time_export_download_batch_result >> get_my_identity >> get_date_format >> get_all_pay_codes >> group_time_entries_by_employee >> has_data_to_export

        has_data_to_export >> rail.Label('Yes') >> process_employees_time_data >> wait_for_processing_employees >> gather_time_entries_errors >> is_time_entry_error

        is_time_entry_error >> rail.Label('Yes') >> fail_time_entry_error >> should_log_history
        is_time_entry_error >> rail.Label('No') >> mark_time_export_as_complete >> should_log_history
        has_data_to_export >> rail.Label('No') >> should_log_history
        has_required_columns >> rail.Label('No') >> should_log_history

        should_log_history >> rail.Label('Yes') >> log_dagrun_details_to_table
        should_log_history >> rail.Label('No') >> delete_this_dagrun

        return dag


rail.for_each_instance(create_dag_instance)