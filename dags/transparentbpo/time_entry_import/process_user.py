import rail
from datetime import timedelta
from uuid import uuid4
from transparentbpo.time_entry_import.utils import request_payload, custom_methods, response_filters

def create_child_dag(config):
    append_dags = []
    for idx in range(0, config.PROCESS_USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_unique_users_child}_batch_{idx+1}",
            description=f'TransparentBPO Time Import Child - Process Unique Users {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            create_process_user_log = rail.CreateLogOperator(
                task_id="create_process_user_log"
            )

            get_all_records_for_user = rail.QueryCollectionOperator(
                task_id="get_all_records_for_user",
                query="""SELECT * FROM valid_entries fd
                        WHERE fd.employee_id = :EMP_ID
                        ORDER BY fd.work_date ASC, fd.start_time ASC""",
                query_params={
                    "EMP_ID": "{{dag_run.conf.employee_id}}"
                },
                name="all_user_records"
            )

            get_user_details = rail.RepliconServiceOperator(
                task_id="get_user_details",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data=lambda dag_run: request_payload.get_user_data_payload(dag_run.conf["employee_id"]),
                data_handler=lambda res: res[0] if (res and res[0]['userDetails']['isEnabled']) else None
            )

            if_user_uri_present = rail.IfOperator(
                task_id ='if_user_uri_present',
                test = lambda: bool(rail.result('get_user_details')),
                yes_task="get_unique_entry_date_for_user",
                no_task="log_user_missing_in_replicon"
            )
            
            
            log_user_missing_in_replicon = rail.WriteLogOperator(
                task_id='log_user_missing_in_replicon',
                log='{{ result("create_process_user_log") }}',
                severity='Exception',
                message='User is not present or is disabled for Employee Id: {{ dag_run.conf.employee_id }}',
                properties={
                    'employee_id': '{{ dag_run.conf.employee_id }}',
                    'work_date': '',
                    'project': '',
                    'task': '',
                    'activity': '',
                    'status': 'Exception',
                    'action': 'Validation',
                    'details': 'User is not present or is disabled for Employee Id: {{ dag_run.conf.employee_id }}'
                },
            )

            get_unique_entry_date_for_user = rail.QueryCollectionOperator(
                task_id="get_unique_entry_date_for_user",
                query="""SELECT DISTINCT work_date FROM valid_entries fd WHERE fd.employee_id =:EMP_ID""",
                query_params={
                    "EMP_ID": "{{dag_run.conf.employee_id}}"
                },
                name="unique_entry_date"
            )

            get_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
                task_id = "get_timesheet_details",
                items="{{ result('get_unique_entry_date_for_user')}}",
                endpoint= "/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
                data=lambda item: {
                    "userUri": rail.result('get_user_details')['userDetails']['uri'],
                    "date": rail.parse_date(
                        item['work_date'], config.entry_dateformat),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
                },
                all_result_data_handler=response_filters.get_timesheet_details
            )

            get_submitted_ts_uris = rail.PythonOperator(
                task_id='get_submitted_ts_uris',
                python_callable=lambda: custom_methods.get_submitted_timesheet_uris(rail.result('get_timesheet_details'))
            )

            reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
                task_id="reopen_timesheets",
                items="{{result('get_submitted_ts_uris') | to_json}}",
                endpoint="/services/TimesheetApprovalService1.svc/Reopen",
                data=lambda item: {
                    "timesheetUri": item['ts_uri'],
                    "unitOfWorkId": str(uuid4()),
                    "comments": "Timesheet is reopened by Integration (Time Data Import)"
                }
            )

            get_time_entry_details = rail.RepliconServiceCallForEachItemOperator(
                task_id="get_time_entry_details",
                items="{{ result('get_unique_entry_date_for_user') }}",
                endpoint="/services/TimeEntryRevisionGroupService1.svc/GetTimeEntryRevisionGroupsForUserAndDateRange",
                data=lambda item: request_payload.get_time_entries_for_user_date_range(
                    user_uri=rail.result('get_user_details')['userDetails']['uri'],
                    work_date=rail.parse_date(item['work_date'], config.entry_dateformat)
                ),
                all_result_data_handler=response_filters.filter_time_entries
            )

            delete_time_entry = rail.RepliconServiceCallForEachItemOperator(
                task_id='delete_time_entry',
                items=lambda: rail.result('get_time_entry_details') if rail.result('get_time_entry_details') else [],
                endpoint='/services/TimeEntryRevisionGroupService1.svc/DeleteTimeEntryRevisionGroup',
                data=lambda item: {
                    "timeEntryRevisionGroupUri": item
                }
            )

            get_time_punch_details = rail.RepliconServiceCallForEachItemOperator(
                task_id = "get_time_punch_details",
                items = "{{ result('get_unique_entry_date_for_user') }}",
                endpoint="/services/TimePunchService1.svc/BulkGetTimePunchDetailsForUsersAndDateRange",
                data=lambda item: request_payload.get_time_punch_details(
                    user_uri=rail.result('get_user_details')['userDetails']['uri'],
                    work_date=rail.parse_date(item['work_date'], config.entry_dateformat)
                ),
                all_result_data_handler=response_filters.filter_punch_entries
            )

            delete_time_punch = rail.RepliconServiceCallForEachItemOperator(
                task_id="delete_time_punch",
                items=lambda: rail.result("get_time_punch_details") if rail.result("get_time_punch_details") else [],
                endpoint="/services/TimePunchService1.svc/Delete",
                data=lambda item:{
                    "timePunchUri": item
                }
            )

            if_no_activity_assigned_to_user = rail.IfOperator(
                task_id="if_no_activity_assigned_to_user",  
                test=lambda: len(rail.result('get_user_details')['assignedActivities']) == 0,
                yes_task="log_no_activity_assigned_to_user",
                no_task="get_user_activities"
            )

            log_no_activity_assigned_to_user = rail.WriteLogOperator(
                task_id='log_no_activity_assigned_to_user',
                log='{{ result("create_process_user_log") }}',
                severity='Exception',
                items="{{ result('get_all_records_for_user') }}",
                message="Activity is not assigned to user",
                properties=lambda item: {
                    'employee_id': item.get('employee_id', ''),
                    'work_date': item.get('work_date', ''),
                    'project': item.get('project', ''),
                    'task': item.get('task', ''),
                    'activity': item.get('activity', ''),
                    'status': 'Exception',
                    'action': 'Add',
                    'details': "No activity is assigned to user in replicon"
                },
            )
            
            get_user_activities = rail.PythonOperator(
                task_id="get_user_activities",
                python_callable=lambda: ", ".join(
                    "'" + a['name'].replace("'", "''") + "'"
                    for a in rail.result('get_user_details')['assignedActivities']
                ),
            )

            query_user_invalid_time_entry_records = rail.QueryCollectionOperator(
                task_id="query_user_invalid_time_entry_records",
                query="""SELECT DISTINCT employee_id, work_date, project, task, activity
                FROM valid_entries where employee_id =:EMP_ID
                and activity NOT IN ({{ result("get_user_activities") }})
                and UPPER(activity) <> 'N/A'
                and UPPER(timesheet_category) NOT IN ('BREAK', 'LUNCH') """,
                query_params={
                    "EMP_ID": "{{dag_run.conf.employee_id}}"
                }
            )

            if_invalid_activity = rail.IfOperator(
                task_id="if_invalid_activity",
                test='{{result("query_user_invalid_time_entry_records", "length") > 0}}',
                yes_task="log_activity_not_assigned_to_user",
                no_task="query_user_time_entry_records"
            )

            log_activity_not_assigned_to_user = rail.WriteLogOperator(
                task_id='log_activity_not_assigned_to_user',
                log='{{ result("create_process_user_log") }}',
                severity='Exception',
                items="{{ result('query_user_invalid_time_entry_records') }}",
                message=lambda item: f"Activity {item.get('activity', '')} is not assigned to user",
                properties=lambda item: {
                    'employee_id': item.get('employee_id', ''),
                    'work_date': item.get('work_date', ''),
                    'project': item.get('project', ''),
                    'task': item.get('task', ''),
                    'activity': item.get('activity', ''),
                    'status': 'Exception',
                    'action': 'Add',
                    'details': f"Activity {item.get('activity', '')} is not assigned to user"
                },
            )

            query_user_time_entry_records = rail.QueryCollectionOperator(
                task_id="query_user_time_entry_records",
                trigger_rule='none_failed_min_one_success',
                query="""SELECT DISTINCT work_date
                FROM valid_entries where employee_id =:EMP_ID
                and (activity IN ({{ result("get_user_activities") }})
                     or UPPER(activity) = 'N/A'
                     or UPPER(timesheet_category) IN ('BREAK', 'LUNCH')) """,
                query_params={
                    "EMP_ID": "{{dag_run.conf.employee_id}}"
                }
            )

            trigger_process_each_entry = rail.trigger_parallel_dagrun(
                task_id='trigger_process_each_entry',
                items="{{ result('query_user_time_entry_records') }}",
                trigger_dag_id=f"{config.process_each_entry_date_child}_batch_{idx+1}",
                parallel_count=5,
                execution_timeout=timedelta(days=config.execution_timeout_days),
                conf=lambda item, dag_run: {
                    **item,
                    **dag_run.conf,
                    'activities': rail.result('get_user_details')['assignedActivities'],
                    'user_ts_template': rail.result('get_user_details')['timesheetTemplate']['displayText'],
                    'user_ts_uri': rail.result('get_user_details')['timesheetTemplate']['uri'],
                    'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                    'user_log': rail.result('create_process_user_log'),
                },
            )

            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log = '{{result("create_process_user_log")}}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    'employee_id': '{{ dag_run.conf.employee_id }}',
                    'work_date': '',
                    'project': '',
                    'task': '',
                    'activity': '',
                    'status': "Error",
                    'action': "Validation",
                    'details': '{{ get_error_message() }}'
                },
            )

            create_process_user_log \
            >> get_all_records_for_user \
            >> get_user_details \
            >> if_user_uri_present

            if_user_uri_present >> rail.Label("No") \
            >> log_user_missing_in_replicon \
            >> catch_and_log_errors

            if_user_uri_present >> rail.Label("Yes") \
            >> get_unique_entry_date_for_user

            get_unique_entry_date_for_user \
            >> get_timesheet_details \
            >> get_submitted_ts_uris \
            >> reopen_timesheets \
            >> get_time_entry_details \
            >> delete_time_entry \
            >> get_time_punch_details \
            >> delete_time_punch \
            >> if_no_activity_assigned_to_user

            if_no_activity_assigned_to_user >> rail.Label("Yes") \
            >> log_no_activity_assigned_to_user

            if_no_activity_assigned_to_user >> rail.Label("No") \
            >> get_user_activities \
            >> query_user_invalid_time_entry_records \
            >> if_invalid_activity

            if_invalid_activity >> rail.Label("No") >> query_user_time_entry_records
            if_invalid_activity >> rail.Label("Yes") \
            >> log_activity_not_assigned_to_user >> query_user_time_entry_records

            query_user_time_entry_records \
            >> trigger_process_each_entry >> catch_and_log_errors

        append_dags.append(dag)
    return append_dags

rail.for_each_instance(create_child_dag)