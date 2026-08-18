from datetime import timedelta
from uuid import uuid4
from tsystems.timesheet_guessing_hours_update.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable
import rail

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_users,
        description=f'T-Systems Guessing Hours Update - Process Unique Users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_process_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_process_user_log',
            end_task='batch_task_end',
        )

        create_process_user_log = rail.CreateLogOperator(
            task_id="create_process_user_log"
        )

        get_guessing_records_for_user = rail.QueryCollectionOperator(
            task_id="get_guessing_records_for_user",
            query="""SELECT *
                FROM validguessinghours
                WHERE LOWER(task_name) = 'guessing hours'
                  AND hours NOT IN ('0.00', '0', '0,00', 0.00, 0)
                  AND user_uri = '{{ dag_run.conf.input_data.user_uri }}'""",
            name="guessing_user_records"
        )

        get_unique_timesheet_start_dates = rail.QueryCollectionOperator(
            task_id="get_unique_timesheet_start_dates",
            query="""SELECT DISTINCT timesheet_start_date
                FROM guessing_user_records
                ORDER BY timesheet_start_date""",
            name="unique_timesheet_dates"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=lambda res: res[0] if (res and res[0]['userDetails']['isEnabled']) else null
        )

        if_user_uri_present = rail.IfOperator(
            task_id='if_user_uri_present',
            test=lambda: rail.result('get_user_details'),
            yes_task="get_timesheet_details",
            no_task="log_user_missing_in_replicon"
        )
        
        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            log='{{ result("create_process_user_log") }}',
            items='{{ result("get_guessing_records_for_user") }}',
            severity='Exception',
            message='User is not present or disabled in Replicon',
            properties=lambda item: {
                'employee_id': item.get('employee_id'),
                'user_name': item.get('user_name'),
                'entry_date': item.get('entry_date'),
                'original_hours': item.get('hours'),
                'task_name': item.get('task_name'),
                'project_name': item.get('project_name'),
                'org_structure_code': item.get('org_structure_code'),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'User is not present or disabled in Replicon'
            }
        )

        get_timesheet_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_timesheet_details",
            items="{{result('get_unique_timesheet_start_dates')}}",
            endpoint="/services/TimesheetService1.svc/GetTimesheetDetailsForDate",
            data=lambda item, dag_run: request_payload.get_timesheet_details_payload(item, dag_run, config.ENTRY_DATE_FORMAT),
            all_result_data_handler=response_filters.get_timesheet_details
        )

        filter_timesheets_to_reopen = rail.PythonOperator(
            task_id='filter_timesheets_to_reopen',
            python_callable=custom_methods.filter_timesheets_for_reopen
        )

        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id="reopen_timesheets",
            items=lambda: rail.result('filter_timesheets_to_reopen', {}).get('reopen', []),
            endpoint="/services/TimesheetApprovalService1.svc/Reopen",
            data=lambda item: {
                "timesheetUri": item['timesheet_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is reopened by the Integration (Guessing Hours Update)"
            }
        )

        batch_task_end = rail.EmptyOperator(task_id='batch_task_end')

        trigger_process_each_entry = rail.trigger_parallel_dagrun(
            task_id='trigger_process_each_entry',
            items=lambda: rail.result('get_guessing_records_for_user'),
            trigger_dag_id=config.process_each_entry,
            parallel_count=config.trigger_process_entries_parallel_dagrun_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'input_data': {
                    **item,
                    'entry_date': item.get('entry_date'),
                    'task_uri': item.get('task_uri'),
                },
                'user_uri': rail.result('get_user_details')['userDetails']['uri'],
                'user_log': rail.result('create_process_user_log'),
            }
        )

        submit_waiting_approval_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id='submit_waiting_approval_timesheets',
            items=lambda: rail.result('filter_timesheets_to_reopen', {}).get('submit_for_approval', []),
            endpoint="/services/TimesheetApprovalService1.svc/Submit2",
            data=lambda item: {
                "timesheetUri": item['timesheet_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is submitted by the Integration (Guessing Hours Update)"
            }
        )

        force_approve_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id='force_approve_timesheets',
            items=lambda: rail.result('filter_timesheets_to_reopen', {}).get('force_approve', []),
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda item: {
                "timesheetUri": item['timesheet_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": "Timesheet is force-approved by the Integration (Guessing Hours Update)"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_process_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employee_id': '{{ dag_run.conf.input_data.employee_id }}',
                'user_name': '{{ dag_run.conf.input_data.user_name }}',
                'entry_date': '',
                'original_hours': '',
                'task_name': '',
                'project_name': '',
                'org_structure_code': '',
                'status': 'Error',
                'action': 'Validation',
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_task_end
        can_run_batch_task >> rail.Label('No') >> create_process_user_log

        create_process_user_log >> get_guessing_records_for_user >> get_unique_timesheet_start_dates >> get_user_details >> if_user_uri_present
        if_user_uri_present >> rail.Label("No") >> log_user_missing_in_replicon
        if_user_uri_present >> rail.Label("Yes") >> get_timesheet_details >> filter_timesheets_to_reopen \
            >> reopen_timesheets >> batch_task_end >> trigger_process_each_entry \
                >> submit_waiting_approval_timesheets >> force_approve_timesheets >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
