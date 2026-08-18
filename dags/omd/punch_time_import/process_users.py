from datetime import timedelta
from airflow.models import Variable
import rail
from omd.punch_time_import.utils import python_callable, request_payload, response_filters

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_unique_users_child,
        description=f'OMD Punch Time Import Child - Process Unique Users {config.instance}',
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
            end_task='catch_and_log_errors',
        )

        create_process_user_log = rail.CreateLogOperator(
            task_id="create_process_user_log"
        )

        create_timesheet_reopened_log = rail.CreateLogOperator(
            task_id="create_timesheet_reopened_log"
        )

        get_all_records_for_user = rail.QueryCollectionOperator(
            task_id="get_all_records_for_user",
            query="""SELECT * FROM valid_entries fd WHERE fd.employee_id =:EMP_ID""",
            query_params={
                "EMP_ID": "{{dag_run.conf.employee_id}}"
            }
        )

        get_user_uri = rail.RepliconServiceOperator(
            task_id="get_user_uri",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_data_payload,
            data_handler=lambda res: res[0]['userDetails']['uri'] if (res and res[0]['userDetails']['isEnabled']) else None
        )

        max_min_date_for_user = rail.PythonOperator(
            task_id='max_min_date_for_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.get_max_min_date_for_user
        )

        punch_in_out_for_each_date = rail.PythonOperator(
            task_id='punch_in_out_for_each_date',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable.get_punch_in_out_for_each_date
        )

        prepare_to_add_in_log = rail.PythonOperator(
            task_id="prepare_to_add_in_log",
            python_callable=python_callable.prepare_to_add_in_log,
            op_args=[punch_in_out_for_each_date.task_id],
            show_return_value_in_logs=False
        )

        if_user_uri_present = rail.IfOperator(
            task_id ='if_user_uri_present',
            test = lambda: bool(rail.result('get_user_uri')),
            yes_task="get_user_timesheet_details",
            no_task="log_user_missing_in_replicon"
        )

        get_user_timesheet_details = rail.RepliconServiceOperator(
            task_id="get_user_timesheet_details",
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=request_payload.get_all_timesheet_for_user,
            data_handler=response_filters.get_timesheet_details
        )

        map_timesheet_with_user_data = rail.PythonOperator(
            task_id="map_timesheet_with_user_data",
            python_callable=python_callable.map_timesheet_with_user_data,
            op_args=[punch_in_out_for_each_date.task_id,
                     get_user_timesheet_details.task_id],
            show_return_value_in_logs=False
        )

        reopen_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id = "reopen_timesheets",
            items="{{result('map_timesheet_with_user_data', 'timesheet_to_reopen') | to_json}}",
            endpoint= "/services/TimesheetApprovalService1.svc/Reopen",
            data={
                "timesheetUri": "{{ item.ts_uri }}",
                "unitOfWorkId": "{{ item.uuid }}",
                "comments": "Timesheet is reopened by Integration"
            }
        )

        log_timesheet_reopened = rail.WriteLogOperator(
            task_id="log_timesheet_reopened",
            log="{{ result('create_timesheet_reopened_log') }}",
            items="{{result('map_timesheet_with_user_data', 'timesheet_to_reopen') | to_json}}",
            message="TS is reopened",
            severity=lambda item:"approved" if item['timesheet_status_uri'].endswith('approved') else "waiting",
            properties=lambda item, dag_run: {
                "ts_uri": item['ts_uri'],
                "timesheet_status_uri": item['timesheet_status_uri'],
                "timesheet_status": item['timesheet_status'],
                "employee_id": dag_run.conf['employee_id'],
                "user_uri": item["user_uri"]
            }
        )

        get_existing_punch_for_user = rail.RepliconServiceOperator(
            task_id="get_existing_punch_for_user",
            endpoint='/services/TimePunchService1.svc/BulkGetTimePunchDetailsForUsersAndDateRange',
            data=lambda:{
                "userUris": [
                    rail.result('get_user_uri')
                ],
                "dateRange": {
                    "startDate": rail.parse_date(rail.result('max_min_date_for_user')[0], python_callable.FEED_ENTRYDATE_DATE_FORMAT),
                    "endDate": rail.parse_date(rail.result('max_min_date_for_user')[1], python_callable.FEED_ENTRYDATE_DATE_FORMAT),
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                },
                "timePunchTimeSegmentDateRangeFilterOption": "urn:replicon:time-punch-time-segment-date-range-filter-option:punch-user-time-zone"
                },
            data_handler=lambda res: res[0]['timePunches']
        )

        map_time_punch_data = rail.PythonOperator(
            task_id="map_time_punch_data",
            python_callable=python_callable.map_time_punch_data,
            op_args=[punch_in_out_for_each_date.task_id,
                     get_existing_punch_for_user.task_id],
            show_return_value_in_logs=False
        )

        if_punches_to_delete = rail.IfOperator(
            task_id='if_punches_to_delete',
            test='''{{ result('map_time_punch_data', 'punch_to_delete') | is_truthy and result('map_time_punch_data', 'punch_to_delete') | length > 0 }}''',
            yes_task="bulk_delete_punches",
            no_task="add_bulk_punches",
        )

        bulk_delete_punches =  rail.RepliconServiceOperator(
            task_id="bulk_delete_punches",
            endpoint='/services/TimePunchService1.svc/BulkDelete',
            data=lambda: {
                "timePunchUris": rail.result('map_time_punch_data', 'punch_to_delete')
            }
        )

        add_bulk_punches =  rail.RepliconServiceOperator(
            task_id="add_bulk_punches",
            endpoint='/services/TimePunchService1.svc/BulkPutTimePunch4',
            data=request_payload.add_bulk_punches_payload
        )

        log_add_update_punches = rail.WriteLogOperator(
            task_id='log_add_update_punches',
            log= "{{ result('create_process_user_log') }}",
            items="{{ result('prepare_to_add_in_log') | to_json }}",
            message="Punch added/updated successfully",
            severity="Success",
            properties={
                "EmployeeCode": "{{ dag_run.conf.employee_id }}",
                "LogDate": "{{ item.entry_date }}",
                "log_time": "{{ item.punch_time }}",
                "status": "Success",
                "details": 'Time Punch Added Successfully'
            }
        )

        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            log= "{{ result('create_process_user_log') }}",
            items="{{ result('prepare_to_add_in_log') | to_json }}",
            message="User Missing in Replicon",
            severity="Exception",
            properties={
                "EmployeeCode": "{{ dag_run.conf.employee_id }}",
                "LogDate": "{{ item.entry_date }}",
                "log_time": "{{ item.punch_time }}",
                "status": "Exception",
                "details": 'EmployeeCode {{ dag_run.conf.employee_id }} is not present or disabled in replicon.'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{result("create_process_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "EmployeeCode": '{{ dag_run.conf.employee_id }}',
                "LogDate": "",
                "log_time": "",
                "status": "Error",
                "details": '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_process_user_log

        create_process_user_log >> create_timesheet_reopened_log >> get_all_records_for_user >> get_user_uri >> max_min_date_for_user
        max_min_date_for_user >> punch_in_out_for_each_date >> prepare_to_add_in_log >> if_user_uri_present
        if_user_uri_present >> rail.Label("Yes") >> get_user_timesheet_details
        if_user_uri_present >> rail.Label("No") >> log_user_missing_in_replicon >> catch_and_log_errors
        get_user_timesheet_details >> \
        map_timesheet_with_user_data >> reopen_timesheets >> log_timesheet_reopened >> get_existing_punch_for_user >> \
        map_time_punch_data >> if_punches_to_delete
        if_punches_to_delete >> rail.Label("Yes") >> bulk_delete_punches >> add_bulk_punches
        if_punches_to_delete >> rail.Label("No") >> add_bulk_punches >> log_add_update_punches >> \
        catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
