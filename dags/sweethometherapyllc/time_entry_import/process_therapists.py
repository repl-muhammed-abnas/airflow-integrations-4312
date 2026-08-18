import rail
from datetime import datetime, timedelta
from uuid import uuid4
from sweethometherapyllc.time_entry_import.utils import request_payload, custom_methods, response_filters
from airflow.models import Variable

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_unique_therapists_child,
        description=f'sweethometherapyllc Time Import Child - Process Unique Therapists {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_records_for_user = rail.QueryCollectionOperator(
            task_id="get_all_records_for_user",
            query="""SELECT * FROM aggregated_entries fd WHERE fd.therapist =:therapist""",
            query_params={
                "therapist": "{{dag_run.conf.therapist}}"
            },
            name="all_user_records"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda dag_run: request_payload.get_user_data_payload(dag_run.conf["therapist"]),
            data_handler=lambda res: res[0] if (res and res[0]['userDetails']['isEnabled']) else None 
        )

        if_user_present = rail.IfOperator(
            task_id ='if_user_present',
            test = lambda: bool(rail.result('get_user_details')),
            yes_task="if_user_assigned_with_correct_timesheet_template",
            no_task="log_user_missing_in_replicon"
        )
        
        log_user_missing_in_replicon = rail.WriteLogOperator(
            task_id='log_user_missing_in_replicon',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Therapist is not present in Replicon or is disabled for Therapist: {{ dag_run.conf.therapist }}',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Therapist is not present or disabled in replicon for Therapist: {{ dag_run.conf.therapist }}'
            },
        )

        if_user_assigned_with_correct_timesheet_template = rail.IfOperator(
            task_id ='if_user_assigned_with_correct_timesheet_template',
            test = lambda: rail.result('get_user_details').get("timesheetTemplate").get("name")=="Time Entry with Activities",
            yes_task="get_unique_entry_date_for_user",
            no_task="log_user_not_assigned_with_correct_timesheet_template"
        )
        
        log_user_not_assigned_with_correct_timesheet_template = rail.WriteLogOperator(
            task_id='log_user_not_assigned_with_correct_timesheet_template',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Therapist is not assigned with correct timesheet template for Therapist: {{ dag_run.conf.therapist }}',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Therapist is not assigned with correct timesheet template for Therapist: {{ dag_run.conf.therapist }}'
            },
        )

        get_unique_entry_date_for_user = rail.QueryCollectionOperator(
            task_id="get_unique_entry_date_for_user",
            query="""SELECT DISTINCT date_of_service FROM aggregated_entries fd WHERE fd.therapist =:therapist""",
            query_params={
                "therapist": "{{dag_run.conf.therapist}}"
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
                    item['date_of_service'], config.entry_dateformat),
                "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=response_filters.get_timesheet_detail_for_item
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

        trigger_process_each_entry = rail.trigger_parallel_dagrun(
            task_id='trigger_process_each_entry',
            items="{{ result('get_all_records_for_user') }}",
            trigger_dag_id=config.process_each_entry_child,
            parallel_count=5,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                **item,
                **dag_run.conf,
                'activities': rail.result('get_user_details')['assignedActivities'],
                'user_uri': rail.result('get_user_details')['userDetails']['uri']
            },
        )

        get_successful_entry_dates = rail.PythonOperator(
            task_id='get_successful_entry_dates',
            python_callable=lambda dag_run: custom_methods.get_successful_entry_dates(dag_run)
        )

        get_ts_uris_to_approve = rail.PythonOperator(
            task_id='get_ts_uris_to_approve',
            python_callable=lambda: custom_methods.get_timesheet_uris_for_dates(
                rail.result('get_timesheet_details'),
                rail.result('get_successful_entry_dates'),
                rail.result('get_submitted_ts_uris')
            )
        )

        force_approve_timesheets = rail.RepliconServiceCallForEachItemOperator(
            task_id="force_approve_timesheets",
            items="{{ result('get_ts_uris_to_approve') | to_json }}",
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda item: {
                "timesheetUri": item['ts_uri'],
                "unitOfWorkId": str(uuid4()),
                "comments": config.timesheet_approve_remarks
            },
        )
        
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': "Error",
                'action': "Validation",
                'details': '{{ get_error_message() }}'
            },
        )

        get_all_records_for_user \
        >> get_user_details \
        >> if_user_present

        if_user_present >> rail.Label("No") \
        >> log_user_missing_in_replicon \
        >> catch_and_log_errors

        if_user_present >> rail.Label("Yes") \
        >>if_user_assigned_with_correct_timesheet_template >> rail.Label("Yes") >> get_unique_entry_date_for_user
        if_user_assigned_with_correct_timesheet_template >> rail.Label("No") >> log_user_not_assigned_with_correct_timesheet_template >> catch_and_log_errors

        get_unique_entry_date_for_user \
        >> get_timesheet_details \
        >> get_submitted_ts_uris \
        >> reopen_timesheets >> trigger_process_each_entry \
        >> get_successful_entry_dates >> get_ts_uris_to_approve \
        >> force_approve_timesheets >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)