from datetime import timedelta
from airflow.models import Variable
from michaelkorstna.timeoff_export_to_workday.utils.custom_methods import (
    format_booking_date,
    create_unique_record_id,
    extract_reason_from_timeoff_details,
    format_date_yyyy_mm_dd
)
from michaelkorstna.timeoff_export_to_workday.utils import request_payload
from rail.lib.ecid import get_dagrun_ecid
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_timeoff_records_to_workday_dag_id,
        description=f'MichaelKors Process Records to Workday {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_records_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Create log for this processing run
        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        # Format booking date (YYYYMMDD format for unique ID)
        format_booking_date_task = rail.PythonOperator(
            task_id='format_booking_date',
            python_callable=format_booking_date
        )

        # Create unique ID for time off entry
        create_unique_id_task = rail.PythonOperator(
            task_id='create_unique_id',
            python_callable=create_unique_record_id
        )

        # Check if record type is New
        if_recordtype_new = rail.IfOperator(
            task_id='if_recordtype_new',
            test=lambda dag_run: dag_run.conf["recordtype"] == "New",
            yes_task="if_timeofftypename_contains_uk_other_paid_leave",
            no_task="if_recordtype_delta"
        )

        # Check if timeoff type contains UK Other Paid Leave
        if_timeofftypename_contains_uk_other_paid_leave = rail.IfOperator(
            task_id='if_timeofftypename_contains_uk_other_paid_leave',
            test=lambda dag_run: '[UK] Other Paid Leave' in dag_run.conf["timeofftypename"],
            yes_task="get_timeoff_details",
            no_task="get_post_timeoff_endpoint_new"
        )

        # Get timeoff details for UK Other Paid Leave (to extract reason)
        get_timeoff_details = rail.RepliconServiceOperator(
            task_id='get_timeoff_details',
            endpoint="/services/TimeOffService1.svc/GetTimeOffDetails2",
            data=request_payload.get_timeoff_details_payload
        )

        # Extract reason from extension fields
        extract_reason = rail.PythonOperator(
            task_id='extract_reason',
            python_callable=extract_reason_from_timeoff_details
        )

        # Get endpoint for new time off entry
        get_post_timeoff_endpoint_new = rail.PythonOperator(
            task_id='get_post_timeoff_endpoint_new',
            python_callable=request_payload.create_get_workday_post_timeoff_endpoint(config)
        )

        # Build payload for new time off entry
        build_payload_timeoff_new = rail.PythonOperator(
            task_id='build_payload_timeoff_new',
            python_callable=lambda dag_run: request_payload.build_enter_time_off_payload(dag_run, include_comment=True)
        )

        # Enter time off for new records (with comment for UK Other Paid Leave)
        enter_time_off_new = rail.SimpleHttpOperator(
            task_id='enter_time_off_new',
            method='POST',
            http_conn_id=config.workday_isu_replicon_inbound_http_conn_id,
            endpoint='{{ result("get_post_timeoff_endpoint_new") }}',
            headers={"Content-Type": "application/json"},
            data='{{ result("build_payload_timeoff_new") }}'
        )

        # Log successful new entry
        log_new_success = rail.WriteLogOperator(
            task_id='log_new_success',
            log='{{ result("create_log") }}',
            severity='Success',
            message=f"Workdayref:{{{{ result('enter_time_off_new') }}}}|Repliconref:{{{{ result('create_unique_id') }}}}",
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'loginname': dag_run.conf['loginname'],
                'timeoffbookingid': dag_run.conf['timeoffbookingid'],
                'timeofftypename|timeoffdescription': f"{dag_run.conf['timeofftypename']}|{dag_run.conf['timeofftypedescription']}",
                'hours': dag_run.conf['hours'],
                'entrydate': dag_run.conf['entrydate'],
                'status': 'Success',
                'details': f"Workdayref:{rail.result('enter_time_off_new')}|Repliconref:{rail.result('create_unique_id')}",
                'jobid': get_dagrun_ecid(dag_run),
                'transactiontype|childjob': f"{dag_run.conf['recordtype']}|{get_dagrun_ecid(dag_run)}"
            }
        )

        # Check if record type is Delta
        if_recordtype_delta = rail.IfOperator(
            task_id='if_recordtype_delta',
            test=lambda dag_run: dag_run.conf["recordtype"] == "Delta",
            yes_task="format_date_for_report",
            no_task="log_unknown_recordtype"
        )

        # Format date for Workday report query (YYYY-MM-DD format)
        format_date_for_report = rail.PythonOperator(
            task_id='format_date_for_report',
            python_callable=lambda dag_run: format_date_yyyy_mm_dd(dag_run.conf['entrydate'])
        )

        # Check if we should skip the actual Workday report query (trial uses mock data from variable)
        if_skip_workday_report = rail.IfOperator(
            task_id='if_skip_workday_report',
            test=lambda: config.skip_workday_report_query,
            yes_task='create_workday_timeoff_collection',
            no_task='get_workday_report_endpoint'
        )

        # Get endpoint for Workday report query
        get_workday_report_endpoint = rail.PythonOperator(
            task_id='get_workday_report_endpoint',
            python_callable=request_payload.create_get_workday_report_endpoint(config)
        )

        # Query Workday custom report for existing record (Get timeoff via raas)
        query_workday_report = rail.SimpleHttpOperator(
            task_id='query_workday_report',
            method='GET',
            http_conn_id=config.workday_isu_replicon_time_off_entries_http_conn_id,
            endpoint='{{ result("get_workday_report_endpoint") }}',
            headers={'Accept': 'application/json'}
        )

        # Create collection from Workday report response
        create_workday_timeoff_collection = rail.CreateCollectionOperator(
            task_id='create_workday_timeoff_collection',
            name='workday_timeoff',
            source=request_payload.create_get_workday_report_entries(config),
            columns={
                "Date_and_Time_Approved": "date_and_time_approved",
                "Effective_date": "effective_date",
                "Time_Off_Code": "time_off_code",
                "Request_or_Correction": "request_or_correction",
                "Amount": "amount",
                "Employee_ID": "employee_id",
                "Employee_Name": "employee_name"
            }
        )

        # Query collection to find existing timeoff records
        query_existing_timeoff = rail.QueryCollectionOperator(
            task_id='query_existing_timeoff',
            query="""SELECT * FROM workday_timeoff WHERE time_off_code = '{{ dag_run.conf.timeofftypedescription }}' AND date_and_time_approved LIKE '{{ result("format_date_for_report") }}%' AND employee_id = '{{ dag_run.conf.employeeid }}' AND request_or_correction = 'Absence Correction'"""
        )

        if_existing_timeoff_found = rail.IfOperator(
            task_id='if_existing_timeoff_found',
            test='{{ result("query_existing_timeoff", "length") > 0 }}',
            yes_task='log_delta_ignored',
            no_task='get_post_timeoff_endpoint_delta'
        )

        # Log delta record as ignored (already present in Workday)
        log_delta_ignored = rail.WriteLogOperator(
            task_id='log_delta_ignored',
            log='{{ result("create_log") }}',
            severity='Exception',
            message='Booking already present - {{ result("query_existing_timeoff") }}',
            properties={
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'timeoffbookingid': '{{ dag_run.conf.timeoffbookingid }}',
                'timeofftypename|timeoffdescription': '{{ dag_run.conf.timeofftypename }}|{{ dag_run.conf.timeofftypedescription }}',
                'hours': '{{ dag_run.conf.hours }}',
                'entrydate': '{{ dag_run.conf.entrydate }}',
                'status': 'Exception',
                'details': 'Booking already present - {{ result("query_existing_timeoff") }}',
                'jobid': '{{ dag_run_ecid() }}',
                'transactiontype|childjob': '{{ dag_run.conf.recordtype }}|{{ dag_run_ecid() }}'
            }
        )

        # Get endpoint for delta time off operations
        get_post_timeoff_endpoint_delta = rail.PythonOperator(
            task_id='get_post_timeoff_endpoint_delta',
            python_callable=request_payload.create_get_workday_post_timeoff_endpoint(config)
        )

        # Build payload for delta correction
        build_payload_timeoff_delta_adjust = rail.PythonOperator(
            task_id='build_payload_timeoff_delta_adjust',
            python_callable=lambda dag_run: request_payload.build_correct_time_off_payload(dag_run)
        )

        # Correct/adjust time off for delta records
        adjust_time_off = rail.SimpleHttpOperator(
            task_id='adjust_time_off',
            method='POST',
            http_conn_id=config.workday_isu_replicon_inbound_http_conn_id,
            endpoint='{{ result("get_post_timeoff_endpoint_delta") }}',
            headers={"Content-Type": "application/json"},
            data='{{ result("build_payload_timeoff_delta_adjust") }}'
        )

        # Check if adjustment failed
        check_adjust_failed = rail.IfOperator(
            task_id='check_adjust_failed',
            test='{{ get_failed_upstream_task_ids() | length > 0 }}',
            yes_task='build_payload_timeoff_delta_new',
            no_task='log_delta_success'
        )

        # Build payload for delta new entry
        build_payload_timeoff_delta_new = rail.PythonOperator(
            task_id='build_payload_timeoff_delta_new',
            python_callable=lambda dag_run: request_payload.build_enter_time_off_payload(dag_run, include_comment=False)
        )

        # Enter new time off if adjustment/correction failed
        enter_time_off_delta = rail.SimpleHttpOperator(
            task_id='enter_time_off_delta',
            method='POST',
            http_conn_id=config.workday_isu_replicon_inbound_http_conn_id,
            endpoint='{{ result("get_post_timeoff_endpoint_delta") }}',
            headers={"Content-Type": "application/json"},
            data='{{ result("build_payload_timeoff_delta_new") }}'
        )

        # Log successful delta entry (new entry added)
        log_delta_new_success = rail.WriteLogOperator(
            task_id='log_delta_new_success',
            log='{{ result("create_log") }}',
            severity='Success',
            message=f"New entry added, since there is no existing entry - Workdayref:{{{{ result('enter_time_off_delta') }}}}|Repliconref:{{{{ result('create_unique_id') }}}}",
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'loginname': dag_run.conf['loginname'],
                'timeoffbookingid': dag_run.conf['timeoffbookingid'],
                'timeofftypename|timeoffdescription': f"{dag_run.conf['timeofftypename']}|{dag_run.conf['timeofftypedescription']}",
                'hours': dag_run.conf['hours'],
                'entrydate': dag_run.conf['entrydate'],
                'status': 'Success',
                'details': f"New entry added, since there is no existing entry - Workdayref:{rail.result('enter_time_off_delta')}|Repliconref:{rail.result('create_unique_id')}",
                'jobid': get_dagrun_ecid(dag_run),
                'transactiontype|childjob': f"{dag_run.conf['recordtype']}|{get_dagrun_ecid(dag_run)}"
            }
        )

        # Log successful delta adjustment
        log_delta_success = rail.WriteLogOperator(
            task_id='log_delta_success',
            log='{{ result("create_log") }}',
            severity='Success',
            message=f"Workdayref:{{{{ result('adjust_time_off') }}}}|Repliconref:{{{{ result('create_unique_id') }}}}",
            properties=lambda dag_run: {
                'employeeid': dag_run.conf['employeeid'],
                'loginname': dag_run.conf['loginname'],
                'timeoffbookingid': dag_run.conf['timeoffbookingid'],
                'timeofftypename|timeoffdescription': f"{dag_run.conf['timeofftypename']}|{dag_run.conf['timeofftypedescription']}",
                'hours': dag_run.conf['hours'],
                'entrydate': dag_run.conf['entrydate'],
                'status': 'Success',
                'details': f"Workdayref:{rail.result('adjust_time_off')}|Repliconref:{rail.result('create_unique_id')}",
                'jobid': get_dagrun_ecid(dag_run),
                'transactiontype|childjob': f"{dag_run.conf['recordtype']}|{get_dagrun_ecid(dag_run)}"
            }
        )

        # Log unknown record type
        log_unknown_recordtype = rail.WriteLogOperator(
            task_id='log_unknown_recordtype',
            log='{{ result("create_log") }}',
            severity='Exception',
            message='Unknown record type: {{ dag_run.conf.recordtype }}',
            properties={
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'timeoffbookingid': '{{ dag_run.conf.timeoffbookingid }}',
                'timeofftypename|timeoffdescription': '{{ dag_run.conf.timeofftypename }}|{{ dag_run.conf.timeofftypedescription }}',
                'hours': '{{ dag_run.conf.hours }}',
                'entrydate': '{{ dag_run.conf.entrydate }}',
                'status': 'Exception',
                'details': 'Unknown record type: {{ dag_run.conf.recordtype }}',
                'jobid': '{{ dag_run_ecid() }}',
                'transactiontype|childjob': '{{ dag_run.conf.recordtype }}|{{ dag_run_ecid() }}'
            }
        )

        # Error handler
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ result("create_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': '{{ dag_run.conf.employeeid }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'timeoffbookingid': '{{ dag_run.conf.timeoffbookingid }}',
                'timeofftypename|timeoffdescription': '{{ dag_run.conf.timeofftypename }}|{{ dag_run.conf.timeofftypedescription }}',
                'hours': '{{ dag_run.conf.hours }}',
                'entrydate': '{{ dag_run.conf.entrydate }}',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'jobid': '{{ dag_run_ecid() }}',
                'transactiontype|childjob': '{{ dag_run.conf.recordtype }}|{{ dag_run_ecid() }}'
            }
        )

        # Task Dependencies
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_log >> format_booking_date_task >> create_unique_id_task >> if_recordtype_new

        # New record flow
        if_recordtype_new >> rail.Label('Yes') >> if_timeofftypename_contains_uk_other_paid_leave

        # UK Other Paid Leave check flow
        if_timeofftypename_contains_uk_other_paid_leave >> rail.Label('Yes') >> get_timeoff_details
        get_timeoff_details >> extract_reason >> get_post_timeoff_endpoint_new >> build_payload_timeoff_new >> enter_time_off_new

        # Regular new record flow (no UK Other Paid Leave)
        if_timeofftypename_contains_uk_other_paid_leave >> rail.Label('No') >> get_post_timeoff_endpoint_new >> build_payload_timeoff_new >> enter_time_off_new
        enter_time_off_new >> log_new_success >> catch_and_log_errors

        # Check for Delta record type
        if_recordtype_new >> rail.Label('No') >> if_recordtype_delta

        # Delta record flow (recordtype == "Delta")
        if_recordtype_delta >> rail.Label('Yes') >> format_date_for_report >> if_skip_workday_report
        if_skip_workday_report >> rail.Label('Yes') >> create_workday_timeoff_collection
        if_skip_workday_report >> rail.Label('No') >> get_workday_report_endpoint >> query_workday_report >> create_workday_timeoff_collection
        create_workday_timeoff_collection >> query_existing_timeoff >> if_existing_timeoff_found
        if_existing_timeoff_found >> rail.Label('Yes') >> log_delta_ignored >> catch_and_log_errors
        if_existing_timeoff_found >> rail.Label('No') >> get_post_timeoff_endpoint_delta >> build_payload_timeoff_delta_adjust >> adjust_time_off >> check_adjust_failed
        check_adjust_failed >> rail.Label('Yes') >> build_payload_timeoff_delta_new >> enter_time_off_delta >> log_delta_new_success >> catch_and_log_errors
        check_adjust_failed >> rail.Label('No') >> log_delta_success >> catch_and_log_errors

        # Non-New/non-Delta records get logged
        if_recordtype_delta >> rail.Label('No') >> log_unknown_recordtype >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
