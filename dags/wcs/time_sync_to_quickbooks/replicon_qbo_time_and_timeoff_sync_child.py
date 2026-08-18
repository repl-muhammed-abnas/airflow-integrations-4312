from datetime import timedelta
import rail
from wcs.time_sync_to_quickbooks.utils import custom_methods, request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.replicon_qbo_time_and_timeoff_sync_child_id,
        description=f"WCS Time Sync from Replicon to QuickBooks sync child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.replicon_qbo_time_and_timeoff_sync_child_max_active_run,
        default_args={
            "execution_timeout": timedelta(hours=1),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_conf"
        )

        lookup_qbo_employee = rail.InternalQuickbooksAPIOperator(
            task_id='lookup_qbo_employee',
            request_method='GET',
            endpoint='/query',
            intuit_conn_id=config.intuit_conn_id,
            query_params=lambda dag_run: {
                'query': (
                    "SELECT * FROM Employee WHERE PrintOnCheckName = '"
                    + (dag_run.conf.get('first_name', '') + ' ' + dag_run.conf.get('last_name', '')).replace("'", "\\'")
                    + "'"
                )
            }
        )

        get_qbo_employee_id = rail.PythonOperator(
            task_id='get_qbo_employee_id',
            python_callable=custom_methods.get_qbo_employee_id
        )

        is_employee_found = rail.IfOperator(
            task_id='is_employee_found',
            test=lambda: rail.result('get_qbo_employee_id') is not None,
            yes_task='calculate_hours_minutes',
            no_task='log_employee_not_found'
        )

        calculate_hours_minutes = rail.PythonOperator(
            task_id='calculate_hours_minutes',
            python_callable=custom_methods.parse_hours_minutes
        )

        is_valid_pay_code = rail.IfOperator(
            task_id='is_valid_pay_code',
            test=lambda dag_run: dag_run.conf.get('pay_code_code', '') in config.VALID_PAY_CODES,
            yes_task='determine_effective_pay_type',
            no_task='log_skipped_pay_type'
        )

        determine_effective_pay_type = rail.PythonOperator(
            task_id='determine_effective_pay_type',
            python_callable=lambda dag_run: custom_methods.determine_effective_pay_type(dag_run)
        )

        find_pay_item_id = rail.PythonOperator(
            task_id='find_pay_item_id',
            python_callable=lambda: custom_methods.find_pay_item_id(config)
        )

        is_pay_item_found = rail.IfOperator(
            task_id='is_pay_item_found',
            test=lambda: rail.result('find_pay_item_id') is not None,
            yes_task='post_time_activity',
            no_task='log_pay_item_not_found'
        )

        post_time_activity = rail.InternalQuickbooksAPIOperator(
            task_id='post_time_activity',
            request_method='POST',
            endpoint='/timeactivity',
            intuit_conn_id=config.intuit_conn_id,
            request_body=lambda dag_run: request_payload.build_time_activity_payload(dag_run)
        )

        log_synced = rail.WriteLogOperator(
            task_id='log_synced',
            log="{{ dag_run.conf.processing_log }}",
            severity='Info',
            message='Synced successfully',
            properties=lambda dag_run: {
                'Jobid': dag_run.conf["process_timesheet_data_child_job_id"],
                'username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype': custom_methods.build_pipe_value(dag_run),
                'Status': 'Success',
                'details': 'Synced successfully',
                'childjobid': rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_pay_item_not_found = rail.WriteLogOperator(
            task_id='log_pay_item_not_found',
            log="{{ dag_run.conf.processing_log }}",
            severity='Info',
            message='Pay item reference not available for the required user',
            properties=lambda dag_run: {
                'Jobid': dag_run.conf["process_timesheet_data_child_job_id"],
                'username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype': custom_methods.build_pipe_value(dag_run),
                'Status': 'Not Synced',
                'details': 'Pay item reference not available for the required user',
                'childjobid': rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_skipped_pay_type = rail.WriteLogOperator(
            task_id='log_skipped_pay_type',
            log="{{ dag_run.conf.processing_log }}",
            severity='Info',
            message='Pay type is not defined in the required pay types',
            properties=lambda dag_run: {
                'Jobid': dag_run.conf["process_timesheet_data_child_job_id"],
                'username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype': custom_methods.build_skipped_pipe_value(dag_run),
                'Status': 'Skipped',
                'details': 'Pay type is not defined in the required pay types',
                'childjobid': rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        log_employee_not_found = rail.WriteLogOperator(
            task_id='log_employee_not_found',
            log="{{ dag_run.conf.processing_log }}",
            severity='Warning',
            message='Required employee not available in QuickBooks Online',
            properties=lambda dag_run: {
                'Jobid': dag_run.conf["process_timesheet_data_child_job_id"],
                'username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype': custom_methods.build_not_found_pipe_value(dag_run),
                'Status': 'Not Synced',
                'details': 'Required employee not available in QuickBooks Online',
                'childjobid': rail.render_template("{{ dag_run_ecid() }}")
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.processing_log }}",
            severity='Error',
            message='Unexpected error during time sync processing',
            properties=lambda dag_run: {
                "Jobid": dag_run.conf.get("process_timesheet_data_child_job_id", ''),
                'username|timesheetperiod|Entrydate|paycodehours|syncedhours|paytype': custom_methods.build_pipe_value(dag_run),
                'Status': 'Error',
                'details': rail.render_template('{{ get_error_message() }}'),
                'childjobid': rail.render_template("{{ dag_run_ecid() }}")
            }

        )

        lookup_qbo_employee >> get_qbo_employee_id >> is_employee_found

        is_employee_found >> rail.Label('Yes') >> calculate_hours_minutes >> is_valid_pay_code
        is_employee_found >> rail.Label('No') >> log_employee_not_found >> catch_and_log_error

        is_valid_pay_code >> rail.Label('Yes') >> determine_effective_pay_type >> find_pay_item_id >> is_pay_item_found
        is_valid_pay_code >> rail.Label('No') >> log_skipped_pay_type >> catch_and_log_error

        is_pay_item_found >> rail.Label('Yes') >> post_time_activity >> log_synced >> catch_and_log_error
        is_pay_item_found >> rail.Label('No') >> log_pay_item_not_found >> catch_and_log_error

        
    return dag


rail.for_each_instance(create_child_dag)