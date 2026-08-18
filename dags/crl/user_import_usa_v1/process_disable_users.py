from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_usa_v1.utils import request_payload

null= None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_disable_users_dagid,
        description='CRL User Import USA - Process Disable Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_disable_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_enddate_greater_than_start_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_enddate_greater_than_start_date',
            end_task='catch_and_log_errors',
        )

        is_enddate_greater_than_start_date = rail.IfOperator(
            task_id ='is_enddate_greater_than_start_date',
            test = request_payload.validate_enddate,
            yes_task="update_end_date",
            no_task="log_endate_exception"
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data= request_payload.update_end_date_payload,
        )

        is_enddate_in_future = rail.IfOperator(
            task_id="is_enddate_in_future",
            test=request_payload.is_enddate_in_future,
            yes_task="log_end_date_future",
            no_task="disable_login"
        )

        log_end_date_future = rail.EmptyOperator(
            task_id = 'log_end_date_future',
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = lambda dag_run:f"User not Disabled,{'End date' if dag_run.conf['end_date'] else 'Change Effective Date'} Prior to Start date",
            severity='Exception',
            properties =lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Validation",
                "status": "Exception",
                'details': f"User not Disabled,{'End date' if dag_run.conf['end_date'] else 'Change Effective Date'} Prior to Start date",
            }
        )

        is_status_ignore_zero_accrual = rail.IfOperator(
            task_id = "is_status_ignore_zero_accrual",
            test=lambda dag_run: dag_run.conf['emp_status'] in config.IGNORE_STATUS_ZERO_ACCRUAL,
            yes_task="log_user_disablement",
            no_task="is_contingent"
        )

        is_contingent= rail.IfOperator(
            task_id = "is_contingent",
            test=lambda dag_run: dag_run.conf['is_contingent'] == 'Y',
            yes_task="log_user_disablement",
            no_task="process_time_off_type_no_accrual"
        )

        log_user_disablement = rail.WriteLogOperator(
            task_id = 'log_user_disablement',
            log = '{{ dag_run.conf.user_log }}',
            message = request_payload.get_disable_message,
            severity=request_payload.get_disable_status,
            properties = lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Disable",
                "status": request_payload.get_disable_status(dag_run),
                'details': request_payload.get_disable_message(dag_run),
            }
        )

        process_time_off_type_no_accrual= rail.TriggerDagRunOperator(
            task_id='process_time_off_type_no_accrual',
            trigger_dag_id=config.process_timeoff_type_no_accrual_dagid,
            conf=lambda dag_run:{
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                'end_date': dag_run.conf['end_date'],
                'useruri': dag_run.conf['useruri'],
                'starting_balance_script_uri': dag_run.conf['starting_balance_script_uri'],
                'prevent_balance_overdraw_uri': dag_run.conf['prevent_balance_overdraw_uri'],
                "user_log": dag_run.conf['user_log'],
                'todays_date':dag_run.conf['todays_date'],
                'action': 'disable',
                "change_effective_date": dag_run.conf['change_effective_date'],
                'event': dag_run.conf['event'],
                'event_reason_code':dag_run.conf['event_reason_code'],
            },
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_time_off_type_no_accrual = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_time_off_type_no_accrual',
            dag_runs='{{ result("process_time_off_type_no_accrual") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_time_off_type_error_logs_disable_user = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_off_type_error_logs_disable_user',
            dag_runs='{{ result("process_time_off_type_no_accrual") }}',
            dagrun_task_id='catch_and_log_errors',
            flatten=True,
        )

        has_any_error_present = rail.IfOperator(
            task_id="has_any_error_present",
            test="{{ result('gather_time_off_type_error_logs_disable_user') | is_truthy }}",
            yes_task= 'log_error_present',
            no_task='log_user_disablement'
        )

        log_error_present = rail.EmptyOperator(
            task_id='log_error_present'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{dag_run.conf.emp_id}}",
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": 'Disable',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_enddate_greater_than_start_date >> rail.Label("No") >> log_endate_exception >> catch_and_log_errors
        is_enddate_greater_than_start_date >> rail.Label("No") >> update_end_date >> is_enddate_in_future

        is_enddate_in_future >> rail.Label('Yes') >> log_end_date_future  >> log_user_disablement
        is_contingent >> rail.Label('Yes') >> log_user_disablement
        is_enddate_in_future >> rail.Label('No') >> disable_login >> is_status_ignore_zero_accrual
        is_status_ignore_zero_accrual >> rail.Label('Yes') >> log_user_disablement
        is_status_ignore_zero_accrual >> rail.Label('No') >> is_contingent
        is_contingent >> rail.Label('No') >> process_time_off_type_no_accrual
        process_time_off_type_no_accrual >> wait_for_process_time_off_type_no_accrual >> gather_time_off_type_error_logs_disable_user
        gather_time_off_type_error_logs_disable_user >> has_any_error_present

        has_any_error_present >> rail.Label('Yes') >> log_error_present >> catch_and_log_errors
        has_any_error_present >> rail.Label('No') >> log_user_disablement >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
