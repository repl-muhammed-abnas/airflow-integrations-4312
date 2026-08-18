from datetime import timedelta
from airflow.models import Variable
import rail
from mercury_systems_inc.user_import.utils import request_payload, custom_methods

null = None

# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_disable_user_dagid,
        description='MercurySystemsInc User Import Process Disable Users',
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
            task_id='is_enddate_greater_than_start_date',
            test=lambda dag_run:  custom_methods.compare_dates(
                (dag_run.conf['Termination_Date'] or dag_run.conf['Effective_Date']), '>', dag_run.conf['Hire_Date']),
            yes_task="update_end_date",
            no_task="log_endate_exception"
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id='log_endate_exception',
            log='{{ dag_run.conf.user_log }}',
            message="User not Disabled, Termination Date is prior to Hire date",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                "action": "Validation",
                "status": "Exception",
                'details': "User not Disabled, Termination Date is prior to Hire date",
            }
        )

        update_end_date = rail.RepliconServiceOperator(
            task_id='update_end_date',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=lambda dag_run: request_payload.update_end_date_payload(
                dag_run, config),
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.user_uri }}'
            }
        )

        log_existing_timeoff_policies_for_disable_user = rail.RepliconServiceOperator(
            task_id='log_existing_timeoff_policies_for_disable_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            },
            data_handler=lambda res: res["policiesByTimeOffType"] if res["policiesByTimeOffType"] else [
            ]
        )

        log_existing_timeoff_uris_for_user = rail.PythonOperator(
            task_id='log_existing_timeoff_uris_for_user',
            python_callable=lambda: [timeoff_details['timeOffType']['uri'] for timeoff_details in rail.result(
                'log_existing_timeoff_policies_for_disable_user')] if rail.result(
                'log_existing_timeoff_policies_for_disable_user') else []
        )

        if_existing_timeoff_for_user = rail.IfOperator(
            task_id='if_existing_timeoff_for_user',
            test=lambda: rail.result('log_existing_timeoff_uris_for_user'),
            yes_task='dummy_process_stop_accruals_for_existing_timeoff_types',
            no_task='log_user_disabled_succesfully'
        )

        dummy_process_stop_accruals_for_existing_timeoff_types = rail.EmptyOperator(
            task_id='dummy_process_stop_accruals_for_existing_timeoff_types'
        )

        stop_accruals_for_disabled_user_timeoffs = rail.TriggerDagRunForEachItemOperator(
            task_id='stop_accruals_for_disabled_user_timeoffs',
            items=lambda: rail.result('log_existing_timeoff_uris_for_user'),
            trigger_dag_id=config.process_stop_accrual_for_timeoff,
            conf=lambda item, dag_run: {
                'timeoff_uri_for_stopping_accrual': item,
                'existing_policyset_schedule_for_timeoff': rail.find_first_by_attr_and_get_attr(rail.result(
                    'log_existing_timeoff_policies_for_disable_user'), 'timeOffType.uri', item, 'policySetSchedule'),
                "starting_balance_set_to_script_uri": dag_run.conf["starting_balance_set_to_script_uri"],
                "prevent_balance_overdraw_script_uri": dag_run.conf["prevent_balance_overdraw_script_uri"],
                'user_uri': dag_run.conf["user_uri"],
                'effective_date': dag_run.conf['Termination_Date'] or dag_run.conf['Effective_Date'],
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_stop_accruals_for_disabled_user_timeoffs = rail.WaitForDagRunsSensor(
            task_id='wait_for_stop_accruals_for_disabled_user_timeoffs',
            dag_runs="{{ result('stop_accruals_for_disabled_user_timeoffs') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        log_user_disabled_succesfully = rail.WriteLogOperator(
            task_id='log_user_disabled_succesfully',
            log='{{ dag_run.conf.user_log }}',
            message="na",
            severity='Success',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                "action": "Disable",
                "status": "Success",
                'details': "User Disabled Successfully",
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": '{{dag_run.conf.Employee_ID}}',
                "first_name": "{{dag_run.conf.First_Name}}",
                "last_name": "{{dag_run.conf.Last_Name}}",
                "action": 'Disable',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> is_enddate_greater_than_start_date

        is_enddate_greater_than_start_date >> rail.Label(
            "No") >> log_endate_exception >> catch_and_log_errors

        is_enddate_greater_than_start_date >> rail.Label(
            "Yes") >> update_end_date >> disable_login >> log_existing_timeoff_policies_for_disable_user

        log_existing_timeoff_policies_for_disable_user >> log_existing_timeoff_uris_for_user >> if_existing_timeoff_for_user

        if_existing_timeoff_for_user >> rail.Label(
            "Yes") >> dummy_process_stop_accruals_for_existing_timeoff_types >> stop_accruals_for_disabled_user_timeoffs

        stop_accruals_for_disabled_user_timeoffs >> wait_for_stop_accruals_for_disabled_user_timeoffs >> log_user_disabled_succesfully

        if_existing_timeoff_for_user >> rail.Label(
            "No") >> log_user_disabled_succesfully

        log_user_disabled_succesfully >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
