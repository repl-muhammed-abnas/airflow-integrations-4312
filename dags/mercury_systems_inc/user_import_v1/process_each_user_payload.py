from datetime import timedelta
from os import wait
from airflow.models import Variable
import rail
import json
from mercury_systems_inc.user_import_v1.utils import request_payload, custom_methods

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_payload_dagid,
        description='MercurySystemsInc User Import Process Each User Payload',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_user_payload,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id="create_user_log"
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{dag_run.conf.Employee_ID}}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else []
        )

        is_user_status_disabled = rail.IfOperator(
            task_id='is_user_status_disabled',
            test=lambda: bool(rail.result('get_user_data')) and rail.result(
                'get_user_data')['userDetails']['isEnabled'] in ['false', False, 'False'],
            yes_task='if_feed_file_employee_status_is_active',
            no_task='check_if_user_is_to_be_disabled'
        )

        if_feed_file_employee_status_is_active = rail.IfOperator(
            task_id='if_feed_file_employee_status_is_active',
            test=lambda dag_run: dag_run.conf.get('Emp_Status') in config.ENABLE_STATUS,
            yes_task='get_user_details_artifact_rehire_user',
            no_task='log_skipped_user_profile_disabled'
        )

        get_user_details_artifact_rehire_user = rail.PythonOperator(
            task_id='get_user_details_artifact_rehire_user',
            python_callable=lambda: rail.write_artifact(
                json.dumps(rail.result('get_user_data')))
        )

        process_rehire_user = rail.TriggerDagRunOperator(
            task_id='process_rehire_user',
            trigger_dag_id=config.process_rehire_user_dagid,
            conf=request_payload.get_process_rehire_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_rehire_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_rehire_user',
            dag_runs='{{ result("process_rehire_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_skipped_user_profile_disabled = rail.WriteLogOperator(
            task_id='log_skipped_user_profile_disabled',
            log='{{ result("create_user_log") }}',
            message='User profile is in disabled status',
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Update',
                'status': 'Exception',
                'details': "User profile is in disabled status"
            }
        )

        check_if_user_is_to_be_disabled = rail.IfOperator(
            task_id='check_if_user_is_to_be_disabled',
            test="{{ dag_run.conf.process == 'disable' }}",
            yes_task='is_user_available_for_disabling',
            no_task='has_valid_data'
        )

        is_user_available_for_disabling = rail.IfOperator(
            task_id='is_user_available_for_disabling',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='process_disable_user',
            no_task='log_user_not_available_for_disabling'
        )

        process_disable_user = rail.TriggerDagRunOperator(
            task_id='process_disable_user',
            trigger_dag_id=config.process_disable_user_dagid,
            conf=request_payload.get_process_disable_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_disable_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_disable_user',
            dag_runs='{{ result("process_disable_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_user_not_available_for_disabling = rail.WriteLogOperator(
            task_id='log_user_not_available_for_disabling',
            log='{{ result("create_user_log") }}',
            message='User not available for disabling',
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Disable',
                'status': 'Exception',
                'details': "User does not exist in Replicon."
            }
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test=lambda dag_run: custom_methods.test_and_log_valid_fields(dag_run)[
                'test_result'],
            yes_task="is_user_available",
            no_task="log_invalid_data"
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            log='{{ result("create_user_log") }}',
            message=lambda dag_run: custom_methods.test_and_log_valid_fields(dag_run)[
                'log'],
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Validation',
                'status': 'Exception',
                'details': custom_methods.test_and_log_valid_fields(dag_run)['log']
            }
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result('get_user_data')),
            yes_task='get_user_details_artifact',
            no_task='process_new_user'
        )

        get_user_details_artifact = rail.PythonOperator(
            task_id='get_user_details_artifact',
            python_callable=lambda: rail.write_artifact(
                json.dumps(rail.result('get_user_data')))
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=config.process_update_user_dagid,
            conf=request_payload.get_process_update_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_new_user = rail.TriggerDagRunOperator(
            task_id='process_new_user',
            trigger_dag_id=config.process_new_user_dagid,
            conf=request_payload.get_process_new_users_conf,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_new_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_user',
            dag_runs='{{ result("process_new_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{result("create_user_log")}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "employee_id": "{{dag_run.conf.Employee_ID}}",
                "first_name": "{{dag_run.conf.First_Name}}",
                "last_name": "{{dag_run.conf.Last_Name}}",
                "action": "Process User",
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_user_log

        create_user_log >> get_user_data >> is_user_status_disabled

        is_user_status_disabled >> rail.Label(
            'No') >> check_if_user_is_to_be_disabled
        is_user_status_disabled >> rail.Label(
            'Yes') >> if_feed_file_employee_status_is_active

        if_feed_file_employee_status_is_active >> rail.Label(
            'Yes') >> get_user_details_artifact_rehire_user >> process_rehire_user >> wait_for_process_rehire_user >> catch_and_log_errors
        if_feed_file_employee_status_is_active >> rail.Label(
            'No') >> log_skipped_user_profile_disabled >> catch_and_log_errors

        check_if_user_is_to_be_disabled >> rail.Label(
            'Yes') >> is_user_available_for_disabling

        is_user_available_for_disabling >> rail.Label(
            'No') >> log_user_not_available_for_disabling >> catch_and_log_errors

        is_user_available_for_disabling >> rail.Label(
            'Yes') >> process_disable_user >> wait_for_process_disable_user >> catch_and_log_errors

        check_if_user_is_to_be_disabled >> rail.Label(
            'No') >> has_valid_data

        has_valid_data >> rail.Label(
            'No') >> log_invalid_data >> catch_and_log_errors
        has_valid_data >> rail.Label('Yes') >> is_user_available

        is_user_available >> rail.Label(
            'No') >> process_new_user >> wait_for_process_new_user >> catch_and_log_errors
        is_user_available >> rail.Label(
            'Yes') >> get_user_details_artifact >> process_update_user >> wait_for_process_update_user >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
