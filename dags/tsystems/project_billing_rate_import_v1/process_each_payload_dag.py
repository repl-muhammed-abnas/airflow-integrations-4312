"""
This DAG processes each payload containing billing rate assignment data for T-Systems.
"""

from pendulum import now
import rail
from datetime import timedelta
from tsystems.project_billing_rate_import_v1.utils import custom_methods
from tsystems.project_billing_rate_import_v1.utils import request_payload

# Required for JSON payload compatibility
null = None


def create_process_each_payload_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_each_payload_dag_id,
        description=f'T-Systems Project Billing Rate Import Process Each Payload {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_payload,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        # Process and transform payload data
        log_payload_data = rail.PythonOperator(
            task_id='log_payload_data',
            python_callable=lambda dag_run: dag_run.conf
        )

        check_if_billing_rate_name_exceeds_length = rail.IfOperator(
            task_id='check_if_billing_rate_name_exceeds_length',
            test=lambda: str(rail.result('log_payload_data')['name_exceeds_length']).lower() == 'true' or (int(
                rail.result('log_payload_data')['length_combined_fields_except_billing_text']) >= (int(config.length_billing_rate_name) - 1)),
            yes_task='log_billing_rate_name_exceeds_length',
            no_task='if_billing_rate_description_exists_in_replicon',
        )

        log_billing_rate_name_exceeds_length = rail.WriteLogOperator(
            task_id='log_billing_rate_name_exceeds_length',
            log="{{ result('create_log') }}",
            message=f"Billing Rate Name without billing text exceeds maximum length of {config.length_billing_rate_name} characters",
            severity="Exception",
            properties=lambda: {
                "billing_rate_id": rail.result('log_payload_data')['Billing_Rate_ID'],
                "billing_rate_name":  rail.result('log_payload_data')['final_billing_rate_name'],
                "project_id": rail.result('log_payload_data')['Project_ID'],
                "ciam_id": rail.result('log_payload_data')['CIAM_ID'],
                "action": "Validation",
                "status": "Exception",
                "details": f"Billing Rate Name without billing text exceeds maximum length of {config.length_billing_rate_name} characters",
            }
        )

        if_billing_rate_description_exists_in_replicon = rail.IfOperator(
            task_id='if_billing_rate_description_exists_in_replicon',
            test=lambda: bool(rail.result('log_payload_data')[
                              'existing_billing_rate_description']),
            yes_task='trigger_update_billing_rate_dag',
            no_task='check_if_billing_rate_name_already_exists_in_replicon',
        )

        #  Trigger Update Billing Rate Child DAG for billing rate updation
        trigger_update_billing_rate_dag = rail.TriggerDagRunOperator(
            task_id='trigger_update_billing_rate_dag',
            trigger_dag_id=config.update_billing_rate_dag_id,
            conf=lambda: request_payload.get_add_update_billing_rate_conf(
                rail.result('log_payload_data'), 'Update'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        #  Wait for Update Billing Rate DAG runs to complete
        wait_for_update_dag_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_dag_completion',
            dag_runs="{{ result('trigger_update_billing_rate_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Below tasks are just for checking and logging if billing rate name already exists in replicon since we will be creating this new billing rate
        check_if_billing_rate_name_already_exists_in_replicon = rail.IfOperator(
            task_id='check_if_billing_rate_name_already_exists_in_replicon',
            test=lambda: str(rail.result('log_payload_data')[
                             'existing_billing_rate_name_in_replicon']).lower() == 'true',
            yes_task='log_billing_rate_name_already_exists',
            no_task='trigger_add_billing_rate_dag',
        )

        log_billing_rate_name_already_exists = rail.WriteLogOperator(
            task_id='log_billing_rate_name_already_exists',
            log="{{ result('create_log') }}",
            message="Billing Rate Name already exists in Replicon",
            severity="Exception",
            properties=lambda: {
                "billing_rate_id": rail.result('log_payload_data')['Billing_Rate_ID'],
                "billing_rate_name": rail.result('log_payload_data')['final_billing_rate_name'],
                "project_id": rail.result('log_payload_data')['Project_ID'],
                "ciam_id": rail.result('log_payload_data')['CIAM_ID'],
                "action": "Add",
                "status": "Exception",
                "details": "Billing Rate Name already exists in Replicon",
            }
        )

        #  Trigger Add Billing Rate Child DAG for billing rate creation
        trigger_add_billing_rate_dag = rail.TriggerDagRunOperator(
            task_id='trigger_add_billing_rate_dag',
            trigger_dag_id=config.add_billing_rate_dag_id,
            conf=lambda: request_payload.get_add_update_billing_rate_conf(
                rail.result('log_payload_data'), 'Add'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        #  Wait for Add Billing Rate DAG runs to complete
        wait_for_add_dag_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_dag_completion',
            dag_runs="{{ result('trigger_add_billing_rate_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Catch and log any unexpected errors
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_log')}}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda: {
                "billing_rate_id": rail.result('log_payload_data')['Billing_Rate_ID'],
                "billing_rate_name": rail.result('log_payload_data')['final_billing_rate_name'],
                "project_id": rail.result('log_payload_data')['Project_ID'],
                "ciam_id": rail.result('log_payload_data')['CIAM_ID'],
                "action": "Process each payload",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        create_log >> log_payload_data >> check_if_billing_rate_name_exceeds_length

        check_if_billing_rate_name_exceeds_length >> rail.Label(
            'No') >> if_billing_rate_description_exists_in_replicon
        check_if_billing_rate_name_exceeds_length >> rail.Label(
            'Yes') >> log_billing_rate_name_exceeds_length >> catch_and_log_errors

        if_billing_rate_description_exists_in_replicon >> rail.Label(
            'Yes') >> trigger_update_billing_rate_dag
        if_billing_rate_description_exists_in_replicon >> rail.Label(
            'No') >> check_if_billing_rate_name_already_exists_in_replicon

        trigger_update_billing_rate_dag >> wait_for_update_dag_completion >> catch_and_log_errors

        check_if_billing_rate_name_already_exists_in_replicon >> rail.Label(
            'Yes') >> log_billing_rate_name_already_exists >> catch_and_log_errors
        check_if_billing_rate_name_already_exists_in_replicon >> rail.Label(
            'No') >> trigger_add_billing_rate_dag

        trigger_add_billing_rate_dag >> wait_for_add_dag_completion >> catch_and_log_errors

    return dag


rail.for_each_instance(create_process_each_payload_dag)
