from airflow.models import Variable
import rail
from datetime import timedelta
from tsystems.project_billing_rate_import.utils import custom_methods
from tsystems.project_billing_rate_import.utils import request_payload

null = None


def create_add_billing_rate_dag(config):
    """
    Create the child DAG for adding new billing rates.

    """

    with rail.create_airflow_dag(
        dag_id=config.add_billing_rate_dag_id,
        description=f'T-Systems Project Billing Rate Import Add Billing Rate {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        # Display DAG run configuration for debugging
        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='add_billing_rate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='add_billing_rate',
            end_task='catch_and_log_errors',
        )

        add_billing_rate = rail.RepliconServiceOperator(
            task_id='add_billing_rate',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            data=request_payload.get_add_billing_rate_payload,
            data_handler=lambda response: response['uri'],
        )

        #  Trigger Project Assignment DAG for records with Project_ID
        trigger_assign_billing_rate_to_project_and_resource_dag = rail.TriggerDagRunOperator(
            task_id='trigger_assign_billing_rate_to_project_and_resource_dag',
            trigger_dag_id=config.add_billing_rate_to_project_and_resource_dag_id,
            conf=lambda dag_run: {
                    **dag_run.conf,
                    'billing_rate_uri': rail.result('add_billing_rate'),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        #  Wait for Project Assignment DAG runs to complete
        wait_for_trigger_assign_billing_rate_to_project_and_resource_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_assign_billing_rate_to_project_and_resource_dag',
            dag_runs="{{ result('trigger_assign_billing_rate_to_project_and_resource_dag') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Catch and log any unexpected errors
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.log}}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda dag_run: {
                "billing_rate_id": dag_run.conf['Billing_Rate_ID'],
                "billing_rate_name": dag_run.conf['Billing_Rate_Name'],
                "project_id": dag_run.conf['Project_ID'],
                "ciam_id": dag_run.conf['CIAM_ID'],
                "action": "Add",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> add_billing_rate

        add_billing_rate >> trigger_assign_billing_rate_to_project_and_resource_dag

        trigger_assign_billing_rate_to_project_and_resource_dag \
            >> wait_for_trigger_assign_billing_rate_to_project_and_resource_dag >> catch_and_log_errors

    return dag


rail.for_each_instance(create_add_billing_rate_dag)
