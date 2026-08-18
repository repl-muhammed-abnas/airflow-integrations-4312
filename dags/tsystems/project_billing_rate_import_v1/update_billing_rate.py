"""
T-Systems Project Billing Rate Import - Update Billing Rate Child DAG

This DAG handles the updating of existing billing rates in Replicon for records
that already exist. It processes individual records from the master DAG
and updates billing rates with the new values.

Flow:
1. Display DAG run configuration for debugging
2. Validate input parameters
3. Get existing billing rate details
4. Update billing rate in Replicon with new values
5. Log success or error results
"""
from airflow.models import Variable
import rail
from datetime import timedelta

# Required for JSON payload compatibility
null = None


def create_update_billing_rate_dag(config):
    """
    Create the child DAG for updating existing billing rates.

    Args:
        config: Configuration object containing DAG settings

    Returns:
        DAG: Configured Airflow DAG
    """

    with rail.create_airflow_dag(
        dag_id=config.update_billing_rate_dag_id,
        description=f'T-Systems Project Billing Rate Import Update Billing Rate ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_billing_rate_name_changed'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='if_billing_rate_name_changed',
            end_task='catch_and_log_errors',
        )

        if_billing_rate_name_changed = rail.IfOperator(
            task_id='if_billing_rate_name_changed',
            test=lambda dag_run: dag_run.conf['Billing_Rate_Name'] != dag_run.conf['existing_billing_rate_name'],
            yes_task="update_billing_rate_name",
            no_task="if_billing_rate_amount_changed"
        )

        update_billing_rate_name = rail.RepliconServiceOperator(
            task_id='update_billing_rate_name',
            endpoint='/services/BillingRateService1.svc/UpdateCompanyBillingRateName',
            data=lambda dag_run: {
                "billingRateUri": dag_run.conf['billing_rate_uri'],
                "name": dag_run.conf['Billing_Rate_Name']
            }
        )

        if_billing_rate_amount_changed = rail.IfOperator(
            task_id='if_billing_rate_amount_changed',
            test=lambda dag_run: float(dag_run.conf['Billing_Rate_Value']) != float(
                dag_run.conf['existing_billing_rate_amount']),
            yes_task="update_billing_rate_amount",
            no_task="trigger_assign_billing_rate_to_project_and_resource_dag"
        )

        update_billing_rate_amount = rail.RepliconServiceOperator(
            task_id='update_billing_rate_amount',
            endpoint='/services/BillingRateService1.svc/UpdateCompanyBillingRateAmount',
            data=lambda dag_run: {
                "billingRateUri": dag_run.conf['billing_rate_uri'],
                "rate": {
                    "amount": float(dag_run.conf['Billing_Rate_Value']),
                    "currencyUri": dag_run.conf['default_currency_uri']
                }
            }
        )

        trigger_assign_billing_rate_to_project_and_resource_dag = rail.TriggerDagRunOperator(
            task_id='trigger_assign_billing_rate_to_project_and_resource_dag',
            trigger_dag_id=config.add_billing_rate_to_project_and_resource_dag_id,
            conf=lambda dag_run: {
                    **dag_run.conf,
                    'billing_rate_amount_updated': 'true' if float(dag_run.conf['Billing_Rate_Value']) != float(
                        dag_run.conf['existing_billing_rate_amount']) else 'false',
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
                "action": "Update",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> if_billing_rate_name_changed

        if_billing_rate_name_changed >> rail.Label(
            'No') >> if_billing_rate_amount_changed
        if_billing_rate_name_changed >> rail.Label(
            'Yes') >> update_billing_rate_name >> if_billing_rate_amount_changed

        if_billing_rate_amount_changed >> rail.Label(
            'No') >> trigger_assign_billing_rate_to_project_and_resource_dag
        if_billing_rate_amount_changed >> rail.Label(
            'Yes') >> update_billing_rate_amount >> trigger_assign_billing_rate_to_project_and_resource_dag

        trigger_assign_billing_rate_to_project_and_resource_dag \
            >> wait_for_trigger_assign_billing_rate_to_project_and_resource_dag >> catch_and_log_errors

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_update_billing_rate_dag)
