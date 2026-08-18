from datetime import timedelta
from airflow.models import Variable
import rail
from lead3rllc.project_import.utils.request_payload import get_create_client_payload


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_add_client_dag_id,
        description='LEAD3R LLC Project Import - Add Client Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_client'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_client',
            end_task='catch_and_log_error',
        )

        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=get_create_client_payload
        )

        add_client_success_log_entry = rail.WriteLogOperator(
            task_id='add_client_success_log_entry',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "Client added : " + dag_run.conf['company_name'],
                "status": "Success",
                "details": "Client created successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.missing_field_value_import_logs}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "action": "Client add : " + dag_run.conf['company_name'],
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_client

        create_client >> add_client_success_log_entry >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
