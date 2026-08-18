from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.macros import get_error_message
from galaxyusopcoinc.tiger_assignee_integration.utils import request_payload


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'vialtopartners_tiger_assignee_integration_child_process_assignee_update_{config.instance}',
        description='Vialto Partners Tiger Assignee Integration Process Assignee IDs Update',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_assignee_ids_update,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_assignee_update_error_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.child_wait_execution_timeout_days),
            start_task='create_assignee_update_error_log',
            end_task='catch_and_log_errors',
        )

        create_assignee_update_error_log = rail.CreateLogOperator(
            task_id='create_assignee_update_error_log'
        )

        update_code = rail.RepliconServiceOperator(
            task_id="update_code",
            endpoint="/services/ObjectExtensionTagService1.svc/UpdateCode",
            data=lambda dag_run: request_payload.update_code('update', dag_run)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{result('create_assignee_update_error_log')}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "projectname": '',
                "clientname": '',
                "clientshortname": '',
                'assigneeid': dag_run.conf['assigneeid'],
                'assigneestatus': '',
                'details': get_error_message(rail.get_current_context()),
                'status': 'Error',
                'jobid': get_dagrun_ecid(rail.get_current_context()['dag_run'])
            },
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_assignee_update_error_log

        create_assignee_update_error_log >> update_code
        update_code >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
