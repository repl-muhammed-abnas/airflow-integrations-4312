from datetime import timedelta
import rail
from airflow.models import Variable


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'pwcfr_timeoff_recal_batch_child_{config.instance}',
        description=f'Pwcfr_timeoff_recal_batch_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_child, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_forceapprove_batch'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_forceapprove_batch',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_forceapprove_batch = rail.RepliconServiceOperator(
            task_id='create_forceapprove_batch',
            endpoint='/services/TimeOffApprovalService1.svc/CreateForcedApproveBatch',
            data=lambda dag_run: {
                "timeOffUris": dag_run.conf['timeoffuris'],
                "comments": dag_run.conf['comments']
            }

        )

        execute_timeoff_approval_batch = rail.RepliconServiceOperator(
            task_id='execute_timeoff_approval_batch',
            endpoint='/services/TimeOffApprovalService1.svc/ExecuteTimeOffApprovalBatch',
            data=lambda: {
                "timeOffApprovalBatchUri": rail.result('create_forceapprove_batch'),
            }

        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_forceapprove_batch
        create_forceapprove_batch >> execute_timeoff_approval_batch >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
