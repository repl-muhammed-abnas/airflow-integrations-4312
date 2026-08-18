import rail
from b2g.time_entry_sync.utils import request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"b2g_submit_timesheet_{config.instance}",
        description=f"B2G Submit Timesheet Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_batch_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        create_submit_batch = rail.RepliconServiceOperator(
            task_id="create_submit_batch",
            endpoint="/services/TimesheetApprovalService1.svc/CreateSubmitBatch2",
            data=request_payload.get_submit_timesheet_payload
        )

        (execute_timesheet_batch, wait_for_timesheet_batch) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=create_submit_batch.task_id,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        create_submit_batch >> execute_timesheet_batch\
            >> wait_for_timesheet_batch >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
