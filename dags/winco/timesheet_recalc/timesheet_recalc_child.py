import rail
from winco.timesheet_recalc.utils import request_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"winco_timesheet_data_process_each_record_child_{config.instance}",
        description=f"Winco Timesheet Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        mark_timesheets_as_outofdate = rail.RepliconServiceOperator(
            task_id="mark_timesheets_as_outofdate",
            endpoint="/services/TimesheetService1.svc/MarkTimesheetsAsOutOfDate",
            data=request_payload.get_timesheets_payload
        )

        enqueue_recalculate_scriptdata = rail.RepliconServiceOperator(
            task_id="enqueue_recalculate_scriptdata",
            endpoint="/services/TimesheetService1.svc/CreateRecalculateScriptDataBatch2",
            data=request_payload.get_timesheets_payload
        )

        (execute_timesheet_batch, wait_for_timesheet_batch) = rail.batch_execution(
            group_id='execute_time_export',
            creation_task_id=enqueue_recalculate_scriptdata.task_id,
            retries=0
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        mark_timesheets_as_outofdate >> enqueue_recalculate_scriptdata >> execute_timesheet_batch\
            >> wait_for_timesheet_batch >> log_to_sumo >> can_fail_dag >> fail_dagrun

    return dag


rail.for_each_instance(create_child_dag)
