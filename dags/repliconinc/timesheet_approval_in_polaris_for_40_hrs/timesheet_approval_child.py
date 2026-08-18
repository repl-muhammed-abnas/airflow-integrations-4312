from datetime import datetime, timedelta
import rail
from repliconinc.timesheet_approval_in_polaris_for_40_hrs.utils import request_payload

null = None
# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_timesheet_approval_child_dag,
        description=f"Auto timesheet approval for all timeoff in polaris for 40 hours child",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        enqueuerecalculatescriptdata = rail.RepliconServiceOperator(
            task_id="enqueuerecalculatescriptdata",
            endpoint="/services/TimesheetService1.svc/EnqueueRecalculateScriptData",
            data=lambda dag_run: request_payload.get_timesheeturi(dag_run),
        )

        force_approve = rail.RepliconServiceOperator(
            task_id="force_approve",
            endpoint="/services/TimesheetApprovalService1.svc/ForceApprove",
            data=lambda dag_run: request_payload.get_force_approve_payload(dag_run),
        )

        enqueuerecalculatescriptdata >> force_approve


    return dag


rail.for_each_instance(create_child_dag)
