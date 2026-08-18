from datetime import datetime, timedelta
import rail
from repliconinc.weekend_shift_assignment.utils import request_payload, response_payload

null = None
# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_weekend_shift_records_child_dag,
        description=f"Weekend Shift Assignment {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        is_employee_id_present = rail.IfOperator(
            task_id="is_employee_id_present",
            test=lambda dag_run: bool(dag_run.conf["EmployeeID"]),
            yes_task="get_shift_schedule_summary",
            no_task="employee_id_not_present"
        )

        employee_id_not_present = rail.FailOperator(
            task_id="employee_id_not_present",
            message="Employee ID is not present in user profile"
        )

        get_shift_schedule_summary = rail.RepliconServiceOperator(
            task_id="get_shift_schedule_summary",
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.get_shift_summary,
            data_handler=response_payload.get_all_shift_assigment
        )

        if_assigment_present = rail.IfOperator(
            task_id="if_assigment_present",
            test=lambda: bool(rail.result("get_shift_schedule_summary")),
            yes_task="delete_assigned_shift",
            no_task="bult_put_shift_assignment"
        )

        delete_assigned_shift = rail.RepliconServiceOperator(
            task_id="delete_assigned_shift",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.get_shift_delete
        )

        bult_put_shift_assignment = rail.RepliconServiceOperator(
            task_id="bult_put_shift_assignment",
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=lambda dag_run: request_payload.get_shift_assignment_payload(dag_run, config)
        )

        post_message_teams = rail.PythonOperator(
            task_id="post_message_teams",
            python_callable=lambda dag_run: request_payload.prepare_teams_message(dag_run, config)
        )

        is_employee_id_present >> rail.Label("Yes") >> get_shift_schedule_summary >> if_assigment_present >> rail.Label(
            "Yes"
        ) >> delete_assigned_shift >> bult_put_shift_assignment >> post_message_teams

        if_assigment_present >> rail.Label("No") >> bult_put_shift_assignment

        is_employee_id_present >> rail.Label("No") >> employee_id_not_present

    return dag


rail.for_each_instance(create_child_dag)
