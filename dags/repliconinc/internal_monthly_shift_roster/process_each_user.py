from datetime import timedelta
from repliconinc.internal_monthly_shift_roster.utils import custom_methods, request_payload, response_filters
import rail


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_child_dag_id,
        description=f"RepliconInc Internal Monthly Shift Roster - Process Each User Child DAG {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_process_each_user,
        default_args={
            "execution_timeout": timedelta(hours=1),
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        if_shift_name_present = rail.IfOperator(
            task_id="if_shift_name_present",
            test=lambda dag_run: bool(dag_run.conf.get("shift_name")),
            yes_task="get_shift_schedule_summary_details",
            no_task="finish"
        )

        get_shift_schedule_summary_details = rail.RepliconServiceOperator(
            task_id="get_shift_schedule_summary_details",
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary",
            data=request_payload.create_shift_schedule_summary_details_payload,
            data_handler=lambda response: response_filters.filter_shift_schedule_summary_details_response(response),
        )

        if_assignment_uri_exist = rail.IfOperator(
            task_id="if_assignment_uri_exist",
            test="{{ result('get_shift_schedule_summary_details') | length > 0 }}",
            yes_task="bulk_delete_for_user_schedule",
            no_task="final_shift_assignment_payload"
        )

        bulk_delete_for_user_schedule = rail.RepliconServiceOperator(
            task_id="bulk_delete_for_user_schedule",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.create_bulk_delete_user_shift_schedule_payload
        )

        final_shift_assignment_payload = rail.PythonOperator(
            task_id="final_shift_assignment_payload",
            python_callable=lambda dag_run: custom_methods.create_final_shift_assignment(dag_run)
        )

        if_final_shift_assignment_has_data = rail.IfOperator(
            task_id="if_final_shift_assignment_has_data",
            test="{{ result('final_shift_assignment_payload') | length > 0 }}",
            yes_task="bulk_put_shift_assignment",
            no_task="finish"
        )

        bulk_put_shift_assignment = rail.RepliconServiceOperator(
            task_id="bulk_put_shift_assignment",
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=lambda: request_payload.create_final_shift_assignment_payload(rail.result("final_shift_assignment_payload"))
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )


        if_shift_name_present >> rail.Label("No") >> finish
        if_shift_name_present >> rail.Label("Yes") >> get_shift_schedule_summary_details >>\
        if_assignment_uri_exist >> rail.Label("No") >> final_shift_assignment_payload
        if_assignment_uri_exist >> rail.Label("Yes") >> bulk_delete_for_user_schedule >> final_shift_assignment_payload
        final_shift_assignment_payload >> if_final_shift_assignment_has_data
        if_final_shift_assignment_has_data >> rail.Label("No") >> finish
        if_final_shift_assignment_has_data >> rail.Label("Yes") >> bulk_put_shift_assignment >> finish
        

    return dag

rail.for_each_instance(create_child_dag)