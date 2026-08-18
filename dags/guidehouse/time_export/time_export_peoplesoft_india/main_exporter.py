from datetime import timedelta
from pendulum import now, datetime as pdt
import rail
from guidehouse.time_export.time_export_peoplesoft_india.utils import (
    custom_methods,
    request_payload,
    date_range,
)
from guidehouse.time_export.time_export_peoplesoft_india.tasks.time_export_task import (
    time_data_export,
)
from guidehouse.time_export.time_export_peoplesoft_india.tasks.update_time_export_status import (
    cancel_time_export,
)

from airflow.exceptions import AirflowException


def create_master_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description="Guidehouse Time Export - Master Daily DAG",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pdt(year=2026, month=5, day=1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_max_active_run,
    ) as dag:


        rail.ViewDagRunConfOperator(task_id= "view_dagrun_conf")

        def get_date_window_callable():
            run_date = now(tz=config.timezone)
            if config.run_type == "hourly":
                start_date, end_date = date_range.get_hourly_date_window(
                    run_date, tz_name=config.timezone
                )
            elif config.run_type == "daily":
                hourly_start, _ = date_range.get_hourly_date_window(
                    run_date, tz_name=config.timezone
                )
                start_date, end_date = date_range.get_daily_date_window(
                    run_date, hourly_start, tz_name=config.timezone
                )
            _datestr = run_date.strftime("%Y%m%d_%H%M%S")
            prefix_map = {"PeopleSoft": "PPSTime", "India": "INDTime"}
            prefix = prefix_map[config.financial_system]
            return {
                "run_date": run_date.strftime("%Y-%m-%d"),
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d"),
                "start_date_formatted": date_range.format_date_for_export(start_date),
                "end_date_formatted": date_range.format_date_for_export(end_date),
                "timestamp": _datestr,
                "time_export_name": f"{prefix}_{_datestr}",
                "no_data_export_name": f"{prefix}_NoData_{_datestr}",
            }
        
        
        response_from_dag_var = rail.SetVariableOperator(
            task_id="response_from_dag_var",
            name="response_from_dag",
            append=False,
            value="Success",
        )

        get_date_window = rail.PythonOperator(
            task_id="get_date_window",
            python_callable=get_date_window_callable,
        )

        get_all_service_centers = rail.RepliconServiceOperator(
            task_id="get_all_service_centers",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
        )

        create_timedata_row_counts_batch = rail.RepliconServiceOperator(
            task_id="create_timedata_row_counts_batch",
            endpoint="/services/TimeDataExportService1.svc/CreateTimeDataItemRowCountsBatch",
            data=lambda dag_run: request_payload.time_export_generate_request(
                dag_run, "row_count",config.financial_system,
            ),
        )

        (execute_row_counts_batch, wait_for_row_counts_batch) = rail.batch_execution(
            group_id="execute_row_counts_batch",
            creation_task_id=create_timedata_row_counts_batch.task_id,
            wait_timeout=60 * 60 * 5,
        )

        get_timeoffdata_row_counts_results = rail.RepliconServiceOperator(
            task_id="get_timeoffdata_row_counts_results",
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataItemRowCountsBatchResults",
            data=lambda: {
                "timeDataItemRowCountsBatchUri": rail.result(
                    "create_timedata_row_counts_batch"
                )
            },
        )

        export_has_data = rail.IfOperator(
            task_id="export_has_data",
            test=lambda: rail.result("get_timeoffdata_row_counts_results")["rowCounts"][
                0
            ]
            > 0,
            yes_task="start_export",
            no_task="final_response_from_dag",
        )


        start_export = rail.EmptyOperator(task_id="start_export")

        time_export_start, mark_as_completed = time_data_export(
            group_id="time_export",
            get_export_name="{{result('get_date_window').time_export_name}}",
            financial_system=config.financial_system
        )

        ps_task_group_start = rail.EmptyOperator(task_id="ps_task_group_start")

        trigger_ps_export = rail.TriggerDagRunOperator(
            task_id="trigger_ps_export",
            trigger_dag_id=config.ps_export_dag_id,
            conf=lambda: {
                "start_date": rail.result("get_date_window")["start_date"],
                "end_date": rail.result("get_date_window")["end_date"],
                "timestamp": rail.result("get_date_window")["timestamp"],
                "export_type": config.run_type,
                "time_export_name": rail.result("get_date_window")["time_export_name"],
                "financial_system": config.financial_system,
                "batch_uri": rail.result("time_export.get_export_uri"),
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        set_export_batch_uri = rail.SetVariableOperator(
            task_id="set_export_batch_uri",
            name="export_batch_uri",
            append=False,
            value="{{ result('time_export.get_export_uri') }}",
        )

        def get_dag_ids_to_wait_callable():
            dag_ids = []
            ps_export_run_id = rail.result("trigger_ps_export")
            if ps_export_run_id:
                dag_ids.append(ps_export_run_id)
            return dag_ids

        get_dag_ids_to_wait = rail.PythonOperator(
            task_id="get_dag_ids_to_wait",
            python_callable=get_dag_ids_to_wait_callable,
        )

        wait_for_exports = rail.WaitForDagRunsSensor(
            task_id="wait_for_exports",
            dag_runs="{{result('get_dag_ids_to_wait') | to_json}}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_results_from_dag_runs = rail.GatherResultsFromDagRunsOperator(
            task_id="gather_results_from_dag_runs",
            dag_runs="{{result('get_dag_ids_to_wait') | to_json}}",
            dagrun_task_id="final_response_from_dag",
            flatten=True,
        )

        check_response_from_all_dags = rail.PythonOperator(
            task_id="check_response_from_all_dags",
            python_callable=lambda: (
                "All triggered dag responses recieved"
                if len(rail.result("get_dag_ids_to_wait"))
                == len(rail.result("gather_results_from_dag_runs"))
                else AirflowException("All triggered dag responses not recieved")
            ),
        )

        mark_timedata_export_error = rail.IfOperator(
            task_id="mark_timedata_export_error",
            test=lambda: all(
                item.startswith("Error")
                for item in rail.result("gather_results_from_dag_runs")
            ),
            yes_task="cancel_export",
            no_task="if_no_data_in_all_child_dags",
        )

        if_no_data_in_all_child_dags = rail.IfOperator(
            task_id="if_no_data_in_all_child_dags",
            test=lambda: all(
                item.startswith("No Data")
                for item in rail.result("gather_results_from_dag_runs")
            ),
            yes_task="rename_export_name_to_no_data",
            no_task="if_employee_id_missing_in_time_entries",
        )

        rename_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="rename_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda: {
                "target": {
                    "uri": rail.result("time_export.get_export_uri"),
                },
                "name": rail.result("get_date_window")["no_data_export_name"],
            },
        )

        set_response_no_data = rail.SetVariableOperator(
            task_id="set_response_no_data",
            name="response_from_dag",
            append=False,
            value="No Data in export",
        )

        # any() is intentional: blank emp_id is a source-data integrity issue.
        # All children read the same Replicon batch_uri, so one child detecting it
        # means the source is invalid for every child.
        if_employee_id_missing_in_time_entries = rail.IfOperator(
            task_id="if_employee_id_missing_in_time_entries",
            test=lambda: any(
                item.startswith("Blank employee id")
                for item in rail.result("gather_results_from_dag_runs")
            ),
            yes_task="dummy_task_revert_to_draft",
            no_task="final_response_from_dag",
        )

        dummy_task_revert_to_draft = rail.EmptyOperator(
            task_id="dummy_task_revert_to_draft"
        )

        get_export_uri_failed_invalid_emp_id = rail.RepliconServiceOperator(
            task_id="get_export_uri_failed_invalid_emp_id",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('time_export.create_export') }}"
            },
            data_handler=custom_methods.retrieve_export_uri,
        )

        (
            mark_export_status_cancel_start_invalid_emp_id,
            mark_export_status_cancel_end_invalid_emp_id,
        ) = cancel_time_export(
            "cancel_timedata_export_invalid_emp_id",
            "get_export_uri_failed_invalid_emp_id",
        )

        update_export_name_cancelled_invalid_emp_id = rail.RepliconServiceOperator(
            task_id="update_export_name_cancelled_invalid_emp_id",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {
                    "uri": "{{ result('get_export_uri_failed_invalid_emp_id') }}"
                },
                "name": "Cancelled_{{ result('get_date_window').time_export_name }}",
            },
        )

        fail_dagrun_employee_id_missing = rail.FailOperator(
            task_id="fail_dagrun_employee_id_missing",
            message="Failure in processing time/timeoff export as employee id is missing for 1 or more employees",
        )

        cancel_export = rail.EmptyOperator(task_id="cancel_export")

        get_export_uri_failed = rail.RepliconServiceOperator(
            task_id="get_export_uri_failed",
            endpoint="/services/TimeDataExportService1.svc/GetCreateTimeDataExportBatchResults",
            data={
                "timeDataExportBatchUri": "{{ result('time_export.create_export') }}"
            },
            data_handler=custom_methods.retrieve_export_uri,
        )

        mark_export_status_cancel_start, mark_export_status_cancel_end = (
            cancel_time_export("cancel_timedata_export")
        )

        update_export_name_cancelled = rail.RepliconServiceOperator(
            task_id="update_export_name_cancelled",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data={
                "target": {"uri": "{{ result('get_export_uri_failed') }}"},
                "name": "Cancelled_{{ result('get_date_window').time_export_name }}",
            },
        )

        fail_time_export = rail.FailOperator(
            task_id="fail_time_export", message="{{ get_error_message() }}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id="final_response_from_dag",
            trigger_rule="all_done",
            python_callable=lambda: {
                "status": rail.get_dag_run_var("response_from_dag"),
                "system":config.fs,
                "batch_uri" : rail.get_dag_run_var("export_batch_uri"),
            },
        )

        (   response_from_dag_var >>  get_date_window >> get_all_service_centers
            >> create_timedata_row_counts_batch
            >> execute_row_counts_batch
            >> wait_for_row_counts_batch
            >> get_timeoffdata_row_counts_results
            >> export_has_data >> rail.Label("No") >>final_response_from_dag
        )
        (
            export_has_data >> rail.Label("Yes")
            >> start_export
            >> time_export_start
            >> mark_as_completed
            >> ps_task_group_start
            >> trigger_ps_export
            >> set_export_batch_uri
        )
        (
            set_export_batch_uri
            >> get_dag_ids_to_wait
        )
        (
            get_dag_ids_to_wait
            >> wait_for_exports
            >> gather_results_from_dag_runs
            >> check_response_from_all_dags
            >> mark_timedata_export_error
            >> rail.Label("No")
            >> if_no_data_in_all_child_dags
            >> rail.Label("Yes")
            >> rename_export_name_to_no_data
            >> set_response_no_data
        )
        (
            mark_timedata_export_error
            >> rail.Label("Yes")
            >> cancel_export
            >> get_export_uri_failed
            >> mark_export_status_cancel_start
            >> mark_export_status_cancel_end
            >> update_export_name_cancelled
            >> fail_time_export
            >> final_response_from_dag
        )
        (
            if_no_data_in_all_child_dags
            >> rail.Label("No")
            >> if_employee_id_missing_in_time_entries
            >> rail.Label("Yes")
            >> dummy_task_revert_to_draft
            >> get_export_uri_failed_invalid_emp_id
            >> mark_export_status_cancel_start_invalid_emp_id
        )
        (
            mark_export_status_cancel_end_invalid_emp_id
            >> update_export_name_cancelled_invalid_emp_id
            >> fail_dagrun_employee_id_missing
            >> final_response_from_dag
        )
        (
                set_response_no_data >> final_response_from_dag
        )
        (
            if_employee_id_missing_in_time_entries >> rail.Label("No") >> final_response_from_dag
        )
    return dag


rail.for_each_instance(create_master_dag)
