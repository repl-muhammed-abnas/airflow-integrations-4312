from datetime import timedelta
from pendulum import now
import json
import rail
from airflow.exceptions import AirflowException
from airflow.models import Variable
from alvarezandmarsalholdings.time_export.time_export_master.utils import custom_methods, request_payload
from alvarezandmarsalholdings.time_export.time_export_master.tasks.time_export_task import time_data_export
from alvarezandmarsalholdings.time_export.time_export_master.tasks.update_time_export_status import cancel_time_export


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description='Alvarez and Marsal Holdings Time Export Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=config.master_dag_interval),
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # First gate: suspend the dagrun inside a maintenance window. The flag
        # disables the feature; an unset/empty mapper runs the usual process.
        check_maintenance_window = rail.PythonOperator(
            task_id='check_maintenance_window',
            python_callable=lambda: custom_methods.is_within_maintenance_window(
                json.loads(Variable.get(config.maintenance_window_mapper, default_var='{}') or '{}'),
                config.time_zone
            ) if Variable.get(config.can_use_maintenance_window, default_var='false').lower() == 'true' else False
        )

        if_in_maintenance_window = rail.IfOperator(
            task_id='if_in_maintenance_window',
            test="{{ result('check_maintenance_window') | is_truthy }}",
            yes_task='delete_this_dagrun',
            no_task='check_can_trigger_next_run'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        check_can_trigger_next_run = rail.PythonOperator(
            task_id='check_can_trigger_next_run',
            python_callable=custom_methods.check_previous_master_dag_runs,
            op_args=[config]
        )

        if_previous_time_export_childs_not_successful = rail.IfOperator(
            task_id='if_previous_time_export_childs_not_successful',
            test=lambda: not (rail.result('check_can_trigger_next_run')[
                              'can_process_further']),
            yes_task='fail_current_run_as_previous_run_not_successful',
            no_task='create_timeoffdata_row_counts_batch'
        )

        fail_current_run_as_previous_run_not_successful = rail.FailOperator(
            task_id='fail_current_run_as_previous_run_not_successful',
            message="Previous time export run not Successful. Thus failing current run"
        )

        create_timeoffdata_row_counts_batch = rail.RepliconServiceOperator(
            task_id='create_timeoffdata_row_counts_batch',
            endpoint='/services/TimeDataExportService1.svc/CreateTimeDataItemRowCountsBatch',
            data=request_payload.get_create_time_data_export_batch_payload(
                config.time_zone, 'row_count'),
        )

        (execute_row_counts_batch, wait_for_row_counts_batch) = rail.batch_execution(
            group_id='execute_row_counts_batch',
            creation_task_id=create_timeoffdata_row_counts_batch.task_id,
            wait_timeout=60*60*5,
        )

        get_timeoffdata_row_counts_results = rail.RepliconServiceOperator(
            task_id='get_timeoffdata_row_counts_results',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataItemRowCountsBatchResults',
            data=lambda: {
                "timeDataItemRowCountsBatchUri": rail.result('create_timeoffdata_row_counts_batch')
            }
        )

        export_has_data = rail.IfOperator(
            task_id='export_has_data',
            test=lambda: rail.result('get_timeoffdata_row_counts_results')[
                'rowCounts'][0] > 0,
            yes_task='get_logging_details',
            no_task='can_fail_dag'
        )

        def get_logging_details_callable():
            current_date = now(tz=config.time_zone)
            return {
                "todays_date": current_date.strftime(custom_methods.EXPORT_DATE_FORMAT),
                "timezone": config.time_zone,
                "process_start_time": current_date.strftime(custom_methods.LOGGING_TIMESTAMP_FORMAT),
                "time_export_name": f"Time_Extract_{current_date.strftime(custom_methods.LOGGING_TIMESTAMP_FORMAT)}",
                "no_data_export_name": f"NO_DATA_{current_date.strftime(custom_methods.LOGGING_TIMESTAMP_FORMAT)}"
            }

        get_logging_details = rail.PythonOperator(
            task_id="get_logging_details",
            python_callable=get_logging_details_callable
        )

        time_export_start, mark_as_completed, time_export_uri_task_id = time_data_export(
            group_id="time_export",
            generate_request=request_payload.get_create_time_data_export_batch_payload(
                config.time_zone),
            get_export_name="{{result('get_logging_details').time_export_name}}",
            retries=0
        )

        trigger_time_export_to_workday = rail.TriggerDagRunOperator(
            task_id="trigger_time_export_to_workday",
            trigger_dag_id=config.time_export_to_workday_dag_id,
            conf=lambda: {
                **rail.result('get_logging_details'),
                **{
                    "time_export_uri": rail.result('time_export.get_export_uri'),
                }
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_timeoff_export_to_workday = rail.TriggerDagRunOperator(
            task_id="trigger_timeoff_export_to_workday",
            trigger_dag_id=config.timeoff_export_to_workday_dag_id,
            conf=lambda: {
                **rail.result('get_logging_details'),
                **{
                    "time_export_uri": rail.result('time_export.get_export_uri'),
                }
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        trigger_time_export_to_s4 = rail.TriggerDagRunOperator(
            task_id="trigger_time_export_to_s4",
            trigger_dag_id=config.time_export_to_s4hc_dag_id,
            conf=lambda: {
                **rail.result('get_logging_details'),
                **{
                    "time_export_uri": rail.result('time_export.get_export_uri')
                }
            },
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_dag_ids_to_wait = rail.PythonOperator(
            task_id='get_dag_ids_to_wait',
            python_callable=lambda: [
                rail.result('trigger_time_export_to_workday'),
                rail.result('trigger_timeoff_export_to_workday'),
                rail.result('trigger_time_export_to_s4')
            ]
        )

        wait_for_dag_runs = rail.WaitForDagRunsSensor(
            task_id="wait_for_dag_runs",
            dag_runs="{{result('get_dag_ids_to_wait') | to_json}}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        gather_results_from_dag_runs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_dag_runs',
            dag_runs="{{result('get_dag_ids_to_wait') | to_json}}",
            dagrun_task_id='final_response_from_dag',
            flatten=True
        )

        check_response_from_all_dags = rail.PythonOperator(
            task_id='check_response_from_all_dags',
            python_callable=lambda: "All triggered dag responses recieved" if len(rail.result('get_dag_ids_to_wait')) == len(
                rail.result('gather_results_from_dag_runs')) else AirflowException('All triggered dag responses not recieved')
        )

        if_no_data_in_all_child_dags = rail.IfOperator(
            task_id='if_no_data_in_all_child_dags',
            test=lambda: all(item.startswith('No Data') for item in rail.result(
                'gather_results_from_dag_runs')),
            yes_task='rename_export_name_to_no_data',
            no_task='if_employee_id_missing_in_time_entries'
        )

        rename_export_name_to_no_data = rail.RepliconServiceOperator(
            task_id="rename_export_name_to_no_data",
            endpoint="/services/TimeDataExportService1.svc/UpdateTimeDataExportName",
            data=lambda: {
                "target": {
                    "uri": rail.result('time_export.get_export_uri'),
                },
                "name": rail.result('get_logging_details')['no_data_export_name']
            }
        )

        if_employee_id_missing_in_time_entries = rail.IfOperator(
            task_id='if_employee_id_missing_in_time_entries',
            test=lambda: any(item.startswith('Blank employee id') for item in rail.result(
                'gather_results_from_dag_runs')),
            yes_task='dummy_task_revert_to_draft',
            no_task='can_fail_dag'
        )

        dummy_task_revert_to_draft = rail.EmptyOperator(
            task_id='dummy_task_revert_to_draft'
        )

        revert_to_draft, cancel_export = cancel_time_export(
            "cancel_time_data_export_invalid_data", time_export_uri_task_id)

        fail_dagrun_employee_id_missing = rail.FailOperator(
            task_id="fail_dagrun_employee_id_missing",
            message='Failure in processing time/timeoff export as employee id is missing for 1 or more employees'
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule="all_done",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='Failure in processing time/timeoff export - {{ get_error_message() }}'
        )

        check_maintenance_window >> if_in_maintenance_window
        if_in_maintenance_window >> rail.Label(
            "In Maintenance Window") >> delete_this_dagrun
        if_in_maintenance_window >> rail.Label(
            "Proceed") >> check_can_trigger_next_run

        check_can_trigger_next_run >> if_previous_time_export_childs_not_successful

        if_previous_time_export_childs_not_successful >> rail.Label(
            "Yes") >> fail_current_run_as_previous_run_not_successful
        if_previous_time_export_childs_not_successful >> rail.Label(
            "No") >> create_timeoffdata_row_counts_batch

        create_timeoffdata_row_counts_batch >> execute_row_counts_batch >> wait_for_row_counts_batch \
            >> get_timeoffdata_row_counts_results >> export_has_data

        export_has_data >> rail.Label(
            "Yes") >> get_logging_details
        export_has_data >> rail.Label(
            "No") >> can_fail_dag

        get_logging_details >> time_export_start

        mark_as_completed >> trigger_time_export_to_workday \
            >> trigger_timeoff_export_to_workday >> trigger_time_export_to_s4 >> get_dag_ids_to_wait

        get_dag_ids_to_wait >> wait_for_dag_runs >> gather_results_from_dag_runs >> check_response_from_all_dags >> if_no_data_in_all_child_dags

        if_no_data_in_all_child_dags >> rail.Label(
            "Yes") >> rename_export_name_to_no_data >> can_fail_dag
        if_no_data_in_all_child_dags >> rail.Label(
            "No") >> if_employee_id_missing_in_time_entries

        revert_to_draft
        cancel_export >> fail_dagrun_employee_id_missing >> can_fail_dag

        if_employee_id_missing_in_time_entries >> rail.Label(
            "Yes") >> dummy_task_revert_to_draft >> revert_to_draft
        if_employee_id_missing_in_time_entries >> rail.Label(
            "No") >> can_fail_dag

        can_fail_dag >> rail.Label(
            "Yes") >> fail_dagrun

    return dag


rail.for_each_instance(create_main_dag)
