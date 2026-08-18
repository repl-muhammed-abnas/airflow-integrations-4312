from datetime import timedelta
from sunovion.project_task_import.utils import request_payload
from airflow.models import Variable
import rail


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_process_update_task_child_{config.instance}',
        description='Sunovion Project and Task Sync - Process Update Task',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_code,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task="batch_task",
            no_task="get_task_info"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_task_info',
            end_task="catch_and_log_errors",
        )

        get_task_info = rail.RepliconServiceOperator(
            task_id="get_task_info",
            endpoint="services/TaskService1.svc/GetTaskDetails",
            data=request_payload.get_task_info
        )

        is_task_name_different = rail.IfOperator(
            task_id='is_task_name_different',
            test=request_payload.is_task_name_different,
            yes_task='update_task_name',
            no_task='update_allow_time_entry'
        )

        update_task_name = rail.RepliconServiceOperator(
            task_id="update_task_name",
            endpoint="services/TaskService1.svc/UpdateName",
            data=request_payload.update_task_name
        )

        log_task_name_updated = rail.WriteLogOperator(
            task_id="log_task_name_updated",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Task name "{{ result("get_task_info").name }}" updated to '+
                '"{{ dag_run.conf.taskname }} - {{ dag_run.conf.taskcode }}"',
            severity='Updated',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Updated',
                'details': '{{ dag_run_ecid() }} - Task name "{{ result("get_task_info").name }}" updated to '+
                    '"{{ dag_run.conf.taskname }} - {{ dag_run.conf.taskcode }}"'
            }
        )

        update_allow_time_entry = rail.RepliconServiceOperator(
            task_id="update_allow_time_entry",
            endpoint="services/TaskService1.svc/UpdateAllowTimeEntry",
            data=request_payload.update_allow_time_entry
        )

        is_task_status_open = rail.IfOperator(
            task_id='is_task_status_open',
            test=request_payload.is_task_status_open,
            yes_task='task_status_open',
            no_task='is_end_date_task_present'
        )

        task_status_open = rail.RepliconServiceOperator(
            task_id="task_status_open",
            endpoint="services/TaskService1.svc/Open",
            data=request_payload.task_status
        )

        is_end_date_task_present = rail.IfOperator(
            task_id='is_end_date_task_present',
            test=request_payload.is_end_date_task_present,
            yes_task='update_end_allow_time_entry',
            no_task='is_task_status_closed'
        )

        update_end_allow_time_entry = rail.RepliconServiceOperator(
            task_id="update_end_allow_time_entry",
            endpoint="services/TaskService1.svc/UpdateAllowTimeEntry",
            data=request_payload.update_end_allow_time_entry
        )

        is_task_status_closed = rail.IfOperator(
            task_id='is_task_status_closed',
            test=request_payload.is_task_status_closed,
            yes_task='task_status_close',
            no_task='log_task_updated'
        )

        task_status_close = rail.RepliconServiceOperator(
            task_id="task_status_close",
            endpoint="services/TaskService1.svc/Close",
            data=request_payload.task_status
        )

        log_task_status_closed = rail.WriteLogOperator(
            task_id="log_task_status_closed",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Task Closed',
            severity='Success',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Task Closed'
            }
        )

        log_task_updated = rail.WriteLogOperator(
            task_id="log_task_updated",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Task Updated',
            severity='Success',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Task Updated'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Failed',
            message='{{ dag_run_ecid() }} - Task Update - {{ get_error_message() }}',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Task Update - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_task_info
        get_task_info >> is_task_name_different >> rail.Label(
            "Yes") >> update_task_name >> log_task_name_updated >> log_task_updated >> catch_and_log_errors >> log_to_sumo
        is_task_name_different >> rail.Label(
            "No") >> update_allow_time_entry >> is_task_status_open
        is_task_status_open >> rail.Label(
            "Yes") >> task_status_open >> is_end_date_task_present
        is_task_status_open >> rail.Label("No") >> is_end_date_task_present
        is_end_date_task_present >> rail.Label(
            "Yes") >> update_end_allow_time_entry >> is_task_status_closed
        is_end_date_task_present >> rail.Label("No") >> is_task_status_closed
        is_task_status_closed >> rail.Label(
            "Yes") >> task_status_close >> log_task_status_closed >> log_task_updated
        is_task_status_closed >> rail.Label(
            "No") >> log_task_updated >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag_wbs)
