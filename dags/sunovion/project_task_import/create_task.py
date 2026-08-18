from datetime import timedelta
from sunovion.project_task_import.utils import request_payload
from airflow.models import Variable
import rail


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_process_create_task_child_{config.instance}',
        description='Sunovion Project and Task Sync - Process Create Task',
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
            no_task="create_task_draft"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_task_draft',
            end_task="catch_and_log_errors",
        )

        create_task_draft = rail.RepliconServiceOperator(
            task_id="create_task_draft",
            endpoint="services/taskService1.svc/CreateNewDraft",
            data=request_payload.create_task_draft
        )

        create_task_name = rail.RepliconServiceOperator(
            task_id="create_task_name",
            endpoint="services/TaskService1.svc/UpdateName",
            data=request_payload.create_task_name
        )

        create_task_code = rail.RepliconServiceOperator(
            task_id="create_task_code",
            endpoint="services/TaskService1.svc/UpdateCode",
            data=request_payload.create_task_code
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id="publish_draft",
            endpoint="services/TaskService1.svc/PublishDraft",
            data=lambda: {
                "draftUri": rail.result('create_task_draft')
            }
        )

        create_allow_time_entry = rail.RepliconServiceOperator(
            task_id="create_allow_time_entry",
            endpoint="services/TaskService1.svc/UpdateAllowTimeEntry",
            data=request_payload.create_allow_time_entry
        )

        update_cost_type = rail.RepliconServiceOperator(
            task_id="update_cost_type",
            endpoint="services/TaskService1.svc/UpdateCostType",
            data=request_payload.update_cost_type
        )

        get_resource_department_uri = rail.RepliconServiceOperator(
            task_id="get_resource_department_uri",
            endpoint="services/DepartmentService1.svc/GetEnabledDepartments",
            data={},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'name',
                        config.default_department_name, 'uri')
        )

        bulk_update_resource_assignment = rail.RepliconServiceOperator(
            task_id="bulk_update_resource_assignment",
            endpoint="services/TaskService1.svc/BulkUpdateResourceAssignments",
            data=request_payload.bulk_update_resource_assignment
        )

        is_task_status_open = rail.IfOperator(
            task_id='is_task_status_open',
            test=request_payload.is_task_status_open,
            yes_task='task_status_open',
            no_task='is_task_status_closed'
        )

        task_status_open = rail.RepliconServiceOperator(
            task_id="task_status_open",
            endpoint="services/TaskService1.svc/Open",
            data=request_payload.task_create_status
        )

        is_task_status_closed = rail.IfOperator(
            task_id='is_task_status_closed',
            test=request_payload.is_task_status_closed,
            yes_task='task_status_close',
            no_task='is_create_start_date_present'
        )

        task_status_close = rail.RepliconServiceOperator(
            task_id="task_status_close",
            endpoint="services/TaskService1.svc/Close",
            data=request_payload.task_create_status
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

        is_create_start_date_present = rail.IfOperator(
            task_id='is_create_start_date_present',
            test=request_payload.is_create_start_date_present,
            yes_task='update_timeentry_date_range',
            no_task='is_create_end_date_present'
        )

        update_timeentry_date_range = rail.RepliconServiceOperator(
            task_id="update_timeentry_date_range",
            endpoint="services/TaskService1.svc/UpdateTimeEntryDateRange",
            data=request_payload.update_timeentry_date_range
        )

        is_create_end_date_present = rail.IfOperator(
            task_id='is_create_end_date_present',
            test=request_payload.is_create_end_date_present,
            yes_task='update_allow_time_entry',
            no_task='log_task_created'
        )

        update_allow_time_entry = rail.RepliconServiceOperator(
            task_id="update_allow_time_entry",
            endpoint="services/TaskService1.svc/UpdateAllowTimeEntry",
            data=request_payload.update_create_allow_time_entry
        )

        log_task_created = rail.WriteLogOperator(
            task_id="log_task_created",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Task Created',
            severity='Success',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Task Created'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Failed',
            message='{{ dag_run_ecid() }} - Task creation - {{ get_error_message() }}',
            properties={
                'projectcode': '{{ dag_run.conf.projectcode }}',
                'taskcode': '{{ dag_run.conf.taskcode }} / {{ dag_run.conf.taskname }}',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Task creation - {{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> create_task_draft
        create_task_draft >> create_task_name >> create_task_code >> publish_draft >> create_allow_time_entry
        create_allow_time_entry >> update_cost_type >> get_resource_department_uri >> bulk_update_resource_assignment
        bulk_update_resource_assignment >> is_task_status_open >> rail.Label(
            "Yes") >> task_status_open >> is_task_status_closed
        is_task_status_open >> rail.Label("No") >> is_task_status_closed
        is_task_status_closed >> rail.Label(
            "Yes") >> task_status_close >> log_task_status_closed >> is_create_start_date_present
        is_task_status_closed >> rail.Label(
            "No") >> is_create_start_date_present
        is_create_start_date_present >> rail.Label(
            "Yes") >> update_timeentry_date_range >> is_create_end_date_present
        is_create_start_date_present >> rail.Label(
            "No") >> is_create_end_date_present
        is_create_end_date_present >> rail.Label(
            "Yes") >> update_allow_time_entry >> log_task_created
        is_create_end_date_present >> rail.Label("No") >> log_task_created
        log_task_created >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
