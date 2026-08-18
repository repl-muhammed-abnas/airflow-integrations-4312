from datetime import timedelta

import rail
from mammoet.project_import_v1.utils import response_filter,request_payload,custom_method
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_task_child_dag_id,
        description='Mammoet Process Each Task Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_task_data_from_query'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_task_data_from_query',
            end_task='catch_and_log_error',
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_task_data_from_query = rail.QueryCollectionOperator(
            task_id='get_task_data_from_query',
            query="""SELECT * from validtaskdata WHERE projectcode == :program_code""",
            query_params = {
                'program_code': '{{ dag_run.conf.projectcode }}'
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.projectcode }}",
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        is_project_available = rail.IfOperator(
            task_id = 'is_project_available',
            test= lambda: bool(rail.result("get_project_details")),
            yes_task= 'get_all_tasks_for_project',
            no_task= 'log_project_not_available'
        )

        log_project_not_available = rail.WriteLogOperator(
            task_id="log_project_not_available",
            log="{{dag_run.conf.log}}",
            message="Project is not available in replicon",
            items=lambda dag_run: rail.load_all_records(
                rail.result("get_task_data_from_query")),
            properties=lambda dag_run, item: {
                "projectcode": item['projectcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Add',
                "details": "Project is not available in replicon",
                "status": 'Skipped'
            }
        )

        get_all_tasks_for_project = rail.RepliconServiceOperator(
            task_id="get_all_tasks_for_project",
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{result('get_project_details').uri}}"
            },
            data_handler=response_filter.format_project_task_details
        )

        get_all_task_to_add_update = rail.PythonOperator(
            task_id="get_all_task_to_add_update",
            python_callable=custom_method.get_task_to_add_update_skip
        )

        has_tasks_to_add = rail.IfOperator(
            task_id = 'has_tasks_to_add',
            test= '{{ result("get_all_task_to_add_update").tasks_to_add | is_truthy }}',
            yes_task= 'add_task',
            no_task= 'has_tasks_to_update'
        )

        add_task = rail.RepliconServiceOperator(
            task_id="add_task",
            endpoint="services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_batch_put_task_payload
        )

        log_task_added_success_error = rail.WriteLogOperator(
            task_id="log_task_added_success_error",
            log="{{dag_run.conf.log}}",
            severity="{{item.status}}",
            message="{{ item.details }}",
            items=lambda dag_run: custom_method.map_task_success_error(
                add_task.task_id, "add","tasks_to_add"),
            properties=lambda dag_run, item: {
                "projectcode": item['projectcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Add',
                "details": item['details'],
                "status": item['status'],
                "ecid": "{{ dag_run_ecid() }}"
            }
        )

        has_tasks_to_update = rail.IfOperator(
            task_id = 'has_tasks_to_update',
            test= '{{ result("get_all_task_to_add_update").tasks_to_update | is_truthy }}',
            yes_task= 'update_task',
            no_task= 'catch_and_log_error'
        )

        update_task = rail.RepliconServiceOperator(
            task_id="update_task",
            endpoint="/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications",
            data=request_payload.get_update_task_payload
        )

        log_task_updated_success_error = rail.WriteLogOperator(
            task_id="log_task_updated_success_error",
            log="{{dag_run.conf.log}}",
            message="{{ item.details }}",
            items=lambda dag_run: custom_method.map_task_success_error(
                update_task.task_id, "update","tasks_to_update"),
            properties=lambda dag_run, item: {
                "projectcode": item['projectcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Update',
                "details": item['details'],
                "status": item['status'],
                "ecid": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            severity="Error",
            trigger_rule="one_failed",
            log="{{dag_run.conf.log}}",
            items='{{ result("get_task_data_from_query") | load_all_records | to_json }}',
            message="{{ get_error_message() }}",
            properties=lambda dag_run, item: {
                "projectcode": item['projectcode'],
                "taskcode": item['taskcode'],
                "taskname": item['taskname'],
                'action': 'Update',
                "details": "{{ get_error_message() }}",
                "status": "Error",
                "ecid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_task_data_from_query

        get_task_data_from_query >> get_project_details >> is_project_available

        is_project_available >> rail.Label(
            "Yes") >> get_all_tasks_for_project >> get_all_task_to_add_update >> \
            has_tasks_to_add

        is_project_available >> rail.Label(
            "No") >> log_project_not_available >> catch_and_log_error

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_task >> log_task_added_success_error >> has_tasks_to_update

        has_tasks_to_add >> rail.Label(
            "No") >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_task >> log_task_updated_success_error >> catch_and_log_error

        has_tasks_to_update >> rail.Label(
            "No") >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
