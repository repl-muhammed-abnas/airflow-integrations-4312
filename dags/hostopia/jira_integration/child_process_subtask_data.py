from datetime import timedelta
import rail
from hostopia.jira_integration.utils import request_payload
from hostopia.jira_integration.utils import response_filter
from hostopia.jira_integration.utils import custom_method
from airflow.models import Variable
# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"hostopia_jira_import_child_process_subtask_data_{config.instance}",
        description=f"hostopia jira import child process subtask data {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='serach_project_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='serach_project_in_replicon',
            end_task='end',
        )

        serach_project_in_replicon = rail.RepliconServiceOperator(
            task_id='serach_project_in_replicon',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {
                    "code": '{{ dag_run.conf.project_code }}'
                }]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        has_project_data = rail.IfOperator(
            task_id='has_project_data',
            test='{{ result("serach_project_in_replicon") | is_truthy }}',
            yes_task='get_all_project_tasks',
            no_task='end'
        )

        get_all_project_tasks = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks",
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails2",
            data={
                "pageIndex": 1,
                "pageSize": 1000,
                "projectUris": ["{{ result('serach_project_in_replicon').uri }}"]
            },
            data_handler=response_filter.map_existing_project_tasks
        )

        check_task_in_replicon= rail.PythonOperator(
            task_id= 'check_task_in_replicon',
            python_callable= custom_method.check_task_data
        )

        get_task_data= rail.PythonOperator(
            task_id= 'get_task_data',
            python_callable= custom_method.get_task_data
        )

        has_tasks_to_update = rail.IfOperator(
            task_id='has_tasks_to_update',
            test=lambda: bool(rail.result("check_task_in_replicon")),
            yes_task="update_tasks_in_replicon",
            no_task="add_tasks_in_replicon",
        )

        update_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items="{{ result('get_task_data') | to_json}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run, update_action_type='update'),
            data_handler=lambda response: response[0]
        )

        add_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items="{{ result('get_task_data') | to_json}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run),
            data_handler=lambda response: response[0]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> end

        can_run_batch_task >> rail.Label(
            'No') >> serach_project_in_replicon

        serach_project_in_replicon >> has_project_data >> rail.Label(
            "Yes") >> get_all_project_tasks >> check_task_in_replicon >> \
                get_task_data >> has_tasks_to_update

        has_project_data >> rail.Label(
            "No") >> end

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_tasks_in_replicon >> end

        has_tasks_to_update >> rail.Label(
            "No") >> add_tasks_in_replicon >> end >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
