from datetime import timedelta
import rail
from odessa.project_team_update_v2.utils import python_callable_method
from odessa.project_team_update_v2.utils import request_payload
from odessa.project_team_update_v2.utils import response_filter


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"odessa_jira_import_child_update_project_v2_{config.instance}",
        description=f"odessa jira import child update project {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        check_parent_jira = rail.IfOperator(
            task_id = 'check_parent_jira',
            test= '{{ dag_run.conf.Epicid != "None" }}',
            yes_task= 'get_parent_issue_data',
            no_task= 'get_task_data'
        )

        get_parent_issue_data = rail.SimpleHttpOperator(
            task_id='get_parent_issue_data',
            method='GET',
            endpoint='rest/api/2/issue/{{ dag_run.conf.Epicid }}',
            http_conn_id='odessa_jira',
            response_filter=lambda response: response.json()['fields']
        )

        get_task_data = rail.RepliconServiceOperator(
            task_id='get_task_data',
            endpoint='/services/TaskListService1.svc/GetData',
            data=request_payload.get_task_payload,
            response_filter=response_filter.check_task_data
        )

        has_task_data = rail.IfOperator(
            task_id='has_task_data',
            test=lambda: bool(rail.result("get_task_data")),
            yes_task='check_valid_task_data_to_process',
            no_task='check_time_and_material_billing_type'
        )

        check_valid_task_data_to_process = rail.EmptyOperator(
            task_id='check_valid_task_data_to_process'
        )

        has_any_valid_task_data = rail.IfOperator(
            task_id='has_any_valid_task_data',
            test=lambda dag_run: bool(python_callable_method.valid_task_data_to_process(
                dag_run, rail.result("get_task_data"))),
            yes_task='update_tasks_in_replicon',
            no_task= 'log_to_sumo'
        )

        update_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items=lambda: rail.load_all_records(
                rail.result('get_task_data')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.get_update_task_payload,
            data_handler=lambda response: response[0]
        )

        check_time_and_material_billing_type = rail.IfOperator(
            task_id='check_time_and_material_billing_type',
            test=bool('{{ dag_run.conf.Billingtype == "Time and Material" }}'),
            yes_task="update_time_and_material_project_task",
            no_task='check_fixed_bid_billing_type'
        )

        update_time_and_material_project_task = rail.RepliconServiceOperator(
            task_id='update_time_and_material_project_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=lambda dag_run: request_payload.update_time_and_material_project_task_data(
                dag_run, dag_run.conf['end_date'])
        )

        get_all_team_members = rail.RepliconServiceOperator(
            task_id='get_all_team_members',
            endpoint='/services/ProjectService1.svc/GetAllProjectTeamMembers',
            data={
                "projectUri": '{{ dag_run.conf.Repliconprojecturi }}'
            },
            response_filter=response_filter.get_all_team_members_data
        )

        bulk_update_task_team_members = rail.RepliconServiceOperator(
            task_id='bulk_update_task_team_members',
            endpoint='/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data=request_payload.bulk_update_task_team_members_data
        )

        check_fixed_bid_billing_type = rail.IfOperator(
            task_id='check_fixed_bid_billing_type',
            test=bool('{{ dag_run.conf.Billingtype == "Fixed Bid" }}'),
            yes_task="update_fixed_bid_project_task",
            no_task='log_to_sumo'
        )

        update_fixed_bid_project_task = rail.RepliconServiceOperator(
            task_id='update_fixed_bid_project_task',
            endpoint='/services/ProjectService1.svc/PutTask',
            data=lambda dag_run: request_payload.update_fixed_bid_project_task_data(
                dag_run, dag_run.conf['end_date'])
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Taskkey': '{{ dag_run.conf.Key }}',
                'projectname': '{{ dag_run.conf.Projectname }}',
                'action': '{{ "Update" if result("update_tasks_in_replicon") else "Add" }}',
                'Status': '''{{ "Processed" if (result("update_tasks_in_replicon") or result("update_time_and_material_project_task") or
                result("update_fixed_bid_project_task")) else "Skipped" }}''',
            }
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        check_parent_jira >> rail.Label(
            "Yes") >> get_parent_issue_data >> get_task_data

        check_parent_jira >> rail.Label(
            "No") >> get_task_data >> has_task_data

        has_task_data >> rail.Label("Yes") >> check_valid_task_data_to_process >> has_any_valid_task_data >> \
            rail.Label("Yes") >> update_tasks_in_replicon >> log_to_sumo

        has_task_data >> \
            rail.Label("No") >> check_time_and_material_billing_type

        has_any_valid_task_data >> \
            rail.Label("No") >> log_to_sumo

        check_time_and_material_billing_type >> \
            rail.Label(
                "Yes") >> update_time_and_material_project_task >> get_all_team_members >> bulk_update_task_team_members >> log_to_sumo

        check_time_and_material_billing_type >> \
            rail.Label("No") >> check_fixed_bid_billing_type

        check_fixed_bid_billing_type >> \
            rail.Label("Yes") >> update_fixed_bid_project_task >> log_to_sumo

        check_fixed_bid_billing_type >> \
            rail.Label("No") >> log_to_sumo >> end

    return dag


rail.for_each_instance(create_child_dag)
