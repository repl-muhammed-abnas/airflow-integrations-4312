from datetime import timedelta
import rail
from hostopia.jira_integration.utils import request_payload
from hostopia.jira_integration.utils import response_filter
from hostopia.jira_integration.utils import custom_method
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"hostopia_jira_import_child_create_project_{config.instance}",
        description=f"hostopia jira import child project create {config.instance}",
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
            no_task='create_project_in_replicon'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_project_in_replicon',
            end_task='finish',
        )

        create_project_in_replicon = rail.RepliconServiceOperator(
            task_id='create_project_in_replicon',
            endpoint='/services/ProjectService1.svc/PutProject5',
            data=request_payload.get_project_creation_payload,
        )

        has_assignee_present = rail.IfOperator(
            task_id='has_assignee_present',
            test='{{ dag_run.conf.assignee | is_truthy }}',
            yes_task='search_project_manager_in_replicon',
            no_task='get_resource_data_from_jira_to_add'
        )

        search_project_manager_in_replicon = rail.RepliconServiceOperator(
            task_id='search_project_manager_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_user_data_payload(dag_run,
                                                                       dag_run.conf['assignee']),
            response_filter=response_filter.get_users_data
        )

        is_project_leader_available = rail.IfOperator(
            task_id='is_project_leader_available',
            test='{{ result("search_project_manager_in_replicon") | is_truthy }}',
            yes_task='update_project_leader',
            no_task='get_resource_data_from_jira_to_add'
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id='update_project_leader',
            endpoint='/services/ProjectService1.svc/UpdateProjectLeader',
            data={
                "projectUri": '{{ result("create_project_in_replicon").uri }}',
                "userUri": '{{ result("search_project_manager_in_replicon")[0].uri }}'
            }
        )

        get_resource_data_from_jira_to_add = rail.PythonOperator(
            task_id="get_resource_data_from_jira_to_add",
            python_callable=custom_method.get_resource_data,
        )

        has_get_resource_data_from_jira_to_add = rail.IfOperator(
            task_id='has_get_resource_data_from_jira_to_add',
            test='{{ result("get_resource_data_from_jira_to_add") | is_truthy }}',
            yes_task='get_user_list',
            no_task='get_all_sub_task_for_the_project_key_to_add'
        )

        get_user_list = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_list",
            endpoint='/services/UserListService1.svc/GetData',
            items='{{ result("get_resource_data_from_jira_to_add") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data=request_payload.get_user_data_payload,
            data_handler=response_filter.map_user_list
        )

        has_get_user_list = rail.IfOperator(
            task_id='has_get_user_list',
            test='{{ result("get_user_list") | is_truthy }}',
            yes_task='project_team_to_add',
            no_task='get_all_sub_task_for_the_project_key_to_add'
        )

        project_team_to_add = rail.RepliconServiceOperator(
            task_id='project_team_to_add',
            endpoint='/services/ProjectService1.svc/PutProjectTeamMemberAssignments',
            data=request_payload.resource_uri_payload
        )

        get_all_sub_task_for_the_project_key_to_add = rail.SimpleHttpOperator(
            task_id='get_all_sub_task_for_the_project_key_to_add',
            method='GET',
            endpoint='rest/api/3/search?jql=issuetype = Sub-task AND parent = {{ dag_run.conf.Key }} order by created DESC',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['issues']
        )

        has_get_all_sub_task_for_the_project_key_to_add = rail.IfOperator(
            task_id='has_get_all_sub_task_for_the_project_key_to_add',
            test='{{ result("get_all_sub_task_for_the_project_key_to_add") | is_truthy}}',
            yes_task='project_task_to_add',
            no_task='finish'
        )

        project_task_to_add = rail.RepliconServiceCallForEachItemOperator(
            task_id='project_task_to_add',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items='{{ result("get_all_sub_task_for_the_project_key_to_add") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=request_payload.get_task_create_payload,
            data_handler=lambda response: response[0]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Projectcode': '{{ dag_run.conf.Key }}',
                'Projectname': '{{ dag_run.conf.summary }}',
                'Action': 'Add',
                'Status': 'Processed',
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> create_project_in_replicon >> has_assignee_present >> rail.Label(
                "Yes") >> search_project_manager_in_replicon >> is_project_leader_available

        has_assignee_present >> rail.Label(
            "No") >> get_resource_data_from_jira_to_add

        is_project_leader_available >> rail.Label(
            "Yes") >> update_project_leader >> get_resource_data_from_jira_to_add >> has_get_resource_data_from_jira_to_add

        has_get_resource_data_from_jira_to_add >> rail.Label(
            "Yes") >> get_user_list >> has_get_user_list

        has_get_user_list >> rail.Label(
            "Yes") >> project_team_to_add >> get_all_sub_task_for_the_project_key_to_add

        has_get_resource_data_from_jira_to_add >> rail.Label(
            "No") >> get_all_sub_task_for_the_project_key_to_add

        has_get_user_list >> rail.Label(
            "No") >> get_all_sub_task_for_the_project_key_to_add >> has_get_all_sub_task_for_the_project_key_to_add

        has_get_all_sub_task_for_the_project_key_to_add >> rail.Label(
            "Yes") >> project_task_to_add >> finish

        has_get_all_sub_task_for_the_project_key_to_add >> rail.Label(
            "No") >> finish >> log_to_sumo

        is_project_leader_available >> rail.Label(
            "No") >> get_resource_data_from_jira_to_add

    return dag


rail.for_each_instance(create_child_dag)
