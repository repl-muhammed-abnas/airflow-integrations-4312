from datetime import datetime, timedelta
import rail
from hostopia.jira_integration.utils import request_payload
from hostopia.jira_integration.utils import response_filter
from hostopia.jira_integration.utils import custom_method
from airflow.models import Variable
# pylint: disable=too-many-statements


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"hostopia_jira_import_child_update_project_{config.instance}",
        description=f"hostopia jira import child project update {config.instance}",
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
                    "code": '{{ dag_run.conf.Key }}'
                }]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        should_update_project_name = rail.IfOperator(
            task_id="should_update_project_name",
            test="{{ result('serach_project_in_replicon').displayText != dag_run.conf.summary }}",
            yes_task="update_project_name",
            no_task="should_update_project_date_range"
        )

        update_project_name = rail.RepliconServiceOperator(
            task_id="update_project_name",
            endpoint="/services/ProjectService1.svc/UpdateName",
            data={
                "projectUri": "{{ result('serach_project_in_replicon').uri }}",
                "name": '{{ dag_run.conf.summary }}'
            }
        )

        def compare_date_range(dag_run):
            replicon_start_date = rail.result('serach_project_in_replicon')[
                'timeEntryDateRange']['startDate']
            replicon_end_date = rail.result('serach_project_in_replicon')[
                'timeEntryDateRange']['endDate']
            start_date = datetime.strptime(
                f"{replicon_start_date['year']}-{replicon_start_date['month']}-{replicon_start_date['day']}", "%Y-%m-%d") if replicon_start_date else None
            end_date = datetime.strptime(
                f"{replicon_end_date['year']}-{replicon_end_date['month']}-{replicon_end_date['day']}", "%Y-%m-%d") if replicon_end_date else None
            jira_start_date = datetime.fromisoformat(
                dag_run.conf['startdate']) if dag_run.conf['startdate'] else None
            jira_end_date = datetime.fromisoformat(
                dag_run.conf['enddate']) if dag_run.conf['enddate'] else None
            if jira_start_date or jira_end_date:
                if start_date != jira_start_date or end_date != jira_end_date:
                    return True
            return False

        should_update_project_date_range = rail.IfOperator(
            task_id="should_update_project_date_range",
            test=compare_date_range,
            yes_task="update_project_date_range",
            no_task="should_update_project_manager"
        )

        update_project_date_range = rail.RepliconServiceOperator(
            task_id="update_project_date_range",
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=request_payload.update_date_payload
        )

        should_update_project_manager= rail.IfOperator(
            task_id= 'should_update_project_manager',
            test= '{{ dag_run.conf.assignee | is_truthy }}',
            yes_task= 'search_project_manager_in_replicon',
            no_task='should_update_status'
        )

        search_project_manager_in_replicon = rail.RepliconServiceOperator(
            task_id='search_project_manager_in_replicon',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: request_payload.get_user_data_payload(dag_run,
                                                            dag_run.conf["assignee"]),
            response_filter=response_filter.get_users_data
        )

        is_project_leader_available = rail.IfOperator(
            task_id='is_project_leader_available',
            test='{{ result("search_project_manager_in_replicon") | is_truthy }}',
            yes_task='update_project_leader',
            no_task='should_update_status'
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id='update_project_leader',
            endpoint='/services/ProjectService1.svc/UpdateProjectLeader',
            data={
                "projectUri": '{{ result("serach_project_in_replicon").uri }}',
                "userUri": '{{ result("search_project_manager_in_replicon")[0].uri }}'
            }
        )

        should_update_status = rail.IfOperator(
            task_id='should_update_status',
            test='{{ dag_run.conf.status == "Done" }}',
            yes_task="update_status",
            no_task='get_resource_data_from_jira'
        )

        update_status = rail.RepliconServiceOperator(
            task_id='update_status',
            endpoint='/services/ProjectService1.svc/UpdateStatus',
            data={
                "projectUri": "{{ result('serach_project_in_replicon').uri }}",
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        get_all_tasks_for_project= rail.RepliconServiceOperator(
            task_id='get_all_tasks_for_project',
            endpoint='/services/ProjectService1.svc/BulkGetTaskDetails',
            data={
                "pageIndex": "1",
                "pageSize": "10000",
                "projectUris": [
                    "{{ result('serach_project_in_replicon').uri }}"
                ]
            },
            response_filter=response_filter.get_task_uris
        )

        update_task_status = rail.RepliconServiceCallForEachItemOperator(
            task_id="update_task_status",
            endpoint='/services/TaskService1.svc/Close',
            items='{{ result("get_all_tasks_for_project") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data={
                "taskUri": '{{ item.uri }}'
            },
        )

        get_resource_data_from_jira = rail.PythonOperator(
            task_id="get_resource_data_from_jira",
            python_callable=custom_method.get_resource_data,
        )

        has_get_resource_data_from_jira = rail.IfOperator(
            task_id='has_get_resource_data_from_jira',
            test='{{ result("get_resource_data_from_jira") | is_truthy }}',
            yes_task='get_user_list',
            no_task='get_all_sub_task_for_the_project_key'
        )

        get_user_list = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_list",
            endpoint='/services/UserListService1.svc/GetData',
            items='{{ result("get_resource_data_from_jira") | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            flatten=True,
            data=request_payload.get_user_data_payload,
            data_handler=response_filter.map_user_list
        )

        has_users_to_assign = rail.IfOperator(
            task_id='has_users_to_assign',
            test='{{ result("get_user_list") | is_truthy }}',
            yes_task='assign_project_team',
            no_task='get_all_sub_task_for_the_project_key'
        )

        assign_project_team = rail.RepliconServiceOperator(
            task_id='assign_project_team',
            endpoint='/services/ProjectService1.svc/PutProjectTeamMemberAssignments',
            data=request_payload.resource_uri_payload
        )

        get_all_sub_task_for_the_project_key = rail.SimpleHttpOperator(
            task_id='get_all_sub_task_for_the_project_key',
            method='GET',
            endpoint='rest/api/3/search?jql=issuetype = Sub-task AND parent = {{ dag_run.conf.Key }} order by created DESC',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['issues']
        )

        has_get_all_sub_task_for_the_project_key = rail.IfOperator(
            task_id='has_get_all_sub_task_for_the_project_key',
            test='{{ result("get_all_sub_task_for_the_project_key") | is_truthy}}',
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

        existing_task_list = rail.CreateCollectionOperator(
            task_id="existing_task_list",
            source="{{ result('get_all_project_tasks') | to_json }}",
            name="existingtasklist"
        )

        new_task_list_schema = rail.DataAdaptorOperator(
            task_id="new_task_list_schema",
            source=lambda: rail.result("get_all_sub_task_for_the_project_key"),
            columns=['taskname', 'taskcode', 'startdate', 'enddate', 'status'],
            data=custom_method.convert_data_to_task_details,
        )

        new_task_list = rail.CreateCollectionOperator(
            task_id="new_task_list",
            source=lambda: rail.result("new_task_list_schema"),
            name="newtasklist",
            columns={
                'taskname',
                'taskcode',
                'startdate',
                'enddate',
                'status'
            }
        )

        get_tasks_to_update = rail.QueryCollectionOperator(
            task_id="get_tasks_to_update",
            query="""SELECT * FROM newtasklist WHERE taskname IN (SELECT DISTINCT name FROM existingtasklist)""",
        )

        get_tasks_to_add = rail.QueryCollectionOperator(
            task_id="get_tasks_to_add",
            query="""SELECT * FROM newtasklist WHERE (taskname) NOT IN (SELECT DISTINCT name FROM existingtasklist)""",
        )


        has_tasks_to_update = rail.IfOperator(
            task_id='has_tasks_to_update',
            test='{{ result("get_tasks_to_update", "length") > 0 }}',
            yes_task="update_tasks_in_replicon",
            no_task="has_tasks_to_add",
        )

        update_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='update_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items="{{ result('get_tasks_to_update') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run, update_action_type='update'),
            data_handler=lambda response: response[0]
        )

        has_tasks_to_add = rail.IfOperator(
            task_id='has_tasks_to_add',
            test='{{ result("get_tasks_to_add", "length") > 0 }}',
            yes_task="add_tasks_in_replicon",
            no_task="end",
        )

        add_tasks_in_replicon = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_tasks_in_replicon',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            items="{{ result('get_tasks_to_add') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            data=lambda dag_run, item: request_payload.get_task_payload(
                item, dag_run),
            data_handler=lambda response: response[0]
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Projectcode': '{{ dag_run.conf.Key }}',
                'Projectname': '{{ dag_run.conf.summary }}',
                'Action': 'Update',
                'Status': 'Processed',
            }
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> end

        can_run_batch_task >> rail.Label(
            'No') >> serach_project_in_replicon

        serach_project_in_replicon >> should_update_project_name

        should_update_project_name >> rail.Label(
            "Yes") >> update_project_name >> should_update_project_date_range

        should_update_project_name >> rail.Label(
            "No") >> should_update_project_date_range

        should_update_project_date_range >> rail.Label(
            "No") >> should_update_project_manager

        should_update_project_date_range >> rail.Label(
            "Yes") >> update_project_date_range >> should_update_project_manager

        should_update_project_manager >> rail.Label(
            "Yes") >> search_project_manager_in_replicon >> is_project_leader_available

        should_update_project_manager >> rail.Label(
            "No") >> should_update_status

        is_project_leader_available >> rail.Label(
            "Yes") >> update_project_leader >> should_update_status

        is_project_leader_available >> rail.Label(
            "No") >> should_update_status

        should_update_status >> rail.Label(
            "Yes") >> update_status >> get_all_tasks_for_project >> update_task_status >> get_resource_data_from_jira

        should_update_status >> rail.Label(
            "No") >> get_resource_data_from_jira >> has_get_resource_data_from_jira

        has_get_resource_data_from_jira >> rail.Label(
            "Yes") >> get_user_list >> has_users_to_assign

        has_users_to_assign >> rail.Label(
            "Yes") >> assign_project_team >> get_all_sub_task_for_the_project_key

        has_users_to_assign >> rail.Label(
            "No") >> get_all_sub_task_for_the_project_key

        has_get_resource_data_from_jira >> rail.Label(
            "No") >> get_all_sub_task_for_the_project_key >> has_get_all_sub_task_for_the_project_key

        has_get_all_sub_task_for_the_project_key >> rail.Label(
            "Yes") >> get_all_project_tasks >> existing_task_list >> new_task_list_schema >> new_task_list >> \
            get_tasks_to_update >> get_tasks_to_add >> has_tasks_to_update

        has_tasks_to_update >> rail.Label(
            "Yes") >> update_tasks_in_replicon >> has_tasks_to_add

        has_tasks_to_update >> rail.Label(
            "No") >> has_tasks_to_add

        has_tasks_to_add >> rail.Label(
            "Yes") >> add_tasks_in_replicon >> end

        has_tasks_to_add >> rail.Label(
            "No") >> end

        has_get_all_sub_task_for_the_project_key >> rail.Label(
            "No") >> end >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
