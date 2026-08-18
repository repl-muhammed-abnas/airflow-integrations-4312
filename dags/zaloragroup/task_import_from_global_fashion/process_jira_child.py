from datetime import timedelta
from airflow.models import Variable
import rail
from zaloragroup.task_import_from_global_fashion.utils import custom_method
from zaloragroup.task_import_from_global_fashion.utils import request_payload
from zaloragroup.task_import_from_global_fashion.utils import response_filter

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_process_jira_dag_id,
        description=f'zaloragroup process jira child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='jira_sync_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='jira_sync_data',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        jira_sync_data= rail.SimpleHttpOperator(
            task_id='jira_sync_data',
            method='GET',
            endpoint='rest/api/3/search?jql=updated >= -1h&maxResults=100&startAt={{ dag_run.conf.start_from }}',
            http_conn_id=config.http_conn_id,
            response_filter=lambda response: response.json()['issues']
        )

        map_to_issue_schema = rail.DataAdaptorOperator(
            task_id="map_to_issue_schema",
            source=lambda: rail.result("jira_sync_data"),
            columns=['issue', 'key', 'self', 'created','summary', 'status', 'taskname', 'projectname'],
            data= custom_method.convert_input_data_to_task_data,
        )

        for_each_issue_key= rail.ForEachOperator(
            task_id= 'for_each_issue_key',
            items= '{{ result("map_to_issue_schema") }}',
            start_task= 'bulk_get_project_details',
            end_task= 'for_each_issue_key_end'
        )

        bulk_get_project_details=rail.RepliconServiceOperator(
            task_id='bulk_get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=request_payload.get_project_input_data
        )

        is_project_not_found = rail.IfOperator(
            task_id='is_project_not_found',
            test="{{ result('bulk_get_project_details')[0].projectDetails | is_falsy }}",
            yes_task="create_project",
            no_task="get_task_listy_by_code",
        )

        create_project= rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint="/services/ProjectService1.svc/PutProjectInfo2",
            data=request_payload.get_project_payload
        )

        get_enabled_departments= rail.RepliconServiceOperator(
            task_id='get_enabled_departments',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data_handler= request_payload.get_required_department
        )

        update_project_team_member_assignment = rail.RepliconServiceOperator(
            task_id='update_project_team_member_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data= request_payload.get_project_team_assign_payload
        )

        get_task_listy_by_code= rail.RepliconServiceOperator(
            task_id='get_task_listy_by_code',
            endpoint="/services/TaskListService1.svc/GetData",
            data=request_payload.get_task_payload,
            data_handler= response_filter.get_filter_task_data
        )

        check_task_uri= rail.IfOperator(
            task_id='check_task_uri',
            test=lambda: custom_method.check_task_data('taskcode'),
            yes_task="for_each_issue_key_end",
            no_task="is_task_synced_competely",
        )

        is_task_synced_competely= rail.IfOperator(
            task_id='is_task_synced_competely',
            test=lambda: custom_method.check_task_data('taskname'),
            yes_task="for_each_issue_key_end",
            no_task="add_task",
        )

        add_task= rail.RepliconServiceOperator(
            task_id='add_task',
            endpoint="/services/ProjectService1.svc/AddTask",
            data=request_payload.create_task_payload
        )

        get_all_project_tem_members = rail.RepliconServiceOperator(
            task_id = 'get_all_project_tem_members',
            endpoint= '/services/ProjectService1.svc/GetAllProjectTeamMemberDetails',
            data= request_payload.get_project_team_member_payload,
            data_handler= response_filter.get_project_team_uris
        )

        assign_project_team_members_to_task = rail.RepliconServiceOperator(
            task_id = 'assign_project_team_members_to_task',
            endpoint= '/services/TaskService1.svc/BulkUpdateResourceAssignments',
            data= request_payload.get_task_team_payload
        )

        for_each_issue_key_end = rail.EmptyOperator(
            task_id = 'for_each_issue_key_end'
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> jira_sync_data >> map_to_issue_schema >> for_each_issue_key >> \
                bulk_get_project_details >> is_project_not_found

        is_project_not_found >> rail.Label(
            'Yes') >> create_project >> get_enabled_departments >> update_project_team_member_assignment >> get_task_listy_by_code

        is_project_not_found >> rail.Label(
            'No') >> get_task_listy_by_code >> check_task_uri

        check_task_uri >> rail.Label(
            'Yes')  >> is_task_synced_competely

        check_task_uri >> rail.Label(
            'No')  >> for_each_issue_key_end

        for_each_issue_key >> for_each_issue_key_end

        is_task_synced_competely >> rail.Label(
            "Yes") >> add_task >> get_all_project_tem_members >> assign_project_team_members_to_task >> for_each_issue_key_end

        is_task_synced_competely >> rail.Label(
            'No')  >> for_each_issue_key_end >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
