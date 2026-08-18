import rail
from odessa.project_team_update_v2.utils import request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"odessa_jira_import_child_create_project_v2_{config.instance}",
        description=f"odessa jira import child create project V2 {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_wbs_max_active_runs
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        check_parent_jira = rail.IfOperator(
            task_id = 'check_parent_jira',
            test= '{{ dag_run.conf.Epicid != "None" }}',
            yes_task= 'get_parent_issue_data',
            no_task= 'create_project'
        )

        get_parent_issue_data = rail.SimpleHttpOperator(
            task_id='get_parent_issue_data',
            method='GET',
            endpoint='rest/api/2/issue/{{ dag_run.conf.Epicid }}',
            http_conn_id='odessa_jira',
            response_filter=lambda response: response.json()['fields']
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data= request_payload.create_project_payload
        )

        apply_new_client = rail.RepliconServiceOperator(
            task_id='apply_new_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data= request_payload.apply_client
        )

        create_task = rail.RepliconServiceOperator(
            task_id='create_task',
            endpoint='/services/TaskService1.svc/CreateTaskHierarchyOrApplyModifications',
            data= request_payload.create_task_payload
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'projectname': '{{ dag_run.conf.Projectname }}',
                'action': 'Add',
                'Status': '{{ "Processed" if (result("create_task")[0]["task"] if result("create_task") else None) else "Skipped" }}',
            }
        )

        check_parent_jira >> rail.Label(
            "Yes") >> get_parent_issue_data >> create_project

        check_parent_jira >> rail.Label(
            "No") >> create_project >> apply_new_client >> create_task >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
