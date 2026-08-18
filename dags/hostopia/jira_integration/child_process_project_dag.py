from datetime import timedelta
import json
import rail
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"hostopia_jira_import_child_process_project_{config.instance}",
        description=f"hostopia jira import child process project {config.instance}",
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
            no_task='get_all_data_for_issue_key'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_all_data_for_issue_key',
            end_task='end',
        )

        get_all_data_for_issue_key = rail.QueryCollectionOperator(
            task_id='get_all_data_for_issue_key',
            query="""SELECT * FROM jiraupdatedata WHERE key== :Key """,
            name='jiralistquery',
            query_params={
                "Key": "{{ dag_run.conf.key }}",
            },
        )

        get_projects_data_from_query = rail.PythonOperator(
            task_id='get_projects_data_from_query',
            python_callable=lambda: rail.load_all_records(
                rail.result("get_all_data_for_issue_key"))[0]
        )

        get_project_data_from_jira = rail.SimpleHttpOperator(
            task_id='get_project_data_from_jira',
            method='GET',
            endpoint='rest/api/2/issue/{{ dag_run.conf.key }}',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['fields']
        )

        serach_project_in_replicon = rail.RepliconServiceOperator(
            task_id='serach_project_in_replicon',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [
                {
                    "code": '{{ dag_run.conf.key }}'
                }]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        has_project_data = rail.IfOperator(
            task_id='has_project_data',
            test='{{ result("serach_project_in_replicon") | is_truthy }}',
            yes_task='update_project_in_replicon',
            no_task='project_status'
        )

        project_status = rail.EmptyOperator(
            task_id='project_status'
        )

        check_projet_status_from_jira = rail.IfOperator(
            task_id='check_projet_status_from_jira',
            test='{{ result("get_projects_data_from_query")["status"] == "Backlog" }}',
            yes_task='create_project_in_replicon',
            no_task='end'
        )

        def get_items():
            return [json.loads(json.dumps(rail.result('get_project_data_from_jira')))]

        create_project_in_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id="create_project_in_replicon",
            items=get_items,
            retries=0,
            trigger_dag_id=f"hostopia_jira_import_child_create_project_{config.instance}",
            conf=lambda dag_run, item: {
                'Key': dag_run.conf['key'],
                "programname": rail.result('get_projects_data_from_query')['programname'],
                "assignee": rail.result('get_projects_data_from_query')['assignee'],
                "startdate": rail.result('get_projects_data_from_query')['startdate'],
                "enddate": rail.result('get_projects_data_from_query')['enddate'],
                "status": rail.result('get_projects_data_from_query')['status'],
                "summary": rail.result('get_projects_data_from_query')['summary'],
                "resource1": item['customfield_10147']['accountId'] if item['customfield_10147'] else None,
                "resource2": item['customfield_10141']['accountId'] if item['customfield_10141'] else None,
                "resource3": item['customfield_10139']['accountId'] if item['customfield_10139'] else None,
                "resource4": item['customfield_10140']['accountId'] if item['customfield_10140'] else None,
                "resource5": item['customfield_10146']['accountId'] if item['customfield_10146'] else None,
                "resource6": item['customfield_10150']['accountId'] if item['customfield_10150'] else None,
                'column_uri': dag_run.conf['column_uri'],
                'filter_uri': dag_run.conf['filter_uri']
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_create_project_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_project_in_replicon',
            dag_runs='{{ result("create_project_in_replicon") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        update_project_in_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id="update_project_in_replicon",
            items=get_items,
            retries=0,
            trigger_dag_id=f"hostopia_jira_import_child_update_project_{config.instance}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                'Key': dag_run.conf['key'],
                "programname": rail.result('get_projects_data_from_query')['programname'],
                "startdate": rail.result('get_projects_data_from_query')['startdate'],
                "assignee": rail.result('get_projects_data_from_query')['assignee'],
                "enddate": rail.result('get_projects_data_from_query')['enddate'],
                "status": rail.result('get_projects_data_from_query')['status'],
                "summary": rail.result('get_projects_data_from_query')['summary'],
                "resource1": item['customfield_10147']['accountId'] if item['customfield_10147'] else None,
                "resource2": item['customfield_10141']['accountId'] if item['customfield_10141'] else None,
                "resource3": item['customfield_10139']['accountId'] if item['customfield_10139'] else None,
                "resource4": item['customfield_10140']['accountId'] if item['customfield_10140'] else None,
                "resource5": item['customfield_10146']['accountId'] if item['customfield_10146'] else None,
                "resource6": item['customfield_10150']['accountId'] if item['customfield_10150'] else None,
                'column_uri': dag_run.conf['column_uri'],
                'filter_uri': dag_run.conf['filter_uri']
            },
        )

        wait_for_update_project_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_project_in_replicon',
            dag_runs='{{ result("update_project_in_replicon") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'Projectcode': '{{ dag_run.conf.key }}',
                'Projectname': '{{ result("get_projects_data_from_query").summary }}',
                'Action': '{{ "Update" if result("update_project_in_replicon") else "Add" }}',
                'Status': 'Processed',
            }
        )

        end = rail.EmptyOperator(
            task_id='end'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> end

        can_run_batch_task >> rail.Label(
            'No') >> get_all_data_for_issue_key

        get_all_data_for_issue_key >> get_projects_data_from_query >> get_project_data_from_jira >> \
            serach_project_in_replicon >> has_project_data

        has_project_data >> rail.Label(
            "Yes") >> update_project_in_replicon >> wait_for_update_project_in_replicon >> end

        has_project_data >> rail.Label(
            "No") >> project_status >> check_projet_status_from_jira

        check_projet_status_from_jira >> rail.Label(
            "Yes") >> create_project_in_replicon >> wait_for_create_project_in_replicon >> end

        check_projet_status_from_jira >> rail.Label(
            "No") >> end >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
