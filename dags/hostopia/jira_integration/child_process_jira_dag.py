from datetime import timedelta
import rail
from hostopia.jira_integration.utils import response_filter
from hostopia.jira_integration.utils import custom_method
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'hostopia_jira_import_child_process_jira_data_{config.instance}',
        description=f'hostopia jira import child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.second_master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='jira_issue_check'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='jira_issue_check',
            end_task='finish',
        )

        jira_issue_check = rail.SimpleHttpOperator(
            task_id='jira_issue_check',
            method='GET',
            endpoint='rest/api/3/search?jql=issuetype = Task AND updated >= -1h&maxResults=100&startAt={{ dag_run.conf.start_from }}',
            http_conn_id='hostopia_jira_connection',
            response_filter=lambda response: response.json()['issues']
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test=lambda: bool(rail.result("jira_issue_check")),
            yes_task='map_to_issue_schema',
            no_task='finish'
        )

        map_to_issue_schema = rail.DataAdaptorOperator(
            task_id="map_to_issue_schema",
            source=lambda: rail.result("jira_issue_check"),
            columns=['key', 'projectid', 'programname', 'projectkey',
                     'status', 'type', 'summary', 'startdate', 'enddate', 'assignee'],
            data=custom_method.convert_input_data_to_task_data,
        )

        jira_list_collection = rail.CreateCollectionOperator(
            task_id='jira_list_collection',
            source="{{result('map_to_issue_schema')}}",
            name='jiraupdatedata',
        )

        query_jira_programs = rail.QueryCollectionOperator(
            task_id='query_jira_programs',
            query="""SELECT DISTINCT projectid,programname,projectkey FROM jiraupdatedata WHERE status == "Backlog" AND type == "Task" """,
            name='queryjiralistforprogram'
        )

        has_query_jira_programs = rail.IfOperator(
            task_id='has_query_jira_programs',
            test='{{ result("query_jira_programs","length") > 0}}',
            yes_task='get_query_jira_programs_data',
            no_task='get_all_unique_issue'
        )

        get_all_user_list_columns = rail.RepliconServiceOperator(
            task_id='get_all_user_list_columns',
            endpoint='/services/UserListService1.svc/GetAllColumns',
            response_filter=response_filter.get_user_column
        )

        get_all_user_list_filters = rail.RepliconServiceOperator(
            task_id='get_all_user_list_filters',
            endpoint='/services/UserListService1.svc/GetAllFilterDefinitions',
            response_filter=response_filter.get_user_list_filters
        )

        get_query_jira_programs_data = rail.PythonOperator(
            task_id='get_query_jira_programs_data',
            python_callable=lambda: rail.load_all_records(
                rail.result("query_jira_programs"))
        )

        process_programs_in_replicon = rail.TriggerDagRunForEachItemOperator(
            task_id='process_programs_in_replicon',
            trigger_dag_id=f'hostopia_jira_import_child_process_program_{config.instance}',
            items="{{ result('query_jira_programs') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                'projectid': '{{ item.projectid }}',
                'programname': '{{ item.programname }}',
                'projectkey': '{{ item.projectkey }}',
                'column_uri': '{{ result("get_all_user_list_columns")[0].uri }}',
                'filter_uri': '{{ result("get_all_user_list_filters")[0].uri }}'
            }
        )

        wait_for_process_programs_in_replicon = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_programs_in_replicon',
            dag_runs='{{ result("process_programs_in_replicon") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_unique_issue = rail.QueryCollectionOperator(
            task_id='get_all_unique_issue',
            query="""SELECT DISTINCT key as key FROM jiraupdatedata WHERE type == "Task" """,
            name='queryjiralistforproject'
        )

        process_jira_for_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_jira_for_projects',
            trigger_dag_id=f'hostopia_jira_import_child_process_project_{config.instance}',
            items="{{ result('get_all_unique_issue') }}",
            conf={
                'key': '{{ item.key }}',
                'column_uri': '{{ result("get_all_user_list_columns")[0].uri }}',
                'filter_uri': '{{ result("get_all_user_list_filters")[0].uri }}'
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_jira_for_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_jira_for_projects',
            dag_runs='{{ result("process_jira_for_projects") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )



        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'No of jira': '{{ result("get_all_unique_issue","length") }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> jira_issue_check

        jira_issue_check >> has_data >> rail.Label("No") >> finish

        has_data >> rail.Label("Yes") >> map_to_issue_schema

        map_to_issue_schema >> get_all_user_list_columns >> get_all_user_list_filters >> \
            jira_list_collection >> query_jira_programs >> has_query_jira_programs

        has_query_jira_programs >> rail.Label(
            "Yes") >> get_query_jira_programs_data >> \
            process_programs_in_replicon >> wait_for_process_programs_in_replicon

        has_query_jira_programs >> rail.Label(
            "No") >> get_all_unique_issue

        wait_for_process_programs_in_replicon >> get_all_unique_issue >> \
            process_jira_for_projects >> wait_for_process_jira_for_projects >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
