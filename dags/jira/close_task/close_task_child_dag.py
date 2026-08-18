from datetime import timedelta
import rail
from airflow.models import Variable
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"standard_jira_{config.region.replace('-', '_')}_close_task_child_dag_{config.instance}",
        description=f'Jira {config.region} Close Task Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_projects'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_projects',
            end_task='catch_close_task_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_filtered_data(response, dag_run):
            projectname = dag_run.conf['project']
            data = response.json()['d']['rows']
            projectinfo = list(filter(lambda x: x['projectname'] == projectname, map(lambda item: {
                "projecturi": item['cells'][0]['uri'],
                "projectname": item['cells'][0].get('textValue'),
            }, data)))
            return projectinfo[0] if projectinfo else {}

        search_projects = rail.RepliconServiceOperator(
            task_id='search_projects',
            endpoint='/services/ProjectListService1.svc/GetData',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data=lambda dag_run: {
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:project-list-column:project",
                    "urn:replicon:project-list-column:code"
                ],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['project'],
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            response_filter=get_filtered_data
        )

        if_project_uri_present = rail.IfOperator(
            task_id='if_project_uri_present',
            test=lambda: rail.result('search_projects') and rail.result(
                'search_projects')['projecturi'],
            yes_task="get_task_details",
            no_task="catch_close_task_error",
        )

        def get_tasks_to_close(response, dag_run):
            issues_set = {issue['key'] for issue in dag_run.conf['issues']}
            return [
                task['uri']
                for task in response
                if task['displayText'] in issues_set
            ]

        get_task_details = rail.RepliconServiceOperator(
            task_id='get_task_details',
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint="/services/TaskService1.svc/GetChildrenTaskDetails",
            data={
                "parentUri": "{{ result('search_projects').projecturi }}"
            },
            data_handler=get_tasks_to_close
        )

        close_tasks = rail.RepliconServiceCallForEachItemOperator(
            task_id="close_tasks",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            endpoint='/services/TaskService1.svc/Close',
            items='{{ result("get_task_details") | to_json }}',
            execution_timeout=timedelta(days=14),
            flatten=True,
            data={
                "taskUri": '{{ item }}'
            },
        )

        def get_downstreamtasks_error(project, error_message):
            return {
                'error': f'Error with {project} - {error_message}'
            }
        catch_close_task_error = rail.PythonOperator(
            task_id='catch_close_task_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ dag_run.conf.project }}',
                     '{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> rail.Label(
                'on Error') >> catch_close_task_error

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_close_task_error
        can_run_batch_task >> rail.Label(
            'No') >> search_projects >> if_project_uri_present
        if_project_uri_present >> rail.Label(
            'Yes') >> get_task_details >> close_tasks >> rail.Label(
            'on Error') >> catch_close_task_error
        if_project_uri_present >> rail.Label(
            'No') >> catch_close_task_error

    return dag


rail.for_each_instance(create_child_dag)
