import itertools
import rail
from dxctechnology.adhoc.page_operator_test import config

with rail.create_airflow_dag(
    dag_id='dxctechnology_page_operator_test',
    description='dxctechnology_page_operator_test project list',
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    schedule_interval=None,
    max_active_runs=1,
) as dag:

    def page_handler(request, result):
        if len(result['rows']) > 0:
            request['page'] += 1
            return request
        return None

    def all_result_data_handler(result):
        flaten_rows = list(itertools.chain(
            *list(map(lambda x: x['rows'], result))))
        return list(map(lambda row: {
            'uri': row['cells'][0]['uri'],
            'name': row['cells'][0]['textValue'],
            'leader': row['cells'][2]
        }, flaten_rows))

    project_list_data = rail.RepliconServicePageOperator(
        task_id='project_list_data',
        endpoint='/services/ProjectListService1.svc/GetData',
        data={
            "page": 1,
            "pagesize": 10000,
            "columnUris": [
                "urn:replicon:project-list-column:project",
                "urn:replicon:project-list-column:name",
                "urn:replicon:project-list-column:project-leader"
            ],
        },
        page_handler=page_handler,
        all_result_data_handler=all_result_data_handler
    )

    project_count = rail.PythonOperator(
        task_id='project_count',
        python_callable=lambda: len(rail.result('project_list_data'))
    )

    project_list_data >> project_count
