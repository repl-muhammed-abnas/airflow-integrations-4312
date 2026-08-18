from datetime import timedelta
import itertools
from airflow.models import Variable
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/balparag3/project_import/config.py


null = None


def create_client_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'balparag3_projectimport_client_validation_{config.instance}',
        description=f'balparag3_projectimport_client validation V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_create_client_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='search_client_from_name'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_client_from_name',
            end_task='log_dagrun_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def page_handler(request, result_resp):
            if len(result_resp['rows']) > 0:
                request['page'] += 1
                return request
            return null

        def get_clienturi(response, dag_run):
            client_name = dag_run.conf['clientname']
            flatten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], response))))
            return rail.smartjoin_by_delim(
                [x['cells'][0]['uri']
                    for x in flatten_rows if x['cells'][2]['textValue'] == client_name]
            ) if flatten_rows else ''
        search_client_from_name = rail.RepliconServicePageOperator(
            task_id='search_client_from_name',
            endpoint='/services/ClientListService1.svc/GetData',
            data=lambda dag_run: {
                'page': 1,
                'pagesize': 10000,
                'columnUris': [
                    'urn:replicon:client-list-column:client',
                    'urn:replicon:client-list-column:code',
                    'urn:replicon:client-list-column:name'
                ],
                'filterExpression': {
                    'leftExpression': {
                        'filterDefinitionUri': 'urn:replicon:client-list-filter:name'
                    },
                    'operatorUri': 'urn:replicon:filter-operator:text-search',
                    'rightExpression': {
                        'value': {
                            'text': dag_run.conf['clientname'],
                        }
                    }
                }
            },
            page_handler=page_handler,
            all_result_data_handler=get_clienturi
        )

        is_clienturi_blank = rail.IfOperator(
            task_id='is_clienturi_blank',
            test="{{ result('search_client_from_name') | is_falsy }}",
            yes_task="query_clientcontact",
            no_task="log_dagrun_to_sumo"
        )

        query_clientcontact = rail.QueryCollectionOperator(
            task_id='query_clientcontact',
            query="""SELECT clientcontact FROM validatedinputdata WHERE
                    clientname = :clientname""",
            query_params={
                'clientname': '{{ dag_run.conf.clientname }}'
            }
        )

        load_clientcontact = rail.LoadCSVFileOperator(
            task_id='load_clientcontact',
            document="{{ result('query_clientcontact') }}"
        )

        def get_clientcontact_from_validatedinputdata():
            load_clientcontact = rail.load_all_records(
                rail.result('load_clientcontact'))
            return rail.smartjoin_by_delim([x['clientcontact'] for x in load_clientcontact])
        get_clientcontact = rail.PythonOperator(
            task_id='get_clientcontact',
            python_callable=get_clientcontact_from_validatedinputdata
        )

        create_client_in_replicon = rail.RepliconServiceOperator(
            task_id='create_client_in_replicon',
            endpoint='/services/ClientService1.svc/PutClient',
            data=lambda dag_run: {
                "client": {
                    "target": {
                        "name": dag_run.conf['clientname']
                    },
                    "name": dag_run.conf['clientname'],
                    "billingContact": rail.result(
                        'get_clientcontact') if rail.result('get_clientcontact') else null,
                    "isActive": True,
                    "customFieldValues": [
                        {
                            "customField": {
                                "uri": dag_run.conf['department_udf_uri']
                            },
                            "dropDownOption": {
                                "uri": dag_run.conf['department_udf_dropdown_uri']
                            }
                        }
                    ]
                }
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_dagrun_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> search_client_from_name
        search_client_from_name >> is_clienturi_blank
        is_clienturi_blank >> rail.Label(
            'Yes') >> query_clientcontact >> load_clientcontact >> get_clientcontact >> \
            create_client_in_replicon >> log_dagrun_to_sumo
        is_clienturi_blank >> rail.Label(
            'No') >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_client_child_dag)
