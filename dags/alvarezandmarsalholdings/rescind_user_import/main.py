from datetime import datetime, timedelta
from pendulum import now
import itertools
from alvarezandmarsalholdings.rescind_user_import.utils import custom_methods, request_payload
import rail

null = None


def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'{config.company_key} Recind User Import - Master Dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2024, 1, 1),
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_data_available = rail.IfOperator(
            task_id='is_data_available',
            test=lambda dag_run: bool(dag_run.conf['payload']),
            yes_task="create_exception_log"
        )

        create_exception_log = rail.CreateLogOperator(
            task_id="create_exception_log"
        )

        create_collection_from_payload = rail.CreateCollectionOperator(
            task_id='create_collection_from_payload',
            source=lambda dag_run: dag_run.conf['payload'],
            name="input_data_collection",
            columns={
                "Employee_ID": "employee_id",
                "Event_Identifier": "event_identifier",
                "Rescind_Date": "rescind_date"
                }
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            query="""SELECT * FROM input_data_collection WHERE NULLIF(employee_id, '') IS NULL"""
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items='{{result("query_invalid_records")}}',
            log="{{result('create_exception_log')}}",
            message="One or more mandatory field is missing.",
            severity='Exception',
            properties=lambda item: {
                'employee_id': item['employee_id'],
                'action':'Validation',
                'status': 'Exception',
                "details": request_payload.get_exception_message(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id="query_valid_records",
            query="""SELECT * FROM input_data_collection WHERE NULLIF(employee_id, '') IS NOT NULL"""
        )

        get_event_identifier_oef_uri = rail.RepliconServiceOperator(
            task_id='get_event_identifier_oef_uri',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:user"},
            data_handler=custom_methods.get_event_identifier_oef_uri
        )

        get_event_identifier_oef_values = rail.RepliconServiceOperator(
            task_id='get_event_identifier_oef_values',
            endpoint='/services/ObjectExtensionTagService1.svc/GetPageOfObjectExtensionTagsFilteredBySearch',
            data=lambda: {
                "page": "1",
                "pageSize": "1000",
                "objectExtensionTagDefinitionUri": rail.result('get_event_identifier_oef_uri'),
                "textSearch": null
            }
        )

        process_disable_users = rail.trigger_parallel_dagrun(
            task_id='process_disable_users',
            items=lambda: rail.result('query_valid_records'),
            parallel_count=config.parallel_dagrun_count_process_disable_users,
            trigger_dag_id=config.process_disable_users_dag_id,
            conf=lambda item: {
                **item,
                "event_identifier_oef_uri": rail.result('get_event_identifier_oef_uri'),
                'event_identifier_value_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_event_identifier_oef_values'), 'displayText', item["event_identifier"], 'uri') if item["event_identifier"] else ""
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_disable_users_dag_ids = rail.PythonOperator(
            task_id='get_process_disable_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_disable_users_{x+1}'), range(config.parallel_dagrun_count_process_disable_users))))),
            show_return_value_in_logs=False
        )

        gather_disabled_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disabled_user_logs',
            dag_runs='{{ result("get_process_disable_users_dag_ids") }}',
            dagrun_task_id='create_disable_user_log',
            execution_timeout=timedelta(
                hours=config.gather_disabled_users_logs_timeout_hours),
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dag_id,
            conf=lambda dag_run: {
                'disabled_user_logs': rail.result('gather_disabled_user_logs'),
                'otherlogs': rail.result('create_exception_log'),
                'log_filename': rail.render_template('{{ get_company_key() }}') + "_rescind_user_import_log_" +
                    now().strftime("%Y%m%dT%H%M%S") + ".csv",
                "total_records": len(dag_run.conf['payload'])
            }
        )


        is_data_available >> rail.Label('Yes') >> create_exception_log >>\
        create_collection_from_payload >> query_invalid_records >> log_invalid_records >> query_valid_records >>\
        get_event_identifier_oef_uri >> get_event_identifier_oef_values >> process_disable_users >>\
        get_process_disable_users_dag_ids >> gather_disabled_user_logs >> process_log_generation

    return dag


rail.for_each_instance(create_main_dag)
