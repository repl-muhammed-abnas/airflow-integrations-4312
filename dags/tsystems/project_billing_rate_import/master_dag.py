'''
T-Systems Project Billing Rate Import - Master DAG
'''

from pendulum import now
import rail
from datetime import timedelta

from tsystems.project_billing_rate_import.utils import custom_methods

# Required for JSON payload compatibility
null = None


def create_master_dag(config):
    """
    Create the master DAG for T-Systems Project Billing Rate Import.
    """

    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'T-Systems Project Billing Rate Import Master {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

        log_job_start_time = rail.PythonOperator(
            task_id='log_job_start_time',
            python_callable=lambda: now(
                config.time_zone).strftime("%Y-%m-%dT%H:%M:%S%z")
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log',
        )

        create_billing_event_records_collection = rail.CreateCollectionOperator(
            task_id='create_billing_event_records_collection',
            source=lambda dag_run: dag_run.conf['billing_event_records_list'],
            name='inputdata'
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_billing_event_records_collection', 'length') > 0 }}",
            yes_task='query_invalid_records'
        )

        query_invalid_records = rail.QueryCollectionOperator(
            task_id="query_invalid_records",
            name="invalid_records",
            query="""SELECT * FROM inputdata WHERE NULLIF(Billing_Rate_ID, '') IS NULL
                or NULLIF(Project_ID, '') IS NULL
                or NULLIF(Rate_Type, '') IS NULL
                or NULLIF(Billing_Rate_Value, '') IS NULL
                or NULLIF(Billing_Rate_Currency, '') IS NULL"""
        )

        are_invalid_records_present = rail.IfOperator(
            task_id='are_invalid_records_present',
            test="{{ result('query_invalid_records', 'length') > 0 }}",
            yes_task='log_invalid_records',
            no_task='query_valid_records'
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            items="{{ result('query_invalid_records') }}",
            log="{{ result('create_master_log') }}",
            message=lambda: "Invalid record",
            severity="Exception",
            properties=lambda item: {
                "billing_rate_id": item.get('Billing_Rate_ID', ''),
                "billing_rate_name": "",  # Billing rate name not generated yet for invalid records
                "project_id": item.get('Project_ID', ''),
                "ciam_id": item.get('CIAM_ID', ''),
                "action": "Validation",
                "status": "Exception",
                "details": "Payload not processed as - " + custom_methods.get_mandatory_field_validation_details(item)
            }
        )

        query_valid_records = rail.QueryCollectionOperator(
            task_id='query_valid_records',
            name='valid_records',
            query="""SELECT * FROM inputdata WHERE NULLIF(Billing_Rate_ID,'') IS NOT NULL 
                and NULLIF(Project_ID,'') IS NOT NULL
                and NULLIF(Rate_Type,'') IS NOT NULL
                and NULLIF(Billing_Rate_Value,'') IS NOT NULL
                and NULLIF(Billing_Rate_Currency,'') IS NOT NULL"""
        )

        are_valid_records_present = rail.IfOperator(
            task_id='are_valid_records_present',
            test="{{ result('query_valid_records', 'length') > 0 }}",
            yes_task='get_default_currency_uri',
            no_task='dummy_process_log_generation'
        )

        get_default_currency_uri = rail.RepliconServiceOperator(
            task_id="get_default_currency_uri",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'symbol', config.default_currency, 'uri')
        )

        #  Get existing billing rates from Replicon
        get_existing_billing_rates = rail.RepliconServiceOperator(
            task_id='get_existing_billing_rates',
            endpoint='/services/BillingRateListService1.svc/GetData',
            data={
                "page": 1,
                "pagesize": 1000000,
                "columnUris": [
                    "urn:replicon:billing-rate-list-column:name",
                    "urn:replicon:billing-rate-list-column:description",
                    "urn:replicon:billing-rate-list-column:amount-by-frequency",
                    "urn:replicon:billing-rate-list-column:amount",
                    "urn:replicon:billing-rate-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(
                lambda item: {
                    'uri': item['cells'][0]['uri'],
                    'name': item['cells'][0]['textValue'],
                    'description': item['cells'][1].get('textValue', ''),
                    'billing_rate_currency': item['cells'][2]['cellCollection'][0]['moneyValue']['baseCurrencyValue']['currency']['name'] if bool(
                        item['cells'][2]['cellCollection']) else '',
                    'billing_rate_amount': item['cells'][3]['numberValue'],
                    'enabled': item['cells'][4]['textValue']
                }, response['rows'])
            ) if response['rows'] else []
        )

        get_all_existing_billing_rate_names = rail.PythonOperator(
            task_id='get_all_existing_billing_rate_names',
            python_callable=lambda: list(map(lambda x: x['name'].lower(), rail.result(
                'get_existing_billing_rates'))) if rail.result('get_existing_billing_rates') else [],
        )

        dummy_trigger_process_each_payload_dag = rail.EmptyOperator(
            task_id='dummy_trigger_process_each_payload_dag',
        )

        trigger_process_each_payload_dag = rail.trigger_parallel_dagrun(
            task_id='trigger_process_each_payload_dag',
            items="{{ result('query_valid_records') }}",
            parallel_count=config.trigger_parallel_dagrun_count_process_each_payload,
            trigger_dag_id=config.process_each_payload_dag_id,
            conf=lambda item: custom_methods.get_each_billing_rate_payload(
                item),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_process_each_payload_dag_ids = rail.PythonOperator(
            task_id='get_process_each_payload_dag_ids',
            python_callable=lambda: custom_methods.get_process_each_user_payload_dag_ids(
                config.trigger_parallel_dagrun_count_process_each_payload),
            show_return_value_in_logs=False
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("get_process_each_payload_dag_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_child_logs_timeout_hours),
            flatten=True
        )

        dummy_process_log_generation = rail.EmptyOperator(
            task_id='dummy_process_log_generation',
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.log_generation_dag_id,
            conf=lambda: {
                'total_records': rail.result('create_billing_event_records_collection', key='length'),
                'master_log': rail.result('create_master_log'),
                'child_logs': rail.result('gather_child_logs'),
                'job_start_time': rail.result('log_job_start_time'),
            }
        )

        wait_for_process_log_generation = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_log_generation',
            dag_runs="{{ result('process_log_generation') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            trigger_rule='all_done',
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        log_job_start_time >> create_master_log >> create_billing_event_records_collection >> has_collection_data

        has_collection_data >> rail.Label(
            'Yes') >> can_fail_dag

        has_collection_data >> rail.Label(
            'No') >> query_invalid_records >> are_invalid_records_present

        are_invalid_records_present >> rail.Label(
            'Yes') >> log_invalid_records >> query_valid_records
        are_invalid_records_present >> rail.Label(
            'No') >> query_valid_records

        query_valid_records >> are_valid_records_present

        are_valid_records_present >> rail.Label(
            'Yes') >> get_default_currency_uri
        are_valid_records_present >> rail.Label(
            'No') >> dummy_process_log_generation

        get_default_currency_uri >> get_existing_billing_rates >> get_all_existing_billing_rate_names >> dummy_trigger_process_each_payload_dag >> trigger_process_each_payload_dag

        trigger_process_each_payload_dag >> get_process_each_payload_dag_ids >> gather_child_logs >> dummy_process_log_generation

        dummy_process_log_generation >> process_log_generation >> wait_for_process_log_generation >> can_fail_dag

        can_fail_dag >> rail.Label(
            'Yes') >> fail_dagrun

    return dag


rail.for_each_instance(create_master_dag)
