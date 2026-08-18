from datetime import timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.prime_contract_child_dag_id,
        description='Computerease to Procore Prime Contract sync Child DAG',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
            'procore_conn_id': config.procore_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='validate_customer_requirements',
            end_task='catch_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        def validate_customer(dag_run):
            customer_name = dag_run.conf.get('customer_name', '').strip()
            customer_code = dag_run.conf.get('customer_code', '').strip()

            if not customer_name or not customer_code:
                return {
                    'is_valid': False,
                    'error_message': "Customer not assigned to the job in Computerease or its name missing."
                }

            return {'is_valid': True}

        validate_customer_requirements = rail.PythonOperator(
            task_id='validate_customer_requirements',
            python_callable=validate_customer
        )

        if_validation_passed = rail.IfOperator(
            task_id='if_validation_passed',
            test=lambda: rail.result(
                'validate_customer_requirements').get('is_valid', False),
            yes_task='create_or_update_customer',
            no_task='search_prime_contract'
        )

        def build_customer_payload(dag_run):
            customer_code = dag_run.conf.get('customer_code', '')

            return {
                "company_id": dag_run.conf['procore_company_id'],
                "updates": [
                    {
                        "name": dag_run.conf.get('customer_name', ''),
                        "abbreviated_name": customer_code,
                        "origin_id": f"CE_CUS_{customer_code}"
                    }
                ]
            }

        create_or_update_customer = rail.ProcoreApiOperator(
            task_id='create_or_update_customer',
            endpoint='/vendors/sync',
            method='PATCH',
            data=build_customer_payload,
            query_params={
                'run_configurable_validations': 'false'
            },
            data_handler=lambda response: {
                'id': response['entities'][0]['id'] if response and len(response['entities']) > 0 else None,
                'response': response
            }
        )

        check_if_customer_exists = rail.IfOperator(
            task_id='check_if_customer_exists',
            test=lambda: rail.result('create_or_update_customer') and rail.result(
                'create_or_update_customer')['id'],
            yes_task='add_customer_to_project',
            no_task='search_prime_contract'
        )

        add_customer_to_project = rail.ProcoreApiOperator(
            task_id='add_customer_to_project',
            endpoint=lambda dag_run: f'/projects/{dag_run.conf["procore_project_id"]}/vendors/{rail.result("create_or_update_customer")["id"]}/actions/add',
            method='POST',
            query_params=lambda dag_run: {
                'company_id': dag_run.conf['procore_company_id']
            }
        )

        search_prime_contract = rail.ProcoreApiOperator(
            task_id='search_prime_contract',
            endpoint='/prime_contracts',
            method='GET',
            query_params={
                'project_id': "{{dag_run.conf.procore_project_id}}",
                'filters[origin_id]': "CE_{{dag_run.conf.job_code}}"
            },
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'origin_id', f'CE_{dag_run.conf["job_code"]}', 'id', None)
        )

        if_prime_contract_exists = rail.IfOperator(
            task_id='if_prime_contract_exists',
            test=lambda: rail.result('search_prime_contract') is not None,
            yes_task='update_prime_contract',
            no_task='create_prime_contract'
        )

        def build_prime_contract_payload(dag_run):
            job_code = dag_run.conf.get('job_code', '')

            if not job_code:
                raise ValueError(
                    "Job code is required to create prime contract")
            vendor_id = rail.result('create_or_update_customer')[
                'id'] if rail.result('create_or_update_customer') else None

            # Generate origin_id using CE prefix and job code
            origin_id = f"CE_{job_code}"

            payload = {
                "project_id": dag_run.conf['procore_project_id'],
                "prime_contract": {
                    "origin_id": origin_id,
                    "title": f"Prime Contract - {job_code}",
                    "number": job_code,
                    "status": "Approved" if dag_run.conf.get('job_status', True) else "Draft"
                }
            }
            if vendor_id:
                payload['prime_contract']['vendor_id'] = vendor_id
            return payload

        create_prime_contract = rail.ProcoreApiOperator(
            task_id='create_prime_contract',
            endpoint='/prime_contract',
            method='POST',
            data=build_prime_contract_payload
        )

        update_prime_contract = rail.ProcoreApiOperator(
            task_id='update_prime_contract',
            endpoint='/prime_contract/{{ result("search_prime_contract") }}',
            method='PATCH',
            data=build_prime_contract_payload
        )

        if_log_to_be_added = rail.IfOperator(
            task_id='if_log_to_be_added',
            test=lambda: not (rail.result('create_or_update_customer') and rail.result(
                'create_or_update_customer')['id']),
            yes_task='log_pc_synced_without_customer',
            no_task='catch_error'
        )

        def get_err_message():
            message = "Prime contract synced without customer assignment: "
            validation_result = rail.result('validate_customer_requirements')
            if not validation_result['is_valid']:
                message += validation_result['error_message']
                return message
            message += "Customer could not be created/updated due to unknown error."
            return message

        log_pc_synced_without_customer = rail.WriteLogOperator(
            task_id='log_pc_synced_without_customer',
            message='na',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PRIME_CONTRACT',
                'entity_code': dag_run.conf.get('job_code', ''),
                'full_code': dag_run.conf.get('job_code', ''),
                'entity_name': f"Prime Contract - {dag_run.conf.get('job_code', '')}",
                'error_message': get_err_message()
            }
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=lambda dag_run: {
                'entity_type': 'PRIME_CONTRACT',
                'entity_code': dag_run.conf.get('job_code', ''),
                'full_code': dag_run.conf.get('job_code', ''),
                'entity_name': f"Prime Contract - {dag_run.conf.get('job_code', '')}",
                'error_message': 'Prime contract not synced - {{ get_error_message() }}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_error
        batch_task >> validate_customer_requirements >> if_validation_passed

        if_validation_passed >> rail.Label(
            'Yes') >> create_or_update_customer >> check_if_customer_exists
        if_validation_passed >> rail.Label('No') >> search_prime_contract

        check_if_customer_exists >> rail.Label(
            'Yes') >> add_customer_to_project >> search_prime_contract
        check_if_customer_exists >> rail.Label('No') >> search_prime_contract

        search_prime_contract >> if_prime_contract_exists

        if_prime_contract_exists >> rail.Label(
            'Yes') >> update_prime_contract >> if_log_to_be_added
        if_prime_contract_exists >> rail.Label(
            'No') >> create_prime_contract >> if_log_to_be_added

        if_log_to_be_added >> rail.Label(
            'Yes') >> log_pc_synced_without_customer >> catch_error
        if_log_to_be_added >> rail.Label('No') >> catch_error

        catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
