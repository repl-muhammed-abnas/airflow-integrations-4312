from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.laborcode_options_child_dag_id,
        description=f'{config.company_key} Syncs the list of Labor Code options from Vantagepoint to Replicon',
        company_key=config.company_key,
        max_active_runs=1,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_labor_code_options',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_labor_code_options = rail.VantagepointAPIOperator(
            task_id='get_all_labor_code_options',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}',
            endpoint='/accountConfiguration/laborCode',
            request_method='GET'
        )

        def filter_by_level():
            filtered_options = {}
            all_options = rail.result('get_all_labor_code_options')
            for option in all_options:
                key = f"laborcodelevel{str(option['LCLevel'])}"
                if key not in filtered_options:
                    filtered_options[key] = []
                filtered_options[key].append(option)
            return filtered_options

        filter_options_by_level = rail.PythonOperator(
            task_id='filter_options_by_level',
            python_callable=filter_by_level
        )

        def get_oef_definitions(oefs):
            oef_definitions = []
            labor_codes_by_level = rail.result('filter_options_by_level')
            for level in range(1, len(labor_codes_by_level) + 1):
                replicon_code = f'Labor Code Level {level}'
                level_key = f'laborcodelevel{level}'
                definition = rail.find_first_by_attr_and_get_attr(
                    oefs, 'code', replicon_code, 'uri')
                oef_name = rail.find_first_by_attr_and_get_attr(
                    oefs, 'code', replicon_code, 'name') or rail.find_first_by_attr_and_get_attr(config.oefs, 'id', level_key, 'name')
                if definition:
                    oef_definitions.append({
                        'name': oef_name,
                        'definition_uri': definition,
                        'options': labor_codes_by_level[level_key]
                    })
            return oef_definitions

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            replicon_conn_id='{{ dag_run.conf.replicon_conn_id }}',
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            # pylint: disable= unnecessary-lambda
            data_handler=lambda oefs: get_oef_definitions(oefs)
        )

        update_each_labor_code_level = rail.TriggerDagRunForEachItemOperator(
            task_id='update_each_labor_code_level',
            retries=0,
            items=lambda: rail.result('get_user_oefs'),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.tag_oef_options_update_child_dag_id,
            conf=lambda item, dag_run: {
                **item,
                'oef_id': 'laborcode',
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'company_key': dag_run.conf['company_key']
            }
        )

        wait_for_labor_code_updates = rail.WaitForDagRunsSensor(
            task_id='wait_for_labor_code_updates',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("update_each_labor_code_level") }}'
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in labor code options update - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> catch_error
        batch_task >> get_all_labor_code_options >> filter_options_by_level >> get_user_oefs >> update_each_labor_code_level >> wait_for_labor_code_updates >> catch_error

        return dag

rail.for_each_instance(create_dag)
