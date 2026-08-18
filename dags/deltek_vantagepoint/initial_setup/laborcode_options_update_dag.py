from datetime import timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_laborcode_options_update_child_{config.instance}',
        description='Syncs the list of Labor Code options from Vantagepoint to Replicon',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_labor_code_options'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_labor_code_options',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_labor_code_options = rail.VantagepointAPIOperator(
            task_id='get_all_labor_code_options',
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
                oef_name = rail.find_first_by_attr_and_get_attr(config.oefs, 'id', f'laborcodelevel{level}', 'name')
                definition = rail.find_first_by_attr_and_get_attr(
                    oefs, 'name', oef_name, 'uri')
                if definition:
                    oef_definitions.append({
                        'name': oef_name,
                        'definition': definition,
                        'options': labor_codes_by_level[f'laborcodelevel{level}']
                    })
            return oef_definitions

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
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
            trigger_dag_id=f'deltek_vantagepoint_tag_oef_options_update_child_{config.instance}',
            conf=lambda item: {
                **item,
                'type': 'laborcode',
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_all_labor_code_options >> filter_options_by_level >> get_user_oefs >> update_each_labor_code_level >> log_to_sumo

        return dag

rail.for_each_instance(create_dag)
