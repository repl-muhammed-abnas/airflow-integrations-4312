from datetime import timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_laborcategory_options_update_child_{config.instance}',
        description='Syncs the list of Labor Category options from Vantagepoint to Replicon',
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
            no_task='get_all_labor_categories'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_labor_categories',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_labor_categories = rail.VantagepointAPIOperator(
            task_id='get_all_labor_categories',
            endpoint='/codeTable/LaborCategory',
            request_method='GET'
        )

        get_user_oefs = rail.RepliconServiceOperator(
            task_id="get_user_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda oefs: {
                'laborcategoryoefdefinition': rail.find_first_by_attr_and_get_attr(oefs, 'name', rail.find_first_by_attr_and_get_attr(
                    config.oefs, 'id', 'laborcategory', 'name'), 'uri')
            }
        )

        trigger_update_tag_oef_options = rail.TriggerDagRunOperator(
            task_id='trigger_update_tag_oef_options',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_tag_oef_options_update_child_{config.instance}',
            conf=lambda: {
                'options': rail.result('get_all_labor_categories'),
                'definition': rail.result('get_user_oefs')['laborcategoryoefdefinition'],
                'type': 'laborcategory',
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
            'No') >> get_all_labor_categories >> get_user_oefs >> trigger_update_tag_oef_options >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
