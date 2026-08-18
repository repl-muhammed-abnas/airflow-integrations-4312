from datetime import timedelta
from airflow.models import Variable
import rail
from cbrefcg.oef_update.utils import response_filter
from cbrefcg.oef_update.tasks.drop_down_update import update_oef

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'cbrefcg_processing_eachoef_child_v1 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_request_oefuri_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_request_oefuri_blank',
            end_task='finish',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_request_oefuri_blank= rail.IfOperator(
            task_id='is_request_oefuri_blank',
            test='{{ dag_run.conf.oefuri | is_falsy }}',
            yes_task="finish",
            no_task="get_object_extension_tag_definition_details",
        )

        finish= rail.EmptyOperator(
            task_id='finish',
        )

        get_object_extension_tag_definition_details= rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details',
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data={
                "objectExtensionTagDefinitionUri": "{{ dag_run.conf.oefuri }}"
            },
            data_handler= response_filter.get_all_oef_tags
        )

        check_broker_value_equals_to_no= rail.IfOperator(
            task_id='check_broker_value_equals_to_no',
            test='{{ dag_run.conf.brokeroefvalue_user == "No" }}',
            yes_task="is_user_available_in_oef",
            no_task="check_broker_value_equals_to_yes",
        )

        is_user_available_in_oef= rail.IfOperator(
            task_id='is_user_available_in_oef',
            test= lambda: bool(rail.result("get_object_extension_tag_definition_details")),
            yes_task="disable_tags_in_replicon",
            no_task="finish",
        )

        disable_tags_in_replicon= rail.RepliconServiceCallForEachItemOperator(
            task_id='disable_tags_in_replicon',
            endpoint="/services/ObjectExtensionTagService1.svc/Disable",
            items= "{{ result('get_object_extension_tag_definition_details') | to_json }}",
            data=lambda item:{
                "objectExtensionTagUri": item['uri']
            }
        )

        check_broker_value_equals_to_yes= rail.IfOperator(
            task_id='check_broker_value_equals_to_yes',
            test='{{ dag_run.conf.brokeroefvalue_user == "Yes" }}',
            yes_task="check_user_available_in_oef",
            no_task="finish",
        )

        check_user_available_in_oef= rail.IfOperator(
            task_id='check_user_available_in_oef',
            test= lambda: bool(rail.result("get_object_extension_tag_definition_details")),
            yes_task="enable_tags_in_replicon",
            no_task="update_oef_drop_down_start",
        )

        enable_tags_in_replicon= rail.RepliconServiceCallForEachItemOperator(
            task_id='enable_tags_in_replicon',
            endpoint="/services/ObjectExtensionTagService1.svc/Enable",
            items= "{{ result('get_object_extension_tag_definition_details') | to_json  }}",
            data=lambda item:{
                "objectExtensionTagUri": item['uri']
            }
        )

        update_oef_drop_down_start = rail.EmptyOperator(
            task_id = 'update_oef_drop_down_start'
        )

        add_dropdown_value= update_oef()

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            'No') >> is_request_oefuri_blank

        is_request_oefuri_blank >> rail.Label(
            'Yes')  >> finish

        is_request_oefuri_blank >> rail.Label(
            'No') >> get_object_extension_tag_definition_details >> check_broker_value_equals_to_no

        check_broker_value_equals_to_no >> rail.Label(
            'Yes')  >> is_user_available_in_oef

        is_user_available_in_oef >> rail.Label(
            "Yes") >> disable_tags_in_replicon >> finish

        is_user_available_in_oef >> rail.Label(
            "No") >> finish

        check_broker_value_equals_to_no >> rail.Label(
            'No')  >> check_broker_value_equals_to_yes

        check_broker_value_equals_to_yes >> rail.Label(
            "Yes") >> check_user_available_in_oef

        check_broker_value_equals_to_yes >> rail.Label(
            "No") >> finish

        check_user_available_in_oef >> rail.Label(
            "Yes") >> enable_tags_in_replicon >> finish

        check_user_available_in_oef >> rail.Label(
            "No") >> update_oef_drop_down_start >> add_dropdown_value >> finish

        finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
