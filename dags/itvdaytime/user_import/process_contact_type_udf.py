import rail
from itvdaytime.user_import.utils import data_handler
from itvdaytime.user_import.utils.request_payload import get_create_add_contract_type_payload


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f"itvdaytime_user_import_process_contract_types_{config.instance}",
        description=f"iTV DayTime User Import process_contract_types {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,

        max_active_runs=config.max_active_runs_master
    ) as dag:

        get_user_custom_field_group = rail.RepliconServiceOperator(
            task_id="get_user_custom_field_group",
            endpoint="/services/CustomFieldService1.svc/GetCustomFieldGroup",
            data={
                "objectTypeUri": "urn:replicon:object-type:user"
            }
        )

        get_all_user_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_user_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "{{result('get_user_custom_field_group').uri}}"
            },
            data_handler=data_handler.get_all_user_custom_fields_filter
        )

        get_employee_contract_types_from_feed = rail.QueryCollectionOperator(
            task_id="get_employee_contract_types_from_feed",
            query="""SELECT DISTINCT contract_type FROM input_data WHERE NULLIF(contract_type, '') IS NOT NULL""",
            name="feed_employee_contract_types"
        )

        get_all_drop_down_options_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_drop_down_options_from_replicon",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Contract Type', 'uri')
            },
            data_handler=data_handler.get_all_drop_down_options_filter
        )

        create_replicon_contract_type_collection = rail.CreateCollectionOperator(
            task_id="create_replicon_contract_type_collection",
            source="{{result('get_all_drop_down_options_from_replicon') | to_json}}",
            name="replicon_contract_types"
        )

        query_contract_types_not_present_in_replicon = rail.QueryCollectionOperator(
            task_id="query_contract_types_not_present_in_replicon",
            query="""SELECT * FROM feed_employee_contract_types WHERE LOWER(contract_type) NOT IN (SELECT LOWER(name) FROM replicon_contract_types)""",
            name="contract_types_to_add"
        )

        has_any_contract_type_to_add = rail.IfOperator(
            task_id="has_any_contract_type_to_add",
            test="{{result('query_contract_types_not_present_in_replicon', 'length') > 0}}",
            yes_task="create_add_contract_type_payload",
            no_task="finish"
        )

        create_add_contract_type_payload = rail.PythonOperator(
            task_id="create_add_contract_type_payload",
            python_callable=get_create_add_contract_type_payload
        )

        put_employee_contract_types = rail.RepliconServiceOperator(
            task_id="put_employee_contract_types",
            endpoint="/services/CustomFieldService1.svc/PutDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_user_custom_fields'), 'name', 'Contract Type', 'uri'),
                "customFieldDropDownOptionUris": rail.result('create_add_contract_type_payload')
            }
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        get_user_custom_field_group >> get_all_user_custom_fields >> get_all_drop_down_options_from_replicon >> create_replicon_contract_type_collection >>\
            get_employee_contract_types_from_feed >> query_contract_types_not_present_in_replicon >> has_any_contract_type_to_add >> rail.Label("Yes")\
                >> create_add_contract_type_payload >> put_employee_contract_types >> finish
        has_any_contract_type_to_add >> rail.Label("No") >> finish

    return dag

rail.for_each_instance(create_child_dag)
