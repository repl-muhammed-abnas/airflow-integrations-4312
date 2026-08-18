import rail
from dxctechnology.c1_cwf_purchase_order_import.utils import custom_method, request_payload


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_c1_cwf_purchase_order_purchaseorder_add_update_blob_child_{config.instance}",
        description=f"DXCTechnology C1 CWF Purchase order add update blob child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_conf")

        get_purchaseorders = rail.QueryCollectionOperator(
            task_id="get_purchaseorders",
            name="new_blob_records",
            query="""SELECT * FROM merge_collection as mc WHERE mc.login_name = :login_name""",
            query_params={
                "login_name": "{{dag_run.conf.login_name}}"
            }
        )

        get_current_key_value = rail.RepliconServiceOperator(
            task_id="get_current_key_value",
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            data=lambda dag_run: {
                "keyNamespace": config.key_namespace,
                "key": dag_run.conf['login_name']
            }
        )
        has_any_value_present = rail.IfOperator(
            task_id="has_any_value_present",
            test="{{result('get_current_key_value') | is_truthy}}",
            yes_task="create_existing_blob_md5",
            no_task="add_key_value"
        )
        create_existing_blob_md5 = rail.DataAdaptorOperator(
            task_id="create_existing_blob_md5",
            source="{{result('get_current_key_value').jsonValue}}",
            columns=["workordernumber", "personnelnumber", "employee_id", "firstname", "lastname", "companycode", "purchaseorder",
                     "poitem", "item_startdate", "item_enddate", "regulartimebalance", "overtimebalance", "doubletimebalance",
                     "effective_date", "login_name", "md5", "id"],
            data=custom_method.get_create_existing_blob_md5
        )

        existing_blob_records = rail.CreateCollectionOperator(
            task_id="existing_blob_records",
            source="{{result('create_existing_blob_md5')}}"
        )

        add_key_value = rail.RepliconServiceOperator(
            task_id="add_key_value",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                "keyNamespace": config.key_namespace,
                "keyValue": {
                    "key": dag_run.conf['login_name'],
                    "jsonValue": request_payload.get_json_value_payload()
                }
            }
        )

        get_unique_existing_records = rail.QueryCollectionOperator(
            task_id="get_unique_existing_records",
            query="""SELECT * FROM existing_blob_records WHERE id NOT IN (SELECT DISTINCT id FROM new_blob_records)"""
        )
        get_new_blob_records_to_add = rail.QueryCollectionOperator(
            task_id="get_new_blob_records_to_add",
            query="""SELECT * FROM new_blob_records WHERE id NOT IN (SELECT DISTINCT id FROM existing_blob_records)"""
        )
        get_existing_blob_records_to_update = rail.QueryCollectionOperator(
            task_id="get_existing_blob_records_to_update",
            query="""SELECT * FROM new_blob_records WHERE id IN (SELECT DISTINCT id FROM existing_blob_records)"""
        )

        is_data_present = rail.IfOperator(
            task_id="is_data_present",
            test="{{result('get_new_blob_records_to_add','length') > 0 or\
                     result('get_existing_blob_records_to_update','length') > 0}}",
            yes_task="get_key_payload",
            no_task="log_success"
        )

        get_key_payload = rail.PythonOperator(
            task_id="get_key_payload",
            python_callable=request_payload.get_updated_json_key_payload
        )

        key_payload_present = rail.IfOperator(
            task_id="key_payload_present",
            test="{{result('get_key_payload') | is_truthy}}",
            yes_task="add_update_key"
        )
        add_update_key = rail.RepliconServiceOperator(
            task_id="add_update_key",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data={
                "keyNamespace": config.key_namespace,
                "keyValue": {
                    "key": "{{dag_run.conf.login_name}}",
                    "jsonValue": "{{result('get_key_payload') | to_json}}"
                }
            }
        )
        log_success = rail.WriteLogOperator(
            task_id="log_success",
            message="Added Purchaseorder balance in Replicon",
            items="{{result('get_purchaseorders')}}",
            severity="success",
            properties={
                "workordernumber": "{{item.workordernumber}}",
                "personnelnumber": "{{item.personnelnumber}}",
                "companycode": "{{item.companycode}}",
                "purchaseorder": "{{item.purchaseorder}}",
                "status": "success",
                "details": "Added Purchaseorder balance in Replicon",
                "action": "Purchaseorder_update"
            }
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            items="{{result('get_purchaseorders')}}",
            trigger_rule='one_failed',
            severity='failed',
            message='{{ get_error_message() }}',
            properties={
                "workordernumber": "{{item.workordernumber}}",
                "personnelnumber": "{{item.personnelnumber}}",
                "companycode": "{{item.companycode}}",
                "purchaseorder": "{{item.purchaseorder}}",
                "status": "failed",
                "details": '{{ get_error_message() }}',
                "action": "Purchaseorder_update"
            }
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_purchaseorders >> get_current_key_value  >> has_any_value_present >> create_existing_blob_md5 >> existing_blob_records
        has_any_value_present >> rail.Label(
            "No") >> add_key_value >> log_success

        existing_blob_records >> [get_unique_existing_records, get_new_blob_records_to_add,
                get_existing_blob_records_to_update] >> is_data_present

        is_data_present >> rail.Label("Yes") >> get_key_payload >> key_payload_present >> rail.Label(
            "Yes") >> add_update_key >> log_success
        key_payload_present >> rail.Label("No") >> log_success

        is_data_present >> rail.Label("No") >> log_success
        log_success >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
