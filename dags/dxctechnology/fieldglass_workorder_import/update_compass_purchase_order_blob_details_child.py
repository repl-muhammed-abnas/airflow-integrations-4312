from datetime import timedelta
import json
from dxctechnology.fieldglass_workorder_import.utils import custom_methods
from airflow.models import Variable
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_compass_purchase_order_blob_dag_id,
        description="purchase order blod details",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_process_child_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "query_valid_records_for_user_in_replicon"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout),
            start_task='query_valid_records_for_user_in_replicon',
            end_task="write_purchase_order_failure_log",
        )

        query_valid_records_for_user_in_replicon = rail.QueryCollectionOperator(
            task_id="query_valid_records_for_user_in_replicon",
            query="""SELECT * FROM merged_report_and_input_compass WHERE loginname='{{dag_run.conf["loginname"]}}' """
        )

        get_bulk_project_details = rail.RepliconServiceOperator(
            task_id="get_bulk_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data=lambda dag_run:{
                    "projects": [
                        {
                            "uri": null,
                            "name": dag_run.conf["CostCenterCode"],
                            "code": null,
                            "parameterCorrelationId": null
                        }
                    ]
            },
            data_handler=lambda response: response["projectDetails"][
                "uri"] if "projectDetails" in response else null
        )

        get_key_value_from_keystore = rail.RepliconServiceOperator(
            task_id="get_key_value_from_keystore",
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            data={
                "keyNamespace": config.keyNamespace_compass,
                "key": '{{dag_run.conf.key}}'
            }
        )

        if_no_key_value_present = rail.IfOperator(
            task_id="if_no_key_value_present",
            test=lambda: bool(not rail.result("get_key_value_from_keystore")),
            yes_task="put_new_records_as_key_value",
            no_task="compose_existing_key_value_csv"
        )

        put_new_records_as_key_value = rail.RepliconServiceOperator(
            task_id="put_new_records_as_key_value",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                "keyNamespace": config.keyNamespace_compass,
                "keyValue": {
                    "key": dag_run.conf["key"],
                    "jsonValue": custom_methods.get_json_new_key_value_blob_compass()
                }
            }
        )

        write_purchase_order_new_log = rail.WriteLogOperator(
            task_id="write_purchase_order_new_log",
            log='{{dag_run.conf.lookuptable}}',
            items='{{result("query_valid_records_for_user_in_replicon")}}',
            severity="success",
            message="Added Workorder balance in Replicon",
            properties=lambda item:{
                "workorderid": item["WorkOrderID"],
                "ContingentWorkerID": item["ContingentWorkerID"],
                "status": "success",
                "details": 'Added Workorder balance in Replicon',
                "Action": "Workorder_update"
            }
        )

        compose_existing_key_value_csv = rail.DataAdaptorOperator(
            task_id="compose_existing_key_value_csv",
            source=lambda:json.loads(rail.result("get_key_value_from_keystore")["jsonValue"]),
            columns=["workOrderId",
                     "revisionNumber",
                     "contingentworkerId",
                     "workOrderStartDate",
                     "workOrderEndDate",
                     "workOrderStatus",
                     "workerFirstName",
                     "workerLastName",
                     "costCenterCode",
                     "billRateCategory",
                     "billRate",
                     "RateUnit",
                     "siteCountryUseWithWorkerBasedReport",
                     "taskCode",
                     "wO_CATW",
                     "wO_workerType",
                     "financeSystem",
                     "remainingSpend",
                     "ccCompanyCode",
                     "actualBillRateCategory",
                     "projectUri",
                     "projectName",
                     "userUri",
                     "loginName",
                     "effectiveDateOfBalance",
                     "md5",
                     "uniqueid"
                     ],
            data=custom_methods.get_existing_key_value_md5_compass
        )

        create_existing_key_value_blob_collection = rail.CreateCollectionOperator(
            task_id="create_existing_key_value_blob_collection",
            source='{{result("compose_existing_key_value_csv")}}',
            name="existingblobrecords"
        )

        create_new_key_value_blob_csv = rail.DataAdaptorOperator(
            task_id="create_new_key_value_blob_csv",
            source='{{result("query_valid_records_for_user_in_replicon")}}',
            columns=["workOrderId",
                     "revisionNumber",
                     "contingentworkerId",
                     "workOrderStartDate",
                     "workOrderEndDate",
                     "workOrderStatus",
                     "workerFirstName",
                     "workerLastName",
                     "costCenterCode",
                     "billRateCategory",
                     "billRate",
                     "RateUnit",
                     "siteCountryUseWithWorkerBasedReport",
                     "taskCode",
                     "wO_CATW",
                     "wO_workerType",
                     "financeSystem",
                     "remainingSpend",
                     "ccCompanyCode",
                     "actualBillRateCategory",
                     "projectUri",
                     "projectName",
                     "userUri",
                     "loginName",
                     "effectiveDateOfBalance",
                     "md5",
                     "uniqueid"
                     ],
            data=custom_methods.get_new_key_value_md5_compass
        )

        create_new_key_value_blob_collection = rail.CreateCollectionOperator(
            task_id="create_new_key_value_blob_collection",
            source='{{result("create_new_key_value_blob_csv")}}',
            name="newblobrecords"
        )

        query_unique_records = rail.QueryCollectionOperator(
            task_id="query_unique_records",
            query="""SELECT * FROM existingblobrecords e WHERE e.md5 IN ( SELECT DISTINCT md5 FROM newblobrecords)""",
            name="unique_records"
        )

        query_unique_records_WorkOrderID = rail.QueryCollectionOperator(
            task_id="query_unique_records_WorkOrderID",
            query="""SELECT * FROM unique_records WHERE WorkOrderID IS NOT NULL"""
        )

        query_new_records_blob = rail.QueryCollectionOperator(
            task_id="query_new_records_blob",
            query="""SELECT * FROM newblobrecords WHERE md5 NOT IN ( SELECT DISTINCT md5 FROM existingblobrecords)""",
            name="validnewrecords"
        )

        query_new_records_blob_WorkOrderID = rail.QueryCollectionOperator(
            task_id="query_new_records_blob_WorkOrderID",
            query="""SELECT * FROM validnewrecords WHERE WorkOrderID IS NOT NULL"""
        )

        query_existing_id_in_new_records_blob = rail.QueryCollectionOperator(
            task_id="query_existing_id_in_new_records_blob",
            query="""SELECT * FROM existingblobrecords WHERE uniqueid IN ( SELECT DISTINCT uniqueid FROM newblobrecords)""",
            name="existingdatawithidinnewdata"
        )

        query_existing_id_in_new_records_blob_WorkOrderID = rail.QueryCollectionOperator(
            task_id="query_existing_id_in_new_records_blob_WorkOrderID",
            query="""SELECT * FROM existingdatawithidinnewdata WHERE WorkOrderID IS NOT NULL""",
            name="validexistingdatawithidinnewdata"
        )

        if_new_records_or_existing_id = rail.IfOperator(
            task_id="if_new_records_or_existing_id",
            test='{{result("query_new_records_blob_WorkOrderID", "length") > 0}}'\
                  or '{{result("query_existing_id_in_new_records_blob_WorkOrderID", "length") > 0}}',
            yes_task="if_any_existing_records",
            no_task="write_purchase_order_failure_log"
        )

        if_any_existing_records = rail.IfOperator(
            task_id="if_any_existing_records",
            test='{{result("query_unique_records_WorkOrderID", "length") > 0}}',
            yes_task="query_new_data_id_in_existing_records",
            no_task="if_key_value_update"
        )

        query_new_data_id_in_existing_records = rail.QueryCollectionOperator(
            task_id="query_new_data_id_in_existing_records",
            query="""SELECT * FROM validexistingdatawithidinnewdata WHERE md5 NOT IN(
            SELECT DISTINCT md5 FROM existingblobrecords)"""
        )

        if_key_value_update = rail.IfOperator(
            task_id="if_key_value_update",
            test='{{result("query_new_records_blob_WorkOrderID", "length") > 0}}'\
                  or '{{result("query_new_data_id_in_existing_records", "length") > 0}}',
            yes_task="put_key_value_exisitng_and_new_records",
            no_task="write_purchase_order_failure_log"
        )

        put_key_value_exisitng_and_new_records = rail.RepliconServiceOperator(
            task_id="put_key_value_exisitng_and_new_records",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                "keyNamespace": config.keyNamespace_compass,
                "keyValue": {
                    "key": dag_run.conf["key"],
                    "jsonValue": custom_methods.get_json_key_value_blob_compass()
                }
            }
        )


        write_purchase_order_update_log = rail.WriteLogOperator(
            task_id="write_purchase_order_update_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="success",
            message="Added Workorder balance in Replicon",
            properties=lambda dag_run:{
                "workorderid": dag_run.conf["WorkOrderID"],
                "ContingentWorkerID": dag_run.conf["ContingentWorkerID"],
                "status": "success",
                "details": 'Added Workorder balance in Replicon',
                "Action": "Workorder_update"
            }
        )

        write_purchase_order_failure_log = rail.WriteLogOperator(
            task_id="write_purchase_order_failure_log",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            message="Failed to add Workorder balance in Replicon",
            trigger_rule="one_failed",
            properties=lambda dag_run:{
                "workorderid": dag_run.conf["WorkOrderID"],
                "ContingentWorkerID": dag_run.conf["ContingentWorkerID"],
                "status": "Error",
                "details": rail.render_template('{{get_error_message()}}'),
                "Action": "Workorder_update"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            trigger_rule="all_done",
            sumo_conn_id="sumologic-dagrunlogger"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> write_purchase_order_failure_log >>log_to_sumo
        can_run_batch_task >> rail.Label("No") >>\
        query_valid_records_for_user_in_replicon >>\
        get_bulk_project_details >>\
        get_key_value_from_keystore >>\
        if_no_key_value_present >> rail.Label("Yes") >>\
        put_new_records_as_key_value >> write_purchase_order_new_log >> write_purchase_order_failure_log
        if_no_key_value_present >> rail.Label("No") >>\
        compose_existing_key_value_csv >>\
        create_existing_key_value_blob_collection >> create_new_key_value_blob_csv >>\
        create_new_key_value_blob_collection >> query_unique_records >>\
        query_unique_records_WorkOrderID >>\
        query_new_records_blob >> query_new_records_blob_WorkOrderID >>\
        query_existing_id_in_new_records_blob >> query_existing_id_in_new_records_blob_WorkOrderID >>\
        if_new_records_or_existing_id >> rail.Label("Yes") >>\
        if_any_existing_records >>rail.Label("Yes") >> query_new_data_id_in_existing_records >>\
        if_key_value_update
        if_any_existing_records >> rail.Label("No") >> \
        if_key_value_update >> rail.Label("Yes") >> put_key_value_exisitng_and_new_records>>\
        write_purchase_order_update_log >> write_purchase_order_failure_log
        if_key_value_update >> rail.Label("No") >> write_purchase_order_failure_log
        if_new_records_or_existing_id >> rail.Label("No") >>\
        write_purchase_order_failure_log >> log_to_sumo

        return dag


rail.for_each_instance(create_airflow_child_dag)
