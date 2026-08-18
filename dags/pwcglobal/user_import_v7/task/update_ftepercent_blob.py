import json
from datetime import datetime
from pendulum import now
import rail
from pwcglobal.user_import_v7.utils import request_payload, custom_method


null = None


def update_ftepercent_blob(config):
    with rail.TaskGroup(group_id="update_fte_blob", prefix_group_id=False) as update_blob:

        if_fte_date_in_last_thirty_days = rail.IfOperator(
            task_id="if_fte_date_in_last_thirty_days",
            test=lambda dag_run: bool(((request_payload.check_past_date() or request_payload.check_past_date() == 0)
                                       and custom_method.check_fte_date_within_range(dag_run))
                                      or not dag_run.conf["ftepercenteffectivedate"]),
            yes_task="if_product_license_present",
            no_task="fte_end"
        )

        if_product_license_present = rail.IfOperator(
            task_id="if_product_license_present",
            test=lambda: bool(
                len(rail.result("bulk_get_user3")["assignedProducts"]) > 0),
            yes_task="if_timesheet_template_assigned",
            no_task="update_validation_log_for_product_fte"
        )

        update_validation_log_for_product_fte = rail.PythonOperator(
            task_id="update_validation_log_for_product_fte",
            python_callable=lambda: request_payload.get_conf().get('validationlog', []).append(
                {"message": "Product license not assigned for user hence ftepercent not updated"})
        )

        if_timesheet_template_assigned = rail.IfOperator(
            task_id="if_timesheet_template_assigned",
            test=lambda: bool(rail.result("bulk_get_user3")
                              ["timesheetTemplate"]),
            yes_task="if_fte_date_in_past",
            no_task="update_validation_log_for_ts_fte"
        )

        update_validation_log_for_ts_fte = rail.PythonOperator(
            task_id="update_validation_log_for_ts_fte",
            python_callable=lambda: request_payload.get_conf().get('validationlog', []).append(
                {"message": "Time sheet not assigned for user hence ftepercent not updated"})
        )

        if_fte_date_in_past = rail.IfOperator(
            task_id="if_fte_date_in_past",
            test=lambda: bool(request_payload.check_past_date()
                              and request_payload.check_past_date() > 0),
            yes_task="get_past_timesheet_period",
            no_task="get_current_timesheet_period"
        )

        get_past_timesheet_period = rail.RepliconServiceOperator(
            task_id="get_past_timesheet_period",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate",
            data=lambda dag_run: {
                    "userUri": dag_run.conf["useruri"],
                    "date": rail.parse_date(dag_run.conf["ftepercenteffectivedate"], "%Y%m%d"),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response: rail.parse_date(
                response["displayText"].split("/")[1], "%Y-%m-%d") if response else null
        )

        get_current_timesheet_period = rail.RepliconServiceOperator(
            task_id="get_current_timesheet_period",
            endpoint="/services/TimesheetService1.svc/GetTimesheetForDate",
            data=lambda dag_run: {
                    "userUri": dag_run.conf["useruri"],
                    "date": rail.parse_date(datetime.strftime(now(tz="Europe/London"), "%m/%d/%Y"), "%m/%d/%Y"),
                    "timesheetGetOptionUri": "urn:replicon:timesheet-get-option:create-timesheet-if-necessary"
            },
            data_handler=lambda response: rail.parse_date(
                response["displayText"].split("/")[1], "%Y-%m-%d") if response else null
        )

        update_ftepercent_udf = rail.RepliconServiceOperator(
            task_id='update_ftepercent_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            data=lambda dag_run: {
                "objectUri": dag_run.conf["useruri"],
                "customFieldUri": dag_run.conf["customfielduri"]["ftepercenturi"],
                "value": request_payload.get_conf()['ftepercent']
            }
        )

        get_all_key_value_for_fte_value = rail.RepliconServiceOperator(
            task_id="get_all_key_value_for_fte_value",
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            data=lambda dag_run: {
                    "keyNamespace": config.keynamespace,
                    "key": dag_run.conf["useruri"]
            },
        )

        if_key_value_exists_for_user = rail.IfOperator(
            task_id="if_key_value_exists_for_user",
            test=lambda: bool(rail.result("get_all_key_value_for_fte_value") and "jsonValue" in rail.result(
                "get_all_key_value_for_fte_value")),
            yes_task="create_md5_for_existing_records",
            no_task="put_key_value_to_ftevalue_space"
        )

        put_key_value_to_ftevalue_space = rail.RepliconServiceOperator(
            task_id="put_key_value_to_ftevalue_space",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                    "keyNamespace": config.keynamespace,
                    "keyValue": {
                        "key": dag_run.conf["useruri"],
                        "jsonValue": json.dumps(request_payload.get_ftevalue_update_json_request()
                                                )
                    }
            }
        )

        create_md5_for_existing_records = rail.DataAdaptorOperator(
            task_id="create_md5_for_existing_records",
            source='{{result("get_all_key_value_for_fte_value").jsonValue}}',
            columns=["value", "effectivedate", "md5"],
            data=custom_method.get_existing_blob_md5

        )

        create_md5_for_new_record = rail.PythonOperator(
            task_id="create_md5_for_new_record",
            python_callable=custom_method.get_new_records_md5
        )

        create_existing_blob_collection = rail.CreateCollectionOperator(
            task_id="create_existing_blob_collection",
            source='{{result("create_md5_for_existing_records")}}',
            name="existingblobrecords"
        )

        create_new_blob_collection = rail.CreateCollectionOperator(
            task_id="create_new_blob_collection",
            source='{{result("create_md5_for_new_record")|to_json}}',
            name="newblobrecords"
        )

        query_new_records = rail.QueryCollectionOperator(
            task_id="query_new_records",
            query="""SELECT * FROM newblobrecords WHERE md5 NOT IN( SELECT DISTINCT md5 FROM existingblobrecords)""",
            name="new_records"
        )

        query_unique_existing_records = rail.QueryCollectionOperator(
            task_id="query_unique_existing_records",
            query="""SELECT * FROM existingblobrecords WHERE effectivedate NOT IN( SELECT DISTINCT effectivedate FROM newblobrecords)""",
            name="existing_records"
        )

        query_blob_fte_date = rail.QueryCollectionOperator(
            task_id="query_blob_fte_date",
            query="""SELECT newr.value, existing.effectivedate FROM new_records newr,existingblobrecords existing
              WHERE newr.effectivedate=existing.effectivedate"""
        )

        if_distinct_new_records = rail.IfOperator(
            task_id="if_distinct_new_records",
            test='{{result("query_new_records", "length") > 0 or result("query_blob_fte_date", "length") > 0}}',
            yes_task="put_new_key_value_to_ftevalue_space",
            no_task="fte_end"
        )

        put_new_key_value_to_ftevalue_space = rail.RepliconServiceOperator(
            task_id="put_new_key_value_to_ftevalue_space",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                    "keyNamespace": config.keynamespace,
                    "keyValue": {
                        "key": dag_run.conf["useruri"],
                        "jsonValue": json.dumps(request_payload.get_ftevalue_blob_update_json_request()
                                                )
                    }
            }
        )

        fte_end = rail.EmptyOperator(task_id="fte_end")

        if_fte_date_in_last_thirty_days >> rail.Label("No") >> fte_end
        if_fte_date_in_last_thirty_days >> rail.Label("Yes") >>\
            if_product_license_present >> rail.Label("Yes") >>\
            if_timesheet_template_assigned >> rail.Label("Yes") >>\
            if_fte_date_in_past >> rail.Label("Yes") >>\
            get_past_timesheet_period >> update_ftepercent_udf
        if_fte_date_in_past >> rail.Label("No") >>\
            get_current_timesheet_period >>\
            update_ftepercent_udf >>\
            get_all_key_value_for_fte_value >> if_key_value_exists_for_user >> rail.Label("Yes") >>\
            create_md5_for_existing_records >> create_md5_for_new_record >>\
            create_existing_blob_collection >> create_new_blob_collection >>\
            query_new_records >> query_unique_existing_records >>\
            query_blob_fte_date >>\
            if_distinct_new_records >> rail.Label(
            "Yes") >> put_new_key_value_to_ftevalue_space >> fte_end
        if_product_license_present >> rail.Label(
            "No") >> update_validation_log_for_product_fte >> fte_end
        if_timesheet_template_assigned >> rail.Label(
            "No") >> update_validation_log_for_ts_fte >> fte_end
        if_distinct_new_records >> rail.Label("No") >> fte_end
        if_key_value_exists_for_user >> rail.Label(
            "No") >> put_key_value_to_ftevalue_space >> fte_end
        return update_blob
