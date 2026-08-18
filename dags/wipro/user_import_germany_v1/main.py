from datetime import timedelta
from wipro.user_import_germany_v1.task import process_user_group_data
from wipro.user_import_germany_v1.utils import custom_methods, request_payload
import rail
null = None


def create_airflow_child(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description="wipro User import process record",
        company_key=config.company_key,
        max_active_runs=config.master_max_active_run,
        replicon_conn_id=config.replicon_conn_id,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        user_start = rail.EmptyOperator(task_id="user_start")
        create_log_for_user_import_global = rail.CreateLogOperator(
            task_id="create_log_for_user_import_global"
        )

        query_valid_user_records = rail.QueryCollectionOperator(
            task_id="query_valid_user_records",
            query="""SELECT * FROM germany_users WHERE NULLIF("employee_id","") IS NOT NULL AND
            NULLIF("employee_first_name","") IS NOT NULL AND
            NULLIF("date_of_joining","") IS NOT NULL AND ("date_of_joining" NOT LIKE "9999-12-31"
            AND "date_of_joining" NOT LIKE "0000-00-00") AND NULLIF("country","") IS NOT NULL AND
            NULLIF("location","") IS NOT NULL AND NULLIF("employment_status","") IS NOT NULL AND
            NULLIF("company_code","") IS NOT NULL AND NULLIF("personnel_area_text","") IS NOT NULL AND
            NULLIF("adid","") IS NOT NULL AND NULLIF("work_council_tagging","") IS NOT NULL""",
            name="valid_users"
        )

        query_invalid_user_records = rail.QueryCollectionOperator(
            task_id="query_invalid_user_records",
            query="""SELECT * FROM germany_users WHERE NULLIF("employee_id","") IS NULL OR
            NULLIF("employee_first_name","") IS NULL OR
            NULLIF("date_of_joining","") IS NULL OR NULLIF("country","") IS NULL OR
            NULLIF("location","") IS NULL OR NULLIF("employment_status","") IS NULL OR
            NULLIF("company_code","") IS NULL OR NULLIF("personnel_area_text","") IS NULL OR
            NULLIF("adid","") IS NULL OR "date_of_joining" LIKE "9999-12-31"
            or "date_of_joining" LIKE 0000-00-00 OR NULLIF("work_council_tagging","") IS  NULL"""
        )

        if_invalid_users = rail.IfOperator(
            task_id="if_invalid_users",
            test='{{result("query_invalid_user_records", "length") > 0}}',
            no_task="if_valid_user_records",
            yes_task="write_invalid_users_log"
        )

        if_valid_user_records = rail.IfOperator(
            task_id="if_valid_user_records",
            test='{{result("query_valid_user_records", "length") > 0}}',
            yes_task="query_to_identify_new_entity",
            no_task="end_germany_user"
        )

        write_invalid_users_log = rail.WriteLogOperator(
            task_id="write_invalid_users_log",
            log='{{result("create_log_for_user_import_global")}}',
            items='{{result("query_invalid_user_records")}}',
            message="Invlalid user data",
            severity="Exception",
            properties=lambda dag_run, item: {
                "employee_id": item["employee_id"],
                "employee_first_name": item["employee_first_name"],
                "employee_last_name": item["employee_last_name"],
                "country": item["country"],
                "company_code": item["company_code"],
                "status": "Exception",
                "details": custom_methods.get_invalid_user_log_details(item),


            }
        )

        query_to_identify_new_entity = rail.QueryCollectionOperator(
            task_id="query_to_identify_new_entity",
            query=f"""SELECT *,
                    CAST(
                    CASE
                    WHEN company_code IN {config.NEW_ENTITY_LIST} THEN 1
                    ELSE 0
                    END AS INTEGER) AS new_entity_flag
                    FROM valid_users""",
            name="germany_valid_users"
            )

        groups_data = rail.EmptyOperator(task_id="groups_data")
        get_all_prerequisite_data = process_user_group_data.create_prerequisite_data(config)

        create_location = rail.TriggerDagRunOperator(
            task_id="create_location",
            trigger_dag_id=config.create_location_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda: {
                "location_details": rail.result("get_all_location_with_hierarchy_details"),
                "lookuptable": rail.result("create_log_for_user_import_global"),

                "country": "Germany",
                "locationcountryuri": rail.result("get_germany_parent_location_details"),
            }
        )

        get_all_location_hierarchy = rail.RepliconServiceOperator(
            task_id="get_all_location_hierarchy",
            endpoint="/services/LocationListService1.svc/GetChildHierarchyData",
            data=request_payload.get_location_hierarchy_payload,
            data_handler=custom_methods.get_location_hierarchy_data
        )

        process_user_start = rail.EmptyOperator(task_id="process_user_start")

        process_user_for_germany = rail.trigger_parallel_dagrun(
            task_id="process_user_for_germany",
            items='{{result("query_to_identify_new_entity")}}',
            trigger_dag_id=config.valid_user_dag_id,
            parallel_count=config.max_active_run_child,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item,config=config:custom_methods.get_germany_user_conf(item,config)
        )

        get_supervisor_pending_logs = rail.FilterLogEntriesOperator(
            task_id="get_supervisor_pending_logs",
            severity="Pending",
            remove_filtered_entries=True,
            log='{{result("create_log_for_user_import_global")}}'
        )

        load_supervisor_data = rail.PythonOperator(
            task_id="load_supervisor_data",
            python_callable=lambda : list(map(lambda i:{**i["properties"]},rail.load_all_records(rail.result("get_supervisor_pending_logs")))
        ))

        process_supervisor_for_germany = rail.trigger_parallel_dagrun(
            task_id="process_supervisor_for_germany",
            items='{{result("load_supervisor_data")|to_json}}',
            trigger_dag_id=config.create_supervisor_dag_id,
            parallel_count=config.max_active_run_child,
            execution_timeout=timedelta(days=config.execution_timeout),
            conf=lambda item,config=config:custom_methods.get_germany_supervisor_conf(item,config)
        )

        end_germany_user = rail.EmptyOperator(
            task_id="end_germany_user"
        )

        process_logs_for_germany = rail.TriggerDagRunOperator(
            task_id="process_logs_for_germany",
            trigger_dag_id=config.log_schedule_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(config.execution_timeout),
            conf=lambda dag_run:{
                "parent_run_id":dag_run.id
            }
        )

        user_start >> create_log_for_user_import_global >>\
            query_valid_user_records >>\
            query_invalid_user_records >>\
            if_invalid_users >> rail.Label(
                "Yes") >> write_invalid_users_log >> if_valid_user_records
        if_invalid_users >> rail.Label("No") >>\
            if_valid_user_records >> rail.Label("Yes") >> query_to_identify_new_entity >>\
            groups_data >>\
            get_all_prerequisite_data >> \
            create_location >>\
            get_all_location_hierarchy >>\
            process_user_start >>\
            process_user_for_germany >>\
            get_supervisor_pending_logs >> load_supervisor_data >>\
            process_supervisor_for_germany >>\
            end_germany_user
        if_valid_user_records >> rail.Label(
            "No") >> end_germany_user >> process_logs_for_germany
        return dag


rail.for_each_instance(create_airflow_child)
