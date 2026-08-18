from datetime import timedelta
from pendulum import datetime
import itertools
from tsystems.user_import_v2.tasks.process_user_groups_data import get_all_groups_data
from tsystems.user_import_v2.utils import custom_methods, request_payload, response_filters
from tsystems.user_import_v2.tasks.process_user_prerequisites import get_all_prerequisites_data
from airflow.models import Variable
import rail

null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f"T-Systems User Import Process Payload Child {config.instance}",
        start_date=datetime(2025, 8, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run_conf")

        logging_details = rail.PythonOperator(
            task_id='logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone, config.STANDARD_EMAIL_DATE_FORMAT, config.YMD_DATE_FORMAT],
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        create_supervisors_pending_log = rail.CreateLogOperator(
            task_id='create_supervisors_pending_log'
        )

        can_use_user_api_source = rail.IfOperator(
            task_id='can_use_user_api_source',
            test=lambda: Variable.get(
                config.can_use_user_api_source_var_name, default_var='true').lower() == 'true',
            yes_task='get_user_details_payload',
            no_task='filter_users_data_from_source'
        )

        get_user_details_payload = rail.PythonOperator(
            task_id='get_user_details_payload',
            python_callable=lambda: request_payload.get_user_details_payload(config.api_keys_mapper, config.data_source, config.data_source_stage, config.changed_since, config.filter_query)
        )

        open_bracket = '{{'
        close_bracket = '}}'

        # Get user details from API using EMP ID
        get_user_details_from_source = rail.SimpleHttpOperator(
            task_id='get_user_details_from_source',
            method='POST',
            http_conn_id=config.http_conn_id,
            endpoint='/a4u/employeeDataApi/v1/employments/search',
            headers={
                'Content-Type': 'application/json',
                'Authorization': f'Bearer {open_bracket}var.value.{config.access_token}{close_bracket}'
            },
            data='{{ result("get_user_details_payload") }}',
            response_filter=lambda response: rail.write_json_artifact(response.text)
        )

        filter_users_data_from_source = rail.PythonOperator(
            task_id='filter_users_data_from_source',
            python_callable=lambda dag_run: response_filters.filter_users_data_from_source(dag_run, config.api_keys_mapper)
        )

        create_users_payload_collection = rail.CreateCollectionOperator(
            task_id="create_users_payload_collection",
            source=custom_methods.load_user_details_from_artifacts,
            columns=[
                "email", "firstname", "lastname", "employeeid", "startdate",
                "enddate", "supervisorempid", "orgstructure", "costcenter", "legalunit", "record_id",
                "dtag_employeenumber", "personalnumber", "uniqueid_of_employment", "unique_user_ident", "type_of_employment",
                "date_of_employment", "type_of_work_relationship", "sub_type_of_employment", "status_of_employment", "functional_state",
                "manager_flag", "job_code_from_employee_central", "original_employment", "primary_employment", "country_of_employment",
                "city_of_employment", "account_active"],
            name="users_payload_data"
        )

        if_user_payload_exists = rail.IfOperator(
            task_id='if_user_payload_exists',
            test='{{ result("create_users_payload_collection", "length") > 0 }}',
            yes_task='query_blank_mandatory_fields',
            no_task='process_log_generation'
        )

        query_blank_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_blank_mandatory_fields",
            query="""SELECT * FROM users_payload_data WHERE NULLIF("email","") IS NULL OR NULLIF("employeeid","") IS NULL OR NULLIF("startdate","") IS NULL"""
        )

        if_blank_mandatory_fields = rail.IfOperator(
            task_id="if_blank_mandatory_fields",
            test='{{result("query_blank_mandatory_fields", "length") > 0}}',
            yes_task="write_blank_mandatory_fields_log",
            no_task="query_mandatory_fields"
        )

        write_blank_mandatory_fields_log = rail.WriteLogOperator(
            task_id="write_blank_mandatory_fields_log",
            log='{{ result("create_log") }}',
            items='{{result("query_blank_mandatory_fields")}}',
            message="Invalid user data",
            severity="Exception",
            properties=lambda item: {
                "employeeid": item.get("employeeid", ""),
                "action": "Validation",
                "status": "Exception",
                "details": custom_methods.get_invalid_user_log_details(item)
            }
        )

        query_mandatory_fields = rail.QueryCollectionOperator(
            task_id="query_mandatory_fields",
            query="""SELECT * FROM users_payload_data WHERE NULLIF("email","") IS NOT NULL AND NULLIF("employeeid","") IS NOT NULL AND NULLIF("startdate","") IS NOT NULL""",
            name="valid_users_payload_data"
        )

        query_all_user_employee_ids = rail.QueryCollectionOperator(
            task_id="query_all_user_employee_ids",
            query="""SELECT DISTINCT employeeid FROM valid_users_payload_data WHERE NULLIF("employeeid","") IS NOT NULL"""
        )

        if_mandatory_fields_data_exists = rail.IfOperator(
            task_id="if_mandatory_fields_data_exists",
            test='{{result("query_mandatory_fields", "length") > 0}}',
            yes_task="prereq_data_start",
            no_task="process_log_generation"
        )

        prereq_data_start = rail.EmptyOperator(
            task_id='prereq_data_start'
        )

        process_create_oef_tags = rail.TriggerDagRunOperator(
            task_id="process_create_oef_tags",
            trigger_dag_id=config.create_oef_tags_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                "log_artifact": rail.result("create_log")
            }
        )

        wait_for_process_create_oef_tags = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_create_oef_tags',
            dag_runs='{{ result("process_create_oef_tags") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        groups_data_start = rail.EmptyOperator(
            task_id='groups_data_start'
        )

        process_get_groups_data = get_all_groups_data()

        process_get_prerequisites_data = get_all_prerequisites_data(config)

        dummy_process_each_user = rail.EmptyOperator(
            task_id='dummy_process_each_user'
        )

        def get_process_each_user_batch_dag_id(record_id):
            modulo = int(record_id)%config.PROCESS_USER_BATCH_COUNT
            return f'{config.process_user_record_child_dag_id}_batch_{modulo+1}'

        # Process each user record using process_user_record_child
        process_user_record = rail.trigger_parallel_dagrun(
            task_id="process_user_record",
            items='{{ result("query_mandatory_fields") }}',
            parallel_count=config.trigger_parallel_dagrun_count_process_users,
            trigger_dag_id=lambda item: get_process_each_user_batch_dag_id(item['record_id']),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **{"modulo": int(item['record_id'])%config.PROCESS_USER_BATCH_COUNT},
                **custom_methods.create_users_payload_from_variable(item, config),
                "oef_data": {
                    oef["oef_name"]: {
                        "oef_uri": custom_methods.get_cached_result("get_all_user_oefs")[f"{oef['field_name']}_oef_uri"],
                        "oef_tag_uri": (
                            rail.find_first_by_attr_and_get_attr(
                                custom_methods.get_cached_result(f'get_{oef["field_name"]}_oef_values') or [],
                                "name",
                                item.get(oef["field_name"], ""),
                                "uri"
                            ) if oef['type'] == 'dropdown' and item.get(oef["field_name"]) else null
                        )
                    } for oef in config.oef_field_mapper_data
                },
                "current_date": custom_methods.get_cached_result("logging_details")["current_date"],
                "all_users_employee_ids": custom_methods.get_cached_result("query_all_user_employee_ids"),
                "supervisor_log": custom_methods.get_cached_result("create_supervisors_pending_log")
            }
        )

        dummy_process_user_ids = rail.EmptyOperator(
            task_id='dummy_process_user_ids'
        )

        # Note: rail.result() is called twice per iteration - first to check if result exists, then to get the value.
        # This pattern is intentional for safely iterating over parallel DAG runs that may or may not have completed.
        get_process_users_dag_ids = rail.PythonOperator(
            task_id='get_process_users_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: (rail.result(
                    f'process_user_record_{x+1}') if rail.result(
                    f'process_user_record_{x+1}') else []), range(config.trigger_parallel_dagrun_count_process_users))))),
            show_return_value_in_logs=False
        )

        gather_user_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_user_logs',
            dag_runs="{{ result('get_process_users_dag_ids') }}",
            dagrun_task_id='create_user_child_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        filter_pending_supervisor_records = rail.FilterLogEntriesOperator(
            task_id='filter_pending_supervisor_records',
            log='{{ result("create_supervisors_pending_log")}}',
            severity='Pending'
        )

        if_filtered_pending_supervisor_records = rail.IfOperator(
            task_id='if_filtered_pending_supervisor_records',
            test='{{ result("filter_pending_supervisor_records", "length") > 0 }}',
            yes_task='create_supervisor_assignment_log',
            no_task='process_log_generation'
        )

        create_supervisor_assignment_log = rail.CreateLogOperator(
            task_id='create_supervisor_assignment_log'
        )

        process_pending_supervisor_records = rail.trigger_parallel_dagrun(
            task_id="process_pending_supervisor_records",
            items='{{ result("filter_pending_supervisor_records") }}',
            parallel_count=config.trigger_parallel_dagrun_count_process_supervisors,
            trigger_dag_id=config.supervisor_assignment_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **dict(item['properties'].items()),
                "current_date": rail.result('logging_details')['current_date'],
                "supervisor_assign_log": rail.result('create_supervisor_assignment_log')
            }
        )

        process_log_generation = rail.EmptyOperator(
            task_id='process_log_generation'
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_child_dag_id,
            conf=lambda: {
                **rail.result("logging_details"),
                'user_logs': rail.result("gather_user_logs") if rail.result("gather_user_logs") else [],
                'create_log': rail.result("create_log"),
                'supervisor_assignment_log': rail.result("create_supervisor_assignment_log") if rail.result("create_supervisor_assignment_log") else null,
                'total_record_count': rail.result("query_mandatory_fields", key="length") or 0
            }
        )

        finish_import = rail.EmptyOperator(
            task_id='finish_import'
        )

        #Define task flow
        logging_details >> create_log >> create_supervisors_pending_log >> can_use_user_api_source
        can_use_user_api_source >> rail.Label("Yes") >> get_user_details_payload >> get_user_details_from_source >> filter_users_data_from_source >> create_users_payload_collection \
            >> if_user_payload_exists
        if_user_payload_exists >> rail.Label("Yes") >> query_blank_mandatory_fields
        if_user_payload_exists >> rail.Label("No") >> process_log_generation
        can_use_user_api_source >> rail.Label("No") >> filter_users_data_from_source >> create_users_payload_collection
        query_blank_mandatory_fields >> if_blank_mandatory_fields
        if_blank_mandatory_fields >> rail.Label("Yes") >> write_blank_mandatory_fields_log >> query_mandatory_fields >> query_all_user_employee_ids >> if_mandatory_fields_data_exists
        if_mandatory_fields_data_exists >> rail.Label("Yes") >> prereq_data_start \
            >> process_create_oef_tags >> wait_for_process_create_oef_tags >> groups_data_start >> process_get_groups_data >> process_get_prerequisites_data \
                >> dummy_process_each_user >> process_user_record >> dummy_process_user_ids >> get_process_users_dag_ids >> gather_user_logs >> filter_pending_supervisor_records >> if_filtered_pending_supervisor_records
        if_mandatory_fields_data_exists >> rail.Label("No") >> process_log_generation
        if_blank_mandatory_fields >> rail.Label("No") >> query_mandatory_fields >> query_all_user_employee_ids

        if_filtered_pending_supervisor_records >> rail.Label("Yes") >> create_supervisor_assignment_log >> process_pending_supervisor_records >> process_log_generation
        if_filtered_pending_supervisor_records >> rail.Label("No") >> process_log_generation
        
        process_log_generation >> trigger_log_generation >> finish_import

    return dag


rail.for_each_instance(create_main_dag)