from pendulum import datetime as dt,now
import itertools
import rail
import os
from datetime import timedelta
from sweethometherapyllc.time_entry_import.utils import custom_methods, request_payload

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'sweethometherapyllc Time Entry Import - Master DAG ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(minutes=5),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        log_start_time = rail.PythonOperator(
            task_id="log_start_time",
            python_callable=lambda: {
                "start_time": now(config.timezone).isoformat(),
                "log_filename": "Log_" + rail.render_template(
                    '{{ result("new_file_sensor") | file_name }}'
                ),
            }
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}', 
            yes_task='download_csv_content',
            no_task='send_invalid_format_email',
        )

        send_invalid_format_email = rail.EmailOperator(
            task_id='send_invalid_format_email',
            to=config.tenant_email,
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - Invalid Format - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/invalid_format_email.html"
        )

        download_csv_content = rail.SFTPDownloadFileOperator(
            task_id='download_csv_content',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(    
            task_id='delete_this_dagrun')

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.input_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )


        load_csv_data = rail.LoadCSVFileOperator(
            task_id='load_csv_data',
            document="{{ result('download_csv_content') }}",
            headers=config.column_mapping,
        )

        create_csv_collection = rail.CreateCollectionOperator(
            task_id='create_csv_collection',
            source="{{ result('load_csv_data') }}",
            name="time_import_records",
        )

        has_any_records = rail.IfOperator(
            task_id='has_any_records',
            test="{{ result('create_csv_collection', 'length') > 0 }}",
            yes_task='create_log',
            no_task='send_no_data_email'
        )

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email, 
            cc=config.internal_logs_email,
            subject='{{get_company_key()}} | Replicon Time Import - No Data - {{ current_time_in_specified_tz() }}',
            html_content="templates/emails/blank_no_data_email.html"
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        store_valid_records = rail.QueryCollectionOperator(
            task_id='store_valid_records',
            query="""SELECT * 
            FROM time_import_records 
            WHERE 
                NULLIF(school, '') IS NOT NULL 
                AND NULLIF(service_name, '') IS NOT NULL 
                AND NULLIF(type1, '') IS NOT NULL 
                AND NULLIF(therapist, '') IS NOT NULL 
                AND NULLIF(hours, '') IS NOT NULL 
                AND TRIM(hours) GLOB '*[0-9]*'
                AND CAST(TRIM(hours) AS FLOAT) > 0
                AND NULLIF(num_students, '') IS NOT NULL 
                AND TRIM(num_students) GLOB '*[0-9]*'
                AND CAST(TRIM(num_students) AS INTEGER) > 0
                AND NULLIF(date_of_service, '') IS NOT NULL""",
            name="valid_entries"
        )

        store_invalid_records = rail.QueryCollectionOperator(
            task_id='store_invalid_records',
            query="""SELECT * 
            FROM time_import_records 
            WHERE 
                NULLIF(school, '') IS NULL 
                OR NULLIF(service_name, '') IS NULL 
                OR NULLIF(type1, '') IS NULL 
                OR NULLIF(therapist, '') IS NULL 
                OR NULLIF(hours, '') IS NULL 
                OR TRIM(hours) NOT GLOB '*[0-9]*'
                OR CAST(TRIM(hours) AS FLOAT) <= 0
                OR NULLIF(num_students, '') IS NULL 
                OR TRIM(num_students) NOT GLOB '*[0-9]*'
                OR CAST(TRIM(num_students) AS INTEGER) <= 0
                OR NULLIF(date_of_service, '') IS NULL""",
            name="invalid_entries"
        )

        log_invalid_records = rail.WriteLogOperator(
            task_id='log_invalid_records',
            log="{{ result('create_log') }}",
            severity="Exception",
            items="{{ result('store_invalid_records') }}",
            message="Invalid time import record - missing mandatory field(s)",
            properties=lambda dag_run,item: {
                'entry_keyid': item.get('entry_keyid', ''),
                'school': item.get('school', ''),
                'service_name': item.get('service_name', ''),
                'therapist': item.get('therapist', ''),
                'hours': item.get('hours', ''),
                'date_of_service': item.get('date_of_service',''),
                'status': "Exception",
                'action': "Validation",
                'details': custom_methods.get_validation_error_message(item)
            }
        )

        has_valid_records = rail.IfOperator(
            task_id='has_valid_records',
            test="{{ result('store_valid_records', 'length') > 0 }}",
            yes_task='aggregate_entries',
            no_task='trigger_log_generation'
        )

        aggregate_entries = rail.PythonOperator(
            task_id='aggregate_entries',
            python_callable=lambda: custom_methods.aggregate_entries(
                rail.load_all_records(rail.result('store_valid_records'))
            )
        )

        create_aggregated_collection = rail.CreateCollectionOperator(
            task_id='create_aggregated_collection',
            source=lambda: rail.result('aggregate_entries'),
            name="aggregated_entries",
        )

        get_unique_therapists = rail.QueryCollectionOperator(
            task_id='get_unique_therapists',
            query="SELECT DISTINCT therapist FROM aggregated_entries WHERE therapist IS NOT NULL",
            name="unique_therapists"
        )

        get_object_extention_fields = rail.RepliconServiceOperator(
            task_id="get_object_extention_fields",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data=lambda: {
                "bindingContextUri": "urn:replicon:object-type:time-entry"
            },
            data_handler=lambda res: [
                {
                    "name": ext_field.get("name"),
                    "uri": ext_field.get("uri")
                }
                for ext_field in res if res
            ]
        )

        get_tags_for_each_dropdown_oef = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_tags_for_each_dropdown_oef',
            endpoint='services/ObjectExtensionTagListService1.svc/GetData',

            items=lambda: list(
                filter(
                    lambda oef: oef['name'] in ["Service Name", "Type Billing"],
                    rail.result('get_object_extention_fields')
                )
            ),

            data=lambda item: {
                "page": 1,
                "pagesize": 100000,
                "columnUris": [
                    "urn:replicon:object-extension-tag-list-column:name",
                    "urn:replicon:object-extension-tag-list-column:object-extension-tag",
                    "urn:replicon:object-extension-tag-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "filterDefinitionUri": "urn:replicon:object-extension-tag-list-filter:definition"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "value": {
                            "uri": item["uri"]
                        }
                    }
                }
            },

            data_handler=lambda response, item: custom_methods.get_tags_object(response, item)
        )
                
        trigger_process_therapists = rail.trigger_parallel_dagrun(
            task_id='trigger_process_therapists',
            trigger_dag_id=config.process_unique_therapists_child,
            items="{{ result('get_unique_therapists')}}",
            conf=lambda item: {
                "therapist": item['therapist'],
                "log": rail.result('create_log'),
                "object_extension_fields": rail.result('get_object_extention_fields'),
                "tags_for_dropdown_oef": rail.result('get_tags_for_each_dropdown_oef'),
            },
            parallel_count=config.process_parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_trigger_process_therapists_dag_ids = rail.PythonOperator(
            task_id='get_trigger_process_therapists_dag_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_process_therapists_{x+1}'),
                    range(config.process_parallel_count))))),
            show_return_value_in_logs=False
        )

        trigger_log_generation = rail.TriggerDagRunOperator(
            task_id='trigger_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation,
            conf=lambda: {
                'log': rail.result("create_log"),
                'start_time': rail.result("log_start_time")["start_time"],
                'input_filename': rail.render_template('{{ result("new_file_sensor") | file_name }}')
            }
        )

        new_file_sensor >> log_start_time >> is_csv
        is_csv >> rail.Label("Yes") >> download_csv_content
        is_csv >> rail.Label("No") >> send_invalid_format_email >> trigger_log_generation
        
        download_csv_content >> load_csv_data
        download_csv_content >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file
        was_new_file_found >> rail.Label("no") >> delete_this_dagrun
        
        send_no_data_email >> trigger_log_generation
        
        load_csv_data >> create_csv_collection >> has_any_records
        
        has_any_records >> rail.Label("Yes") >> create_log
        has_any_records >> rail.Label("No") >> send_no_data_email >> trigger_log_generation
        
        create_log >>  [store_valid_records, store_invalid_records] >> log_invalid_records >> has_valid_records
        
        has_valid_records >> rail.Label("Yes") >> aggregate_entries >> create_aggregated_collection >> get_unique_therapists >> get_object_extention_fields >> get_tags_for_each_dropdown_oef >> trigger_process_therapists
        has_valid_records >> rail.Label("No") >> trigger_log_generation
                
        trigger_process_therapists >> get_trigger_process_therapists_dag_ids >> trigger_log_generation

    return dag
rail.for_each_instance(create_dag)