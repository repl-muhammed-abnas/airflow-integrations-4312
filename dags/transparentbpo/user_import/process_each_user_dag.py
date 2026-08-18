from datetime import timedelta
import json
from airflow.models import Variable
from transparentbpo.user_import.utils import request_payload, custom_methods
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_each_user_dag_id,
        description=f'TransparentBPO User Import Process Each User Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_user,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_error',
        )

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
        )
        
        create_project_log = rail.CreateLogOperator(
            task_id='create_project_log'
        )

        create_reference_log = rail.CreateLogOperator(
            task_id='create_reference_log'
        )

        get_employee_details = rail.BambooHROperator(
            task_id='get_employee_details',
            bamboohr_conn_id=config.bamboohr_conn_id,
            company_domain='',
            request_method='GET',
            endpoint="/employees/{{ dag_run.conf.id }}?fields=" + ",".join(
                config.BAMBOO_STANDARD_FIELDS + config.BAMBOO_CUSTOM_FIELDS),
            data_handler=custom_methods.get_employee_details_callable
        )

        get_table_record_of_employees = rail.BambooHROperator(
            task_id='get_table_record_of_employees',
            bamboohr_conn_id=config.bamboohr_conn_id,
            company_domain='',
            request_method='GET',
            endpoint="/employees/{{ dag_run.conf.id }}/tables/" +
            config.CUSTOM_TABLE_NAME
        )

        get_daydiff_effectivedate_for_table_records = rail.PythonOperator(
            task_id='get_daydiff_effectivedate_for_table_records',
            python_callable=lambda dag_run: custom_methods.get_daydiff_data(
                dag_run.conf['job_run_date'], rail.result('get_table_record_of_employees'), config.DATE_FORMAT)
        )

        get_min_daydiff_table_entry = rail.PythonOperator(
            task_id='get_min_daydiff_table_entry',
            python_callable=lambda: custom_methods.get_min_effectivedate_entry(rail.result(
                "get_daydiff_effectivedate_for_table_records"))
        )

        if_work_email_employeeid_present = rail.IfOperator(
            task_id='if_work_email_employeeid_present',
            test=lambda: rail.result("get_employee_details").get(
                'workEmail') and rail.result("get_employee_details").get('employeeNumber'),
            yes_task='validate_employee_data',
            no_task='catch_error'
        )

        # Validate employee data
        validate_employee_data = rail.PythonOperator(
            task_id='validate_employee_data',
            python_callable=lambda: custom_methods.validate_employee_data(
                rail.result('get_employee_details'), rail.result('get_table_record_of_employees'), config.MANDATORY_FIELDS)
        )

        is_record_invalid = rail.IfOperator(
            task_id='is_record_invalid',
            test=lambda: not (rail.result(
                "validate_employee_data")['is_valid']),
            yes_task='log_invalid_record_skipped',
            no_task='create_md5_for_input_fields'
        )

        log_invalid_record_skipped = rail.WriteLogOperator(
            task_id='log_invalid_record_skipped',
            log="{{result('create_user_log')}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": rail.result('get_employee_details').get('employeeNumber') if rail.result('get_employee_details') else "",
                "user_name": rail.result('get_employee_details').get('firstName', '') + ' ' + rail.result(
                    'get_employee_details').get('middleName', '') + ' ' + rail.result('get_employee_details').get('lastName', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "precheck",
                'status': 'ignored',
                'details': rail.result('validate_employee_data')['validation_details']
            }
        )

        create_md5_for_input_fields = rail.PythonOperator(
            task_id='create_md5_for_input_fields',
            python_callable=lambda: custom_methods.get_record_with_md5(
                rail.result('get_employee_details'), rail.result('get_min_daydiff_table_entry')['min_daydiff_entry'])
        )

        query_id_in_reference_file_data = rail.QueryCollectionOperator(
            task_id='query_id_in_reference_file_data',
            query="""SELECT * FROM reference_file_data WHERE reference_file_data.id == :record_id""",
            query_params={
                'record_id': "{{dag_run.conf.id}}"
            }
        )

        is_record_present_with_same_id = rail.IfOperator(
            task_id='is_record_present_with_same_id',
            test=lambda: int(rail.result(
                'query_id_in_reference_file_data', 'length')) > 0,
            yes_task='check_md5_value_not_equals_to_ref',
            no_task='add_entry_add_user_reference_data_log'
        )

        check_md5_value_not_equals_to_ref = rail.IfOperator(
            task_id='check_md5_value_not_equals_to_ref',
            test=lambda: custom_methods.md5_value_check(rail.result(
                'query_id_in_reference_file_data'), rail.result('create_md5_for_input_fields').get('md5', '')),
            yes_task='add_entry_update_user_reference_data_log',
            no_task='catch_error'
        )

        add_entry_update_user_reference_data_log = rail.WriteLogOperator(
            task_id='add_entry_update_user_reference_data_log',
            log="{{ result('create_reference_log') }}",
            severity='update_entry',
            message='na',
            properties={
                'id': "{{dag_run.conf.id}}",
                'md5': "{{result('create_md5_for_input_fields').md5}}",
                'jobdate': "{{dag_run.conf.log_timestamp}}",
                'ecid': "{{dag_run_ecid()}}"
            }
        )

        add_entry_add_user_reference_data_log = rail.WriteLogOperator(
            task_id='add_entry_add_user_reference_data_log',
            log="{{ result('create_reference_log') }}",
            severity='add_entry',
            message='na',
            properties={
                'id': "{{dag_run.conf.id}}",
                'md5': "{{result('create_md5_for_input_fields').md5}}",
                'jobdate': "{{dag_run.conf.log_timestamp}}",
                'ecid': "{{dag_run_ecid()}}"
            }
        )

        search_location_in_holiday_timezone_mapper = rail.PythonOperator(
            task_id='search_location_in_holiday_timezone_mapper',
            python_callable=lambda: list(filter(lambda x: x["location"] == rail.result(
                'get_employee_details')['location'], config.HOLIDAY_AND_TIMEZONE_MAPPER))
        )

        if_location_not_present_in_mapper = rail.IfOperator(
            task_id='if_location_not_present_in_mapper',
            test=lambda: len(rail.result(
                'search_location_in_holiday_timezone_mapper')) < 1,
            yes_task='log_location_not_found',
            no_task='get_user_data'
        )

        log_location_not_found = rail.WriteLogOperator(
            task_id='log_location_not_found',
            log="{{result('create_user_log')}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": rail.result('get_employee_details').get('employeeNumber') if rail.result('get_employee_details') else "",
                "user_name": rail.result('get_employee_details').get('firstName', '') + ' ' + rail.result(
                    'get_employee_details').get('middleName', '') + ' ' + rail.result('get_employee_details').get('lastName', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "precheck",
                'status': 'ignored',
                'details': f"User not processed as the recived location '{rail.result('get_employee_details')['location']}' is not part of the Integration mapper."
            }
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id="get_user_data",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": rail.result('get_employee_details').get('employeeNumber'),
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0] if response else []
        )

        if_user_present = rail.IfOperator(
            task_id='if_user_present',
            test=lambda: rail.result('get_user_data'),
            yes_task='get_current_payrule_for_user',
            no_task='dummy_trigger_add_user'
        )
 
        get_current_payrule_for_user = rail.PythonOperator(
            task_id='get_current_payrule_for_user',
            python_callable=lambda dag_run: custom_methods.get_current_value_from_schedule_list_for_user(
                rail.result("get_user_data")['payRuleScriptSchedule'], 'payRuleScript', 'displayText', dag_run.conf['job_run_date'], config)
        )

        get_user_details_artifact = rail.PythonOperator(
            task_id='get_user_details_artifact',
            python_callable=lambda: rail.write_artifact(
                json.dumps(rail.result('get_user_data')))
        )

        process_update_user = rail.TriggerDagRunOperator(
            task_id='process_update_user',
            trigger_dag_id=config.process_update_user_dag_id,
            conf=lambda dag_run: request_payload.get_process_update_or_add_user_payload(rail.result(
                'get_employee_details'), rail.result('get_min_daydiff_table_entry')['min_daydiff_entry'], 'update', dag_run, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_update_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_update_user',
            dag_runs='{{ result("process_update_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        dummy_trigger_add_user = rail.EmptyOperator(
            task_id='dummy_trigger_add_user'
        )

        if_user_status_active = rail.IfOperator(
            task_id='if_user_status_active',
            test=lambda: rail.result('get_employee_details').get(
                'status') == 'Active',
            yes_task='process_add_user',
            no_task='log_inactive_user_in_bamboohr'
        )

        log_inactive_user_in_bamboohr = rail.WriteLogOperator(
            task_id='log_inactive_user_in_bamboohr',
            log="{{result('create_user_log')}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": rail.result('get_employee_details').get('employeeNumber') if rail.result('get_employee_details') else "",
                "user_name": rail.result('get_employee_details').get('firstName', '') + ' ' + rail.result(
                    'get_employee_details').get('middleName', '') + ' ' + rail.result('get_employee_details').get('lastName', ''),
                "timelog": dag_run.conf['log_timestamp'],
                "integrationaction": "precheck",
                "status": "ignored",
                "details": "Inactive Bamboohr user - There no user profile in Replicon "
            }
        )

        process_add_user = rail.TriggerDagRunOperator(
            task_id='process_add_user',
            trigger_dag_id=config.process_add_user_dag_id,
            conf=lambda dag_run: request_payload.get_process_update_or_add_user_payload(rail.result(
                'get_employee_details'), rail.result('get_min_daydiff_table_entry')['min_daydiff_entry'], 'add', dag_run, config),
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        wait_for_process_add_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_add_user',
            dag_runs='{{ result("process_add_user") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_error = rail.IfOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            test=lambda: "specified target field is ambiguous" in json.dumps(
                rail.render_template("{{get_error_message()}}")),
            yes_task='log_miltiple_users_ambiguity',
            no_task='log_error'
        )

        log_miltiple_users_ambiguity = rail.WriteLogOperator(
            task_id='log_miltiple_users_ambiguity',
            log="{{result('create_user_log')}}",
            severity='skipped',
            message='na',
            properties=lambda dag_run: {
                "employeenumber": rail.result('get_employee_details').get('employeeNumber') if rail.result('get_employee_details') else "",
                "user_name": rail.result('get_employee_details').get('firstName', '') + ' ' + rail.result('get_employee_details').get('lastName', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "precheck",
                'status': 'ignored',
                'details': "Multiple users available in Replicon with the same employee id"
            }
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            log="{{result('create_user_log')}}",
            severity='error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "employeenumber": rail.result('get_employee_details').get('employeeNumber') if rail.result('get_employee_details') else "",
                "user_name": rail.result('get_employee_details').get('firstName', '') + ' ' + rail.result(
                    'get_employee_details').get('middleName', '') + ' ' + rail.result('get_employee_details').get('lastName', ''),
                'timelog': dag_run.conf['log_timestamp'],
                "integrationaction": "process user",
                'status': 'error',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> create_user_log

        create_user_log >> create_project_log >> create_reference_log >> get_employee_details >> get_table_record_of_employees \
            >> get_daydiff_effectivedate_for_table_records >> get_min_daydiff_table_entry >> if_work_email_employeeid_present

        if_work_email_employeeid_present >> rail.Label(
            'No') >> catch_error
        if_work_email_employeeid_present >> rail.Label(
            'Yes') >> validate_employee_data

        validate_employee_data >> is_record_invalid

        is_record_invalid >> rail.Label(
            'Yes') >> log_invalid_record_skipped >> catch_error
        is_record_invalid >> rail.Label(
            'No') >> create_md5_for_input_fields

        create_md5_for_input_fields >> query_id_in_reference_file_data >> is_record_present_with_same_id

        is_record_present_with_same_id >> rail.Label(
            'No') >> add_entry_add_user_reference_data_log >> search_location_in_holiday_timezone_mapper
        is_record_present_with_same_id >> rail.Label(
            'Yes') >> check_md5_value_not_equals_to_ref

        check_md5_value_not_equals_to_ref >> rail.Label(
            'No') >> add_entry_update_user_reference_data_log
        check_md5_value_not_equals_to_ref >> rail.Label(
            'Yes') >> catch_error

        add_entry_update_user_reference_data_log >> search_location_in_holiday_timezone_mapper

        search_location_in_holiday_timezone_mapper >> if_location_not_present_in_mapper

        if_location_not_present_in_mapper >> rail.Label(
            'No') >> get_user_data
        if_location_not_present_in_mapper >> rail.Label(
            'Yes') >> log_location_not_found >> catch_error

        get_user_data >> if_user_present

        if_user_present >> rail.Label(
            'No') >> dummy_trigger_add_user >> if_user_status_active
        if_user_present >> rail.Label(
            'Yes') >> get_current_payrule_for_user

        get_current_payrule_for_user >> get_user_details_artifact >> process_update_user >> wait_for_process_update_user >> catch_error

        if_user_status_active >> rail.Label(
            'No') >> log_inactive_user_in_bamboohr >> catch_error
        if_user_status_active >> rail.Label(
            'Yes') >> process_add_user

        process_add_user >> wait_for_process_add_user >> catch_error

        catch_error >> rail.Label(
            'No') >> log_error
        catch_error >> rail.Label(
            'Yes') >> log_miltiple_users_ambiguity

    return dag


rail.for_each_instance(create_dag)
