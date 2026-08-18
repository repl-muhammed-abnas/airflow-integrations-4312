from datetime import timedelta
import rail
from airflow.models import Variable
from sweethometherapyllc.time_entry_import.utils import custom_methods, request_payload, response_filters

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_each_entry_child,
        description=f'sweethometherapyllc Time Import Child - Process Each Entry {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_activity_assigned_to_user'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_activity_assigned_to_user',
            end_task='catch_and_log_errors',
        )

        get_activity_assigned_to_user= rail.PythonOperator(
            task_id='get_activity_assigned_to_user',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                dag_run.conf['activities'], "displayText", dag_run.conf['school'],"uri"),
        )

        if_activity_assigned_to_user = rail.IfOperator(
            task_id ='if_activity_assigned_to_user',
            test=lambda dag_run: rail.result('get_activity_assigned_to_user'),
            yes_task="get_tag_details_for_service_name_oef",
            no_task="log_activity_not_assigned_to_user"
        )

        log_activity_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_activity_not_assigned_to_user',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Activity {{ dag_run.conf.school }} is not assigned to user',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Activity {{ dag_run.conf.school }} is not assigned to user'
            },
        )

        get_tag_details_for_service_name_oef = rail.PythonOperator(
            task_id='get_tag_details_for_service_name_oef',
            python_callable=lambda dag_run: response_filters.find_tag_uri_by_name(rail.find_first_by_attr_and_get_attr(
            dag_run.conf['tags_for_dropdown_oef'], 'name','Service Name','tags'), dag_run.conf['service_name'],dag_run.conf['object_extension_fields'],"Service Name"),
        )

        if_service_name_oef_assigned_to_user = rail.IfOperator(
            task_id ='if_service_name_oef_assigned_to_user',
            test=lambda: rail.result('get_tag_details_for_service_name_oef').get('oef_value_uri'),
            yes_task="get_tag_details_for_type_billing_oef",
            no_task="log_service_name_not_assigned_to_user"
        )

        log_service_name_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_service_name_not_assigned_to_user',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Service Name {{ dag_run.conf.service_name }} is not assigned to user',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Service Name {{ dag_run.conf.service_name }} is not assigned to user'
            },
        )

        get_tag_details_for_type_billing_oef = rail.PythonOperator(
            task_id='get_tag_details_for_type_billing_oef',
            python_callable=lambda dag_run: response_filters.find_tag_uri_by_name(rail.find_first_by_attr_and_get_attr(
                dag_run.conf['tags_for_dropdown_oef'], 'name','Type Billing','tags'), dag_run.conf['type1'],dag_run.conf['object_extension_fields'],"Type Billing"),
        )

        if_type_billing_oef_assigned_to_user = rail.IfOperator(
            task_id ='if_type_billing_oef_assigned_to_user',
            test=lambda: rail.result('get_tag_details_for_type_billing_oef').get('oef_value_uri'),
            yes_task="add_time_entry",
            no_task="log_type_billing_not_assigned_to_user"
        )

        log_type_billing_not_assigned_to_user = rail.WriteLogOperator(
            task_id='log_type_billing_not_assigned_to_user',
            log='{{ dag_run.conf.log }}',
            severity='Exception',
            message='Type Billing {{ dag_run.conf.type1 }} is not assigned to user',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Exception',
                'action': 'Validation',
                'details': 'Type Billing {{ dag_run.conf.type1 }} is not assigned to user'
            },
        )

        add_time_entry = rail.RepliconServiceOperator(
            task_id="add_time_entry",
            endpoint="/services/TimeEntryRevisionGroupService1.svc/PutTimeEntryRevisionGroup",
            data=lambda dag_run: request_payload.put_time_entry_payload(dag_run, config.entry_dateformat),
            retries=0
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            log='{{ dag_run.conf.log }}',
            severity="Success",
            message='{{ get_error_message() }}',
            properties=lambda dag_run:{
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': "Success",
                'action': "Validation",
                'details': 'Time entry added successfully'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{dag_run.conf.log}}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'entry_keyid': dag_run.conf.get('entry_keyid', ''),
                'school': dag_run.conf.get('school', ''),
                'service_name': dag_run.conf.get('service_name', ''),
                'therapist': dag_run.conf.get('therapist', ''),
                'hours': dag_run.conf.get('hours', ''),
                'date_of_service': dag_run.conf.get('date_of_service', ''),
                'status': 'Error',
                'action': 'Validation',
                'details': '{{ get_error_message() }}'
            },
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_activity_assigned_to_user >> if_activity_assigned_to_user >> rail.Label('No') >>\
        log_activity_not_assigned_to_user >> catch_and_log_errors
        get_activity_assigned_to_user >> if_activity_assigned_to_user >> rail.Label('Yes') >> get_tag_details_for_service_name_oef >> if_service_name_oef_assigned_to_user >> rail.Label('No') >> log_service_name_not_assigned_to_user >> catch_and_log_errors
        if_service_name_oef_assigned_to_user >> rail.Label('Yes') >> get_tag_details_for_type_billing_oef >> if_type_billing_oef_assigned_to_user >> rail.Label('No') >> log_type_billing_not_assigned_to_user >> catch_and_log_errors
        if_type_billing_oef_assigned_to_user >> rail.Label('Yes') >> add_time_entry >> log_success
        log_success >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)
