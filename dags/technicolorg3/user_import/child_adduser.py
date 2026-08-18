from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.user_import.task.process_mappers import process_mappers_task_group
from technicolorg3.user_import.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from technicolorg3.user_import.utils import python_callable_method
from technicolorg3.user_import.utils.request_payload import get_put_product_assignments_payload, get_put_user_payload


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


# pylint:disable=too-many-statements
def create_adduser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_adduser_{config.instance}',
        description=f'Technicolor_Child_Workflow to add user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_adduser_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
        )

        get_adduser_fieldlevel_exception = rail.PythonOperator(
            task_id='get_adduser_fieldlevel_exception',
            python_callable=python_callable_method.get_adduser_field_exception
        )

        is_adduser_exception = rail.IfOperator(
            task_id='is_adduser_exception',
            test="{{ result('get_adduser_fieldlevel_exception').error | is_truthy }}",
            yes_task='write_adduser_exception',
            no_task='get_mandatoryfield_exception'
        )

        write_adduser_exception = rail.WriteLogOperator(
            task_id='write_adduser_exception',
            log="{{ result('create_user_log') }}",
            severity='Exception',
            message="{{ result('get_adduser_fieldlevel_exception').error }}",
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Add',
                'status': 'Exception',
                'details': "{{ result('get_adduser_fieldlevel_exception').error }}",
                'username': '{{ dag_run.conf.username }}',
                'new_location': 'No',
                'location': "{{ result('get_adduser_fieldlevel_exception').location }}"
            }
        )

        get_mandatoryfield_exception = rail.PythonOperator(
            task_id='get_mandatoryfield_exception',
            python_callable=python_callable_method.get_mandatoryfield_skipped_log
        )

        is_mandatory_field_exception = rail.IfOperator(
            task_id='is_mandatory_field_exception',
            test="{{ result('get_mandatoryfield_exception') | is_truthy }}",
            yes_task='write_adduser_mandatory_exception',
            no_task='process_mappers'
        )

        write_adduser_mandatory_exception = rail.WriteLogOperator(
            task_id='write_adduser_mandatory_exception',
            log="{{ result('create_user_log') }}",
            severity='Skipped',
            message="{{ result('get_mandatoryfield_exception') }}",
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Add',
                'status': 'Skipped',
                'details': "{{ result('get_mandatoryfield_exception') }}",
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        process_mappers = rail.EmptyOperator(
            task_id='process_mappers'
        )

        (is_businessunitname_servicelinename,
         get_default_mapper_entries_from_country) = process_mappers_task_group(config.user_master_mapper)

        createuser_in_replicon = rail.RepliconServiceOperator(
            task_id='createuser_in_replicon',
            endpoint='/services/ImportService1.svc/PutUser3',
            data=get_put_user_payload
        )

        remove_user_timeoffs = rail.RepliconServiceOperator(
            task_id='remove_user_timeoffs',
            endpoint='/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser',
            data={
                'userUri': "{{ result('createuser_in_replicon').uri }}",
                'timeOffTypeUris': []
            }
        )

        update_udf_for_department = rail.RepliconServiceOperator(
            task_id='update_udf_for_department',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data={
                'objectUri': "{{ result('createuser_in_replicon').uri }}",
                'customFieldUri': '{{ dag_run.conf.departmentudf_uri }}',
                'customFieldDropDownOptionUri': '{{ dag_run.conf.departmentvalue_uri }}'
            }
        )

        put_product_assignments = rail.RepliconServiceOperator(
            task_id='put_product_assignments',
            endpoint='/services/AccountManagementService1.svc/PutProductAssignmentsForUser',
            data=get_put_product_assignments_payload
        )

        should_process_supervisor = rail.IfOperator(
            task_id='should_process_supervisor',
            test='{{ dag_run.conf.managerid | is_truthy }}',
            yes_task='process_supervisors',
            no_task='process_customfields_add'
        )

        process_supervisors = rail.EmptyOperator(
            task_id='process_supervisors'
        )

        (should_update_supervisor,
         finish_supervisor_assignment) = process_supervisor_assignment_task_group()

        process_customfields_add = rail.EmptyOperator(
            task_id='process_customfields_add'
        )

        get_customfields_to_add = rail.PythonOperator(
            task_id='get_customfields_to_add',
            python_callable=python_callable_method.get_customfields_to_adduser
        )

        is_customfield_numericvalues_to_add = rail.IfOperator(
            task_id='is_customfield_numericvalues_to_add',
            test="{{ result('get_customfields_to_add').numeric_udf_payloads | length > 0 }}",
            yes_task='add_usercustomfields_numericvalues',
            no_task='process_customfields_dropdowns'
        )

        add_usercustomfields_numericvalues = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_usercustomfields_numericvalues',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            items=lambda: rail.result('get_customfields_to_add')[
                'numeric_udf_payloads'],
            data=lambda item: item,
            flatten=True
        )

        process_customfields_dropdowns = rail.EmptyOperator(
            task_id='process_customfields_dropdowns')

        is_customfield_dropdowns_to_add = rail.IfOperator(
            task_id='is_customfield_dropdowns_to_add',
            test="{{ result('get_customfields_to_add').dropdownudf_payloads | length > 0 }}",
            yes_task='add_usercustomfields_dropdown',
            no_task='trigger_timeoff_assignment_newuser'
        )

        add_usercustomfields_dropdown = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_usercustomfields_dropdown',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            items=lambda: rail.result('get_customfields_to_add')[
                'dropdownudf_payloads'],
            data=lambda item: item,
            flatten=True
        )

        trigger_timeoff_assignment_newuser = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_assignment_newuser',
            retries=0,
            trigger_dag_id=f'technicolorg3_user_import_child_timeoff_assignment_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'useruri': rail.result('createuser_in_replicon')['uri'],
                'login_name': rail.result('createuser_in_replicon')['loginName'],
                'country': dag_run.conf['country'],
                'businessunitname': dag_run.conf['businessunitname'],
                'jobcategory': dag_run.conf['jobcategory'],
                'action': 'add'
            }
        )

        get_adduser_exception_logs = rail.PythonOperator(
            task_id='get_adduser_exception_logs',
            python_callable=python_callable_method.get_adduser_exception_logs,
            op_args=['should_update_supervisor', 'is_single_supervisor']
        )

        write_adduser_log = rail.WriteLogOperator(
            task_id='write_adduser_log',
            log="{{ result('create_user_log') }}",
            severity='\
                {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                    Exception\
                {%- else -%} \
                    Success\
                {%- endif -%}',
            message='\
                {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                    Partialy created - {{ result("get_adduser_exception_logs") }}\
                {%- else -%} \
                    Successfully created\
                {%- endif -%}',
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Add',
                'status': '\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_adduser_exception_logs") | is_truthy -%} \
                        Partialy created - {{ result("get_adduser_exception_logs") }}\
                    {%- else -%} \
                        Successfully created\
                    {%- endif -%}',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '\
                    {%- if result("get_mapper_entries_from_country_location") | length > 0 -%} \
                        No\
                    {%- else -%} \
                        Yes\
                    {%- endif -%}',
                'location': '{{ dag_run.conf.location }}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_user_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message="{{ get_error_message() }}",
            properties={
                'globalid': '{{ dag_run.conf.globalid }}',
                'action': 'Add',
                'status': 'Error',
                'details': "{{ get_error_message() }}",
                'username': '{{ dag_run.conf.username }}',
                'new_location': '\
                    {%- if result("get_mapper_entries_from_country_location") | length > 0 -%} \
                        No\
                    {%- else -%} \
                        Yes\
                    {%- endif -%}',
                'location': '{{ dag_run.conf.location }}'
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors >> log_dagrun_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> create_user_log

        create_user_log >> get_adduser_fieldlevel_exception >> is_adduser_exception

        is_adduser_exception >> rail.Label(
            'Yes') >> write_adduser_exception >> catch_and_log_errors

        is_adduser_exception >> rail.Label(
            'No') >> get_mandatoryfield_exception >> is_mandatory_field_exception

        is_mandatory_field_exception >> rail.Label(
            'Yes') >> write_adduser_mandatory_exception >> catch_and_log_errors

        is_mandatory_field_exception >> rail.Label(
            'No') >> process_mappers >> is_businessunitname_servicelinename

        get_default_mapper_entries_from_country >> createuser_in_replicon >> \
            remove_user_timeoffs >> update_udf_for_department >> put_product_assignments >> should_process_supervisor

        should_process_supervisor >> rail.Label(
            'Yes') >> process_supervisors >> should_update_supervisor

        finish_supervisor_assignment >> process_customfields_add

        should_process_supervisor >> rail.Label(
            'No') >> process_customfields_add

        process_customfields_add >> get_customfields_to_add >> is_customfield_numericvalues_to_add

        is_customfield_numericvalues_to_add >> rail.Label(
            'Yes') >> add_usercustomfields_numericvalues >> process_customfields_dropdowns

        is_customfield_numericvalues_to_add >> rail.Label(
            'No') >> process_customfields_dropdowns

        process_customfields_dropdowns >> is_customfield_dropdowns_to_add

        is_customfield_dropdowns_to_add >> rail.Label(
            'Yes') >> add_usercustomfields_dropdown >> trigger_timeoff_assignment_newuser

        is_customfield_dropdowns_to_add >> rail.Label(
            'No') >> trigger_timeoff_assignment_newuser

        trigger_timeoff_assignment_newuser >> get_adduser_exception_logs >> write_adduser_log >> \
            catch_and_log_errors

        catch_and_log_errors >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_adduser_child_dag)
