from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils.request_payload import validate_email, get_search_user_param, get_adduser_updateuser_conf
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils.response_filter import map_user_details
from dxctechnology.cwf_user_profile_v1.user_profile_sync.task.process_updateuser import process_updateuser_task_group


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile_v1/user_profile_sync/config.py


def create_process_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.cwf_process_userprofiles_dagid,
        description=f'DXC_Fieldglass CWFUserProfiles_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_user_profile_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='catch_and_log_errors',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        should_skip_user = rail.IfOperator(
            task_id='should_skip_user',
            test="{{ dag_run.conf.skip_user == 'yes' }}",
            yes_task='log_user_skipped',
            no_task='process_emailaddress_validation'
        )

        log_user_skipped = rail.WriteLogOperator(
            task_id='log_user_skipped',
            log="{{ result('create_log') }}",
            message='workertype and financesystem is skipped',
            severity='Skipped',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Validation',
                'status': 'Skipped',
                'details': '{{ dag_run.conf.skipped_message }}'
            }
        )

        process_emailaddress_validation = rail.EmptyOperator(
            task_id='process_emailaddress_validation'
        )

        is_emailaddress_not_valid = rail.IfOperator(
            task_id='is_emailaddress_not_valid',
            test=lambda dag_run: bool(
                validate_email(dag_run.conf['emailaddress'])),
            yes_task='log_emailaddress_not_valid',
            no_task='get_user_data_from_employeeid'
        )

        log_emailaddress_not_valid = rail.WriteLogOperator(
            task_id='log_emailaddress_not_valid',
            log="{{ result('create_log') }}",
            message='email address is invalid',
            severity='Exception',
            properties=lambda dag_run: {
                'userid': dag_run.conf['hpid'],
                'email': dag_run.conf['emailaddress'],
                'action': 'Validation',
                'status': 'Exception',
                'details': validate_email(dag_run.conf['emailaddress'])
            }
        )

        get_user_data_from_employeeid = rail.RepliconServiceOperator(
            task_id='get_user_data_from_employeeid',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_search_user_param,
            data_handler=map_user_details
        )

        is_employeeid_present = rail.IfOperator(
            task_id='is_employeeid_present',
            test="{{ result('get_user_data_from_employeeid') | filter_by_attr('employeeid', 'equals', dag_run.conf.hpid) | \
                length > 0 }}",
            yes_task='process_child_update_user',
            no_task='get_user_data_from_loginname'
        )

        process_child_update_user = rail.EmptyOperator(
            task_id='process_child_update_user'
        )

        is_employeeid_not_unique = rail.IfOperator(
            task_id='is_employeeid_not_unique',
            test="{{ result('get_user_data_from_employeeid') | filter_by_attr('employeeid', 'equals', dag_run.conf.hpid) | \
                length > 1 }}",
            yes_task='log_multiple_employeeid_exception',
            no_task='update_userprofile_child_dag_emp_id'
        )

        log_multiple_employeeid_exception = rail.WriteLogOperator(
            task_id='log_multiple_employeeid_exception',
            log="{{ result('create_log') }}",
            message='Multiple users available with employee id',
            severity='Exception',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': 'Multiple users available with employee id: "{{ dag_run.conf.hpid }}"'
            }
        )

        update_user_employeeid, wait_for_update_user_employeeid = process_updateuser_task_group(
            config.execution_timeout_days, config.cwf_update_userprofiles_dagid, 'emp_id')

        get_user_data_from_loginname = rail.RepliconServiceOperator(
            task_id='get_user_data_from_loginname',
            endpoint='/services/UserListService1.svc/GetData',
            data=get_search_user_param,
            data_handler=map_user_details
        )

        is_user_with_loginname_present = rail.IfOperator(
            task_id='is_user_with_loginname_present',
            test="{{ result('get_user_data_from_loginname') | find_first_by_attr_and_get_attr('loginname', dag_run.conf.emailaddress, 'uri') | \
                is_truthy }}",
            yes_task='update_userprofile_child_dag_login_name',
            no_task='process_child_add_user'
        )

        update_user_loginname, wait_for_update_user_loginname = process_updateuser_task_group(
            config.execution_timeout_days, config.cwf_update_userprofiles_dagid, 'login_name')

        process_child_add_user = rail.EmptyOperator(
            task_id='process_child_add_user'
        )

        is_financesystemuri_workertypeuri_present = rail.IfOperator(
            task_id='is_financesystemuri_workertypeuri_present',
            test=lambda dag_run: bool(dag_run.conf['finance_system_value_uri']) and bool(
                dag_run.conf['worker_type_uri']),
            yes_task='validate_contract_dates',
            no_task='log_financesystem_workertype_not_available'
        )

        validate_contract_dates = rail.EmptyOperator(
            task_id='validate_contract_dates'
        )

        validate_contract_startdate_enddate = rail.IfOperator(
            task_id='validate_contract_startdate_enddate',
            test=lambda dag_run: datetime.fromisoformat(
                dag_run.conf['contractenddate']) < datetime.fromisoformat(
                    dag_run.conf['contractstartdate']) if dag_run.conf['contractenddate'] else False,
            yes_task='log_date_exception',
            no_task='add_userprofile_child_dag'
        )

        log_date_exception = rail.WriteLogOperator(
            task_id='log_date_exception',
            log="{{ result('create_log') }}",
            message='End date is before the contract start date',
            severity='Skipped',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Add',
                'status': 'Exception',
                'details': 'End date is before the contract start date'
            }
        )

        add_userprofile_child_dag = rail.TriggerDagRunForEachItemOperator(
            task_id='add_userprofile_child_dag',
            retries=0,
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.cwf_add_userprofiles_dagid,
            conf=get_adduser_updateuser_conf
        )

        wait_for_add_userprofile_child_dag = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_userprofile_child_dag',
            dag_runs='{{ result("add_userprofile_child_dag") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        def get_skipped_message(dag_run):
            workertype_exception = f"{dag_run.conf['financesystem']} not available as a Finance System (CWF) drop down option in Replicon" if dag_run.conf[
                'finance_system_value_uri'] else ""
            financesystem_exception = f"{dag_run.conf['workertype']} not available as a Worker Type drop down option in Replicon" if dag_run.conf[
                'worker_type_uri'] == 0 else ""
            return ','.join([workertype_exception, financesystem_exception])

        log_financesystem_workertype_not_available = rail.WriteLogOperator(
            task_id='log_financesystem_workertype_not_available',
            log="{{ result('create_log') }}",
            message='financesystem_or_workertype_not_available',
            severity='Exception',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Validation',
                'status': 'Exception',
                'details': get_skipped_message
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_log') }}",
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Validation',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> create_log

        create_log >> should_skip_user >> rail.Label(
            'Yes') >> log_user_skipped >> catch_and_log_errors

        should_skip_user >> rail.Label(
            'No') >> process_emailaddress_validation >> is_emailaddress_not_valid

        is_emailaddress_not_valid >> rail.Label(
            'Yes') >> log_emailaddress_not_valid >> catch_and_log_errors

        is_emailaddress_not_valid >> rail.Label(
            'No') >> get_user_data_from_employeeid >> is_employeeid_present

        is_employeeid_present >> rail.Label(
            'Yes') >> process_child_update_user >> is_employeeid_not_unique

        is_employeeid_not_unique >> rail.Label(
            'Yes') >> log_multiple_employeeid_exception >> catch_and_log_errors

        is_employeeid_not_unique >> rail.Label(
            'No') >> update_user_employeeid

        update_user_employeeid >> wait_for_update_user_employeeid >> catch_and_log_errors

        is_employeeid_present >> rail.Label(
            'No') >> get_user_data_from_loginname >> is_user_with_loginname_present

        is_user_with_loginname_present >> rail.Label(
            'Yes') >> update_user_loginname

        is_user_with_loginname_present >> rail.Label(
            'No') >> process_child_add_user >> is_financesystemuri_workertypeuri_present

        update_user_loginname >> wait_for_update_user_loginname >> catch_and_log_errors

        is_financesystemuri_workertypeuri_present >> rail.Label(
            'Yes') >> validate_contract_dates >> validate_contract_startdate_enddate

        validate_contract_startdate_enddate >> rail.Label(
            'Yes') >> log_date_exception >> catch_and_log_errors

        validate_contract_startdate_enddate >> rail.Label(
            'No') >> add_userprofile_child_dag >> wait_for_add_userprofile_child_dag >> catch_and_log_errors

        is_financesystemuri_workertypeuri_present >> rail.Label(
            'No') >> log_financesystem_workertype_not_available >> catch_and_log_errors

        return dag


rail.for_each_instance(create_process_userprofile_child_dag)
