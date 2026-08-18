from datetime import datetime, timedelta, timezone
import rail
from airflow.models import Variable
from dxctechnology.cwf_user_profile_v1.user_profile_sync.task.process_supervisor_assignment import process_supervisor_assignment_task_group
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils import python_callable_method
from dxctechnology.cwf_user_profile_v1.user_profile_sync.utils import request_payload

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/cwf_user_profile_v1/user_profile_sync/config.py


# pylint:disable = too-many-statements
def create_update_userprofile_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.cwf_update_userprofiles_dagid,
        description=f'DXC_FieldglassCWF_Child_Update User {config.instance}',
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
            no_task='get_effective_group_date'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_effective_group_date',
            end_task='catch_and_log_errors',
        )

        get_effective_group_date = rail.PythonOperator(
            task_id='get_effective_group_date',
            python_callable=python_callable_method.get_effective_group_date_payload,
            op_args=['{{ dag_run.conf.financesystem }}']
        )

        bulk_get_user = rail.RepliconServiceOperator(
            task_id='bulk_get_user',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                'users': [
                    {
                        'uri': '{{ dag_run.conf.user_uri }}'
                    }
                ],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission'
            },
            data_handler=lambda response: response[0] if response else None
        )

        is_user_disabled = rail.IfOperator(
            task_id='is_user_disabled',
            test=lambda dag_run: not rail.result('bulk_get_user')['userDetails']['isEnabled'] and dag_run.conf[
                'timetracking'] == 'no',
            yes_task='log_user_already_disabled_exception',
            no_task='disable_for_sow_contractor'
        )

        log_user_already_disabled_exception = rail.WriteLogOperator(
            task_id='log_user_already_disabled_exception',
            log='{{ dag_run.conf.log }}',
            message='User already disabled in Replicon',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Update',
                'status': 'Skipped',
                'details': 'User already disabled in Replicon'
            }
        )

        disable_for_sow_contractor = rail.IfOperator(
            task_id='disable_for_sow_contractor',
            test=lambda dag_run: rail.result('bulk_get_user')['userDetails']['isEnabled'] and dag_run.conf[
                'timetracking'] == 'no' and dag_run.conf['workertype'] != 'Agency Contractor',
            yes_task='validate_contract_enddate_with_user_startdate',
            no_task='enable_user_for_rehire'
        )

        validate_contract_enddate_with_user_startdate = rail.IfOperator(
            task_id='validate_contract_enddate_with_user_startdate',
            test=lambda dag_run: not request_payload.is_enddate_less_than_startdate(rail.result(
                'bulk_get_user')['userDetails']['employmentDateRange']['startDate'], dag_run.conf['contractenddate'], '%Y-%m-%d%z'),
            yes_task='disable_user_login',
            no_task='enable_user_for_rehire'
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id='disable_user_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        is_contract_end_date_present = rail.IfOperator(
            task_id='is_contract_end_date_present',
            test="{{ dag_run.conf.contractenddate | is_truthy }}",
            yes_task='update_employee_date_range_sow_contractor',
            no_task='log_user_disabled_in_replicon'
        )

        update_employee_date_range_sow_contractor = rail.RepliconServiceOperator(
            task_id='update_employee_date_range_sow_contractor',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: request_payload.get_update_employee_date_range_payload(
                dag_run.conf['user_uri'], dag_run.conf['contractenddate'])
        )

        log_user_disabled_in_replicon = rail.WriteLogOperator(
            task_id='log_user_disabled_in_replicon',
            log='{{ dag_run.conf.log }}',
            message='User disabled in Replicon',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'User disabled in Replicon'
            }
        )

        enable_user_for_rehire = rail.IfOperator(
            task_id='enable_user_for_rehire',
            test=lambda dag_run: not rail.result('bulk_get_user')['userDetails']['isEnabled'] and dag_run.conf[
                'timetracking'] == 'yes',
            yes_task='validate_contract_enddate_with_contract_startdate',
            no_task='enddate_update'
        )

        validate_contract_enddate_with_contract_startdate = rail.IfOperator(
            task_id='validate_contract_enddate_with_contract_startdate',
            test=lambda dag_run: not request_payload.is_enddate_less_than_startdate(
                dag_run.conf['contractstartdate'], dag_run.conf['contractenddate']),
            yes_task='enable_user_login',
            no_task='enddate_update'
        )

        enable_user_login = rail.RepliconServiceOperator(
            task_id='enable_user_login',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        update_employee_date_range_rehire = rail.RepliconServiceOperator(
            task_id='update_employee_date_range_rehire',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: request_payload.get_update_employee_date_range_payload(
                dag_run.conf['user_uri'], dag_run.conf['contractenddate'], dag_run.conf['contractstartdate'])
        )

        def validate_enddate_update(end_date):
            if end_date:
                user_start_date = rail.result('bulk_get_user')[
                    'userDetails']['employmentDateRange']['startDate']
                user_end_date = rail.result('bulk_get_user')[
                    'userDetails']['employmentDateRange']['endDate']
                user_enabled = rail.result('bulk_get_user')[
                    'userDetails']['isEnabled']
                user_end_datetime = datetime.strptime(
                    f"{user_end_date['year']}-{user_end_date['month']}-{user_end_date['day']}+0000", '%Y-%m-%d%z') if user_end_date else 'No Date'
                validate_date = request_payload.is_enddate_less_than_startdate(
                    user_start_date, end_date, '%Y-%m-%d%z')
                return datetime.fromisoformat(end_date) != user_end_datetime and user_enabled and not validate_date
            return False

        enddate_update = rail.IfOperator(
            task_id='enddate_update',
            test=lambda dag_run: validate_enddate_update(
                dag_run.conf['contractenddate']),
            yes_task='is_contract_enddate_greater_than_user_startdate_enddate_update',
            no_task='should_disable_user'
        )

        is_contract_enddate_greater_than_user_startdate_enddate_update = rail.IfOperator(
            task_id='is_contract_enddate_greater_than_user_startdate_enddate_update',
            test=lambda dag_run: not request_payload.is_enddate_less_than_startdate(rail.result(
                'bulk_get_user')['userDetails']['employmentDateRange']['startDate'], dag_run.conf['contractenddate'], '%Y-%m-%d%z'),
            yes_task='update_user_enddate',
            no_task='should_disable_user'
        )

        update_user_enddate = rail.RepliconServiceOperator(
            task_id='update_user_enddate',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: request_payload.get_update_employee_date_range_payload(
                dag_run.conf['user_uri'], dag_run.conf['contractenddate'])
        )

        should_disable_user = rail.IfOperator(
            task_id='should_disable_user',
            test=lambda dag_run: rail.result('bulk_get_user')[
                    'userDetails']['isEnabled'] and dag_run.conf[
                        'timetracking'] == 'no' and dag_run.conf[
                            'contractenddate'],
            yes_task='is_contract_enddate_greater_than_user_startdate_disable_user',
            no_task='get_effective_group_membership'
        )

        is_contract_enddate_greater_than_user_startdate_disable_user = rail.IfOperator(
            task_id='is_contract_enddate_greater_than_user_startdate_disable_user',
            test=lambda dag_run: not request_payload.is_enddate_less_than_startdate(rail.result(
                'bulk_get_user')['userDetails']['employmentDateRange']['startDate'], dag_run.conf['contractenddate'], '%Y-%m-%d%z'),
            yes_task='update_employee_date_range_disabled_user',
            no_task='get_effective_group_membership'
        )

        update_employee_date_range_disabled_user = rail.RepliconServiceOperator(
            task_id='update_employee_date_range_disabled_user',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: request_payload.get_update_employee_date_range_payload(
                dag_run.conf['user_uri'], dag_run.conf['contractenddate'])
        )

        should_disable_login = rail.IfOperator(
            task_id='should_disable_login',
            test=lambda dag_run: datetime.fromisoformat(
                dag_run.conf['contractenddate']) < datetime.now(timezone.utc),
            yes_task='disable_login',
            no_task='get_effective_group_membership'
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        log_user_disabled = rail.WriteLogOperator(
            task_id='log_user_disabled',
            log='{{ dag_run.conf.log }}',
            message='User disabled in Replicon',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Update',
                'status': 'Success',
                'details': 'User disabled in Replicon'
            }
        )

        get_effective_group_membership = rail.RepliconServiceOperator(
            task_id='get_effective_group_membership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        apply_user_modifications2 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: request_payload.apply_user_modifications2(
                dag_run, config.should_add_emailaddress)
        )

        should_update_supervisor = rail.IfOperator(
            task_id='should_update_supervisor',
            test=lambda dag_run: not (dag_run.conf['managerid'] == dag_run.conf['hpid'] or dag_run.conf[
                    'manageremail'] == dag_run.conf['emailaddress']),
            yes_task='get_data_for_supervisor',
            no_task='get_update_activities_payload'
        )

        (update_supervisor_over_date_range, log_supervisor_check, get_update_activities_payload) = process_supervisor_assignment_task_group(
            config.execution_timeout_days, should_update_supervisor, is_update_user=True)

        should_update_activities = rail.IfOperator(
            task_id='should_update_activities',
            test="{{ result('get_update_activities_payload') | length > 0 }}",
            yes_task='update_activities',
            no_task='should_update_employeetype'
        )

        update_activities = rail.RepliconServiceOperator(
            task_id='update_activities',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {
                    'uri': dag_run.conf['user_uri']
                },
                'modifications': {
                    'activitiesToApply': rail.result('get_update_activities_payload')
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        should_update_employeetype = rail.IfOperator(
            task_id='should_update_employeetype',
            test=lambda dag_run: request_payload.should_update_employeetype(
                dag_run.conf['employee_type_uri'], rail.result('get_effective_group_membership')['employeeTypes']),
            yes_task='update_employeetype_group',
            no_task='should_update_timesheet_template'
        )

        update_employeetype_group = rail.RepliconServiceOperator(
            task_id='update_employeetype_group',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.update_employeetype
        )

        is_timeentry_approvalpath_exist = rail.IfOperator(
            task_id='is_timeentry_approvalpath_exist',
            test='{{ dag_run.conf.timeentry_approval_path | sn | is_truthy }}',
            yes_task='get_user_timeentry_approval_path',
            no_task='should_update_timesheet_template'
        )

        get_user_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='get_user_timeentry_approval_path',
            endpoint='/services/TimeEntryRevisionGroupApprovalService1.svc/GetApprovalPathForUser',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}'
            }
        )

        should_update_timeentry_approvalpath = rail.IfOperator(
            task_id='should_update_timeentry_approvalpath',
            test=lambda dag_run: not rail.result('get_user_timeentry_approval_path') or (
                rail.result('get_user_timeentry_approval_path') and rail.result(
                    'get_user_timeentry_approval_path')['displayText'] != dag_run.conf['timeentry_approval_path']),
            yes_task='update_timeentry_approval_path',
            no_task='should_update_timesheet_template'
        )

        update_timeentry_approval_path = rail.RepliconServiceOperator(
            task_id='update_timeentry_approval_path',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data={
                'user': {
                    'uri': '{{ dag_run.conf.user_uri }}'
                },
                'modifications': {
                    'timeEntryRevisionGroupApprovalPathToApply': {
                        'name': '{{ dag_run.conf.timeentry_approval_path }}'
                    }
                },
                'userModificationOptionUri': 'urn:replicon:user-modification-option:save'
            }
        )

        should_update_timesheet_template = rail.IfOperator(
            task_id='should_update_timesheet_template',
            test=request_payload.should_update_timesheet_template,
            yes_task='update_timesheet_template',
            no_task='should_update_workweek_start_day'
        )

        update_timesheet_template = rail.RepliconServiceOperator(
            task_id='update_timesheet_template',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}',
                'policySetUri': '{{ dag_run.conf.timesheet_uri }}'
            }
        )

        should_update_workweek_start_day = rail.IfOperator(
            task_id='should_update_workweek_start_day',
            test=request_payload.should_update_workweek_start_day,
            yes_task='update_workweek_start_day',
            no_task='get_exception_logs'
        )

        update_workweek_start_day = rail.RepliconServiceOperator(
            task_id='update_workweek_start_day',
            endpoint='/services/UserService1.svc/UpdateWorkWeekStartDayForUser',
            data={
                'userUri': '{{ dag_run.conf.user_uri }}',
                'dayOfWeekUri': '{{ dag_run.conf.work_week_uri }}'
            }
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=python_callable_method.get_exception_log_message,
            op_args=['validate_contract_enddate_with_user_startdate', 'is_contract_enddate_greater_than_user_startdate_disable_user',
                     'is_contract_enddate_greater_than_user_startdate_enddate_update', 'should_update_supervisor']
        )

        log_update_user = rail.WriteLogOperator(
            task_id='log_update_user',
            log='{{ dag_run.conf.log }}',
            message='\
                {%- if result("get_exception_logs") | is_truthy -%} \
                    Partialy updated - {{ result("get_exception_logs") }}\
                {%- else -%} \
                    Updated successfully\
                {%- endif -%}',
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Update',
                'status': '\
                    {%- if result("get_exception_logs") | is_truthy -%} \
                        Exception\
                    {%- else -%} \
                        Success\
                    {%- endif -%}',
                'details': '\
                    {%- if result("get_exception_logs") | is_truthy -%} \
                        Partialy updated - {{ result("get_exception_logs") }}\
                    {%- else -%} \
                        Updated successfully\
                    {%- endif -%}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            properties={
                'userid': '{{ dag_run.conf.hpid }}',
                'email': '{{ dag_run.conf.emailaddress }}',
                'action': 'Update',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> get_effective_group_date

        get_effective_group_date >> bulk_get_user >> is_user_disabled

        is_user_disabled >> rail.Label(
            'Yes') >> log_user_already_disabled_exception >> catch_and_log_errors

        is_user_disabled >> rail.Label(
            'No') >> disable_for_sow_contractor

        disable_for_sow_contractor >> rail.Label(
            'Yes') >> validate_contract_enddate_with_user_startdate

        validate_contract_enddate_with_user_startdate >> rail.Label(
            'Yes') >> disable_user_login >> is_contract_end_date_present

        is_contract_end_date_present >> rail.Label(
            'Yes') >> update_employee_date_range_sow_contractor >> log_user_disabled_in_replicon >> catch_and_log_errors

        is_contract_end_date_present >> rail.Label(
            'No') >> log_user_disabled_in_replicon >> catch_and_log_errors

        validate_contract_enddate_with_user_startdate >> rail.Label(
            'No') >> enable_user_for_rehire

        disable_for_sow_contractor >> rail.Label(
            'No') >> enable_user_for_rehire

        enable_user_for_rehire >> rail.Label(
            'Yes') >> validate_contract_enddate_with_contract_startdate

        validate_contract_enddate_with_contract_startdate >> rail.Label(
            'Yes') >> enable_user_login >> update_employee_date_range_rehire >> enddate_update

        validate_contract_enddate_with_contract_startdate >> rail.Label(
            'No') >> enddate_update

        enable_user_for_rehire >> rail.Label(
            'No') >> enddate_update

        enddate_update >> rail.Label(
            'Yes') >> is_contract_enddate_greater_than_user_startdate_enddate_update

        is_contract_enddate_greater_than_user_startdate_enddate_update >> rail.Label(
            'Yes') >> update_user_enddate >> should_disable_user

        is_contract_enddate_greater_than_user_startdate_enddate_update >> rail.Label(
            'No') >> should_disable_user

        enddate_update >> rail.Label(
            'No') >> should_disable_user

        should_disable_user >> rail.Label(
            'Yes') >> is_contract_enddate_greater_than_user_startdate_disable_user

        is_contract_enddate_greater_than_user_startdate_disable_user >> rail.Label(
            'Yes') >> update_employee_date_range_disabled_user >> should_disable_login

        should_disable_login >> rail.Label(
            'Yes') >> disable_login >> log_user_disabled >> catch_and_log_errors

        should_disable_login >> rail.Label(
            'No') >> get_effective_group_membership

        is_contract_enddate_greater_than_user_startdate_disable_user >> rail.Label(
            'No') >> get_effective_group_membership

        should_disable_user >> rail.Label(
            'No') >> get_effective_group_membership

        get_effective_group_membership >> apply_user_modifications2 >> should_update_supervisor

        should_update_supervisor >> rail.Label(
            'No') >> get_update_activities_payload

        update_supervisor_over_date_range >> get_update_activities_payload

        log_supervisor_check >> get_update_activities_payload

        get_update_activities_payload >> should_update_activities

        should_update_activities >> rail.Label(
            'Yes') >> update_activities >> should_update_employeetype

        should_update_activities >> rail.Label(
            'No') >> should_update_employeetype

        should_update_employeetype >> rail.Label(
            'Yes') >> update_employeetype_group >> is_timeentry_approvalpath_exist

        is_timeentry_approvalpath_exist >> rail.Label(
            'Yes') >> get_user_timeentry_approval_path >> should_update_timeentry_approvalpath

        should_update_timeentry_approvalpath >> rail.Label(
            'Yes') >> update_timeentry_approval_path >> should_update_timesheet_template

        should_update_timeentry_approvalpath >> rail.Label(
            'No') >> should_update_timesheet_template

        is_timeentry_approvalpath_exist >> rail.Label(
            'No') >> should_update_timesheet_template

        should_update_employeetype >> rail.Label(
            'No') >> should_update_timesheet_template

        should_update_timesheet_template >> rail.Label(
            'Yes') >> update_timesheet_template >> should_update_workweek_start_day

        should_update_timesheet_template >> rail.Label(
            'No') >> should_update_workweek_start_day

        should_update_workweek_start_day >> rail.Label(
            'Yes') >> update_workweek_start_day >> get_exception_logs

        should_update_workweek_start_day >> rail.Label(
            'No') >> get_exception_logs

        get_exception_logs >> log_update_user >> catch_and_log_errors >> dagrun_log_to_sumo

        return dag


rail.for_each_instance(create_update_userprofile_child_dag)
