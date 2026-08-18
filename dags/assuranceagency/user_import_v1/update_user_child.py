# pylint: disable=too-many-statements
from datetime import timedelta
import rail
from assuranceagency.user_import_v1.utils.python_callable import get_today, get_current_data
from assuranceagency.user_import_v1.utils import python_callable
from assuranceagency.user_import_v1.utils import request_payload
from airflow.models import Variable


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dag_id,
        description=f'assuranceagency_user_import_update_user_child_{config.instance}_v1',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_update_user_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                 config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='exception_log'
            )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='exception_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            )

        exception_log = rail.CreateLogOperator(
            task_id = "exception_log"
        )

        get_user_data = rail.RepliconServiceOperator(
            task_id = "get_user_data",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_bulk_user_data
        )

        check_rehire = rail.IfOperator(
            task_id="check_rehire",
            test="{{ result('get_user_data')[0].userDetails.isEnabled | lower() != 'true' and dag_run.conf.enabled.lower() == 'yes' }}",
            yes_task="update_user_data_to_old",
            no_task="if_loginname_mismatch"
        )

        update_user_data_to_old = rail.RepliconServiceOperator(
            task_id = "update_user_data_to_old",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.update_userdata_to_old
        )

        trigger_add_user = rail.TriggerDagRunOperator(
            task_id='trigger_add_user',
            trigger_dag_id = config.add_user_child_dag_id,
            wait_for_completion=True,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.add_user_from_update
        )

        if_loginname_mismatch = rail.IfOperator(
            task_id="if_loginname_mismatch",
            test="{{ result('get_user_data')[0].securityConfiguration.loginName.lower() != dag_run.conf.loginname.lower() }}",
            yes_task="update_loginname",
            no_task="if_firstname_mismatch"
        )

        update_loginname = rail.RepliconServiceOperator(
            task_id = "update_loginname",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.update_user_loginname
        )

        if_firstname_mismatch = rail.IfOperator(
            task_id="if_firstname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.firstName | is_falsy or \
                result('get_user_data')[0].userDetails.firstName.lower() != dag_run.conf.firstname.lower()) and \
                dag_run.conf.firstname | is_truthy }}",
            yes_task="update_firstname",
            no_task="if_lastname_mismatch"
        )

        update_firstname = rail.RepliconServiceOperator(
            task_id = "update_firstname",
            endpoint = "/services/UserService1.svc/UpdateFirstName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "firstname" : "{{ dag_run.conf.firstname }}"
            }
        )

        if_lastname_mismatch = rail.IfOperator(
            task_id="if_lastname_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.lastName | is_falsy or \
                result('get_user_data')[0].userDetails.lastName.lower() != dag_run.conf.lastname.lower()) and \
                dag_run.conf.lastname | is_truthy }}",
            yes_task="update_lastname",
            no_task="if_email_mismatch"
        )

        update_lastname = rail.RepliconServiceOperator(
            task_id = "update_lastname",
            endpoint = "/services/UserService1.svc/UpdateLastName",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "lastname" : "{{ dag_run.conf.lastname }}"
            }
        )

        if_email_mismatch = rail.IfOperator(
            task_id="if_email_mismatch",
            test="{{ (result('get_user_data')[0].userDetails.emailAddress | is_falsy or \
                result('get_user_data')[0].userDetails.emailAddress.lower() != dag_run.conf.emailaddress.lower()) and \
                dag_run.conf.emailaddress | is_truthy }}",
            yes_task="update_email_address",
            no_task="get_workday_id_from_user_data"
        )

        update_email_address = rail.RepliconServiceOperator(
            task_id = "update_email_address",
            endpoint = "/services/UserService1.svc/UpdateEmail",
            data ={
                "userUri" : "{{ dag_run.conf.useruri }}",
                "email" : "{{ dag_run.conf.emailaddress }}"
            }
        )

        get_workday_id_from_user_data = rail.PythonOperator(
            task_id = "get_workday_id_from_user_data",
            python_callable=lambda : rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_data')[0]['userDetails']['customFieldValues'], 'customField.displayText',
                'Work day ID', 'text', '')
        )

        if_workdayid_mismatch = rail.IfOperator(
            task_id="if_workdayid_mismatch",
            test="{{ result('get_workday_id_from_user_data').lower() != dag_run.conf.workdayid.lower() and \
                dag_run.conf.workdayid | is_truthy }}",
            yes_task="update_workdayid",
            no_task="get_position_from_user_data"
        )

        update_workdayid = rail.RepliconServiceOperator(
            task_id = "update_workdayid",
            endpoint = "/services/CustomFieldService1.svc/UpdateTextValue",
            data ={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.workdayidudfuri }}",
                "value": "{{ dag_run.conf.workdayid }}"
                }
        )

        get_position_from_user_data = rail.PythonOperator(
            task_id = "get_position_from_user_data",
            python_callable=lambda : rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_data')[0]['userDetails']['customFieldValues'], 'customField.displayText',
                'Position', 'text', '')
        )

        if_position_mismatch = rail.IfOperator(
            task_id="if_position_mismatch",
            test="{{ result('get_position_from_user_data').lower() != dag_run.conf.position.lower() and \
                dag_run.conf.position | is_truthy }}",
            yes_task="update_position",
            no_task="get_manager_from_user_data"
        )

        update_position = rail.RepliconServiceOperator(
            task_id = "update_position",
            endpoint = "/services/CustomFieldService1.svc/UpdateTextValue",
            data ={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.positionudf }}",
                "value": "{{ dag_run.conf.position }}"
                }
        )

        get_manager_from_user_data = rail.PythonOperator(
            task_id = "get_manager_from_user_data",
            python_callable=lambda : rail.find_first_by_attr_and_get_attr(
                rail.result('get_user_data')[0]['userDetails']['customFieldValues'], 'customField.displayText',
                'Manager', 'text', '')
        )

        if_manager_mismatch = rail.IfOperator(
            task_id="if_manager_mismatch",
            test="{{ result('get_manager_from_user_data').lower() != dag_run.conf.manager.lower() and \
                dag_run.conf.manager | is_truthy }}",
            yes_task="if_managerudf_present",
            no_task="if_initial_payrule_present"
        )

        if_managerudf_present = rail.IfOperator(
            task_id="if_managerudf_present",
            test="{{ dag_run.conf.managerudfuri | is_truthy }}",
            yes_task="update_manager",
            no_task="log_manager_exception"
        )

        update_manager = rail.RepliconServiceOperator(
            task_id = "update_manager",
            endpoint = "/services/CustomFieldService1.svc/UpdateDropdownValue",
            data ={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.managerudfuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.managerudfvalueuri }}"
                }
        )

        log_manager_exception = rail.WriteLogOperator(
            task_id='log_manager_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Manager value \"{{ dag_run.conf.manager }}\" not available in Replicon"
            }
        )

        if_initial_payrule_present = rail.IfOperator(
            task_id="if_initial_payrule_present",
            test="{{ dag_run.conf.initialpayrulename | is_truthy }}",
            yes_task="check_payrulescriptschedule_present_for_user",
            no_task="if_timesheet_periodtype_present"
        )

        check_payrulescriptschedule_present_for_user = rail.IfOperator(
            task_id="check_payrulescriptschedule_present_for_user",
            test="{{ result('get_user_data')[0].payRuleScriptSchedule | length > 0 }}",
            yes_task="get_current_payrule_uri",
            no_task="if_payruleuri_present"
        )

        get_current_payrule_uri = rail.PythonOperator(
            task_id = "get_current_payrule_uri",
            python_callable=lambda: get_current_data('payRuleScriptSchedule','payRuleScript')
        )

        if_payrule_uri_mismatch = rail.IfOperator(
            task_id="if_payrule_uri_mismatch",
            test="{{ result('get_current_payrule_uri') | is_falsy or \
                result('get_current_payrule_uri') != dag_run.conf.payruleuri }}",
            yes_task="if_payruleuri_present",
            no_task="if_timesheet_periodtype_present"
        )

        if_payruleuri_present = rail.IfOperator(
            task_id="if_payruleuri_present",
            test="{{ dag_run.conf.payruleuri | is_truthy }}",
            yes_task="update_payrule",
            no_task="log_payrule_exception"
        )

        update_payrule = rail.RepliconServiceOperator(
            task_id = "update_payrule",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data =request_payload.payrule_update_payload
        )

        log_payrule_exception = rail.WriteLogOperator(
            task_id='log_payrule_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Payrule \"{{ dag_run.conf.initialpayrulename }}\" not available in Replicon"
            }
        )

        if_timesheet_periodtype_present = rail.IfOperator(
            task_id="if_timesheet_periodtype_present",
            test="{{ dag_run.conf.timesheetperiodtype | is_truthy }}",
            yes_task="check_timesheetperiodschedule_present_for_user",
            no_task="if_initial_schedulename_present"
        )

        check_timesheetperiodschedule_present_for_user = rail.IfOperator(
            task_id="check_timesheetperiodschedule_present_for_user",
            test="{{ result('get_user_data')[0].timesheetPeriodSchedule | length > 0 }}",
            yes_task="get_current_timesheetperiod_schedule_uri",
            no_task="if_timesheetperioduri_present"
        )

        get_current_timesheetperiod_schedule_uri = rail.PythonOperator(
            task_id = "get_current_timesheetperiod_schedule_uri",
            python_callable=lambda: get_current_data('timesheetPeriodSchedule','timesheetPeriod')
        )

        if_timeheetperiod_uri_mismatch = rail.IfOperator(
            task_id="if_timeheetperiod_uri_mismatch",
            test="{{ result('get_current_timesheetperiod_schedule_uri') | is_falsy or \
                result('get_current_timesheetperiod_schedule_uri') != dag_run.conf.timesheetperioduri }}",
            yes_task="if_timesheetperioduri_present",
            no_task="if_initial_schedulename_present"
        )

        if_timesheetperioduri_present = rail.IfOperator(
            task_id="if_timesheetperioduri_present",
            test="{{ dag_run.conf.timesheetperioduri | is_truthy }}",
            yes_task="update_timesheet_period",
            no_task="log_timesheet_period_exception"
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id = "update_timesheet_period",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data =request_payload.timesheet_period_update_payload
        )

        log_timesheet_period_exception = rail.WriteLogOperator(
            task_id='log_timesheet_period_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timesheet period \"{{ dag_run.conf.timesheetperiodtype }}\" not available in Replicon"
            }
        )

        if_initial_schedulename_present = rail.IfOperator(
            task_id="if_initial_schedulename_present",
            test="{{ dag_run.conf.initialschedulename | is_truthy }}",
            yes_task="check_schedulepolicies_present_for_user",
            no_task="get_effectiveusergroupmembership"
        )

        check_schedulepolicies_present_for_user = rail.IfOperator(
            task_id="check_schedulepolicies_present_for_user",
            test="{{ result('get_user_data')[0].schedulePolicies | length > 0 }}",
            yes_task="get_current_schedule_policy_uri",
            no_task="if_officescheduleuri_present"
        )

        get_current_schedule_policy_uri = rail.PythonOperator(
            task_id = "get_current_schedule_policy_uri",
            python_callable=python_callable.get_current_officeschedule_uri
        )

        if_officeschedule_uri_mismatch = rail.IfOperator(
            task_id="if_officeschedule_uri_mismatch",
            test="{{ result('get_current_schedule_policy_uri') | is_falsy or \
                result('get_current_schedule_policy_uri') != dag_run.conf.officescheduleuri }}",
            yes_task="if_officescheduleuri_present",
            no_task="get_effectiveusergroupmembership"
        )

        if_officescheduleuri_present = rail.IfOperator(
            task_id="if_officescheduleuri_present",
            test="{{ dag_run.conf.officescheduleuri | is_truthy }}",
            yes_task="update_officeschedule",
            no_task="log_officeschedule_exception"
        )

        update_officeschedule = rail.RepliconServiceOperator(
            task_id = "update_officeschedule",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data =request_payload.officeschedule_update_payload
        )

        log_officeschedule_exception = rail.WriteLogOperator(
            task_id='log_officeschedule_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Schedule \"{{ dag_run.conf.initialschedulename }}\" not available in Replicon"
            }
        )

        get_effectiveusergroupmembership = rail.RepliconServiceOperator(
            task_id = "get_effectiveusergroupmembership",
            endpoint = "/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_department_mismatch = rail.IfOperator(
            task_id="if_department_mismatch",
            test="{{ dag_run.conf.department | is_truthy and \
                ( result('get_effectiveusergroupmembership').departments | length == 0 or \
                    result('get_effectiveusergroupmembership').departments[0].department.department.displayText != dag_run.conf.department) }}",
            yes_task="if_department_uri_present",
            no_task="if_location_mismatch"
        )

        if_department_uri_present = rail.IfOperator(
            task_id="if_department_uri_present",
            test="{{ dag_run.conf.departmenturi | is_truthy }}",
            yes_task="update_department",
            no_task="log_department_exception"
        )

        update_department = rail.RepliconServiceOperator(
            task_id = "update_department",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.department_update_payload
        )

        log_department_exception = rail.WriteLogOperator(
            task_id='log_department_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Department \"{{ dag_run.conf.department }}\" not available in Replicon"
            }
        )

        if_location_mismatch = rail.IfOperator(
            task_id="if_location_mismatch",
            test="{{ dag_run.conf.location | is_truthy and \
                ( result('get_effectiveusergroupmembership').locations | length == 0 or \
                    result('get_effectiveusergroupmembership').locations[0].location.location.displayText != dag_run.conf.locationname) }}",
            yes_task="if_location_uri_present",
            no_task="if_costcenter_mismatch"
        )

        if_location_uri_present = rail.IfOperator(
            task_id="if_location_uri_present",
            test="{{ dag_run.conf.locationuri | is_truthy }}",
            yes_task="update_location",
            no_task="log_location_exception"
        )

        update_location = rail.RepliconServiceOperator(
            task_id = "update_location",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.location_update_payload
        )

        log_location_exception = rail.WriteLogOperator(
            task_id='log_location_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Location with code \"{{ dag_run.conf.location }}\" not available in Replicon"
            }
        )

        if_costcenter_mismatch = rail.IfOperator(
            task_id="if_costcenter_mismatch",
            test="{{ dag_run.conf.businessunit | is_truthy and \
                ( result('get_effectiveusergroupmembership').costCenters | length == 0 or \
                    result('get_effectiveusergroupmembership').costCenters[0].costCenter.costCenter.displayText != dag_run.conf.businessunit) }}",
            yes_task="update_costcenter",
            no_task="timesheet_template_variable"
        )

        update_costcenter = rail.RepliconServiceOperator(
            task_id = "update_costcenter",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.costcenter_update_payload
        )


        timesheet_template_variable = rail.SetVariableOperator(
            task_id='timesheet_template_variable',
            append=False,
            name='timesheettemplateassignment',
            value='yes'
        )

        if_employeetypename_mismatch = rail.IfOperator(
            task_id="if_employeetypename_mismatch",
            test="{{ dag_run.conf.employeetypename | is_truthy and \
                ( result('get_effectiveusergroupmembership').employeeTypes | length == 0 or \
                    result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText != dag_run.conf.employeetypename) }}",
            yes_task="check_current_employeetypename",
            no_task="if_employeetypename_present"
        )

        if_employeetypename_present = rail.IfOperator(
            task_id="if_employeetypename_present",
            test="{{ dag_run.conf.employeetypename | is_truthy }}",
            yes_task="if_initialsupervisor_loginname_present",
            no_task="log_employeetype_exception"
        )

        check_current_employeetypename = rail.IfOperator(
            task_id="check_current_employeetypename",
            test="{{ result('get_effectiveusergroupmembership').employeeTypes | length == 0 or \
                    ( result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText != 'Managers' or \
                        'Non-Exempt' in result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText) }}",
            yes_task="if_timesheet_templateuri_present_and_employeetype_is_exempt",
            no_task="if_current_employeetype_is_nonexempt"
        )

        if_timesheet_templateuri_present_and_employeetype_is_exempt = rail.IfOperator(
            task_id="if_timesheet_templateuri_present_and_employeetype_is_exempt",
            test="{{ dag_run.conf.employeetypename == 'Exempt' and  \
                    result('get_user_data')[0].timesheetTemplate | is_truthy and \
                        result('get_user_data')[0].timesheetTemplate.uri | is_truthy }}",
            yes_task="remove_policysettouser_timesheettemplate",
            no_task="if_current_employeetype_is_nonexempt"
        )

        remove_policysettouser_timesheettemplate = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timesheettemplate",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_data')[0].timesheetTemplate.uri }}" 
                }
        )

        update_timesheet_template_variable_1 = rail.SetVariableOperator(
            task_id='update_timesheet_template_variable_1',
            append=False,
            name='{{ result("timesheet_template_variable").name }}',
            value='no'
        )

        if_current_employeetype_is_nonexempt = rail.IfOperator(
            task_id="if_current_employeetype_is_nonexempt",
            test="{{ result('get_effectiveusergroupmembership').employeeTypes | is_truthy and \
                  'Non-Exempt' in result('get_effectiveusergroupmembership').employeeTypes[0].employeeType.employeeType.displayText }}",
            yes_task="if_timesheet_templateuri_present_and_employeetype_is_manager",
            no_task="if_employeetypeuri_present"
        )

        if_timesheet_templateuri_present_and_employeetype_is_manager = rail.IfOperator(
            task_id="if_timesheet_templateuri_present_and_employeetype_is_manager",
            test="{{ dag_run.conf.employeetypename == 'Managers' and \
                    result('get_user_data')[0].timesheetTemplate | is_truthy and \
                        result('get_user_data')[0].timesheetTemplate.uri | is_truthy}}",
            yes_task="remove_policysettouser_timesheettemplate_for_manager",
            no_task="if_employeetypeuri_present"
        )

        remove_policysettouser_timesheettemplate_for_manager = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timesheettemplate_for_manager",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_data')[0].timesheetTemplate.uri }}" 
                }
        )

        update_timesheet_template_variable_2 = rail.SetVariableOperator(
            task_id='update_timesheet_template_variable_2',
            append=False,
            name='{{ result("timesheet_template_variable").name }}',
            value='no'
        )

        if_employeetypeuri_present = rail.IfOperator(
            task_id="if_employeetypeuri_present",
            test="{{ dag_run.conf.employeetypeuri | is_truthy }}",
            yes_task="update_employeetytpe",
            no_task="log_employeetype_exception"
        )

        update_employeetytpe = rail.RepliconServiceOperator(
            task_id = "update_employeetytpe",
            endpoint = "/services/ImportService1.svc/ApplyUserModifications2",
            data = request_payload.employeetype_update_payload
        )

        log_employeetype_exception = rail.WriteLogOperator(
            task_id='log_employeetype_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Employee type group \"{{ dag_run.conf.employeetypename }}\" derived based on Employee \
                    type, worker category and manager not available in Replicon"
            }
        )

        if_initialsupervisor_loginname_present = rail.IfOperator(
            task_id="if_initialsupervisor_loginname_present",
            test="{{ dag_run.conf.initialsupervisorloginname | is_truthy }}",
            yes_task="if_initial_supervisor_equal_loginname",
            no_task="if_manager_yes_and_timeoff_template_present"
        )

        if_initial_supervisor_equal_loginname = rail.IfOperator(
            task_id="if_initial_supervisor_equal_loginname",
            test="{{ dag_run.conf.initialsupervisorloginname == dag_run.conf.loginname }}",
            yes_task="log_same_user_and_supervisor_exception",
            no_task="get_supervisor_assignment_details"
        )

        log_same_user_and_supervisor_exception = rail.WriteLogOperator(
            task_id='log_same_user_and_supervisor_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Supervisor not updated  - Supervisor login name is same as User login name"
            }
        )

        get_supervisor_assignment_details = rail.RepliconServiceOperator(
            task_id = "get_supervisor_assignment_details",
            endpoint = "/services/UserService1.svc/GetSupervisorAssignmentDetails",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "asOfDate": {
                    "year": get_today().split('/')[2],
                    "month": get_today().split('/')[0],
                    "day": get_today().split('/')[1]
                }
            }
        )

        if_supervisorloginanme_matches = rail.IfOperator(
            task_id="if_supervisorloginanme_matches",
            test="{{ result('get_supervisor_assignment_details') | is_truthy and \
                    result('get_supervisor_assignment_details').supervisor.user.loginName.lower() == \
                        dag_run.conf.initialsupervisorloginname.lower() }}",
            yes_task="if_manager_yes_and_timeoff_template_present",
            no_task="if_supervisor_uri_present"
        )

        if_supervisor_uri_present = rail.IfOperator(
            task_id="if_supervisor_uri_present",
            test="{{ dag_run.conf.supervisoruri | is_truthy }}",
            yes_task="get_supervisor_details",
            no_task="log_to_supervisor_lookup"
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id = "get_supervisor_details",
            endpoint = "/services/ImportService1.svc/BulkGetUsers3",
            data = request_payload.get_supervisor_data
        )

        is_supervisor_is_enabled = rail.IfOperator(
            task_id="is_supervisor_is_enabled",
            test="{{ result('get_supervisor_details')[0].userDetails.isEnabled | lower() == 'true' }}",
            yes_task="get_supervisor_permission_sets",
            no_task="log_to_supervisor_lookup"
        )

        get_supervisor_permission_sets = rail.PythonOperator(
            task_id = "get_supervisor_permission_sets",
            python_callable=lambda: {
                'manager_permission' : rail.find_first_by_attr_and_get_attr(
                    rail.result('get_supervisor_details')[0]['permissionSets'], 'displayText', 'Manager', 'uri', ''),
                'enduser_permission' : rail.find_first_by_attr_and_get_attr(
                    rail.result('get_supervisor_details')[0]['permissionSets'], 'displayText', 'End user with reports view', 'uri', '')
            }
        )

        check_if_manager_permission_absent = rail.IfOperator(
            task_id="check_if_manager_permission_absent",
            test="{{ result('get_supervisor_permission_sets').manager_permission | is_falsy }}",
            yes_task="assign_manager_permission_to_supervisor",
            no_task="check_if_enduser_permission_absent"
        )

        assign_manager_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_manager_permission_to_supervisor",
            endpoint = "/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf['supervisorpermissionuri'] }}"
            }
        )

        check_if_enduser_permission_absent = rail.IfOperator(
            task_id="check_if_enduser_permission_absent",
            test="{{ result('get_supervisor_permission_sets').enduser_permission | is_falsy }}",
            yes_task="assign_enduser_permission_to_supervisor",
            no_task="update_supervisor_for_user"
        )

        assign_enduser_permission_to_supervisor = rail.RepliconServiceOperator(
            task_id = "assign_enduser_permission_to_supervisor",
            endpoint = "/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ dag_run.conf.supervisoruri }}",
                "permissionSetUri": "{{ dag_run.conf['enduserpermissionformanager'] }}"
            }
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id = "update_supervisor_for_user",
            endpoint = "/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "supervisorUri": "{{ dag_run.conf.supervisoruri }}",
                "dateRange": {
                    "startDate": {
                        "year": get_today().split('/')[2],
                        "month": get_today().split('/')[0],
                        "day": get_today().split('/')[1]
                    }
                }
            }
        )

        log_to_supervisor_lookup = rail.WriteLogOperator(
            task_id='log_to_supervisor_lookup',
            log = "{{ dag_run.conf.supervisor_logger }}",
            message="na",
            severity="Skipped",
            properties={
                "userloginname" : "{{ dag_run.conf.loginname }}",
                "useruri" : "{{ dag_run.conf.useruri }}",
                "username" : "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "supervisorloginname" : "{{ dag_run.conf.initialsupervisorloginname }}",
                "emplid" : "{{ dag_run.conf.employeeid }}",
                "action" : "Update",
                "status": ""
            }
        )

        if_manager_yes_and_timeoff_template_present = rail.IfOperator(
            task_id="if_manager_yes_and_timeoff_template_present",
            test="{{ dag_run.conf.manager.lower() == 'yes' and \
                    result('get_user_data')[0].timeOffTemplate | is_truthy and \
                         result('get_user_data')[0].timeOffTemplate.displayText | is_truthy }}",
            yes_task="remove_policysettouser_timeofftemplate_for_manager",
            no_task="check_timeoff_template_mismatch"
        )

        remove_policysettouser_timeofftemplate_for_manager = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timeofftemplate_for_manager",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_data')[0].timeOffTemplate.uri }}" 
                }
        )

        check_timeoff_template_mismatch = rail.IfOperator(
            task_id="check_timeoff_template_mismatch",
            test="{{ dag_run.conf.manager.lower() == 'no' and \
                dag_run.conf.timeofftemplate | is_truthy and \
                    (result('get_user_data')[0].timeOffTemplate | is_falsy or \
                        result('get_user_data')[0].timeOffTemplate.displayText != dag_run.conf.timeofftemplate) }}",
            yes_task="if_timeoff_templateuri_present",
            no_task="if_manager_yes_and_timesheet_template_present"
        )

        if_timeoff_templateuri_present = rail.IfOperator(
            task_id="if_timeoff_templateuri_present",
            test="{{ dag_run.conf.timeofftemplateuri | is_truthy }}",
            yes_task="update_timeofftemplate",
            no_task="log_timeofftemplate_exception"
        )

        update_timeofftemplate = rail.RepliconServiceOperator(
            task_id = "update_timeofftemplate",
            endpoint = "/services/PolicySetService1.svc/AssignPolicySetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.timeofftemplateuri }}"
                }
        )

        log_timeofftemplate_exception = rail.WriteLogOperator(
            task_id='log_timeofftemplate_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timeoff template  \"{{ dag_run.conf.timeofftemplate }}\" not available in Replicon"
            }
        )

        if_manager_yes_and_timesheet_template_present = rail.IfOperator(
            task_id="if_manager_yes_and_timesheet_template_present",
            test="{{ dag_run.conf.manager.lower() == 'yes' and \
                    result('get_user_data')[0].timesheetTemplate | is_truthy }}",
            yes_task="remove_policysettouser_timesheet_template_for_manager",
            no_task="get_timesheet_template_variable"
        )

        remove_policysettouser_timesheet_template_for_manager = rail.RepliconServiceOperator(
            task_id = "remove_policysettouser_timesheet_template_for_manager",
            endpoint = "/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data = {
                "userUri":"{{ dag_run.conf.useruri}}",
                "policySetUri": "{{ result('get_user_data')[0].timesheetTemplate.uri }}" 
                }
        )

        get_timesheet_template_variable = rail.GetVariableOperator(
            task_id='get_timesheet_template_variable',
            name="{{ result('timesheet_template_variable').name }}"
        )

        if_timesheettemplate_variable_is_yes = rail.IfOperator(
            task_id='if_timesheettemplate_variable_is_yes',
            test="{{ result('get_timesheet_template_variable').value == 'yes' }}",
            yes_task="if_timesheet_template_mismatch",
            no_task="if_timesheet_approval_path_mismatch"
        )

        if_timesheet_template_mismatch = rail.IfOperator(
            task_id="if_timesheet_template_mismatch",
            test="{{ dag_run.conf.manager.lower() == 'no' and \
                dag_run.conf.timesheettemplate | is_truthy and \
                    (result('get_user_data')[0].timesheetTemplate | is_falsy or \
                        result('get_user_data')[0].timesheetTemplate.name != dag_run.conf.timesheettemplate) and \
                        dag_run.conf.employeetypename.lower() != 'exempt'}}",
            yes_task="if_timesheet_templateuri_present",
            no_task="if_timesheet_approval_path_mismatch"
        )

        if_timesheet_templateuri_present = rail.IfOperator(
            task_id="if_timesheet_templateuri_present",
            test="{{ dag_run.conf.timesheettemplateuri | is_truthy }}",
            yes_task="update_timesheettemplate",
            no_task="log_timesheettemplate_exception"
        )

        update_timesheettemplate = rail.RepliconServiceOperator(
            task_id = "update_timesheettemplate",
            endpoint = "/services/PolicySetService1.svc/AssignPolicySetToUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ dag_run.conf.timesheettemplateuri }}"
                }
        )

        log_timesheettemplate_exception = rail.WriteLogOperator(
            task_id='log_timesheettemplate_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timesheet template \"{{ dag_run.conf.timesheettemplate }}\" not available in Replicon"
            }
        )

        if_timesheet_approval_path_mismatch = rail.IfOperator(
            task_id="if_timesheet_approval_path_mismatch",
            test="{{ dag_run.conf.timesheetapprovalpath | is_truthy and \
                    result('get_user_data')[0].timesheetApprovalPath.displayText != dag_run.conf.timesheetapprovalpath }}",
            yes_task="if_timesheet_approval_uri_present",
            no_task="if_timeoff_approval_path_mismatch"
        )

        if_timesheet_approval_uri_present = rail.IfOperator(
            task_id="if_timesheet_approval_uri_present",
            test="{{ dag_run.conf.timesheetapprovalpathuri | is_truthy }}",
            yes_task="update_timesheetapproval",
            no_task="log_timesheetapproval_exception"
        )

        update_timesheetapproval = rail.RepliconServiceOperator(
            task_id = "update_timesheetapproval",
            endpoint = "/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "approvalPathUri": "{{ dag_run.conf.timesheetapprovalpathuri }}"
                }
        )

        log_timesheetapproval_exception = rail.WriteLogOperator(
            task_id='log_timesheetapproval_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timesheet approval path \"{{ dag_run.conf.timesheetapprovalpath }}\" not available in Replicon"
            }
        )

        if_timeoff_approval_path_mismatch = rail.IfOperator(
            task_id="if_timeoff_approval_path_mismatch",
            test="{{ dag_run.conf.timeoffapprovalpath | is_truthy and \
                    result('get_user_data')[0].timeOffApprovalPath.displayText != dag_run.conf.timeoffapprovalpath }}",
            yes_task="if_timeoff_approval_uri_present",
            no_task="if_timezone_is_present"
        )

        if_timeoff_approval_uri_present = rail.IfOperator(
            task_id="if_timeoff_approval_uri_present",
            test="{{ dag_run.conf.timeoffapprovalpathuri | is_truthy }}",
            yes_task="update_timeoffapproval",
            no_task="log_timeoffapproval_exception"
        )

        update_timeoffapproval = rail.RepliconServiceOperator(
            task_id = "update_timeoffapproval",
            endpoint = "/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "approvalPathUri": "{{ dag_run.conf.timeoffapprovalpathuri }}"
                }
        )

        log_timeoffapproval_exception = rail.WriteLogOperator(
            task_id='log_timeoffapproval_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Timeoff approval path \"{{ dag_run.conf.timeoffapprovalpath }}\" not available in Replicon"
            }
        )

        if_timezone_is_present = rail.IfOperator(
            task_id="if_timezone_is_present",
            test="{{ dag_run.conf.timezone | is_truthy }}",
            yes_task="if_timezoneuri_is_present",
            no_task="if_workweek_mismatch"
        )

        if_timezoneuri_is_present = rail.IfOperator(
            task_id="if_timezoneuri_is_present",
            test="{{ dag_run.conf.timezoneuri | is_truthy }}",
            yes_task="if_timezone_mismatch",
            no_task="log_timezone_exception"
        )

        if_timezone_mismatch = rail.IfOperator(
            task_id="if_timezone_mismatch",
            test="{{ dag_run.conf.timezoneuri != result('get_user_data')[0].timeZone.uri }}",
            yes_task="update_timezone",
            no_task="if_workweek_mismatch"
        )

        update_timezone = rail.RepliconServiceOperator(
            task_id = "update_timezone",
            endpoint = "/services/InternationalizationService1.svc/UpdateTimeZoneForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "timeZoneUri": "{{ dag_run.conf.timezoneuri }}"
                }
        )

        log_timezone_exception = rail.WriteLogOperator(
            task_id='log_timezone_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Time zone  \"{{ dag_run.conf.timezone }}\" not available in Replicon"
            }
        )

        if_workweek_mismatch = rail.IfOperator(
            task_id="if_workweek_mismatch",
            test="{{ dag_run.conf.workweek | is_truthy and \
                dag_run.conf.workweekuri != result('get_user_data')[0].userDetails.workWeekStartDay.uri}}",
            yes_task="update_workweek_startday",
            no_task="if_holiday_calender_mismatch"
        )

        update_workweek_startday = rail.RepliconServiceOperator(
            task_id = "update_workweek_startday",
            endpoint = "/services/UserService1.svc/UpdateWorkWeekStartDayForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "dayOfWeekUri": "{{ dag_run.conf.workweekuri }}"
                }
        )

        if_holiday_calender_mismatch = rail.IfOperator(
            task_id="if_holiday_calender_mismatch",
            test="{{ dag_run.conf.holidaycalendar | is_truthy and \
                dag_run.conf.holidaycalendar != result('get_user_data')[0].holidayCalendar.displayText }}",
            yes_task="if_holidaycalender_uri_present",
            no_task="write_log_user_import"
        )

        if_holidaycalender_uri_present = rail.IfOperator(
            task_id="if_holidaycalender_uri_present",
            test="{{ dag_run.conf.holidaycalendaruri | is_truthy }}",
            yes_task="update_holiday_calendar",
            no_task="log_holiday_calendar_exception"
        )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id = "update_holiday_calendar",
            endpoint = "/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}",
                "holidayCalendarUri": "{{ dag_run.conf.holidaycalendaruri }}"
                }
        )

        log_holiday_calendar_exception = rail.WriteLogOperator(
            task_id='log_holiday_calendar_exception',
            log = "{{ result('exception_log') }}",
            message="na",
            severity="Exception",
            properties={
                "value" : "Holiday calendar \"{{ dag_run.conf.holidaycalendar }}\" not available in Replicon"
            }
        )

        write_log_user_import = rail.WriteCSVFileOperator(
            task_id='write_log_user_import',
            source="{{ result('exception_log') }}",
            header=['value'],
            row=lambda item: [
                item['properties']['value']
            ]
        )

        log_user_import = rail.WriteLogOperator(
            task_id='log_user_import',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity=lambda: 'Exception' if rail.load_all_records(rail.result('write_log_user_import')) else 'Success',
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.logger }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "username" : "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "login_name": "{{ dag_run.conf.loginname }}",
                "emplid" : "{{ dag_run.conf.employeeid }}",
                "action" : "update",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )


        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> exception_log

        exception_log >> get_user_data >> check_rehire

        check_rehire >> rail.Label("Yes") >> update_user_data_to_old >> trigger_add_user >> catch_and_log_errors
        check_rehire >> rail.Label("No") >> if_loginname_mismatch

        if_loginname_mismatch >> rail.Label("Yes") >> update_loginname >> if_firstname_mismatch
        if_loginname_mismatch >> rail.Label("No") >> if_firstname_mismatch

        if_firstname_mismatch >> rail.Label("Yes") >> update_firstname >> if_lastname_mismatch
        if_firstname_mismatch >> rail.Label("No") >> if_lastname_mismatch

        if_lastname_mismatch >> rail.Label("Yes") >> update_lastname >> if_email_mismatch
        if_lastname_mismatch >> rail.Label("No") >> if_email_mismatch

        if_email_mismatch >> rail.Label("Yes") >> update_email_address >> get_workday_id_from_user_data
        if_email_mismatch >> rail.Label("No") >> get_workday_id_from_user_data

        get_workday_id_from_user_data >> if_workdayid_mismatch

        if_workdayid_mismatch >> rail.Label("Yes") >> update_workdayid >> get_position_from_user_data
        if_workdayid_mismatch >> rail.Label("No") >> get_position_from_user_data

        get_position_from_user_data >> if_position_mismatch

        if_position_mismatch >> rail.Label("Yes") >> update_position >> get_manager_from_user_data
        if_position_mismatch >> rail.Label("No") >> get_manager_from_user_data

        get_manager_from_user_data >> if_manager_mismatch

        if_manager_mismatch >> rail.Label("Yes") >> if_managerudf_present
        if_manager_mismatch >> rail.Label("No") >> if_initial_payrule_present

        if_managerudf_present >> rail.Label("Yes") >> update_manager >> if_initial_payrule_present
        if_managerudf_present >> rail.Label("No") >> log_manager_exception >> if_initial_payrule_present

        if_initial_payrule_present >> rail.Label("Yes") >> check_payrulescriptschedule_present_for_user

        check_payrulescriptschedule_present_for_user >> rail.Label("Yes") >> get_current_payrule_uri >> if_payrule_uri_mismatch
        check_payrulescriptschedule_present_for_user >> rail.Label("No") >> if_payruleuri_present

        if_payrule_uri_mismatch >> rail.Label("Yes") >> if_payruleuri_present
        if_payrule_uri_mismatch >> rail.Label("No") >> if_timesheet_periodtype_present

        if_payruleuri_present >> rail.Label("Yes") >> update_payrule >> if_timesheet_periodtype_present
        if_payruleuri_present >> rail.Label("No") >> log_payrule_exception >> if_timesheet_periodtype_present

        if_initial_payrule_present >> rail.Label("No") >> if_timesheet_periodtype_present

        if_timesheet_periodtype_present >> rail.Label("Yes") >> check_timesheetperiodschedule_present_for_user
        if_timesheet_periodtype_present >> rail.Label("No") >> if_initial_schedulename_present

        check_timesheetperiodschedule_present_for_user >> rail.Label("Yes") >> get_current_timesheetperiod_schedule_uri >> \
        if_timeheetperiod_uri_mismatch
        check_timesheetperiodschedule_present_for_user >> rail.Label("No") >> if_timesheetperioduri_present

        if_timeheetperiod_uri_mismatch >> rail.Label("Yes") >> if_timesheetperioduri_present
        if_timeheetperiod_uri_mismatch >> rail.Label("No") >> if_initial_schedulename_present

        if_timesheetperioduri_present >> rail.Label("Yes") >> update_timesheet_period >> if_initial_schedulename_present
        if_timesheetperioduri_present >> rail.Label("No") >> log_timesheet_period_exception >> if_initial_schedulename_present

        if_initial_schedulename_present >> rail.Label("Yes") >> check_schedulepolicies_present_for_user
        if_initial_schedulename_present >> rail.Label("No") >> get_effectiveusergroupmembership

        check_schedulepolicies_present_for_user >> rail.Label("Yes") >> get_current_schedule_policy_uri >> if_officeschedule_uri_mismatch
        check_schedulepolicies_present_for_user >> rail.Label("No") >> if_officescheduleuri_present

        if_officeschedule_uri_mismatch >> rail.Label("Yes") >> if_officescheduleuri_present
        if_officeschedule_uri_mismatch >> rail.Label("No") >> get_effectiveusergroupmembership

        if_officescheduleuri_present >> rail.Label("Yes") >> update_officeschedule >> get_effectiveusergroupmembership
        if_officescheduleuri_present >> rail.Label("No") >> log_officeschedule_exception >> get_effectiveusergroupmembership

        get_effectiveusergroupmembership >> if_department_mismatch

        if_department_mismatch >> rail.Label("Yes") >> if_department_uri_present
        if_department_mismatch >> rail.Label("No") >> if_location_mismatch

        if_department_uri_present >> rail.Label("Yes") >> update_department >> if_location_mismatch
        if_department_uri_present >> rail.Label("No") >> log_department_exception >> if_location_mismatch

        if_location_mismatch >> rail.Label("Yes") >> if_location_uri_present
        if_location_mismatch >> rail.Label("No") >> if_costcenter_mismatch

        if_location_uri_present >> rail.Label("Yes") >> update_location >> if_costcenter_mismatch
        if_location_uri_present >> rail.Label("No") >> log_location_exception >> if_costcenter_mismatch

        if_costcenter_mismatch >> rail.Label("Yes") >> update_costcenter >> timesheet_template_variable
        if_costcenter_mismatch >> rail.Label("No") >> timesheet_template_variable

        timesheet_template_variable >> if_employeetypename_mismatch

        if_employeetypename_mismatch >> rail.Label("Yes") >> check_current_employeetypename
        if_employeetypename_mismatch >> rail.Label("No") >> if_employeetypename_present

        if_employeetypename_present >> rail.Label("Yes") >> if_initialsupervisor_loginname_present
        if_employeetypename_present >> rail.Label("No") >> log_employeetype_exception >> if_initialsupervisor_loginname_present

        check_current_employeetypename >> rail.Label("Yes") >> if_timesheet_templateuri_present_and_employeetype_is_exempt
        check_current_employeetypename >> rail.Label("No") >> if_current_employeetype_is_nonexempt

        if_timesheet_templateuri_present_and_employeetype_is_exempt >> rail.Label("Yes") >> remove_policysettouser_timesheettemplate >> \
        update_timesheet_template_variable_1 >> if_current_employeetype_is_nonexempt
        if_timesheet_templateuri_present_and_employeetype_is_exempt >> rail.Label("No") >> if_current_employeetype_is_nonexempt

        if_current_employeetype_is_nonexempt >> rail.Label("Yes") >> if_timesheet_templateuri_present_and_employeetype_is_manager
        if_current_employeetype_is_nonexempt >> rail.Label("No") >> if_employeetypeuri_present

        if_timesheet_templateuri_present_and_employeetype_is_manager >> rail.Label("Yes") >> remove_policysettouser_timesheettemplate_for_manager >> \
        update_timesheet_template_variable_2 >> if_employeetypeuri_present
        if_timesheet_templateuri_present_and_employeetype_is_manager >> rail.Label("No") >> if_employeetypeuri_present

        if_employeetypeuri_present >> rail.Label("Yes") >> update_employeetytpe >> if_initialsupervisor_loginname_present
        if_employeetypeuri_present >> rail.Label("No") >> log_employeetype_exception >> if_initialsupervisor_loginname_present

        if_initialsupervisor_loginname_present >> rail.Label("Yes") >> if_initial_supervisor_equal_loginname
        if_initialsupervisor_loginname_present >> rail.Label("No") >> if_manager_yes_and_timeoff_template_present

        if_initial_supervisor_equal_loginname >> rail.Label("Yes") >> log_same_user_and_supervisor_exception >> \
            if_manager_yes_and_timeoff_template_present
        if_initial_supervisor_equal_loginname >> rail.Label("No") >> get_supervisor_assignment_details >> if_supervisorloginanme_matches

        if_supervisorloginanme_matches >> rail.Label("Yes") >> if_manager_yes_and_timeoff_template_present
        if_supervisorloginanme_matches >> rail.Label("No") >> if_supervisor_uri_present

        if_supervisor_uri_present >> rail.Label("Yes") >> get_supervisor_details >> is_supervisor_is_enabled
        if_supervisor_uri_present >> rail.Label("No") >> log_to_supervisor_lookup >> if_manager_yes_and_timeoff_template_present

        is_supervisor_is_enabled >> rail.Label('Yes') >> get_supervisor_permission_sets >> check_if_manager_permission_absent
        is_supervisor_is_enabled >> rail.Label('No') >> log_to_supervisor_lookup >> if_manager_yes_and_timeoff_template_present

        check_if_manager_permission_absent >> rail.Label('Yes') >> assign_manager_permission_to_supervisor >> check_if_enduser_permission_absent
        check_if_manager_permission_absent >> rail.Label('No') >> check_if_enduser_permission_absent

        check_if_enduser_permission_absent >> rail.Label('Yes') >> assign_enduser_permission_to_supervisor >> update_supervisor_for_user
        check_if_enduser_permission_absent >> rail.Label('No') >> update_supervisor_for_user

        update_supervisor_for_user >> if_manager_yes_and_timeoff_template_present

        if_manager_yes_and_timeoff_template_present >> rail.Label("Yes") >> remove_policysettouser_timeofftemplate_for_manager >> \
        check_timeoff_template_mismatch
        if_manager_yes_and_timeoff_template_present >> rail.Label("No") >> check_timeoff_template_mismatch

        check_timeoff_template_mismatch >> rail.Label('Yes') >> if_timeoff_templateuri_present
        check_timeoff_template_mismatch >> rail.Label('No') >> if_manager_yes_and_timesheet_template_present

        if_timeoff_templateuri_present >> rail.Label('Yes') >> update_timeofftemplate >> if_manager_yes_and_timesheet_template_present
        if_timeoff_templateuri_present >> rail.Label('No') >> log_timeofftemplate_exception >> if_manager_yes_and_timesheet_template_present

        if_manager_yes_and_timesheet_template_present >> rail.Label('Yes') >> remove_policysettouser_timesheet_template_for_manager >> \
            get_timesheet_template_variable
        if_manager_yes_and_timesheet_template_present >> rail.Label('No') >> get_timesheet_template_variable

        get_timesheet_template_variable >> if_timesheettemplate_variable_is_yes

        if_timesheettemplate_variable_is_yes >> rail.Label('Yes') >> if_timesheet_template_mismatch
        if_timesheettemplate_variable_is_yes >> rail.Label('No') >> if_timesheet_approval_path_mismatch

        if_timesheet_template_mismatch >> rail.Label('Yes') >> if_timesheet_templateuri_present
        if_timesheet_template_mismatch >> rail.Label('No') >> if_timesheet_approval_path_mismatch

        if_timesheet_templateuri_present >> rail.Label('Yes') >> update_timesheettemplate >> if_timesheet_approval_path_mismatch
        if_timesheet_templateuri_present >> rail.Label('No') >> log_timesheettemplate_exception >> if_timesheet_approval_path_mismatch

        if_timesheet_approval_path_mismatch >> rail.Label('Yes') >> if_timesheet_approval_uri_present
        if_timesheet_approval_path_mismatch >> rail.Label('No') >> if_timeoff_approval_path_mismatch

        if_timesheet_approval_uri_present >> rail.Label('Yes') >> update_timesheetapproval >> if_timeoff_approval_path_mismatch
        if_timesheet_approval_uri_present >> rail.Label('No') >> log_timesheetapproval_exception >> if_timeoff_approval_path_mismatch

        if_timeoff_approval_path_mismatch >> rail.Label('Yes') >> if_timeoff_approval_uri_present
        if_timeoff_approval_path_mismatch >> rail.Label('No') >> if_timezone_is_present

        if_timeoff_approval_uri_present >> rail.Label('Yes') >> update_timeoffapproval >> if_timezone_is_present
        if_timeoff_approval_uri_present >> rail.Label('No') >> log_timeoffapproval_exception >> if_timezone_is_present

        if_timezone_is_present >> rail.Label('Yes') >> if_timezoneuri_is_present
        if_timezone_is_present >> rail.Label('No') >> if_workweek_mismatch

        if_timezoneuri_is_present >> rail.Label('Yes') >> if_timezone_mismatch
        if_timezoneuri_is_present >> rail.Label('No') >> log_timezone_exception >> if_workweek_mismatch

        if_timezone_mismatch >> rail.Label('Yes') >> update_timezone >> if_workweek_mismatch
        if_timezone_mismatch >> rail.Label('No') >> if_workweek_mismatch

        if_workweek_mismatch >> rail.Label('Yes') >> update_workweek_startday >> if_holiday_calender_mismatch
        if_workweek_mismatch >> rail.Label('No') >> if_holiday_calender_mismatch

        if_holiday_calender_mismatch >> rail.Label('Yes') >> if_holidaycalender_uri_present
        if_holiday_calender_mismatch >> rail.Label('No') >> write_log_user_import

        if_holidaycalender_uri_present >> rail.Label('Yes') >> update_holiday_calendar >> write_log_user_import
        if_holidaycalender_uri_present >> rail.Label('No') >> log_holiday_calendar_exception >> write_log_user_import

        write_log_user_import >> log_user_import >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
