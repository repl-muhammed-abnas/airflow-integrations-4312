from datetime import timedelta
from airflow.models import Variable
import rail

from valleychildrens.user_import.utils import request_payload, response_filter

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_user_update_dagid,
        description='ValleyChildrens User Import - Process User Update',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_user_update,
    ) as dag:
        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_existing_user',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_existing_user',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_existing_user = rail.RepliconServiceOperator(
            task_id='get_existing_user',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                'users': [{
                    'loginName': dag_run.conf.get('existing_login_name') or dag_run.conf['loginname'],
                    'uri': dag_run.conf.get('useruri'),
                    'employeeId': null,
                    'parameterCorrelationId': null,
                }],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission',
            },
            data_handler=response_filter.first_or_none,
        )

        get_user_group_membership = rail.RepliconServiceOperator(
            task_id='get_user_group_membership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data=lambda dag_run: {'userUri': dag_run.conf['useruri'], 'dateRange': null}
        )

        is_disabled = rail.IfOperator(
            task_id='is_disabled',
            test=lambda dag_run: str(dag_run.conf.get('existing_user_status') or '').lower() == 'disabled',
            yes_task='enable_login',
            no_task='check_should_stop',
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityService1.svc/EnableLogin',
            data=lambda dag_run: {'userUri': dag_run.conf['useruri']},
        )

        check_should_stop = rail.IfOperator(
            task_id='check_should_stop',
            test=lambda dag_run: not request_payload.test_valid_fields(dag_run),
            yes_task='log_invalid_required_fields',
            no_task='update_employment_date_range_start',
        )

        log_invalid_required_fields = rail.WriteLogOperator(
            task_id='log_invalid_required_fields',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Exception',
            message=lambda dag_run: request_payload.get_mandatory_fields_exception_message(dict(dag_run.conf)),
            properties=lambda dag_run: {
                'employee_id': dag_run.conf.get('employeeid', ''),
                'first_name': dag_run.conf.get('firstname', ''),
                'last_name': dag_run.conf.get('lastname', ''),
                'action': 'Update',
                'status': 'Exception',
                'details': request_payload.get_mandatory_fields_exception_message(dict(dag_run.conf)),
            },
        )

        update_employment_date_range_start = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_start',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'dateRange': {
                    'startDate': request_payload.to_date_struct(dag_run.conf.get('startdate')),
                    'endDate': request_payload.to_date_struct(dag_run.conf.get('existing_end_date')),
                },
            },
        )

        update_employment_date_range_end = rail.RepliconServiceOperator(
            task_id='update_employment_date_range_end',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'dateRange': {
                    'startDate': request_payload.to_date_struct(dag_run.conf.get('startdate')),
                    'endDate': request_payload.to_date_struct(dag_run.conf.get('enddate')),
                },
            },
        )

        is_login_changed = rail.IfOperator(
            task_id='is_login_changed',
            test=lambda dag_run: bool(dag_run.conf.get('existing_login_name'))
                and dag_run.conf['loginname'] != dag_run.conf.get('existing_login_name'),
            yes_task='update_login_name',
            no_task='is_first_name_changed',
        )

        update_login_name = rail.RepliconServiceOperator(
            task_id='update_login_name',
            endpoint='/services/securityService1.svc/SetSSOAuthenticationForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'loginName': dag_run.conf['loginname'],
            },
        )

        is_first_name_changed = rail.IfOperator(
            task_id='is_first_name_changed',
            test=lambda dag_run: bool(dag_run.conf.get('firstname')),
            yes_task='update_first_name',
            no_task='is_last_name_changed',
        )

        update_first_name = rail.RepliconServiceOperator(
            task_id='update_first_name',
            endpoint='/services/userService1.svc/UpdateFirstName',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'firstname': dag_run.conf['firstname'],
            },
        )

        is_last_name_changed = rail.IfOperator(
            task_id='is_last_name_changed',
            test=lambda dag_run: bool(dag_run.conf.get('lastname')),
            yes_task='update_last_name',
            no_task='is_email_changed',
        )

        update_last_name = rail.RepliconServiceOperator(
            task_id='update_last_name',
            endpoint='/services/userService1.svc/UpdateLastName',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'lastname': dag_run.conf['lastname'],
            },
        )

        is_email_changed = rail.IfOperator(
            task_id='is_email_changed',
            test=lambda dag_run: bool(dag_run.conf.get('email')),
            yes_task='update_email',
            no_task='update_fte_value',
        )

        update_email = rail.RepliconServiceOperator(
            task_id='update_email',
            endpoint='/services/userService1.svc/UpdateEmail',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'email': dag_run.conf['email'],
            },
        )

        update_fte_value = rail.RepliconServiceOperator(
            task_id='update_fte_value',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            data=lambda dag_run: {
                'objectUri': dag_run.conf['useruri'],
                'customFieldUri': dag_run.conf.get('fte_field_uri'),
                'value': dag_run.conf.get('ftetotal'),
            },
        )

        update_fte_effective_date = rail.RepliconServiceOperator(
            task_id='update_fte_effective_date',
            endpoint='/services/CustomFieldService1.svc/UpdateDateValue',
            data=lambda dag_run: {
                'objectUri': dag_run.conf['useruri'],
                'customFieldUri': dag_run.conf.get('fte_effective_date_field_uri'),
                'value': request_payload.today_date_struct(config.pacific_timezone),
            },
        )

        update_cme_entitlement = rail.RepliconServiceOperator(
            task_id='update_cme_entitlement',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data=lambda dag_run: {
                'objectUri': dag_run.conf['useruri'],
                'customFieldUri': dag_run.conf.get('cmeentitlement_field_uri'),
                'customFieldDropDownOptionUri': dag_run.conf.get('cmeentitlement_option_uri'),
            },
        )

        update_employee_classification = rail.RepliconServiceOperator(
            task_id='update_employee_classification',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data=lambda dag_run: {
                'objectUri': dag_run.conf['useruri'],
                'customFieldUri': dag_run.conf.get('employee_classification_field_uri'),
                'customFieldDropDownOptionUri': dag_run.conf.get('employee_classification_option_uri'),
            },
        )

        update_timesheet_period = rail.RepliconServiceOperator(
            task_id='update_timesheet_period',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
                'modifications': [{'timesheetPeriodUri': dag_run.conf.get('timesheetperioduri')}],
            },
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id='update_office_schedule',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
                'modifications': [{'officeScheduleUri': dag_run.conf.get('officescheduleuri')}],
            },
        )

        update_department = rail.RepliconServiceOperator(
            task_id='update_department',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
                'modifications': [{'departmentUri': dag_run.conf.get('departmenturi')}],
            },
        )

        update_activity_assignments = rail.RepliconServiceOperator(
            task_id='update_activity_assignments',
            endpoint='/services/ActivityService1.svc/UpdateActivityAssignmentsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'activityUris': dag_run.conf.get('activities') or [],
            },
        )

        update_employee_type = rail.RepliconServiceOperator(
            task_id='update_employee_type',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
                'modifications': [{'employeeTypeUri': dag_run.conf.get('employeetypeuri')}],
            },
        )

        update_service_center = rail.RepliconServiceOperator(
            task_id='update_service_center',
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=lambda dag_run: {
                'user': {'uri': dag_run.conf['useruri']},
                'modifications': [{'serviceCenterUri': dag_run.conf.get('companyuri')}],
            },
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data={},
        )

        has_supervisor_input = rail.IfOperator(
            task_id='has_supervisor_input',
            test=lambda dag_run: bool(dag_run.conf.get('supid')) or bool(dag_run.conf.get('supname')),
            yes_task='get_supervisor_user',
            no_task='assign_timeoff_template',
        )

        get_supervisor_user = rail.RepliconServiceOperator(
            task_id='get_supervisor_user',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=lambda dag_run: {
                'users': [{
                    'employeeId': dag_run.conf.get('supid'),
                    'loginName': null,
                    'uri': null,
                    'parameterCorrelationId': null,
                }],
                'dataLoadOptionUri': 'urn:replicon:data-load-option:omit-data-if-insufficient-access-permission',
            },
            data_handler=response_filter.first_or_none,
        )

        get_existing_supervisor_assignment = rail.RepliconServiceOperator(
            task_id='get_existing_supervisor_assignment',
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {'user': {'uri': dag_run.conf['useruri']}},
        )

        log_pending_supervisor_update = rail.WriteLogOperator(
            task_id='log_pending_supervisor_update',
            log='{{ dag_run.conf["supervisor_log_id"] }}',
            severity='Pending',
            message=lambda dag_run: f"Supervisor update pending for employee {dag_run.conf['employeeid']}",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'sup_id': dag_run.conf.get('supid'),
                'sup_name': dag_run.conf.get('supname'),
                'useruri': dag_run.conf['useruri'],
                'supervisorpermissionuri': dag_run.conf.get('supervisorpermissionuri'),
                'userpermissionuri': dag_run.conf.get('userpermissionuri'),
            },
        )

        assign_timeoff_template = rail.RepliconServiceOperator(
            task_id='assign_timeoff_template',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'policySetUri': dag_run.conf.get('timeofftemplateuri'),
            },
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id='assign_timesheet_template',
            endpoint='/services/PolicySetService1.svc/AssignPolicySetToUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'policySetUri': dag_run.conf.get('timesheettemplateuri'),
            },
        )

        update_timesheet_approval = rail.RepliconServiceOperator(
            task_id='update_timesheet_approval',
            endpoint='/services/TimesheetApprovalService1.svc/UpdateApprovalPathForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'approvalPathUri': dag_run.conf.get('timesheetapprovaluri'),
            },
        )

        update_timeoff_approval = rail.RepliconServiceOperator(
            task_id='update_timeoff_approval',
            endpoint='/services/TimeOffApprovalService1.svc/UpdateApprovalPathForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'approvalPathUri': dag_run.conf.get('timeoffapprovaluri'),
            },
        )

        is_work_week_present = rail.IfOperator(
            task_id='is_work_week_present',
            test=lambda dag_run: bool(dag_run.conf.get('workweekstartdayuri')),
            yes_task='update_work_week',
            no_task='update_holiday_calendar',
        )

        update_work_week = rail.RepliconServiceOperator(
            task_id='update_work_week',
            endpoint='/services/UserService1.svc/UpdateWorkWeekStartDayForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'dayOfWeekUri': dag_run.conf.get('workweekstartdayuri'),
            },
        )

        # is_holiday_calendar_present = rail.IfOperator(
        #     task_id='is_holiday_calendar_present',
        #     test=lambda dag_run: bool(dag_run.conf.get('holidaycalendaruri')),
        #     yes_task='update_holiday_calendar',
        #     no_task='is_rehire',
        # )

        update_holiday_calendar = rail.RepliconServiceOperator(
            task_id='update_holiday_calendar',
            endpoint='/services/HolidayCalendarService1.svc/UpdateHolidayCalendarForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'holidayCalendarUri': dag_run.conf.get('holidaycalendaruri'),
            },
        )

        is_rehire = rail.IfOperator(
            task_id='is_rehire',
            test=lambda dag_run: bool(dag_run.conf.get('existing_end_date'))
                and not bool(dag_run.conf.get('enddate')),
            yes_task='trigger_rehire_update_user_time_off_assign',
            no_task='trigger_update_user_time_off_assign',
        )

        trigger_update_user_time_off_assign = rail.TriggerDagRunOperator(
            task_id='trigger_update_user_time_off_assign',
            trigger_dag_id=config.process_update_user_time_off_assign_dagid,
            conf=lambda dag_run: request_payload.get_process_update_user_time_off_assign_conf(
                dict(dag_run.conf), config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_update_user_time_off_assign = rail.WaitForDagRunsSensor(
            task_id='wait_update_user_time_off_assign',
            dag_runs="{{ result('trigger_update_user_time_off_assign') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        trigger_rehire_update_user_time_off_assign = rail.TriggerDagRunOperator(
            task_id='trigger_rehire_update_user_time_off_assign',
            trigger_dag_id=config.process_rehire_update_user_time_off_assign_dagid,
            conf=lambda dag_run: request_payload.get_process_update_user_time_off_assign_conf(
                dict(dag_run.conf), config, dag_run.conf.get('log_id')),
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0,
        )

        wait_rehire_update_user_time_off_assign = rail.WaitForDagRunsSensor(
            task_id='wait_rehire_update_user_time_off_assign',
            dag_runs="{{ result('trigger_rehire_update_user_time_off_assign') }}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Info',
            message=lambda dag_run: f"User {dag_run.conf['loginname']} updated",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Update',
                'status': 'Success',
                'details': f"User {dag_run.conf['loginname']} updated",
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ dag_run.conf["log_id"] }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['employeeid'],
                'first_name': dag_run.conf['firstname'],
                'last_name': dag_run.conf['lastname'],
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
            },
        )
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_existing_user
        get_existing_user >> get_user_group_membership >> is_disabled
        is_disabled >> rail.Label('Yes') >> enable_login >> check_should_stop
        is_disabled >> rail.Label('No') >> check_should_stop
        check_should_stop >> rail.Label('Yes') >> log_invalid_required_fields
        check_should_stop >> rail.Label('No') >> update_employment_date_range_start \
            >> update_employment_date_range_end >> is_login_changed
        is_login_changed >> rail.Label('Yes') >> update_login_name >> is_first_name_changed
        is_login_changed >> rail.Label('No') >> is_first_name_changed
        is_first_name_changed >> rail.Label('Yes') >> update_first_name >> is_last_name_changed
        is_first_name_changed >> rail.Label('No') >> is_last_name_changed
        is_last_name_changed >> rail.Label('Yes') >> update_last_name >> is_email_changed
        is_last_name_changed >> rail.Label('No') >> is_email_changed
        is_email_changed >> rail.Label('Yes') >> update_email >> update_fte_value
        is_email_changed >> rail.Label('No') >> update_fte_value
        update_fte_value >> update_fte_effective_date >> update_cme_entitlement
        update_cme_entitlement >> update_employee_classification >> update_timesheet_period
        update_timesheet_period >> update_office_schedule
        update_office_schedule >> update_department >> update_activity_assignments
        update_activity_assignments >> update_employee_type >> update_service_center
        update_service_center >> get_all_permission_sets >> has_supervisor_input
        has_supervisor_input >> rail.Label('Yes') >> get_supervisor_user >> get_existing_supervisor_assignment \
            >> log_pending_supervisor_update >> assign_timeoff_template
        has_supervisor_input >> rail.Label('No') >> assign_timeoff_template
        assign_timeoff_template >> assign_timesheet_template >> update_timesheet_approval \
            >> update_timeoff_approval >> is_work_week_present
        is_work_week_present >> rail.Label("Yes") >> update_work_week >> update_holiday_calendar
        is_work_week_present >> rail.Label("No") >> update_holiday_calendar

        update_holiday_calendar >> is_rehire
        is_rehire >> rail.Label('Yes') >> trigger_rehire_update_user_time_off_assign \
            >> wait_rehire_update_user_time_off_assign >> log_success
        is_rehire >> rail.Label('No') >> trigger_update_user_time_off_assign \
            >> wait_update_user_time_off_assign >> log_success
        log_success >> catch_and_log_error
    return dag

rail.for_each_instance(create_child_dag)
