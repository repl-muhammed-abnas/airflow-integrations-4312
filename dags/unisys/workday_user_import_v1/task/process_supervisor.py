"""
Process Supervisor Assignment Task Group - Unisys Workday User Import

Handles supervisor assignment and validation for users.
This reusable task group manages the complete supervisor assignment workflow including
supervisor search, validation, permission assignment, and schedule updates.

Key features:
    - Searches for supervisor in Replicon by employee ID
    - Validates supervisor existence and status
    - Checks supervisor end date validity
    - Enables disabled supervisor logins when appropriate
    - Assigns supervisor permissions if missing
    - Detects supervisor changes for existing users
    - Updates supervisor assignment schedules
    - Logs exceptions for invalid supervisors
    - Handles both new user and update scenarios

Functions:
    process_supervisor_assignment_task_group(user_uri, status, config): Creates supervisor task group
"""
import rail
from unisys.workday_user_import_v1.utils.request_payload import get_supervisor_data_payload, get_today_date, get_replicon_date
from unisys.workday_user_import_v1.utils.response_filters import map_supervisor_list_data, is_assign_supervisorpermission

def process_supervisor_assignment_task_group(user_uri, status, config):
    """
    Create task group for processing supervisor assignments.

    This task group handles complete supervisor validation and assignment logic,
    including supervisor existence checks, status validation, permission management,
    and schedule updates. Works for both new user creation and user updates.

    Args:
        user_uri (str): Key in dag_run.conf containing user URI (e.g., 'useruri' or 'add_new_user')
        status (str): Processing context - either 'new_user' or 'update_user'
        config: Configuration object (not currently used but available for future needs)

    Returns:
        tuple: (entry_task, exit_task)
            - entry_task: First task in the group (search_supervisor_in_replicon)
            - exit_task: Last task in the group (finish_process_supervisor)

    Replicon API Endpoints Used:
        - /services/ImportService1.svc/BulkGetUsers3: Search for supervisor
        - /services/securityservice1.svc/EnableLogin: Enable supervisor login
        - /services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2: Check permissions
        - /services/PermissionSetService1.svc/AssignPermissionSetToUser: Assign supervisor permission
        - /services/UserService1.svc/GetSupervisorAssignmentDetails: Get current supervisor
        - /services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange: Update supervisor

    DAG Configuration Requirements:
        dag_run.conf should contain:
            - supervisor_id: Supervisor's employee ID
            - last_name: User's last name (for logging)
            - first_name: User's first name (for logging)
            - email: User's email (for logging)
            - employee_id: User's employee ID (for logging)
            - change_effective_date: Effective date for changes
            - supervisor_permission_uri: URI of supervisor permission set
            - supervisor_log: Log artifact for tracking operations
            - {user_uri}: User URI (key name depends on user_uri parameter)

    Task Group Results:
        - search_supervisor_in_replicon: Supervisor user data including URI, status, end date
        - get_effective_supervisor_of_user: Current supervisor assignment (for updates)

    Logging:
        Logs supervisor-related exceptions to supervisor_log including:
        - Supervisor not found in Replicon
        - Supervisor disabled with past end date
    """
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=get_supervisor_data_payload,
            data_handler=map_supervisor_list_data
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: bool(rail.result('search_supervisor_in_replicon')),
            yes_task='is_supervisor_disabled',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.WriteLogOperator(
            task_id='log_supervisor_not_present',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor not found in Replicon",
            severity='Pending',
            properties= lambda dag_run:{
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['login_name'],
                'employeeid': dag_run.conf['employee_id'],
                'effectivedate': dag_run.conf['change_effective_date'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                'manager': dag_run.conf['supervisor_id'],
                "userstatus": dag_run.conf['user_status'],
                "co_costcenter": dag_run.conf['cost_center_description'],
                "location": dag_run.conf['location_description'],
                'action':'Add' if status == "new_user" else 'Update',
                'status':'Pending',
                'details':'Supervisor not found in Replicon'
            }
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: not rail.result('search_supervisor_in_replicon')['status'],
            yes_task='is_enddate_in_past',
            no_task='get_missing_supervisor_permission'
        )

        is_enddate_in_past = rail.IfOperator(
            task_id='is_enddate_in_past',
            test=lambda: rail.result('search_supervisor_in_replicon')['is_enddate_in_past'],
            yes_task='log_supervisor_disabled_with_past_enddate',
            no_task='enable_supervisor_login'
        )

        log_supervisor_disabled_with_past_enddate = rail.WriteLogOperator(
            task_id='log_supervisor_disabled_with_past_enddate',
            log='{{ dag_run.conf.supervisor_log }}',
            message="Supervisor disabled with past enddate",
            severity='Exception',
            properties= lambda dag_run:{
                'lastname': dag_run.conf['last_name'],
                'firstname': dag_run.conf['first_name'],
                'loginname':  dag_run.conf['login_name'],
                'employeeid': dag_run.conf['employee_id'],
                'effectivedate': dag_run.conf['change_effective_date'],
                'useruri': dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                'manager': dag_run.conf['supervisor_id'],
                "userstatus": dag_run.conf['user_status'],
                "co_costcenter": dag_run.conf['cost_center_description'],
                "location": dag_run.conf['location_description'],
                'action': 'Add' if status == "new_user" else 'Update',
                'status': 'Exception',
                'details':'Supervisor disabled with past enddate'
            }
        )

        enable_supervisor_login = rail.RepliconServiceOperator(
            task_id='enable_supervisor_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ result('search_supervisor_in_replicon').uri }}"
            }
        )

        get_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {
                'userUri': rail.result('search_supervisor_in_replicon')['uri']
            },
            data_handler=is_assign_supervisorpermission
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test="{{ result('get_missing_supervisor_permission') | is_truthy }}",
            yes_task='add_missing_supervisor_permission',
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permission = rail.RepliconServiceOperator(
            task_id='add_missing_supervisor_permission',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data=lambda dag_run: {
                'userUri': rail.result('search_supervisor_in_replicon')['uri'],
                'permissionSetUri': dag_run.conf['supervisor_permission_uri']
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='update_supervisor_for_user',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user  = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                "asOfDate": get_today_date()
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: rail.result('search_supervisor_in_replicon')['loginname'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['loginName']
            if rail.result('get_effective_supervisor_of_user') and rail.result('search_supervisor_in_replicon') else True,
            yes_task='update_supervisor_for_user',
            no_task='same_supervisor_already_assigned'
        )

        same_supervisor_already_assigned = rail.EmptyOperator(
            task_id="same_supervisor_already_assigned",
        )

        update_supervisor_for_user = rail.RepliconServiceOperator(
            task_id="update_supervisor_for_user",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf[user_uri] if status != 'new_user' else rail.result(user_uri)['user']['uri'],
                "supervisorUri": rail.result('search_supervisor_in_replicon')['uri'],
                "dateRange": None if status == 'new_user' else {
                    "startDate": get_today_date()
                }
            }
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> finish_process_supervisor

        is_supervisor_disabled >> rail.Label(
            'Yes') >> is_enddate_in_past >> rail.Label(
            "Yes") >> log_supervisor_disabled_with_past_enddate >> finish_process_supervisor
        is_enddate_in_past >> rail.Label(
            "No") >> enable_supervisor_login >> get_missing_supervisor_permission
        
        is_supervisor_exists >> rail.Label('Yes') >>  is_supervisor_disabled >> rail.Label('No') >> get_missing_supervisor_permission

        get_missing_supervisor_permission >> should_add_missing_permissions >> rail.Label('Yes') >> add_missing_supervisor_permission
        should_add_missing_permissions >> rail.Label('No') >> is_new_user_supervisor_assignment

        add_missing_supervisor_permission >> is_new_user_supervisor_assignment >> rail.Label('Yes') >> update_supervisor_for_user
        is_new_user_supervisor_assignment >> rail.Label('No') >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label(
            'Yes') >> update_supervisor_for_user >> finish_process_supervisor
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> finish_process_supervisor
        is_supervisor_disabled >> rail.Label(
            'Yes') >> is_enddate_in_past >> rail.Label(
            "Yes") >> log_supervisor_disabled_with_past_enddate >> finish_process_supervisor
        is_enddate_in_past >> rail.Label(
            "No") >> enable_supervisor_login >> get_missing_supervisor_permission

    return search_supervisor_in_replicon, finish_process_supervisor
