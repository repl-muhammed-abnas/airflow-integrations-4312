import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload



def supervisor_assignment(caller, can_queue_assignment=True):
    with rail.TaskGroup(group_id=f'supervisor_assignment', prefix_group_id=False) as supervisor_assignment:

        if_workermanager_present = rail.IfOperator(
            task_id='if_workermanager_present',
            test=lambda dag_run :bool(dag_run.conf['workermanager']),
            yes_task="if_supervisorid_not_useremployeeid",
            no_task="supervisor_assignement_end",
        )

        if_supervisorid_not_useremployeeid = rail.IfOperator(
            task_id='if_supervisorid_not_useremployeeid',
            test=lambda dag_run :bool(dag_run.conf['workermanager'] != dag_run.conf['employeeid']),
            yes_task="search_for_supervisor",
            no_task="log_supervisor_same_as_user",
        )

        search_for_supervisor = rail.RepliconServiceOperator(
            task_id='search_for_supervisor',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data = {
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.workermanager }}"
                    }
                ]
            }
        )

        if_supervisor_uri_present_and_enabled = rail.IfOperator(
            task_id='if_supervisor_uri_present_and_enabled',
            test='''{{ result('search_for_supervisor') | is_truthy and \
                result('search_for_supervisor')[0].userDetails.uri | is_truthy and \
                    result('search_for_supervisor')[0].userDetails.isEnabled | lower == 'true' }}''',
            yes_task="get_assigned_permissionset_foruser",
            no_task="can_queue_supervisor_assignment",
        )

        get_assigned_permissionset_foruser = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data = {
                "userUri": "{{ result('search_for_supervisor')[0].userDetails.uri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:supervision', 'permissionSet.uri')
        )

        if_supervisor_permissionset_present = rail.IfOperator(
            task_id='if_supervisor_permissionset_present',
            test='''{{ result('get_assigned_permissionset_foruser') | is_truthy }}''',
            yes_task="assign_supervisor",
            no_task="assign_supervisorpermissionset_foruser",
        )

        assign_supervisorpermissionset_foruser = rail.RepliconServiceOperator(
            task_id='assign_supervisorpermissionset_foruser',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data = {
                "userUri": "{{ result('search_for_supervisor')[0].userDetails.uri }}",
                "permissionSetUri":"{{ dag_run.conf.supervisor_assignment_permission }}"
            }
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data = request_payload.get_supervisor_assign_payload
        )

        can_queue_supervisor_assignment = rail.IfOperator(
            task_id='can_queue_supervisor_assignment',
            test=lambda: can_queue_assignment,
            yes_task='log_supervisor_lookup',
            no_task='log_supervisor_assign_skipped'
        )

        log_supervisor_assign_skipped = rail.WriteLogOperator(
            task_id="log_supervisor_assign_skipped",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Skipped",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "action": caller,
                "status": "Exception",
                "details": "Supervisor not assigned since not available or disabled"
            }
        )

        log_supervisor_lookup = rail.WriteLogOperator(
            task_id="log_supervisor_lookup",
            log = '{{ dag_run.conf.supervisor_logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "workermanager": dag_run.conf['workermanager'],
                "employeeid": dag_run.conf['employeeid'],
                "caller": caller,
                "useruri": dag_run.conf['useruri'] if 'useruri' in dag_run.conf else rail.result("create_user")['user']['uri']
            }
        )

        log_supervisor_same_as_user = rail.WriteLogOperator(
            task_id="log_supervisor_same_as_user",
            log = '{{ dag_run.conf.logger}}',
            message="Exception",
            severity="Exception",
            properties=lambda dag_run: {
                "employeeid": dag_run.conf['employeeid'],
                "action": caller,
                "status": "Exception",
                "details": "Worker Manager Id and user EmployeeID are same hence not assigned."
            }
        )

        supervisor_assignement_end = rail.EmptyOperator(
            task_id = 'supervisor_assignement_end'
        )

        if_workermanager_present

        if_workermanager_present >> rail.Label('Yes') >> if_supervisorid_not_useremployeeid
        if_workermanager_present >> rail.Label('No') >> supervisor_assignement_end

        if_supervisorid_not_useremployeeid >> rail.Label('Yes') >> search_for_supervisor >> if_supervisor_uri_present_and_enabled
        if_supervisorid_not_useremployeeid >> rail.Label('No') >> log_supervisor_same_as_user >> supervisor_assignement_end

        if_supervisor_uri_present_and_enabled >> rail.Label('Yes') >> get_assigned_permissionset_foruser >> if_supervisor_permissionset_present
        if_supervisor_uri_present_and_enabled >> rail.Label('No') >> can_queue_supervisor_assignment

        can_queue_supervisor_assignment >> rail.Label('Yes') >>  log_supervisor_lookup >> supervisor_assignement_end
        can_queue_supervisor_assignment >> rail.Label('No') >>  log_supervisor_assign_skipped >> supervisor_assignement_end

        if_supervisor_permissionset_present >> rail.Label('Yes') >> assign_supervisor >> supervisor_assignement_end
        if_supervisor_permissionset_present >> rail.Label('No') >> assign_supervisorpermissionset_foruser >> \
            assign_supervisor >> supervisor_assignement_end

        return supervisor_assignment
