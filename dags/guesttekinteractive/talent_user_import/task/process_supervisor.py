"""
Process Supervisor Task Group - GuestTek Talent User Import Integration

Reusable task group for processing supervisor assignments.
"""
import rail
from guesttekinteractive.talent_user_import.utils import request_payload, response_filters
from guesttekinteractive.talent_user_import import config as base_config

null = None

SUPERVISOR_OUTCOME_SUCCESS = 'success'
SUPERVISOR_OUTCOME_NOT_FOUND = 'not_found'
SUPERVISOR_OUTCOME_DISABLED = 'disabled'


def _get_supervisor_outcome():
    """Determine supervisor assignment outcome by inspecting task results."""
    supervisor_data = rail.result('search_supervisor_in_replicon')
    if not supervisor_data:
        return SUPERVISOR_OUTCOME_NOT_FOUND
    if not supervisor_data[0].get('userDetails', {}).get('isEnabled', False):
        return SUPERVISOR_OUTCOME_DISABLED
    return SUPERVISOR_OUTCOME_SUCCESS


def process_supervisor_assignment_task_group(user_result_key, user_type, config):
    """Create task group for processing supervisor assignment."""
    with rail.TaskGroup(group_id=f'process_supervisor_{user_type}', prefix_group_id=False) as supervisor_group:

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_supervisor_data_payload,
            data_handler=response_filters.get_filtered_user_data
        )

        has_supervisor_in_replicon = rail.IfOperator(
            task_id='has_supervisor_in_replicon',
            test="{{ result('search_supervisor_in_replicon') | is_truthy }}",
            yes_task='check_supervisor_status',
            no_task='supervisor_assignment_end'
        )

        check_supervisor_status = rail.IfOperator(
            task_id='check_supervisor_status',
            test=lambda: rail.result('search_supervisor_in_replicon')[0].get('userDetails', {}).get('isEnabled', False),
            yes_task='check_supervisor_permission',
            no_task='supervisor_assignment_end'
        )

        check_supervisor_permission = rail.IfOperator(
            task_id='check_supervisor_permission',
            test=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('search_supervisor_in_replicon')[0].get('permissionSets', []),
                'displayText',
                base_config.SUPERVISOR_PERMISSION,
                'uri'),
            yes_task='assign_supervisor',
            no_task='update_supervisor_permission'
        )

        update_supervisor_permission = rail.RepliconServiceOperator(
            task_id='update_supervisor_permission',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.get_update_supervisor_permission_payload
        )

        assign_supervisor = rail.RepliconServiceOperator(
            task_id='assign_supervisor',
            endpoint='/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange',
            data=lambda dag_run: request_payload.get_supervisor_assignment_payload(
                dag_run,
                rail.result('search_supervisor_in_replicon')[0]['userDetails']['uri'],
                user_uri=dag_run.conf.get('useruri') or rail.result(user_result_key).get('user', {}).get('uri')
            )
        )

        supervisor_assignment_end = rail.PythonOperator(
            task_id='supervisor_assignment_end',
            trigger_rule='none_failed_min_one_success',
            python_callable=_get_supervisor_outcome
        )

        search_supervisor_in_replicon >> has_supervisor_in_replicon
        has_supervisor_in_replicon >> [check_supervisor_status, supervisor_assignment_end]
        check_supervisor_status >> [check_supervisor_permission, supervisor_assignment_end]
        check_supervisor_permission >> [assign_supervisor, update_supervisor_permission]
        update_supervisor_permission >> assign_supervisor
        assign_supervisor >> supervisor_assignment_end

    return search_supervisor_in_replicon, supervisor_assignment_end
