from datetime import timedelta
import json
import rail
from mercury_systems_inc.user_import_v1.utils.response_filter import get_missing_permissions
from mercury_systems_inc.user_import_v1.utils import custom_methods

null = None
DATE_FORMAT = "%Y-%m-%d"


def process_supervisor_assignment_task_group(status):
    with rail.TaskGroup(group_id='process_supervisor_assignment_task', prefix_group_id=False):

        search_supervisor_in_replicon = rail.RepliconServiceOperator(
            task_id='search_supervisor_in_replicon',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": null,
                        "loginName": null,
                        "employeeId": "{{ dag_run.conf.Supervisor_ADP_Person_ID }}",
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: {
                'uri': res[0]['userDetails']['uri'],
                'employee_id': res[0]['userDetails']['employeeId'],
                'status': res[0]['userDetails']['isEnabled'],
                'end_date': (res[0]['userDetails']['employmentDateRange']['endDate']) if res[0]['userDetails']['employmentDateRange']['endDate'] else null
            } if res else []
        )

        is_supervisor_exists = rail.IfOperator(
            task_id='is_supervisor_exists',
            test=lambda: rail.result('search_supervisor_in_replicon') != [],
            yes_task='is_supervisor_end_date_in_past',
            no_task='log_supervisor_not_present'
        )

        log_supervisor_not_present = rail.PythonOperator(
            task_id='log_supervisor_not_present',
            python_callable=lambda: "Supervisor does not exist in Replicon"
        )

        is_supervisor_end_date_in_past = rail.IfOperator(
            task_id='is_supervisor_end_date_in_past',
            test=lambda dag_run: custom_methods.compare_dates(
                (dag_run.conf['Effective_Date'] if status != 'new_user' else dag_run.conf['integration_run_date']), '>', rail.result(
                    'search_supervisor_in_replicon')['end_date']) if rail.result('search_supervisor_in_replicon')['end_date'] else False,
            yes_task='log_supervisor_end_date_in_past',
            no_task='is_supervisor_disabled'
        )

        log_supervisor_end_date_in_past = rail.PythonOperator(
            task_id="log_supervisor_end_date_in_past",
            python_callable=lambda: "Supervisor end date is in past"
        )

        is_supervisor_disabled = rail.IfOperator(
            task_id='is_supervisor_disabled',
            test=lambda: not rail.result(
                'search_supervisor_in_replicon')['status'],
            yes_task='log_supervisor_is_disabled',
            no_task='get_missing_supervisor_permissions'
        )

        log_supervisor_is_disabled = rail.PythonOperator(
            task_id="log_supervisor_is_disabled",
            python_callable=lambda: "Supervisor Profile is disabled"
        )

        get_missing_supervisor_permissions = rail.RepliconServiceOperator(
            task_id='get_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').uri }}"
            },
            log_response=True,
            data_handler=get_missing_permissions
        )

        if_error_in_get_missing_supervisor_permissions = rail.IfOperator(
            task_id='if_error_in_get_missing_supervisor_permissions',
            test=lambda: "Error" in rail.result(
                'get_missing_supervisor_permissions'),
            yes_task='log_error_in_get_missing_supervisor_permissions',
            no_task='should_add_missing_permissions'
        )

        log_error_in_get_missing_supervisor_permissions = rail.PythonOperator(
            task_id='log_error_in_get_missing_supervisor_permissions',
            python_callable=lambda: "Permission set with name : 'Supervisor' not found in Replicon"
        )

        should_add_missing_permissions = rail.IfOperator(
            task_id='should_add_missing_permissions',
            test=lambda: "urn" in json.dumps(
                rail.result('get_missing_supervisor_permissions')),
            yes_task='add_missing_supervisor_permissions',
            no_task='is_new_user_supervisor_assignment'
        )

        add_missing_supervisor_permissions = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_missing_supervisor_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            items=lambda: rail.result('get_missing_supervisor_permissions'),
            execution_timeout=timedelta(days=14),
            data={
                'userUri': "{{ result('search_supervisor_in_replicon').uri }}",
                'permissionSetUri': '{{ item }}'
            }
        )

        is_new_user_supervisor_assignment = rail.IfOperator(
            task_id='is_new_user_supervisor_assignment',
            test=status == 'new_user',
            yes_task='get_final_result_from_supervisor_assignment_workflow',
            no_task='get_effective_supervisor_of_user'
        )

        get_effective_supervisor_of_user = rail.RepliconServiceOperator(
            task_id="get_effective_supervisor_of_user",
            endpoint='/services/UserService1.svc/GetSupervisorAssignmentDetails',
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "asOfDate": rail.parse_date(dag_run.conf['integration_run_date'], DATE_FORMAT)
            }
        )

        is_supervisor_changed = rail.IfOperator(
            task_id='is_supervisor_changed',
            test=lambda: (rail.result('search_supervisor_in_replicon')['uri'] != rail.result(
                'get_effective_supervisor_of_user')['supervisor']['user']['uri']) if (
                    rail.result('get_effective_supervisor_of_user') and rail.result('search_supervisor_in_replicon')) else True,
            yes_task='get_final_result_from_supervisor_assignment_workflow',
            no_task='same_supervisor_already_assigned'
        )

        same_supervisor_already_assigned = rail.PythonOperator(
            task_id="same_supervisor_already_assigned",
            python_callable=lambda: "Same supervisor already assigned"
        )

        get_final_result_from_supervisor_assignment_workflow = rail.PythonOperator(
            task_id="get_final_result_from_supervisor_assignment_workflow",
            python_callable=custom_methods.final_result_from_sup_assignment_workflow
        )

        finish_process_supervisor = rail.EmptyOperator(
            task_id="finish_process_supervisor",
        )

        search_supervisor_in_replicon >> is_supervisor_exists >> rail.Label(
            'No') >> log_supervisor_not_present >> get_final_result_from_supervisor_assignment_workflow
        is_supervisor_exists >> rail.Label(
            'Yes') >> is_supervisor_end_date_in_past >> rail.Label('No') >> is_supervisor_disabled

        is_supervisor_end_date_in_past >> rail.Label(
            'Yes') >> log_supervisor_end_date_in_past >> get_final_result_from_supervisor_assignment_workflow

        is_supervisor_disabled >> rail.Label(
            'No') >> get_missing_supervisor_permissions
        is_supervisor_disabled >> rail.Label(
            'Yes') >> log_supervisor_is_disabled >> get_final_result_from_supervisor_assignment_workflow

        get_missing_supervisor_permissions >> if_error_in_get_missing_supervisor_permissions

        if_error_in_get_missing_supervisor_permissions >> rail.Label(
            'No') >> should_add_missing_permissions
        if_error_in_get_missing_supervisor_permissions >> rail.Label(
            'Yes') >> log_error_in_get_missing_supervisor_permissions >> get_final_result_from_supervisor_assignment_workflow

        should_add_missing_permissions >> rail.Label(
            'Yes') >> add_missing_supervisor_permissions >> is_new_user_supervisor_assignment

        should_add_missing_permissions >> rail.Label(
            'No') >> is_new_user_supervisor_assignment

        is_new_user_supervisor_assignment >> rail.Label(
            'Yes') >> get_final_result_from_supervisor_assignment_workflow >> finish_process_supervisor
        is_new_user_supervisor_assignment >> rail.Label(
            'No') >> get_effective_supervisor_of_user >> is_supervisor_changed

        is_supervisor_changed >> rail.Label(
            'Yes') >> get_final_result_from_supervisor_assignment_workflow
        is_supervisor_changed >> rail.Label(
            'No') >> same_supervisor_already_assigned >> get_final_result_from_supervisor_assignment_workflow
        get_final_result_from_supervisor_assignment_workflow >> finish_process_supervisor

    return search_supervisor_in_replicon, finish_process_supervisor
