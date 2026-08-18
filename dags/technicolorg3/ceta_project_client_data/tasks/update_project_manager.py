import rail
from technicolorg3.ceta_project_client_data.utils import request_payload
from technicolorg3.ceta_project_client_data.utils import response_filter

null = None


def get_update_project_manager_id(caller):
    with rail.TaskGroup(group_id=f'update_project_manager_id_group_{caller}', prefix_group_id=False) as update_project_manager_id_group:

        check_projectmanagerid_present = rail.IfOperator(
            task_id=f'check_projectmanagerid_present_{caller}',
            test=lambda dag_run: bool(dag_run.conf['projectmanagerid']),
            yes_task=f'search_users_{caller}',
            no_task=f'add_projectmanagerid_blank_exception_{caller}'
        )

        search_users = rail.RepliconServiceOperator(
            task_id=f'search_users_{caller}',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.get_search_users_payload,
            data_handler=response_filter.get_users_details
        )

        is_user_present = rail.IfOperator(
            task_id=f'is_user_present_{caller}',
            test=lambda: bool(rail.result(f'search_users_{caller}')),
            yes_task=f'is_user_enabled_{caller}',
            no_task=f'add_projectmanagerid_notfound_exception_{caller}'
        )

        is_user_enabled = rail.IfOperator(
            task_id=f'is_user_enabled_{caller}',
            test=lambda: bool(rail.result(f'search_users_{caller}') and rail.result(f'search_users_{caller}')
                              [0]['status'] == 'True'),
            yes_task=f'get_permissions_assigned_to_user_{caller}',
            no_task=f'add_projectmanagerid_disabled_exception_{caller}'
        )

        get_permissions_assigned_to_user = rail.RepliconServiceOperator(
            task_id=f'get_permissions_assigned_to_user_{caller}',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=lambda: {'userUri': rail.result(
                f'search_users_{caller}')[0]['uri']},
            data_handler=lambda data: rail.result(
                f'search_users_{caller}')[0]['uri']
            if rail.find_first_by_attr_and_get_attr(data, 'policyUri', 'urn:replicon:policy:project-management', 'policyUri') else null
        )

        is_user_project_manager = rail.IfOperator(
            task_id=f'is_user_project_manager_{caller}',
            test=lambda: bool(rail.result(
                f'get_permissions_assigned_to_user_{caller}')),
            yes_task=f'is_add_or_update_project_{caller}',
            no_task=f'add_required_permission_notfound_exception_{caller}'
        )

        is_add_or_update_project = rail.IfOperator(
            task_id=f'is_add_or_update_project_{caller}',
            test=lambda: bool(caller == 'add_project'),
            yes_task=f'update_project_manager_{caller}',
            no_task=f'update_project_leader_{caller}'
        )

        def add_project_manager():
            return {
                "user": {
                    "uri": rail.result(f'get_permissions_assigned_to_user_{caller}'),
                    "loginName": null,
                    "parameterCorrelationId": null
                }
            }

        update_project_manager = rail.PythonOperator(
            task_id=f'update_project_manager_{caller}',
            python_callable=add_project_manager
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id=f'update_project_leader_{caller}',
            endpoint='/services/ProjectService1.svc/UpdateProjectLeader',
            data=lambda dag_run: {
                'projectUri': dag_run.conf['projecturi'],
                'userUri': rail.result(f'search_users_{caller}')[0]['uri']
            }
        )

        add_required_permission_notfound_exception = rail.PythonOperator(
            task_id=f'add_required_permission_notfound_exception_{caller}',
            # pylint: disable=line-too-long
            python_callable=lambda dag_run: f'Project Manager with global ID { dag_run.conf["projectmanagerid"] } does not have project manager permission in Replicon hence no project manager assigned'
        )

        add_projectmanagerid_disabled_exception = rail.PythonOperator(
            task_id=f'add_projectmanagerid_disabled_exception_{caller}',
            python_callable=lambda dag_run: f'PM with global ID { dag_run.conf["projectmanagerid"] } is disabled in Replicon hence no PM assigned'
        )

        add_projectmanagerid_notfound_exception = rail.PythonOperator(
            task_id=f'add_projectmanagerid_notfound_exception_{caller}',
            python_callable=lambda dag_run: f'PM with global ID { dag_run.conf["projectmanagerid"] } not found hence no PM assigned'
        )

        add_projectmanagerid_blank_exception = rail.PythonOperator(
            task_id=f'add_projectmanagerid_blank_exception_{caller}',
            python_callable=lambda: 'Project manager global ID received blank hence no PM assigned'
        )

        check_projectmanagerid_present >> rail.Label(
            'Yes') >> search_users >> is_user_present
        check_projectmanagerid_present >> rail.Label(
            'No') >> add_projectmanagerid_blank_exception

        is_user_present >> rail.Label(
            'Yes') >> is_user_enabled
        is_user_present >> rail.Label(
            'No') >> add_projectmanagerid_notfound_exception
        is_user_enabled >> rail.Label(
            'Yes') >> get_permissions_assigned_to_user >> is_user_project_manager
        is_user_enabled >> rail.Label(
            'No') >> add_projectmanagerid_disabled_exception

        is_user_project_manager >> rail.Label(
            'Yes') >> is_add_or_update_project
        is_user_project_manager >> rail.Label(
            'No') >> add_required_permission_notfound_exception

        is_add_or_update_project >> rail.Label(
            'Yes') >> update_project_manager
        is_add_or_update_project >> rail.Label(
            'No') >> update_project_leader

        return update_project_manager_id_group
