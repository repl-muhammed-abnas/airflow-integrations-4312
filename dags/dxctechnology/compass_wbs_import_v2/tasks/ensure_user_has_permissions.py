import json
from datetime import date
import rail


def ensure_user_has_permissions(field, dxc_field_label, replicon_field_label):

    with rail.TaskGroup(group_id=f'ensure_{field}_has_permissions', prefix_group_id=False) as task_group:

        def do_determine_needed_user_updates(field):
            should_enable = False
            should_apply = False
            user_permanently_disabled = False
            userinfo = rail.result('load_persons_responsible_userinfo')[field]
            if userinfo:
                if userinfo['status'] == 'True':
                    should_apply = True
                elif userinfo['status'] == 'False':
                    end_date = userinfo['enddate']
                    if end_date is None or end_date >= date.today().isoformat():
                        should_enable = True
                        should_apply = True
                    else:
                        user_permanently_disabled = True

            return {
                'user_uri': userinfo['useruri'] if userinfo else None,
                'should_apply': should_apply,
                'should_enable': should_enable,
                'user_permanently_disabled': user_permanently_disabled,
            }
        determine_updates = rail.PythonOperator(
            task_id=f'determine_necessary_{field}_updates',
            python_callable=do_determine_needed_user_updates,
            op_args=[field]
        )

        enable_user_needed = rail.IfOperator(
            task_id=f'enable_{field}_needed',
            test=f"{{{{ result('determine_necessary_{field}_updates').should_enable }}}}",
            yes_task=f'enable_{field}',
            no_task=f'is_{field}_permanently_disabled',
        )

        enable_user = rail.RepliconServiceOperator(
            task_id=f'enable_{field}',
            endpoint='/services/SecurityService1.svc/EnableLogin',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}"}
        )

        is_user_permanently_disabled = rail.IfOperator(
            task_id=f'is_{field}_permanently_disabled',
            test=f"{{{{ result('determine_necessary_{field}_updates').user_permanently_disabled }}}}",
            yes_task=f'record_{field}_disabled',
            no_task=f'should_apply_{field}_to_project'
        )

        record_user_disabled = rail.WriteLogOperator(
            task_id=f'record_{field}_disabled',
            log='{{ result("create_exception_log") }}',
            message=f"{dxc_field_label} is not added as {replicon_field_label} since the user is disabled and their end date is in the past",
        )

        should_apply_to_project = rail.IfOperator(
            task_id=f'should_apply_{field}_to_project',
            test=f"{{{{ result('determine_necessary_{field}_updates').should_apply }}}}",
            yes_task=f'get_{field}_permission_sets',
            no_task=f'needed_{field}_permissions_granted'
        )

        get_permission_sets = rail.RepliconServiceOperator(
            task_id=f'get_{field}_permission_sets',
            endpoint='/services/PermissionSetService1.svc/BulkGetAssignedPermissionSetsForUsers',
            data={'userUris': [
                f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}"]},
            data_handler=lambda sets: {
                ps['policyUri']: ps['permissionSet']['name'] for ps in sets},
        )

        has_admin_policy = rail.IfOperator(
            task_id=f'{field}_has_admin_policy',
            test=f"{{{{ result('get_{field}_permission_sets').get('urn:replicon:policy:administration') is not none }}}}",
            yes_task=f'{field}_has_projectmanagement_policy',
            no_task=f'assign_{field}_admin_policy',
        )

        assign_admin_policy = rail.RepliconServiceOperator(
            task_id=f'assign_{field}_admin_policy',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}",
                'permissionSetUri': "{{ dag_run.conf['adminpermissionuri'] }}",
            }
        )

        has_projectmanagement_policy = rail.IfOperator(
            task_id=f'{field}_has_projectmanagement_policy',
            test=f"{{{{ result('get_{field}_permission_sets').get('urn:replicon:policy:project-management') is not none }}}}",
            yes_task=f'{field}_has_manager_permissions',
            no_task=f'assign_{field}_projectmanager_policy',
        )

        assign_projectmanager_policy = rail.RepliconServiceOperator(
            task_id=f'assign_{field}_projectmanager_policy',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}",
                'permissionSetUri': "{{ dag_run.conf['projectmanagerpermissionuri'] }}",
            }
        )

        has_manager_permissions = rail.IfOperator(
            task_id=f'{field}_has_manager_permissions',
            test=f"{{{{ result('get_{field}_permission_sets').get('urn:replicon:policy:user') == 'Manager' }}}}",
            yes_task=f'needed_{field}_permissions_granted',
            no_task=f'assign_{field}_manager_permissions',
        )

        assign_manager_permissions = rail.RepliconServiceOperator(
            task_id=f'assign_{field}_manager_permissions',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}",
                'permissionSetUri': "{{ dag_run.conf['enduserpermissionuri'] }}",
            }
        )

        def restricted_manager_scope():
            context = rail.get_current_context()
            employeeTypeGroups = json.loads(
                context['dag_run'].conf['employeetyperestrictiongroups'])

            divisions = None
            if rail.result('load_persons_responsible_userinfo')[
                    field]['country'] == 'Russia':
                divisions = [
                    {
                        "groupSpecificationModeUri": "urn:replicon:data-access-scope-group-specification-mode:users-membership-group",
                        "groupDescendantModeUri": "urn:replicon:data-access-scope-group-descendant-mode:include-descendants"
                    }
                ]

            return {
                "userUri": rail.result(f'determine_necessary_{field}_updates')['user_uri'],
                "policyDataAccessScopes": [
                    {
                        "policyUri": "urn:replicon:policy:user",
                        "employeeTypeGroups": [{'employeeTypeGroup': {'uri': et['uri']}} for et in employeeTypeGroups],
                        "divisions": divisions
                    }
                ]
            }
        restrict_manager_scope = rail.RepliconServiceOperator(
            task_id=f'restrict_{field}_manager_scope',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=restricted_manager_scope,
        )

        needed_permissions_granted = rail.EmptyOperator(
            task_id=f'needed_{field}_permissions_granted')

        determine_updates >> enable_user_needed

        enable_user_needed >> rail.Label(
            'Yes') >> enable_user >> is_user_permanently_disabled
        enable_user_needed >> rail.Label('No') >> is_user_permanently_disabled

        is_user_permanently_disabled >> rail.Label(
            'Yes') >> record_user_disabled >> should_apply_to_project
        is_user_permanently_disabled >> rail.Label(
            'No') >> should_apply_to_project

        should_apply_to_project >> rail.Label(
            'No') >> needed_permissions_granted
        should_apply_to_project >> rail.Label(
            'Yes') >> get_permission_sets

        get_permission_sets >> has_admin_policy
        has_admin_policy >> rail.Label(
            'Yes') >> has_projectmanagement_policy
        has_admin_policy >> rail.Label(
            'No') >> assign_admin_policy >> has_projectmanagement_policy

        has_projectmanagement_policy >> rail.Label(
            'Yes') >> has_manager_permissions
        has_projectmanagement_policy >> rail.Label(
            'No') >> assign_projectmanager_policy >> has_manager_permissions

        has_manager_permissions >> rail.Label(
            'Yes') >> needed_permissions_granted
        has_manager_permissions >> rail.Label(
            'No') >> assign_manager_permissions >> restrict_manager_scope >> needed_permissions_granted

    return task_group
