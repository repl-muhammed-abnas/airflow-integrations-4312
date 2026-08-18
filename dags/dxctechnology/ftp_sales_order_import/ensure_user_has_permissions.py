from datetime import date,datetime
import dateutil.parser
import rail

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

# pylint: disable=too-many-statements
def ensure_user_has_permissions(field):

    with rail.TaskGroup(group_id=f'ensure_{field}_has_permissions', prefix_group_id=False):

        def do_determine_needed_user_updates(field):
            dag_run_conf = get_dag_run_conf()
            if field == 'projectmanager':
                should_enable = False
                should_apply = False
                user_permanently_disabled = False

                useruri = rail.result('user_details')['useruri']
                status = rail.result('user_details')['status']
                end_date = rail.result('user_details')['enddate']
                def end_date_check():
                    end_date_format = str(dateutil.parser.parse(end_date)).split(' ', maxsplit=1)[0]
                    return ((((datetime.strptime(end_date_format,'%Y-%m-%d')).date()).isoformat() >= date.today().isoformat()) if end_date else False)
                empid = dag_run_conf['Projectmanager']
                name = 'Project Manager'
                if useruri:
                    if status == 'True':
                        should_apply = True
                    elif  status == 'False':
                        if end_date is None or end_date == '' or end_date_check():
                            should_enable = True
                            should_apply = True
                        else:
                            user_permanently_disabled = True
            else:
                should_enable = False
                should_apply = False
                user_permanently_disabled = False

                useruri = rail.result('user_details')['comanageruri']
                status = rail.result('user_details')['comanagerstatus']
                end_date = rail.result('user_details')['comanagerenddate']
                def end_date_check():
                    end_date_format = str(dateutil.parser.parse(end_date)).split(' ', maxsplit=1)[0]
                    return ((((datetime.strptime(end_date_format,'%Y-%m-%d')).date()).isoformat() >= date.today().isoformat()) if end_date else False)
                empid = dag_run_conf['Coprojectmanager']
                name = 'Project CoManager'
                if useruri:
                    if status == 'True':
                        should_apply = True
                    elif  status == 'False':
                        if end_date is None or end_date == '' or end_date_check():
                            should_enable = True
                            should_apply = True
                        else:
                            user_permanently_disabled = True

            return {
                    'user_uri': useruri if useruri else None,
                    'should_apply': should_apply,
                    'should_enable': should_enable,
                    'user_permanently_disabled': user_permanently_disabled,
                    'empid' : empid,
                    'name' : name,
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
            no_task=f'should_apply_{field}_to_project',
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
            message=f'{{{{ result("determine_necessary_{field}_updates").empid}}}} is not added \
             as {{{{ result("determine_necessary_{field}_updates").name}}}} since the user is disabled and their end date is in the past',
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
            yes_task=f'needed_{field}_permissions_granted',
            no_task=f'assign_{field}_admin_policy',
        )

        assign_admin_policy = rail.RepliconServiceOperator(
            task_id=f'assign_{field}_admin_policy',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}",
                'permissionSetUri': "{{ dag_run.conf['Adminpermissionuri'] }}",
            }
        )

        has_projectmanagement_policy = rail.IfOperator(
            task_id=f'{field}_has_projectmanagement_policy',
            test=f"{{{{ result('get_{field}_permission_sets').get('urn:replicon:policy:project-management') is not none }}}}",
            yes_task=f'needed_{field}_permissions_granted',
            no_task=f'assign_{field}_projectmanager_policy',
        )

        assign_projectmanager_policy = rail.RepliconServiceOperator(
            task_id=f'assign_{field}_projectmanager_policy',
            endpoint='/services/PermissionSetService1.svc/AssignPermissionSetToUser',
            data={
                'userUri': f"{{{{ result('determine_necessary_{field}_updates').user_uri }}}}",
                'permissionSetUri': "{{ dag_run.conf['Projectmanagerpermissionuri'] }}",
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
                'permissionSetUri': "{{ dag_run.conf['Enduserpermissionuri'] }}",
            }
        )

        needed_permissions_granted = rail.EmptyOperator(
            task_id=f'needed_{field}_permissions_granted')

        determine_updates >> enable_user_needed >> rail.Label('Yes') >> enable_user >> should_apply_to_project >> rail.Label('Yes') >> get_permission_sets
        determine_updates >> is_user_permanently_disabled >> rail.Label("Yes") >> record_user_disabled >> should_apply_to_project
        get_permission_sets >> has_admin_policy >> rail.Label('Yes') >> needed_permissions_granted
        get_permission_sets >> has_projectmanagement_policy >> rail.Label('Yes') >> needed_permissions_granted
        get_permission_sets >> has_manager_permissions >> rail.Label('Yes') >> needed_permissions_granted
        enable_user_needed >> rail.Label('No') >> should_apply_to_project
        is_user_permanently_disabled >> rail.Label('No') >> should_apply_to_project
        should_apply_to_project >> rail.Label('No') >> needed_permissions_granted
        has_admin_policy >> rail.Label('No') >> assign_admin_policy >> needed_permissions_granted
        has_projectmanagement_policy >> rail.Label('No') >> assign_projectmanager_policy >> needed_permissions_granted
        has_manager_permissions >> rail.Label('No') >> assign_manager_permissions >> needed_permissions_granted

    return determine_updates, needed_permissions_granted
