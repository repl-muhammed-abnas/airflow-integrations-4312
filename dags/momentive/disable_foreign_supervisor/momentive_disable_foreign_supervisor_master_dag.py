import rail
from pendulum import datetime
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'momentive_disable_foreign_supervisor_master_{config.instance}',
        description=f'Momentive Disable Foreign Supervisor Monthly master_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2022, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs,
    ) as dag:

        get_all_employeetype_groups = rail.RepliconServiceOperator(
            task_id="get_all_employeetype_groups",
            endpoint="/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups",
            data = {}
        )

        get_all_foreign_supervisors = rail.RepliconServiceOperator(
            task_id='get_all_foreign_supervisors',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda: {
                "page": "1",
                "pagesize": "100000",
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:employee-type-group"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_employeetype_groups'),'displayText','Foreign Supervisors','uri',''),
                        "uris": [],
                        "bool": null,
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                    },
                    "operatorUri": "urn:replicon:filter-operator:and",
                    "rightExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                        "uri": null,
                        "uris": [],
                        "bool": "true",
                        "date": null,
                        "money": null,
                        "number": null,
                        "text": null,
                        "time": null,
                        "calendarDayDurationValue": null,
                        "workdayDurationValue": null,
                        "dateRange": null,
                        "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        declare_userdata_list = rail.SetVariableOperator(
            task_id = 'declare_userdata_list',
            append=False,
            name='userdata',
            value=[]
        )

        declare_userdata_logs_list = rail.SetVariableOperator(
            task_id = 'declare_userdata_logs_list',
            append=False,
            name='userdata_logs',
            value=[]
        )

        for_each_foreign_supervisor = rail.ForEachOperator(
            task_id='for_each_foreign_supervisor',
            items=lambda: rail.result('get_all_foreign_supervisors')['rows'],
            start_task='insert_userdata',
            end_task='for_each_foreign_supervisor_end'
        )

        insert_userdata = rail.SetVariableOperator(
            task_id = 'insert_userdata',
            append=True,
            name='{{ result("declare_userdata_list").name }}',
            value = lambda: {
                'Username': rail.find_first_by_attr_and_get_attr(
                    rail.result('for_each_foreign_supervisor')['cells'],'dataType','urn:replicon:list-type:object','textValue'),
                'Loginname': rail.find_first_by_attr_and_get_attr(
                    rail.result('for_each_foreign_supervisor')['cells'],'dataType','urn:replicon:list-type:string','textValue'),
                'Useruri': rail.find_first_by_attr_and_get_attr(
                    rail.result('for_each_foreign_supervisor')['cells'],'dataType','urn:replicon:list-type:object','uri')
            }
        )

        for_each_foreign_supervisor_end = rail.EmptyOperator(
            task_id='for_each_foreign_supervisor_end'
        )

        for_each_item_in_userdata = rail.ForEachOperator(
            task_id='for_each_item_in_userdata',
            items=lambda: rail.get_dag_run_var(rail.result('declare_userdata_list')['name']),
            start_task='get_assigned_permissionsets_for_user',
            end_task='for_each_item_in_userdata_end'
        )

        get_assigned_permissionsets_for_user = rail.RepliconServiceOperator(
            task_id='get_assigned_permissionsets_for_user',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data={
                "userUri": "{{result('for_each_item_in_userdata').Useruri}}"
            }
        )

        def is_supervisor_permission_assigned():
            policy_Uri = ''
            if rail.result('get_assigned_permissionsets_for_user') and rail.result('get_assigned_permissionsets_for_user')[0]['policyUri']:
                policy_Uri = rail.find_first_by_attr_and_get_attr(
                    rail.result('get_assigned_permissionsets_for_user'),'policyUri','urn:replicon:policy:supervision','policyUri','')
            return bool(policy_Uri)

        if_supervisor_permission_assigned = rail.IfOperator(
            task_id = 'if_supervisor_permission_assigned',
            test=is_supervisor_permission_assigned,
            yes_task='get_direct_reports',
            no_task='for_each_item_in_userdata_end'
        )

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint='/services/UserService1.svc/GetDirectReportsForUser',
            data = {
                "userUri": "{{result('for_each_item_in_userdata').Useruri}}",
                "asOfDate": null,
                "userStatusOptionUri": "urn:replicon:user-status-option:include-only-enabled-users"
            }
        )

        if_direct_report_not_present = rail.IfOperator(
            task_id = 'if_direct_report_not_present',
            test= lambda: not bool( rail.result('get_direct_reports') and
                        rail.result('get_direct_reports')[0] and
                        rail.result('get_direct_reports')[0]['uri']) and
                        bool(rail.result('for_each_item_in_userdata')['Loginname'].lower() != 'replicon.admin'),
            yes_task='disable_login',
            no_task='for_each_item_in_userdata_end'
        )

        disable_login = rail.RepliconServiceOperator(
            task_id= 'disable_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data = {
                "userUri": "{{result('for_each_item_in_userdata').Useruri}}"
            }
        )

        insert_to_userdata_logs = rail.SetVariableOperator(
            task_id = 'insert_to_userdata_logs',
            append=True,
            name='{{ result("declare_userdata_logs_list").name }}',
            value ={
                'Username': "{{result('for_each_item_in_userdata').Username}}",
                'Loginname': "{{result('for_each_item_in_userdata').Loginname}}",
                'Useruri': "{{result('for_each_item_in_userdata').Useruri}}"
            }

        )

        for_each_item_in_userdata_end = rail.EmptyOperator(
            task_id = 'for_each_item_in_userdata_end'
        )

        logs = rail.GetVariableOperator(
            task_id = 'logs',
            name= '{{ result("declare_userdata_logs_list").name }}'
        )

        get_all_employeetype_groups >> get_all_foreign_supervisors >> declare_userdata_list >> declare_userdata_logs_list >> for_each_foreign_supervisor
        for_each_foreign_supervisor >> insert_userdata >> for_each_foreign_supervisor_end
        for_each_foreign_supervisor >> for_each_foreign_supervisor_end >> for_each_item_in_userdata >> get_assigned_permissionsets_for_user
        get_assigned_permissionsets_for_user >> if_supervisor_permission_assigned >> rail.Label("Yes") >> get_direct_reports >> if_direct_report_not_present
        if_direct_report_not_present >> rail.Label("Yes") >> disable_login >> insert_to_userdata_logs >> for_each_item_in_userdata_end
        if_direct_report_not_present >> rail.Label("No") >> for_each_item_in_userdata_end
        if_supervisor_permission_assigned >> rail.Label("No") >> for_each_item_in_userdata_end >> logs
        for_each_item_in_userdata >> for_each_item_in_userdata_end

    return dag


rail.for_each_instance(create_dag)
