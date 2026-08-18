from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_payroll_and_admin_permission_add_update_dag_id,
        description=f'Assured Partners User Import Payroll and Admin permissions add/update Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='policydataaccessprocess',
            value='no'
        )

        declare_variable_4 = rail.SetVariableOperator(
            task_id='declare_variable_4',
            append=False,
            name='policyDataAccessScopes_payroll',
            value=[{
                "policyUri": "urn:replicon:policy:payroll-management",
                "locations": [],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }]
        )

        declare_variable_5 = rail.SetVariableOperator(
            task_id='declare_variable_5',
            append=False,
            name='policyDataAccessScopes_admin',
            value=[{
                "policyUri": "urn:replicon:policy:administration",
                "locations": [],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }]
        )

        response_from_dag = rail.SetVariableOperator(
            task_id="response_from_dag",
            name='response_from_dag',
            append=False,
            value=None
        )

        if_request_payrollpermission_blank_6 = rail.IfOperator(
            task_id='if_request_payrollpermission_blank_6',
            test='''{{ dag_run.conf.PayrollPermission | is_falsy  or dag_run.conf.AdminPermission | is_falsy }}''',
            yes_task="get_assigned_permission_sets_for_user2_7",
            no_task="if_request_payrollpermission_blank_18",
        )

        get_assigned_permission_sets_for_user2_7 = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets_for_user2_7',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: {
                "payroll_permission": rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:payroll-management', 'permissionSet.uri'),
                "admin_permission": rail.find_first_by_attr_and_get_attr(response, 'policyUri', 'urn:replicon:policy:administration', 'permissionSet.uri')
            }
        )

        if_request_payrollpermission_blank_8 = rail.IfOperator(
            task_id='if_request_payrollpermission_blank_8',
            test=lambda dag_run: bool(not (dag_run.conf['PayrollPermission']) and rail.result(
                "get_assigned_permission_sets_for_user2_7")['payroll_permission']),
            yes_task="remove_permission_set_assignment_from_user_for_payroll_permission_11",
            no_task="if_request_adminpermission_blank_13",
        )

        remove_permission_set_assignment_from_user_for_payroll_permission_11 = rail.RepliconServiceOperator(
            task_id='remove_permission_set_assignment_from_user_for_payroll_permission_11',
            endpoint="/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_assigned_permission_sets_for_user2_7').payroll_permission }}"
            }
        )

        put_policy_data_access_scopes_for_user_for_payroll_permission_12 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_for_payroll_permission_12',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policyDataAccessScopes": [{
                    "policyUri": "urn:replicon:policy:payroll-management",
                    "locations": [],
                    "divisions": [],
                    "costCenters": [],
                    "serviceCenters": [],
                    "departmentGroups": [],
                    "employeeTypeGroups": []
                }]
            }
        )

        if_request_adminpermission_blank_13 = rail.IfOperator(
            task_id='if_request_adminpermission_blank_13',
            test=lambda dag_run: bool(not (dag_run.conf['AdminPermission']) and rail.result(
                "get_assigned_permission_sets_for_user2_7")['admin_permission']),
            yes_task="remove_permission_set_assignment_from_user_for_admin_permission_16",
            no_task="if_request_payrollpermission_blank_18",
        )

        remove_permission_set_assignment_from_user_for_admin_permission_16 = rail.RepliconServiceOperator(
            task_id='remove_permission_set_assignment_from_user_for_admin_permission_16',
            endpoint="/services/PermissionSetService1.svc/RemovePermissionSetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ result('get_assigned_permission_sets_for_user2_7').admin_permission  }}"
            }
        )

        put_policy_data_access_scopes_for_user_for_admin_permission_17 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_for_admin_permission_17',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policyDataAccessScopes": [{
                    "policyUri": "urn:replicon:policy:administration",
                    "locations": [],
                    "divisions": [],
                    "costCenters": [],
                    "serviceCenters": [],
                    "departmentGroups": [],
                    "employeeTypeGroups": []
                }]
            }
        )

        if_request_payrollpermission_blank_18 = rail.IfOperator(
            task_id='if_request_payrollpermission_blank_18',
            test='''{{ dag_run.conf.PayrollPermission | is_falsy  and dag_run.conf.AdminPermission | is_falsy }}''',
            yes_task="catch_and_log_error",
            no_task="if_request_payrollpermissionuri_blank_20",
        )

        if_request_payrollpermissionuri_blank_20 = rail.IfOperator(
            task_id='if_request_payrollpermissionuri_blank_20',
            test='''{{ dag_run.conf.payrollpermissionuri | is_falsy  and dag_run.conf.adminpermissionuri | is_falsy }}''',
            yes_task="response_from_dag_21",
            no_task="if_request_payrollpermission_present_23",
        )

        response_from_dag_21 = rail.SetVariableOperator(
            task_id='response_from_dag_21',
            name='response_from_dag',
            append=False,
            value=lambda dag_run: str("" if dag_run.conf['payrollpermissionuri'] else "Required payroll permission is not available in Replicon") + str(
                "" if dag_run.conf['adminpermissionuri'] else "Required admin permission is not available in Replicon")
        )

        if_request_payrollpermission_present_23 = rail.IfOperator(
            task_id='if_request_payrollpermission_present_23',
            test=lambda dag_run: bool(
                dag_run.conf['PayrollPermission'] and dag_run.conf['PayrollPermission'].lower() != 'no replacement'),
            yes_task="assign_permission_set_to_user_for_payroll_permission_24",
            no_task="if_request_adminpermission_present_26",
        )

        assign_permission_set_to_user_for_payroll_permission_24 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_for_payroll_permission_24',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.payrollpermissionuri }}"
            }
        )

        update_variable_25 = rail.SetVariableOperator(
            task_id='update_variable_25',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_request_adminpermission_present_26 = rail.IfOperator(
            task_id='if_request_adminpermission_present_26',
            test=lambda dag_run: bool(
                dag_run.conf['AdminPermission'] and dag_run.conf['AdminPermission'].lower() != 'no replacement'),
            yes_task="assign_permission_set_to_user_for_admin_permission_27",
            no_task="if_declare_variable_3_value_equals_to_no_29",
        )

        assign_permission_set_to_user_for_admin_permission_27 = rail.RepliconServiceOperator(
            task_id='assign_permission_set_to_user_for_admin_permission_27',
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "permissionSetUri": "{{ dag_run.conf.adminpermissionuri }}"
            }
        )

        update_variable_28 = rail.SetVariableOperator(
            task_id='update_variable_28',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value='yes'
        )

        if_declare_variable_3_value_equals_to_no_29 = rail.IfOperator(
            task_id='if_declare_variable_3_value_equals_to_no_29',
            test=lambda: rail.get_dag_run_var(
                'policydataaccessprocess') == 'no',
            yes_task="catch_and_log_error",
            no_task="if_request_payrollgroupinggroups_present_31",
        )

        if_request_payrollgroupinggroups_present_31 = rail.IfOperator(
            task_id='if_request_payrollgroupinggroups_present_31',
            test=lambda dag_run:  bool(dag_run.conf['PayrollGroupingGroups'] or dag_run.conf['ProfitCenterGroups'] or dag_run.conf['AgencyGroups'] or
                                       dag_run.conf['PayGroupGroups'] or dag_run.conf['LocationGroups'] or dag_run.conf['DepartmentGroups']),
            yes_task="if_request_conditionrestrict_blank_32",
            no_task="if_request_payrollgroupinggroups_blank_35",
        )

        if_request_conditionrestrict_blank_32 = rail.IfOperator(
            task_id='if_request_conditionrestrict_blank_32',
            test='''{{ dag_run.conf.ConditionRestrict | is_falsy }}''',
            yes_task="response_from_dag_33",
            no_task="if_request_payrollgroupinggroups_blank_35",
        )

        response_from_dag_33 = rail.SetVariableOperator(
            task_id='response_from_dag_33',
            name='response_from_dag',
            append=False,
            value="Condition- Restrict coumn is blank hence no restriction is applied"
        )

        if_request_payrollgroupinggroups_blank_35 = rail.IfOperator(
            task_id='if_request_payrollgroupinggroups_blank_35',
            test='''{{ dag_run.conf.PayrollGroupingGroups | is_falsy  and dag_run.conf.ProfitCenterGroups | is_falsy  and dag_run.conf.AgencyGroups | is_falsy  and dag_run.conf.PayGroupGroups | is_falsy  and dag_run.conf.LocationGroups | is_falsy  and dag_run.conf.DepartmentGroups | is_falsy }}''',
            yes_task="response_from_dag_36",
            no_task="get_all_department_groups_38",
        )

        response_from_dag_36 = rail.SetVariableOperator(
            task_id='response_from_dag_36',
            name='response_from_dag',
            append=False,
            value="Payroll Groups are blank hence no restriction is applied"
        )

        get_all_department_groups_38 = rail.RepliconServiceOperator(
            task_id='get_all_department_groups_38',
            endpoint="/services/DepartmentGroupService1.svc/GetAllDepartmentGroups",
        )

        def get_required_groups_list(groups_string, key_name):
            if bool(groups_string):
                grouping_groups_list = groups_string.split("|")
                return [{
                    "groupSpecificationModeUri": null,
                    "groupDescendantModeUri": null,
                    key_name: {
                        "uri": null,
                        "parentUri": null,
                        "name": item
                    }
                }for item in grouping_groups_list]
            return []

        def get_agency_or_emptype_groups_list(groups_string, key_name, all_department_groups):
            if bool(groups_string):
                groups_list = groups_string.split("|")
                if key_name == "departmentGroup":
                    return [{
                        "groupSpecificationModeUri": null,
                        "groupDescendantModeUri": null,
                        key_name: {
                            "parent": null,
                            "name": null,
                            "uri": rail.find_first_by_attr_and_get_attr(all_department_groups, 'displayText', item.strip(), 'uri'),
                            "parameterCorrelationId": null
                        }
                    }for item in groups_list]

                elif key_name == "employeeTypeGroup":
                    return [{
                        "groupSpecificationModeUri": null,
                        "groupDescendantModeUri": null,
                        key_name: {
                            "parent": null,
                            "name": item,
                            "uri": null,
                            "parameterCorrelationId": null
                        }
                    }for item in groups_list]
            return []

        log_all_payroll_grouping_groups_cost_centers_50 = rail.PythonOperator(
            task_id='log_all_payroll_grouping_groups_cost_centers_50',
            python_callable=lambda dag_run: get_required_groups_list(
                dag_run.conf['PayrollGroupingGroups'], "costCenter")
        )

        log_allprofit_center_groups_service_centers_56 = rail.PythonOperator(
            task_id='log_allprofit_center_groups_service_centers_56',
            python_callable=lambda dag_run: get_required_groups_list(
                dag_run.conf['ProfitCenterGroups'], "serviceCenter")
        )

        log_allagency_groups_department_groups_62 = rail.PythonOperator(
            task_id='log_allagency_groups_department_groups_62',
            python_callable=lambda dag_run: get_agency_or_emptype_groups_list(
                dag_run.conf['AgencyGroups'], "departmentGroup", rail.result('get_all_department_groups_38'))
        )

        log_all_pay_group_groups_locations_68 = rail.PythonOperator(
            task_id='log_all_pay_group_groups_locations_68',
            python_callable=lambda dag_run: get_required_groups_list(
                dag_run.conf['PayGroupGroups'], "location")
        )

        log_all_location_groups_divisions_74 = rail.PythonOperator(
            task_id='log_all_location_groups_divisions_74',
            python_callable=lambda dag_run: get_required_groups_list(
                dag_run.conf['LocationGroups'], "division")
        )

        log_all_deparatment_groups_employee_type_groups_80 = rail.PythonOperator(
            task_id='log_all_deparatment_groups_employee_type_groups_80',
            python_callable=lambda dag_run: get_agency_or_emptype_groups_list(
                dag_run.conf['DepartmentGroups'], "employeeTypeGroup", '')
        )

        def policy_data_access_scopes_payroll_admin_list_values_and_condition(dag_run):
            policy_data_access_scopes_payroll = []
            policy_data_access_scopes_admin = []
            if bool(dag_run.conf['PayrollPermission']) and dag_run.conf['PayrollPermission'].lower() != 'no replacement':
                policy_data_access_scopes_payroll.append({
                    "policyUri": "urn:replicon:policy:payroll-management",
                    "locations": rail.result('log_all_pay_group_groups_locations_68'),
                    "divisions": rail.result('log_all_location_groups_divisions_74'),
                    "costCenters": rail.result('log_all_payroll_grouping_groups_cost_centers_50'),
                    "serviceCenters": rail.result('log_allprofit_center_groups_service_centers_56'),
                    "departmentGroups": rail.result('log_allagency_groups_department_groups_62'),
                    "employeeTypeGroups": rail.result('log_all_deparatment_groups_employee_type_groups_80')
                })

            if bool(dag_run.conf['AdminPermission']) and dag_run.conf['AdminPermission'].lower() != 'no replacement':
                policy_data_access_scopes_admin.append({
                    "policyUri": "urn:replicon:policy:administration",
                    "locations": rail.result('log_all_pay_group_groups_locations_68'),
                    "divisions": rail.result('log_all_location_groups_divisions_74'),
                    "costCenters": rail.result('log_all_payroll_grouping_groups_cost_centers_50'),
                    "serviceCenters": rail.result('log_allprofit_center_groups_service_centers_56'),
                    "departmentGroups": rail.result('log_allagency_groups_department_groups_62'),
                    "employeeTypeGroups": rail.result('log_all_deparatment_groups_employee_type_groups_80')
                })

            return {
                'policy_data_access_scopes_payroll': policy_data_access_scopes_payroll,
                'policy_data_access_scopes_admin': policy_data_access_scopes_admin
            }

        def policy_data_access_scopes_payroll_admin_list_values_or_condition(dag_run):
            policy_data_access_scopes_payroll = [{
                "policyUri": "urn:replicon:policy:payroll-management",
                "locations": [],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }]
            policy_data_access_scopes_admin = [{
                "policyUri": "urn:replicon:policy:administration",
                "locations": [],
                "divisions": [],
                "costCenters": [],
                "serviceCenters": [],
                "departmentGroups": [],
                "employeeTypeGroups": []
            }]

            if bool(dag_run.conf['PayrollPermission']) and dag_run.conf['PayrollPermission'].lower() != 'no replacement':
                if bool(dag_run.conf['PayrollGroupingGroups']):
                    policy_data_access_scopes_payroll[0]["costCenters"] = rail.result(
                        'log_all_payroll_grouping_groups_cost_centers_50')

                if bool(dag_run.conf['ProfitCenterGroups']):
                    policy_data_access_scopes_payroll[0]["serviceCenters"] = rail.result(
                        'log_allprofit_center_groups_service_centers_56')

                if bool(dag_run.conf['AgencyGroups']):
                    policy_data_access_scopes_payroll[0]["departmentGroups"] = rail.result(
                        'log_allagency_groups_department_groups_62')

                if bool(dag_run.conf['PayGroupGroups']):
                    policy_data_access_scopes_payroll[0]["locations"] = rail.result(
                        'log_all_pay_group_groups_locations_68')

                if bool(dag_run.conf['LocationGroups']):
                    policy_data_access_scopes_payroll[0]["divisions"] = rail.result(
                        'log_all_location_groups_divisions_74')

                if bool(dag_run.conf['DepartmentGroups']):
                    policy_data_access_scopes_payroll[0]["employeeTypeGroups"] = rail.result(
                        'log_all_deparatment_groups_employee_type_groups_80')

            if bool(dag_run.conf['AdminPermission']) and dag_run.conf['AdminPermission'].lower() != 'no replacement':
                if bool(dag_run.conf['PayrollGroupingGroups']):
                    policy_data_access_scopes_admin[0]["costCenters"] = rail.result(
                        'log_all_payroll_grouping_groups_cost_centers_50')

                if bool(dag_run.conf['ProfitCenterGroups']):
                    policy_data_access_scopes_admin[0]["serviceCenters"] = rail.result(
                        'log_allprofit_center_groups_service_centers_56')

                if bool(dag_run.conf['AgencyGroups']):
                    policy_data_access_scopes_admin[0]["departmentGroups"] = rail.result(
                        'log_allagency_groups_department_groups_62')

                if bool(dag_run.conf['PayGroupGroups']):
                    policy_data_access_scopes_admin[0]["locations"] = rail.result(
                        'log_all_pay_group_groups_locations_68')

                if bool(dag_run.conf['LocationGroups']):
                    policy_data_access_scopes_admin[0]["divisions"] = rail.result(
                        'log_all_location_groups_divisions_74')

                if bool(dag_run.conf['DepartmentGroups']):
                    policy_data_access_scopes_admin[0]["employeeTypeGroups"] = rail.result(
                        'log_all_deparatment_groups_employee_type_groups_80')

            return {
                'policy_data_access_scopes_payroll': policy_data_access_scopes_payroll,
                'policy_data_access_scopes_admin': policy_data_access_scopes_admin
            }

        if_conditionrestrict_downcase_equals_to_and_81 = rail.IfOperator(
            task_id='if_conditionrestrict_downcase_equals_to_and_81',
            test=lambda dag_run: dag_run.conf['ConditionRestrict'].lower(
            ) == 'and',
            yes_task="get_policy_data_access_scopes_payroll_admin_list_values",
            no_task="if_conditionrestrict_downcase_equals_to_or_86",
        )

        get_policy_data_access_scopes_payroll_admin_list_values = rail.PythonOperator(
            task_id='get_policy_data_access_scopes_payroll_admin_list_values',
            python_callable=policy_data_access_scopes_payroll_admin_list_values_and_condition
        )

        if_conditionrestrict_downcase_equals_to_or_86 = rail.IfOperator(
            task_id='if_conditionrestrict_downcase_equals_to_or_86',
            test=lambda dag_run: dag_run.conf['ConditionRestrict'].lower(
            ) == 'or',
            yes_task="get_policy_data_access_scopes_payroll_admin_list_values_or_condition",
            no_task="get_policy_data_access_scopes_final",
        )

        get_policy_data_access_scopes_payroll_admin_list_values_or_condition = rail.PythonOperator(
            task_id='get_policy_data_access_scopes_payroll_admin_list_values_or_condition',
            python_callable=policy_data_access_scopes_payroll_admin_list_values_or_condition
        )

        get_policy_data_access_scopes_final = rail.PythonOperator(
            task_id='get_policy_data_access_scopes_final',
            python_callable=lambda: rail.result("get_policy_data_access_scopes_payroll_admin_list_values") or rail.result(
                "get_policy_data_access_scopes_payroll_admin_list_values_or_condition") or ({
                    'policy_data_access_scopes_payroll': [],
                    'policy_data_access_scopes_admin': []})
        )

        if_payrollpermission_downcase_not_equals_to_noreplacement_147 = rail.IfOperator(
            task_id='if_payrollpermission_downcase_not_equals_to_noreplacement_147',
            test=lambda dag_run: dag_run.conf['PayrollPermission'].lower(
            ) != 'no replacement',
            yes_task="put_policy_data_access_scopes_for_user_for_payroll_permission_148",
            no_task="if_adminpermission_downcase_not_equals_to_noreplacement_149",
        )

        put_policy_data_access_scopes_for_user_for_payroll_permission_148 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_for_payroll_permission_148',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policyDataAccessScopes": rail.result('get_policy_data_access_scopes_final')['policy_data_access_scopes_payroll']
            }
        )

        if_adminpermission_downcase_not_equals_to_noreplacement_149 = rail.IfOperator(
            task_id='if_adminpermission_downcase_not_equals_to_noreplacement_149',
            test=lambda dag_run: dag_run.conf['AdminPermission'].lower(
            ) != 'no replacement',
            yes_task="put_policy_data_access_scopes_for_user_for_admin_permission_150",
            no_task="catch_and_log_error",
        )

        put_policy_data_access_scopes_for_user_for_admin_permission_150 = rail.RepliconServiceOperator(
            task_id='put_policy_data_access_scopes_for_user_for_admin_permission_150',
            endpoint="/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "policyDataAccessScopes": rail.result('get_policy_data_access_scopes_final')['policy_data_access_scopes_admin']
            }
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=False,
            value="Error - {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var('response_from_dag')
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error >> final_response_from_dag
        can_run_batch_task >> rail.Label('No') >> declare_variable_3

        declare_variable_3 >> declare_variable_4 >> declare_variable_5 >> response_from_dag >> if_request_payrollpermission_blank_6

        if_request_payrollpermission_blank_6 >> rail.Label(
            'No') >> if_request_payrollpermission_blank_18
        if_request_payrollpermission_blank_6 >> rail.Label(
            'Yes') >> get_assigned_permission_sets_for_user2_7 >> if_request_payrollpermission_blank_8

        if_request_payrollpermission_blank_8 >> rail.Label(
            'No') >> if_request_adminpermission_blank_13
        if_request_payrollpermission_blank_8 >> rail.Label('Yes') >> remove_permission_set_assignment_from_user_for_payroll_permission_11 \
            >> put_policy_data_access_scopes_for_user_for_payroll_permission_12 >> if_request_adminpermission_blank_13

        if_request_adminpermission_blank_13 >> rail.Label(
            'No') >> if_request_payrollpermission_blank_18
        if_request_adminpermission_blank_13 >> rail.Label('Yes') >> remove_permission_set_assignment_from_user_for_admin_permission_16 \
            >> put_policy_data_access_scopes_for_user_for_admin_permission_17 >> if_request_payrollpermission_blank_18

        if_request_payrollpermission_blank_18 >> rail.Label(
            'Yes') >> catch_and_log_error
        if_request_payrollpermission_blank_18 >> rail.Label(
            'No') >> if_request_payrollpermissionuri_blank_20

        if_request_payrollpermissionuri_blank_20 >> rail.Label(
            'Yes') >> response_from_dag_21 >> catch_and_log_error
        if_request_payrollpermissionuri_blank_20 >> rail.Label(
            'No') >> if_request_payrollpermission_present_23

        if_request_payrollpermission_present_23 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_for_payroll_permission_24 >> update_variable_25 >> if_request_adminpermission_present_26
        if_request_payrollpermission_present_23 >> rail.Label(
            'No') >> if_request_adminpermission_present_26

        if_request_adminpermission_present_26 >> rail.Label(
            'Yes') >> assign_permission_set_to_user_for_admin_permission_27 >> update_variable_28 >> if_declare_variable_3_value_equals_to_no_29
        if_request_adminpermission_present_26 >> rail.Label(
            'No') >> if_declare_variable_3_value_equals_to_no_29

        if_declare_variable_3_value_equals_to_no_29 >> rail.Label(
            'Yes') >> catch_and_log_error
        if_declare_variable_3_value_equals_to_no_29 >> rail.Label(
            'No') >> if_request_payrollgroupinggroups_present_31

        if_request_payrollgroupinggroups_present_31 >> rail.Label(
            'Yes') >> if_request_conditionrestrict_blank_32

        if_request_conditionrestrict_blank_32 >> rail.Label(
            'Yes') >> response_from_dag_33 >> catch_and_log_error
        if_request_conditionrestrict_blank_32 >> rail.Label(
            'No') >> if_request_payrollgroupinggroups_blank_35

        if_request_payrollgroupinggroups_present_31 >> rail.Label(
            'No') >> if_request_payrollgroupinggroups_blank_35

        if_request_payrollgroupinggroups_blank_35 >> rail.Label(
            'Yes') >> response_from_dag_36 >> catch_and_log_error
        if_request_payrollgroupinggroups_blank_35 >> rail.Label(
            'No') >> get_all_department_groups_38

        get_all_department_groups_38 >> log_all_payroll_grouping_groups_cost_centers_50 >> log_allprofit_center_groups_service_centers_56 \
            >> log_allagency_groups_department_groups_62 >> log_all_pay_group_groups_locations_68 >> log_all_location_groups_divisions_74 \
            >> log_all_deparatment_groups_employee_type_groups_80 >> if_conditionrestrict_downcase_equals_to_and_81

        if_conditionrestrict_downcase_equals_to_and_81 >> rail.Label(
            'Yes') >> get_policy_data_access_scopes_payroll_admin_list_values >> get_policy_data_access_scopes_final
        if_conditionrestrict_downcase_equals_to_and_81 >> rail.Label(
            'No') >> if_conditionrestrict_downcase_equals_to_or_86

        if_conditionrestrict_downcase_equals_to_or_86 >> rail.Label(
            'Yes') >> get_policy_data_access_scopes_payroll_admin_list_values_or_condition >> get_policy_data_access_scopes_final
        if_conditionrestrict_downcase_equals_to_or_86 >> rail.Label(
            'No') >> get_policy_data_access_scopes_final

        get_policy_data_access_scopes_final >> if_payrollpermission_downcase_not_equals_to_noreplacement_147

        if_payrollpermission_downcase_not_equals_to_noreplacement_147 >> rail.Label(
            'Yes') >> put_policy_data_access_scopes_for_user_for_payroll_permission_148 >> if_adminpermission_downcase_not_equals_to_noreplacement_149
        if_payrollpermission_downcase_not_equals_to_noreplacement_147 >> rail.Label(
            'No') >> if_adminpermission_downcase_not_equals_to_noreplacement_149

        if_adminpermission_downcase_not_equals_to_noreplacement_149 >> rail.Label(
            'Yes') >> put_policy_data_access_scopes_for_user_for_admin_permission_150 >> catch_and_log_error
        if_adminpermission_downcase_not_equals_to_noreplacement_149 >> rail.Label(
            'No') >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
