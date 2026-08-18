from datetime import timedelta
from airflow.models import Variable
import rail
from cbrefcg.project_team_member_assignment.utils import request_payload, custom_method
from cbrefcg.project_team_member_assignment.tasks.check_groups_data import get_respective_groups_data

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id= config.child_dag_id,
        description=f'cbrefcg_update_projectteam_billingrate {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs= config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='bulk_get_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='bulk_get_project_details',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        bulk_get_project_details= rail.RepliconServiceOperator(
            task_id='bulk_get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                  {
                    "uri": "{{ dag_run.conf.projecturi }}",
                  }
                ]
            }
        )

        is_project_found= rail.IfOperator(
            task_id='is_project_found',
            test=lambda: bool(rail.result('bulk_get_project_details')[
                              0]['projectDetails']),
            yes_task="create_resource_list",
            no_task="finish",
        )

        finish= rail.EmptyOperator(
            task_id='finish',
        )

        create_resource_list=rail.SetVariableOperator(
            task_id='create_resource_list',
            append=False,
            name='resourcestoassign',
            value=[]
        )

        get_project_team_change_summary= rail.RepliconServiceOperator(
            task_id='get_project_team_change_summary',
            endpoint="/services/ProjectService1.svc/GetProjectTeamChangeSummary2",
            data= request_payload.get_project_team_change_payload
        )

        for_each_team_member_added= rail.ForEachOperator(
            task_id='for_each_team_member_added',
            items= lambda: rail.result('get_project_team_change_summary')['teamMembersAdded'],
            start_task = 'is_costcenter_uri_present',
            end_task = 'for_each_team_member_added_end'
        )

        is_costcenter_uri_present= rail.IfOperator(
            task_id='is_costcenter_uri_present',
            test="{{ result('for_each_team_member_added').resource.costCenter | is_truthy }}",
            yes_task="cost_center_group_start",
            no_task="is_departmentgroup_uri_present",
        )

        cost_center_group_start = rail.EmptyOperator(
            task_id = 'cost_center_group_start'
        )

        has_cost_center_present_in_project, put_project_team_member_billing_rates_for_costCenter = get_respective_groups_data(
          "costCenter","cost-center", "UpdateCostCenter2")

        has_cost_center_data= rail.IfOperator(
            task_id='has_group_data_costCenter',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_costCenter")),
            yes_task= "add_group_data_to_list_costCenter",
            no_task= 'is_departmentgroup_uri_present',
        )

        add_cost_center_data_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_costCenter',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_costCenter"), 'costCenter')
        )

        is_departmentgroup_uri_present= rail.IfOperator(
            task_id='is_departmentgroup_uri_present',
            test="{{ result('for_each_team_member_added').resource.departmentGroup | is_truthy }}",
            yes_task="department_group_start",
            no_task="is_employeetype_uri_present",
        )

        department_group_start = rail.EmptyOperator(
            task_id = 'department_group_start'
        )

        has_department_present_in_project, put_project_team_member_billing_rates_for_departmentGroup = get_respective_groups_data(
            'departmentGroup','department-group', 'UpdateDepartmentGroup2')

        has_department_data= rail.IfOperator(
            task_id='has_group_data_departmentGroup',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_departmentGroup")),
            yes_task= "add_group_data_to_list_departmentGroup",
            no_task= 'is_employeetype_uri_present',
        )

        add_department_data_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_departmentGroup',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_departmentGroup"), 'departmentGroup')
        )

        is_employeetype_uri_present=rail.IfOperator(
            task_id='is_employeetype_uri_present',
            test="{{ result('for_each_team_member_added').resource.employeeTypeGroup | is_truthy }}",
            yes_task="employeetype_group_start",
            no_task="is_location_uri_present",
        )

        employeetype_group_start = rail.EmptyOperator(
            task_id = 'employeetype_group_start'
        )

        has_employeetype_present_in_project, put_project_team_member_billing_rates_for_employeeTypeGroup = get_respective_groups_data(
            'employeeTypeGroup','employee-type-group', 'UpdateEmployeeTypeGroup2')

        has_employee_type_data= rail.IfOperator(
            task_id='has_group_data_employeeTypeGroup',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_employeeTypeGroup")),
            yes_task= "add_group_data_to_list_employeeTypeGroup",
            no_task= 'is_location_uri_present',
        )

        add_employee_type_data_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_employeeTypeGroup',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_employeeTypeGroup"), 'employeeTypeGroup')
        )

        is_location_uri_present= rail.IfOperator(
            task_id='is_location_uri_present',
            test="{{ result('for_each_team_member_added').resource.location | is_truthy }}",
            yes_task="location_group_start",
            no_task="is_servicecenter_uri_present",
        )

        location_group_start = rail.EmptyOperator(
            task_id = 'location_group_start'
        )

        has_location_present_in_project, put_project_team_member_billing_rates_for_location = get_respective_groups_data(
            'location','location', 'UpdateLocation2')

        has_location_data= rail.IfOperator(
            task_id='has_group_data_location',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_location")),
            yes_task= "add_group_data_to_list_location",
            no_task= 'is_servicecenter_uri_present',
        )

        add_location_data_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_location',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_location"), 'location')
        )

        is_servicecenter_uri_present= rail.IfOperator(
            task_id='is_servicecenter_uri_present',
            test="{{ result('for_each_team_member_added').resource.serviceCenter | is_truthy }}",
            yes_task="servicecenter_group_start",
            no_task="is_division_uri_present",
        )

        servicecenter_group_start = rail.EmptyOperator(
            task_id = 'servicecenter_group_start'
        )

        has_servicecenter_present_in_project, put_project_team_member_billing_rates_for_service_center = get_respective_groups_data(
            'serviceCenter','service-center', 'UpdateServiceCenter2')

        has_servicecenter_data= rail.IfOperator(
            task_id='has_group_data_serviceCenter',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_serviceCenter")),
            yes_task= "add_group_data_to_list_serviceCenter",
            no_task= 'is_division_uri_present',
        )

        add_servicecenter_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_serviceCenter',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_serviceCenter"), 'serviceCenter')
        )

        is_division_uri_present= rail.IfOperator(
            task_id='is_division_uri_present',
            test="{{ result('for_each_team_member_added').resource.division | is_truthy }}",
            yes_task="division_group_start",
            no_task="for_each_team_member_added_end",
        )

        division_group_start = rail.EmptyOperator(
            task_id = 'division_group_start'
        )

        has_division_present_in_project, put_project_team_member_billing_rates_for_division = get_respective_groups_data(
            'division','cost-center', 'UpdateCostCenter2', update_group = True)

        has_division_data= rail.IfOperator(
            task_id='has_group_data_division',
            test=lambda: custom_method.has_group_data(rail.result("get_data_for_specific_group_division")),
            yes_task= "add_group_data_to_list_division",
            no_task= 'for_each_team_member_added_end',
        )

        add_division_data_to_list= rail.SetVariableOperator(
            task_id='add_group_data_to_list_division',
            append=True,
            name='{{ result("create_resource_list").name }}',
            value= lambda: custom_method.add_items_to_list(
                    rail.result("get_data_for_specific_group_division"), 'division')
        )

        for_each_team_member_added_end=rail.EmptyOperator(
            task_id='for_each_team_member_added_end',
        )

        get_resource_list_data = rail.GetVariableOperator(
            task_id = 'get_resource_list_data',
            name= '{{ result("create_resource_list").name }}'
        )

        has_resouse_list_data_present= rail.IfOperator(
            task_id='has_resouse_list_data_present',
            test="{{ result('get_resource_list_data').value | is_truthy }}",
            yes_task="get_all_user_uris",
            no_task="finish",
        )

        get_all_user_uris= rail.PythonOperator(
            task_id='get_all_user_uris',
            python_callable= custom_method.get_user_uris
        )

        bulk_update_project_team_members_assignment=rail.RepliconServiceOperator(
            task_id='bulk_update_project_team_members_assignment',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data= request_payload.get_update_project_team_payload
        )

        put_project_team_member_billing_rates=rail.RepliconServiceCallForEachItemOperator(
            task_id='put_project_team_member_billing_rates',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            items="{{ result('get_all_user_uris') | to_json }}",
            data={
                "projectUri": "{{ dag_run.conf.projecturi }}",
                "resourceUri": "{{ item }}",
                "billingRateUris": ["urn:replicon:user-specific-billing-rate"]
              }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
          'Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label(
          'No') >> bulk_get_project_details >> is_project_found

        is_project_found >> rail.Label(
          'No') >> finish

        is_project_found >> rail.Label(
          'Yes') >> create_resource_list >> get_project_team_change_summary >> for_each_team_member_added >> is_costcenter_uri_present

        is_costcenter_uri_present >> rail.Label(
            "Yes") >> cost_center_group_start >> has_cost_center_present_in_project

        is_costcenter_uri_present >> rail.Label(
            "No") >> is_departmentgroup_uri_present
        
        put_project_team_member_billing_rates_for_costCenter >> has_cost_center_data
        
        has_cost_center_data >> rail.Label(
            "Yes") >> add_cost_center_data_to_list

        has_cost_center_data >> rail.Label(
            "No") >> is_departmentgroup_uri_present

        add_cost_center_data_to_list >> is_departmentgroup_uri_present

        is_departmentgroup_uri_present >> rail.Label(
            "Yes") >> department_group_start >> has_department_present_in_project

        is_departmentgroup_uri_present >> rail.Label(
            "No") >> is_employeetype_uri_present

        put_project_team_member_billing_rates_for_departmentGroup >> has_department_data
        
        has_department_data >> rail.Label(
            "Yes") >> add_department_data_to_list

        has_department_data >> rail.Label(
            "No") >> is_employeetype_uri_present

        add_department_data_to_list >> is_employeetype_uri_present

        is_employeetype_uri_present >> rail.Label(
            "Yes") >> employeetype_group_start >> has_employeetype_present_in_project

        is_employeetype_uri_present >> rail.Label(
            "No") >> is_location_uri_present
        
        put_project_team_member_billing_rates_for_employeeTypeGroup >> has_employee_type_data
        
        has_employee_type_data >> rail.Label(
            "Yes") >> add_employee_type_data_to_list

        has_employee_type_data >> rail.Label(
            "No") >> is_location_uri_present

        add_employee_type_data_to_list >> is_location_uri_present

        is_location_uri_present >> rail.Label(
            "Yes") >> location_group_start >> has_location_present_in_project

        is_location_uri_present >> rail.Label(
            "No") >> is_servicecenter_uri_present
        
        put_project_team_member_billing_rates_for_location >> has_location_data
        
        has_location_data >> rail.Label(
            "Yes") >> add_location_data_to_list

        has_location_data >> rail.Label(
            "No") >> is_servicecenter_uri_present

        add_location_data_to_list >> is_servicecenter_uri_present

        is_servicecenter_uri_present >> rail.Label(
            "Yes") >> servicecenter_group_start >> has_servicecenter_present_in_project

        is_servicecenter_uri_present >> rail.Label(
            "No") >> is_division_uri_present
        
        put_project_team_member_billing_rates_for_service_center >> has_servicecenter_data
        
        has_servicecenter_data >> rail.Label(
            "Yes") >> add_servicecenter_to_list

        has_servicecenter_data >> rail.Label(
            "No") >> is_division_uri_present

        add_servicecenter_to_list >> is_division_uri_present

        is_division_uri_present >> rail.Label(
            "Yes") >> division_group_start >> has_division_present_in_project

        is_division_uri_present >> rail.Label(
            "No") >> for_each_team_member_added_end
        
        put_project_team_member_billing_rates_for_division >> has_division_data
        
        has_division_data >> rail.Label(
            "Yes") >> add_division_data_to_list

        has_division_data >> rail.Label(
            "No") >> for_each_team_member_added_end

        add_division_data_to_list >> for_each_team_member_added_end

        for_each_team_member_added >> for_each_team_member_added_end

        for_each_team_member_added_end >> get_resource_list_data >> has_resouse_list_data_present

        has_resouse_list_data_present >> rail.Label(
            "Yes") >> get_all_user_uris

        has_resouse_list_data_present >> rail.Label(
            "No") >> finish

        get_all_user_uris >> bulk_update_project_team_members_assignment >> put_project_team_member_billing_rates >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
