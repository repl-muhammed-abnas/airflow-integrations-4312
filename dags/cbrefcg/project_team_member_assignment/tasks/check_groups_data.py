import rail
from cbrefcg.project_team_member_assignment.utils import request_payload


def get_respective_groups_data(group_name,filter_name, service_call_name, update_group = False):
    with rail.TaskGroup(group_id=f"get_respective_groups_data_{group_name}", prefix_group_id=False):

        can_update_group_data = rail.IfOperator(
            task_id = f'can_update_group_data_{group_name}',
            test= bool(update_group),
            yes_task= f'get_data_for_specific_group_{group_name}',
            no_task= f'has_group_data_present_in_project_{group_name}'
        )

        has_group_data_present_in_project = rail.IfOperator(
            task_id=f'has_group_data_present_in_project_{group_name}',
            test="{{ result('bulk_get_project_details')[0].projectDetails."+group_name+" | is_falsy }}",
            yes_task= f"update_group_{group_name}",
            no_task= f"get_data_for_specific_group_{group_name}",
        )

        update_group= rail.RepliconServiceOperator(
            task_id=f'update_group_{group_name}',
            endpoint="/services/ProjectService1.svc/"+service_call_name,
            data={
                  "projectUri": "{{ dag_run.conf.projecturi }}",
                  group_name: {
                    "uri": "{{ result('for_each_team_member_added').resource."+group_name+".uri }}"
                }
            }
        )

        get_data_for_specific_group= rail.RepliconServiceOperator(
            task_id=f'get_data_for_specific_group_{group_name}',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.get_groups_data_payload(filter_name, group_name)
        )

        put_project_team_member_billing_rates_for = rail.RepliconServiceOperator(
            task_id=f'put_project_team_member_billing_rates_for_{group_name}',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data={
                  "projectUri": "{{ dag_run.conf.projecturi }}",
                  "resourceUri": "{{ result('for_each_team_member_added').resource."+group_name+".uri }}",
                  "billingRateUris": []
              }
        )

        # has_group_data= rail.IfOperator(
        #     task_id=f'has_group_data_{group_name}',
        #     test=lambda: custom_method.has_group_data(rail.result(f"get_data_for_specific_group_{group_name}")),
        #     yes_task= f"add_group_data_to_list_{group_name}",
        #     no_task= end_task,
        # )

        # add_group_data_to_list= rail.SetVariableOperator(
        #     task_id=f'add_group_data_to_list_{group_name}',
        #     append=True,
        #     name='{{ result("create_resource_list").name }}',
        #     value= lambda: custom_method.add_items_to_list(
        #             rail.result(f"get_data_for_specific_group_{group_name}"), group_name)
        # )

        can_update_group_data >> rail.Label(
            "Yes") >> get_data_for_specific_group

        can_update_group_data >> rail.Label(
            "No") >> has_group_data_present_in_project

        has_group_data_present_in_project >> rail.Label(
            "Yes") >> update_group >> get_data_for_specific_group

        has_group_data_present_in_project >> rail.Label(
            "No") >> get_data_for_specific_group

        get_data_for_specific_group >> put_project_team_member_billing_rates_for

        # has_group_data >> rail.Label(
        #     "Yes") >> add_group_data_to_list

    return can_update_group_data, put_project_team_member_billing_rates_for
