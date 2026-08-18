import rail

def update_timesheet_template_task(_group_id, config, user_details_task_id):
    with rail.TaskGroup(group_id=_group_id, prefix_group_id=False):

        start = rail.EmptyOperator(
            task_id = "start"
        )

        is_profile_status_enabled = rail.IfOperator(
            task_id = f"{_group_id}.is_profile_status_enabled",
            test=lambda dag_run: dag_run.conf['mapper_data']['profile_status'].lower() == "enabled",
            yes_task="is_timesheet_template_value_present",
            no_task="finish_update_timesheet_template"
        )

        is_timesheet_template_value_present = rail.IfOperator(
            task_id = "is_timesheet_template_value_present",
            test=lambda dag_run: bool(dag_run.conf['mapper_data']['timesheet_template']),
            yes_task="user_has_any_timesheet_template",
            no_task="finish_update_timesheet_template"
        )


        user_has_any_timesheet_template = rail.IfOperator(
            task_id = "user_has_any_timesheet_template",
            test=lambda: bool(rail.result(user_details_task_id)['timesheetTemplate']),
            yes_task="is_management_level_in_l1l2",
            no_task="can_update_the_timesheet"
        )

        is_management_level_in_l1l2 = rail.IfOperator(
            task_id = "is_management_level_in_l1l2",
            test=lambda dag_run: dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2'],
            yes_task="remove_timesheet_template_assignment",
            no_task="can_update_the_timesheet" 
        )

        remove_timesheet_template_assignment = rail.RepliconServiceOperator(
            task_id = "remove_timesheet_template_assignment",
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data=lambda dag_run:{
                "userUri": dag_run.conf['user_uri'],
                "policySetUri" : rail.result(user_details_task_id)['timesheetTemplate']['uri']
            }
        )

        def can_update_the_timesheet_test(dag_run):
            if (not rail.result(user_details_task_id)['timesheetTemplate']) or (
                rail.result(user_details_task_id)['timesheetTemplate']['name'] != dag_run.conf['mapper_data']['timesheet_template']):
                return True
            return False

        can_update_the_timesheet = rail.IfOperator(
            task_id = "can_update_the_timesheet",
            test=can_update_the_timesheet_test,
            yes_task="is_management_level_not_in_l1l2",
            no_task="finish_update_timesheet_template"
        )

        is_management_level_not_in_l1l2 = rail.IfOperator(
            task_id = "is_management_level_not_in_l1l2",
            test=lambda dag_run: dag_run.conf['file_data']['management_lvl'] not in ['L1', 'L2'],
            yes_task="is_timesheet_uri_present",
            no_task="finish_update_timesheet_template" 
        )

        is_timesheet_uri_present = rail.IfOperator(
            task_id = "is_timesheet_uri_present",
            test=lambda dag_run: bool(dag_run.conf['policy_sets']['timesheet_template'] and dag_run.conf['policy_sets']['timesheet_template']['uri']),
            yes_task="assign_timesheet_template",
            no_task="log_timesheet_template_exception"
        )

        assign_timesheet_template = rail.RepliconServiceOperator(
            task_id="assign_timesheet_template",
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data= lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "policySetUri": dag_run.conf['policy_sets']['timesheet_template']['uri']
            }
        )

        log_timesheet_template_exception = rail.PythonOperator(
            task_id = "log_timesheet_template_exception",
            python_callable=lambda dag_run: f"""Timesheet template {dag_run.conf['mapper_data']['timesheet_template']} not available in Replicon"""
        )

        finish_update_timesheet_template = rail.EmptyOperator(
            task_id = "finish_update_timesheet_template"
        )

        start >> is_profile_status_enabled >> rail.Label("No") >> finish_update_timesheet_template

        is_profile_status_enabled >> rail.Label("Yes") >> is_timesheet_template_value_present >> rail.Label("No") >> finish_update_timesheet_template

        is_timesheet_template_value_present >> rail.Label("Yes") >> user_has_any_timesheet_template >> rail.Label("No") >> can_update_the_timesheet
        user_has_any_timesheet_template >> rail.Label("Yes") >> is_management_level_in_l1l2 >> rail.Label("No") >> can_update_the_timesheet
        is_management_level_in_l1l2 >> rail.Label("Yes") >> remove_timesheet_template_assignment >> can_update_the_timesheet

        can_update_the_timesheet >> rail.Label("No") >> finish_update_timesheet_template
        can_update_the_timesheet >> rail.Label("Yes") >> is_management_level_not_in_l1l2 >> rail.Label("No") >> finish_update_timesheet_template
        is_management_level_not_in_l1l2 >> rail.Label("Yes") >> is_timesheet_uri_present >> rail.Label("Yes") >> assign_timesheet_template >> finish_update_timesheet_template
        is_timesheet_uri_present >> rail.Label("No") >> log_timesheet_template_exception >> finish_update_timesheet_template

        return start, finish_update_timesheet_template
