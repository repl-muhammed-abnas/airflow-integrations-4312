import rail
from pwcglobal.user_import_australia import custom_methods
from pwcglobal.user_import_australia.tasks.get_users_data import get_users_data_task
from pwcglobal.user_import_australia.mappers.caller_task_service_call_mapper import caller_task_service_call_mapper


def process_allowance_dag(caller_task):
    with rail.TaskGroup(group_id="process_allowance_dag", prefix_group_id=False):

        get_details_by_caller_task = rail.PythonOperator(
            task_id="get_details_by_caller_task",
            python_callable=lambda: caller_task_service_call_mapper[caller_task]
        )

        is_record_to_ignore = rail.IfOperator(
            task_id="is_record_to_ignore",
            test=custom_methods.can_record_be_ignored,
            yes_task="log_record_to_ignore",
            no_task="is_both_date_not_present"
        )
        log_record_to_ignore = rail.WriteLogOperator(
            task_id="log_record_to_ignore",
            log="{{dag_run.conf.log}}",
            message="{{dag_run.conf.compensation_element}} is not allowed",
            severity="Ignored",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "status": "Ignored",
                "details": f"{dag_run.conf['compensation_element']} is not allowed",
                "employeeid": dag_run.conf['employee_id']
            }
        )
        is_both_date_not_present = rail.IfOperator(
            task_id="is_both_date_not_present",
            test="{{dag_run.conf.compensation_plan_effective_date | is_truthy or dag_run.conf.expected_end_date | is_truthy }}",
            yes_task="is_expected_end_date_present",
            no_task="log_record_not_allowed"
        )
        log_record_not_allowed = rail.WriteLogOperator(
            task_id="log_record_not_allowed",
            log="{{dag_run.conf.log}}",
            message=lambda dag_run: f"{dag_run.conf['compensation_element']} is not allowed" if (dag_run.conf['mapper_details']['replicongroup'] and
                                        "Ignored" in dag_run.conf['mapper_details']['replicongroup']) else "compensationeffectivedate is missing",
            severity="Ignored",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "status": "Ignored",
                "details": "{{dag_run.conf.compensation_element}} is not allowed" if (not dag_run.conf['mapper_details']['replicongroup'] and
                                "Ignored" in dag_run.conf['mapper_details']['replicongroup']) else "compensationeffectivedate is missing",
                "employeeid": dag_run.conf['employee_id']
            }
        )

        is_expected_end_date_present = rail.IfOperator(
            task_id="is_expected_end_date_present",
            test="{{dag_run.conf.expected_end_date != None}}",
            yes_task="process_expected_end_date",
            no_task="get_users_data",
        )
        process_expected_end_date = rail.PythonOperator(
            task_id="process_expected_end_date",
            python_callable=custom_methods.process_allowance_dates
        )

        is_invalid_dates = rail.IfOperator(
            task_id="is_invalid_dates",
            test="{{ result('process_expected_end_date')['log_invalid_dates'] == 'True' }}",
            yes_task="log_invalid_dates",
            no_task="get_users_data"
        )
        log_invalid_dates = rail.WriteLogOperator(
            task_id="log_invalid_dates",
            log="{{dag_run.conf.log}}",
            message="Allowance end date is before the start date. Start date: {{dag_run.conf.compensation_plan_effective_date}}\
                 & End date: {{dag_run.conf.expected_end_date}}",
            severity="Ignored",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "status": "Ignored",
                "details": f"Allowance end date is before the start date. Start date: {dag_run.conf['confcompensation_plan_effective_date']}\
                 & End date: {dag_run.conf['expected_end_date']}",
                "employeeid": dag_run.conf['employee_id']
            }
        )

        get_users_data, is_user_enabled, finish = get_users_data_task(
            caller="process_allowance_dag", next_task_id=f"is_replicon_group_{caller_task}")

        is_replicon_group_correct = rail.IfOperator(
            task_id=f"is_replicon_group_{caller_task}",
            test=lambda dag_run: dag_run.conf['mapper_details']['replicongroup'] == rail.result(
                'get_details_by_caller_task')['Group'],
            yes_task=[f"get_enabled_{caller_task}",
                      "get_schedule_for_user"]
        )

        get_enabled_caller_groups = rail.RepliconServiceOperator(
            task_id=f"get_enabled_{caller_task}",
            endpoint="{{result('get_details_by_caller_task').get_details}}",
            response_filter=custom_methods.get_cost_center_response_filter
        )
        get_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_schedule_for_user",
            endpoint="services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{result('get_users_data')[0].user_uri}}"
            },
            response_filter=custom_methods.get_schedule_for_user_response_filter
        )

        has_user_any_group_assigned = rail.IfOperator(
            task_id=f"has_user_any_{caller_task}",
            test="{{result('get_schedule_for_user') | is_truthy}}",
            # process with user's cost centers
            yes_task="get_can_update_status",
            # process without user's cost centers
            no_task="is_both_dates_present"
        )

        is_both_dates_present = rail.IfOperator(
            task_id="is_both_dates_present",
            test="{{dag_run.conf.compensation_plan_effective_date | is_truthy and dag_run.conf.expected_end_date | is_truthy }}",
            yes_task=f"add_{caller_task}_schedule_for_user",
            no_task="is_compensation_plan_effective_date_present"
        )

        get_can_update_status = rail.PythonOperator(
            task_id="get_can_update_status",
            python_callable=custom_methods.get_can_update_status
        )

        can_update_group = rail.IfOperator(
            task_id=f"can_update_group_{caller_task}",
            test=custom_methods.bool_can_update_cost_center,
            yes_task=f"add_{caller_task}_schedule_for_user",
            no_task="log_update_skipped"
        )
        log_update_skipped = rail.WriteLogOperator(
            task_id="log_update_skipped",
            log="{{dag_run.conf.log}}",
            message="Start & End allowances schedule is already present",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "Start & End allowances schedule is already present",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_compensation_plan_effective_date_present = rail.IfOperator(
            task_id="is_compensation_plan_effective_date_present",
            test="{{dag_run.conf.compensation_plan_effective_date | is_truthy}}",
            yes_task=f"add_{caller_task}_schedule_for_user",
        )
        # is_both_dates_present, can_update_group, log_update_skipped, finish
        is_record_to_ignore >> rail.Label(
            "Yes") >> log_record_to_ignore >> finish
        is_record_to_ignore >> rail.Label("No") >> is_both_date_not_present >> rail.Label(
            "Yes") >> is_expected_end_date_present >> rail.Label("No") >> get_users_data

        is_both_date_not_present >> rail.Label(
            "No") >> log_record_not_allowed >> finish
        is_expected_end_date_present >> rail.Label("Yes") >> process_expected_end_date >> is_invalid_dates >> rail.Label(
            "Yes") >> log_invalid_dates >> finish
        is_invalid_dates >> rail.Label("No") >> get_users_data

        is_user_enabled >> rail.Label("Yes") >> is_replicon_group_correct

        get_details_by_caller_task >> is_record_to_ignore
        is_replicon_group_correct >> rail.Label(
            "Yes") >> [get_enabled_caller_groups, get_schedule_for_user]
        [get_enabled_caller_groups, get_schedule_for_user] >> has_user_any_group_assigned >> rail.Label(
            "No") >> is_both_dates_present

        is_both_dates_present
        is_both_dates_present >> rail.Label(
            "No") >> is_compensation_plan_effective_date_present

        has_user_any_group_assigned >> rail.Label(
            "Yes") >> get_can_update_status >> can_update_group

        can_update_group >> rail.Label("No") >> log_update_skipped

    return is_both_dates_present, can_update_group, log_update_skipped, is_compensation_plan_effective_date_present, finish
