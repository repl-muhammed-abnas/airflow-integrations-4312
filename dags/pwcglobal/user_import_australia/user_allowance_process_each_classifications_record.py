import rail
from pwcglobal.user_import_australia import request_payload, custom_methods


# pylint: disable=too-many-statements
def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_classifications_records_{config.instance}",
        description=f"PwCGlobal User Import Australia User Allowance child process each classifications records {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.child_max_active_runs
    )as dag:
        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        caller_task = "classifications"

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
                "details": "{{dag_run.conf.compensation_element}} is not allowed",
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
            message=lambda dag_run: "{{dag_run.conf.compensation_element}} is not allowed" if (not dag_run.conf['mapper_details']['replicongroup'] and
                             "Ignored" in dag_run.conf['mapper_details']['replicongroup']) else "compensation effective date is missing",
            severity="Ignored",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "status": "Ignored",
                "details": "{{dag_run.conf.compensation_element}} is not allowed" if (not dag_run.conf['mapper_details']['replicongroup'] and
                            "Ignored" in dag_run.conf['mapper_details']['replicongroup']) else "compensation effective date is missing",
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
                "details": "Allowance end date is before the start date. Start date: {{dag_run.conf.compensation_plan_effective_date}}\
                 & End date: {{dag_run.conf.expected_end_date}}",
                "employeeid": dag_run.conf['employee_id']
            }
        )

        # get_users_data, is_user_enabled, finish = get_users_data_task(
        #     caller=None, next_task_id="is_replicon_group_classifications")

        caller=None

        get_users_data = rail.RepliconServiceOperator(
            task_id="get_users_data",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_get_data_payload,
            response_filter=custom_methods.get_user_data
        )

        is_user_exists = rail.IfOperator(
            task_id="is_user_exists",
            test="{{result('get_users_data') | is_truthy}}",
            yes_task="is_user_enabled",
            no_task="log_user_does_not_exists"
        )

        log_user_does_not_exists = rail.WriteLogOperator(
            task_id="log_user_does_not_exists",
            log="{{dag_run.conf.log}}",
            message="User not found",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User not found",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id="is_user_enabled",
            test="{{result('get_users_data')[0].enabled}}"
            if caller else "{{result('get_users_data')[0].enabled}}",
            yes_task="is_replicon_group_classifications",
            no_task="log_user_already_disabled"
        )


        log_user_already_disabled = rail.WriteLogOperator(
            task_id="log_user_already_disabled",
            log="{{dag_run.conf.log}}",
            message="User is already disabled",
            severity="Ignored",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "User is already disabled",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        is_replicon_group_correct = rail.IfOperator(
            task_id=f"is_replicon_group_{caller_task}",
            test="{{dag_run.conf.mapper_details.replicongroup == 'Classification'}}",
            yes_task=["get_enabled_classifications",
                      "get_schedule_for_user"]
        )

        get_enabled_classifications = rail.RepliconServiceOperator(
            task_id="get_enabled_classifications",
            endpoint="/services/ServiceCenterService1.svc/GetEnabledServiceCenters",
            response_filter=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response.json()['d'], "displayText", (dag_run.conf['compensation_element'].split("-")[-1]).strip())
        )
        is_classification_present = rail.IfOperator(
            task_id="is_classification_present",
            test="{{result('get_enabled_classifications') | is_truthy}}",
            yes_task="process",
            no_task="log_no_classification_present"
        )
        log_no_classification_present = rail.WriteLogOperator(
            task_id="log_no_classification_present",
            log="{{dag_run.conf.log}}",
            message="{{dag_run.conf.compensation_element | split('-') | last }} is not available under classification in Replicon",
            severity="Success",
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Success",
                "details": "{{dag_run.conf.compensation_element | split('-') | last }} is not available under classification in Replicon",
                "employeeid": "{{dag_run.conf.employee_id}}"
            }
        )

        get_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_schedule_for_user",
            endpoint="services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{result('get_users_data')[0].user_uri}}"
            },
            response_filter=custom_methods.get_classification_schedule_for_user_response_filter
        )

        process = rail.IfOperator(
            task_id="process",
            test="{{result('get_schedule_for_user') | is_truthy}}",
            yes_task="get_can_update_status",
            no_task="get_previous_service_center_schedule_for_user"
        )

        get_can_update_status = rail.PythonOperator(
            task_id="get_can_update_status",
            python_callable=custom_methods.get_classification_can_update_status
        )

        can_update_group_classifications = rail.IfOperator(
            task_id="can_update_group_classifications",
            test=custom_methods.bool_can_update_cost_center,
            yes_task="get_previous_service_center_schedule_for_user",
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

        def get_previous_schedule_response_filter(response):
            response = response.json()['d']
            if not response:
                return None
            current_schedule = rail.result('get_schedule_for_user')[0]
            current_schedule_date = custom_methods.convert_to_date(
                current_schedule['effective_date_json'], "json").strftime("%d-%m-%Y")
            if not current_schedule_date:
                return None
            data = list(filter(lambda x: x['effective_date'] <= current_schedule_date if x['effective_date'] else True, map(
                lambda item: {
                    "effective_date": custom_methods.convert_to_date(item['effectiveDate'], "json").strftime("%d-%m-%Y") if item['effectiveDate'] else None,
                    "name": item['serviceCenter']['displayText'],
                    "uri": item['serviceCenter']['uri']
                }, response)))

            return data[-2] if len(data) > 1 else data[0]

        get_previous_service_center_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_previous_service_center_schedule_for_user",
            endpoint="services/ServiceCenterService1.svc/GetServiceCenterScheduleForUser",
            data={
                "userUri": "{{result('get_users_data')[0].user_uri}}"
            },
            response_filter=get_previous_schedule_response_filter
        )

        add_classifications_schedule_for_user_with_both_dates = rail.RepliconServiceOperator(
            task_id="add_classifications_schedule_for_user_with_both_dates",
            endpoint="services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_add_classifications_schedule_for_user_applyModification
        )

        log_success = rail.WriteLogOperator(
            task_id="log_success",
            log="{{dag_run.conf.log}}",
            message=lambda dag_run: "Allowance added" if dag_run.conf['compensation_plan_effective_date']
            and dag_run.conf['expected_end_date'] else "Start date allowance added",
            severity="Success",
            properties=lambda dag_run: {
                "guid": dag_run.conf['guid'],
                "status": "Success",
                "details": "Allowance added" if dag_run.conf['compensation_plan_effective_date']
                    and dag_run.conf['expected_end_date'] else "Start date allowance added",
                "employeeid": dag_run.conf['employee_id']
            }
        )
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "employeeid": "{{dag_run.conf.employee_id}}"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        is_record_to_ignore >> rail.Label(
            "Yes") >> log_record_to_ignore >> catch_and_log_errors
        is_record_to_ignore >> rail.Label("No") >> is_both_date_not_present >> rail.Label(
            "Yes") >> is_expected_end_date_present >> rail.Label("No") >> get_users_data

        is_both_date_not_present >> rail.Label(
            "No") >> log_record_not_allowed >> catch_and_log_errors
        is_expected_end_date_present >> rail.Label("Yes") >> process_expected_end_date >> is_invalid_dates >> rail.Label(
            "Yes") >> log_invalid_dates >> catch_and_log_errors
        is_invalid_dates >> rail.Label("No") >> get_users_data

        get_users_data >> is_user_exists >> rail.Label(
            "No") >> log_user_does_not_exists >> catch_and_log_errors
        is_user_exists >> rail.Label("Yes") >> is_user_enabled
        is_user_enabled >> rail.Label(
            "No") >> log_user_already_disabled >> catch_and_log_errors

        is_user_enabled >> rail.Label("Yes") >> is_replicon_group_correct

        is_replicon_group_correct >> rail.Label(
            "Yes") >> [get_enabled_classifications, get_schedule_for_user]
        get_enabled_classifications >> is_classification_present >> rail.Label(
            "No") >> log_no_classification_present >> catch_and_log_errors
        get_enabled_classifications >> is_classification_present >> rail.Label(
            "Yes") >> process
        get_schedule_for_user >> process

        process >> rail.Label(
            "Yes") >> get_can_update_status >> can_update_group_classifications
        process >> rail.Label(
            "No") >> get_previous_service_center_schedule_for_user
        can_update_group_classifications >> rail.Label(
            "No") >> log_update_skipped

        can_update_group_classifications >> rail.Label(
            "Yes") >> get_previous_service_center_schedule_for_user >> add_classifications_schedule_for_user_with_both_dates \
            >> log_success >> catch_and_log_errors
        log_update_skipped >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
