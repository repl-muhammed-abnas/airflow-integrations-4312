from datetime import timedelta
import json
from airflow.models import Variable
import rail
from ascendmaterials.user_import.mappers.ascend_master_mapper_file_mapper import ascend_master_mapper_file

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.timeoff_add_dag_id,
        description=f'Ascend_Child Workflow to add timeoff type for new user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_enabled_timeoff_types'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_enabled_timeoff_types',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=None
        )

        if_first_displaytext_present = rail.IfOperator(
            task_id='if_first_displaytext_present',
            test='''{{ result('get_enabled_timeoff_types')[0].displayText | is_truthy }}''',
            yes_task="log_locationswhereemployeetypeneedstobeconsidered",
            no_task="catch_and_log_errors",
        )

        # Merged: mapper_search_entries_1 + log_locationswhereemployeetypeneedstobeconsidered
        # Filters mapper for "Timeoff Type"/"Employee Type Check" and returns the first entry's value
        log_locationswhereemployeetypeneedstobeconsidered = rail.PythonOperator(
            task_id='log_locationswhereemployeetypeneedstobeconsidered',
            python_callable=lambda: next(
                (x['value'] for x in ascend_master_mapper_file
                 if x["location"] == "Timeoff Type" and x["type"] == "Employee Type Check"),
                None
            )
        )

        def normalize_udf(val):
            return "" if not val or str(val).strip().lower() in ('none', 'null') else str(val).strip()

        def get_employee_type(dag_run):
            employee_type = "All"
            if normalize_udf(dag_run.conf.get("scheduledhours")) and \
                    rail.result('log_locationswhereemployeetypeneedstobeconsidered') and \
                    rail.result('log_locationswhereemployeetypeneedstobeconsidered') in dag_run.conf["location"]:
                return dag_run.conf["employeetype"]
            return employee_type

        mapper_search_entries_2 = rail.PythonOperator(
            task_id='mapper_search_entries_2',
            python_callable=lambda dag_run: list(filter(lambda x: x["location"] == dag_run.conf["location"] and
                                                        x["type"] == "Timeoff Type" and
                                                        normalize_udf(x["udf"]) == normalize_udf(dag_run.conf.get("scheduledhours")) and
                                                        x['employee_type'] == get_employee_type(dag_run), ascend_master_mapper_file))
        )

        if_entry_col1_blank = rail.IfOperator(
            task_id='if_entry_col1_blank',
            test='''{{ result('mapper_search_entries_2') | is_falsy }}''',
            yes_task="send_reply",
            no_task="log_final_set_timeoff_uris",
        )

        send_reply = rail.PythonOperator(
            task_id='send_reply',
            python_callable=lambda dag_run: f'Timeoff not assigned/updated as no timeoff is defined in mapper for {dag_run.conf["location"]} - {dag_run.conf["employeetype"]} - {dag_run.conf["scheduledhours"]}'
        )

        # Replaced: foreach_ascend_master_mapper_file_search_entries + accumulate_list_items_1
        #           + foreach_mapper_end + original log_final_set_timeoff_uris
        # Builds list of timeoff URIs directly from mapper results and enabled timeoff types
        log_final_set_timeoff_uris = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris',
            python_callable=lambda: (
                lambda entries: [x['uri'] for x in entries if x['uri']] if entries and entries[0]['name'] else ""
            )([
                {
                    "name": entry['value'],
                    "uri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_enabled_timeoff_types'),
                        'displayText',
                        entry['value'],
                        'uri'
                    )
                }
                for entry in rail.result('mapper_search_entries_2')
            ])
        )

        if_present_1 = rail.IfOperator(
            task_id='if_present_1',
            test='''{{ result('log_final_set_timeoff_uris') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user",
            no_task="catch_and_log_errors",
        )

        put_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf["useruri"],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris')
            }
        )

        get_eligible_time_off_types_for_booking_time_off = rail.RepliconServiceOperator(
            task_id='get_eligible_time_off_types_for_booking_time_off',
            endpoint="/services/TimeOffService1.svc/GetEligibleTimeOffTypesForBookingTimeOff",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        # Removed: accumulate_list_items_2 — stored displayText into 'assigned_timeoff_types'
        # variable which was never read by any downstream task
        foreach_response = rail.ForEachOperator(
            task_id='foreach_response',
            items="{{ result('get_eligible_time_off_types_for_booking_time_off') | to_json }}",
            start_task='get_default_time_off_type_policy_schedule_for_user',
            end_task='foreach_response_17_end'
        )

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": '{{ dag_run.conf["useruri"] }}',
                    "timeOffTypeUri": "{{ result('foreach_response').uri }}"
                }
            }
        )

        log_timeoff_policy = rail.PythonOperator(
            task_id='log_timeoff_policy',
            python_callable=lambda: json.loads(
                json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user'))
                .replace('null', '"effective"')
                .replace('"script"', '"scriptTarget"')
            ) if rail.result('get_default_time_off_type_policy_schedule_for_user') and
            rail.result('get_default_time_off_type_policy_schedule_for_user')[0]['policySet'] else None
        )

        if_present_2 = rail.IfOperator(
            task_id='if_present_2',
            test='''{{ result('log_timeoff_policy') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule",
            no_task="foreach_response_17_end",
        )

        put_user_time_off_account_policy_set_schedule = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": rail.result('foreach_response')['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy')
            }
        )

        foreach_response_17_end = rail.EmptyOperator(
            task_id='foreach_response_17_end',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": "",
                "userloginname": dag_run.conf.get('userloginname', ''),
                "action": "Timeoff Assignment",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        # ── Wiring ──────────────────────────────────────────────────────
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_enabled_timeoff_types
        get_enabled_timeoff_types >> if_first_displaytext_present
        if_first_displaytext_present >> rail.Label('Yes') >> log_locationswhereemployeetypeneedstobeconsidered >> mapper_search_entries_2 >> if_entry_col1_blank
        if_entry_col1_blank >> rail.Label('Yes') >> send_reply >> catch_and_log_errors
        if_entry_col1_blank >> rail.Label('No') >> log_final_set_timeoff_uris >> if_present_1
        if_present_1 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user >> get_eligible_time_off_types_for_booking_time_off >> foreach_response >> get_default_time_off_type_policy_schedule_for_user >> log_timeoff_policy >> if_present_2
        if_present_2 >> rail.Label('Yes') >> put_user_time_off_account_policy_set_schedule >> foreach_response_17_end
        if_present_2 >> rail.Label('No') >> foreach_response_17_end
        foreach_response >> foreach_response_17_end >> catch_and_log_errors
        if_present_1 >> rail.Label('No') >> catch_and_log_errors
        if_first_displaytext_present >> rail.Label('No') >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
