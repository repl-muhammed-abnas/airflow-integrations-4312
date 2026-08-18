from datetime import timedelta, datetime
import json
from airflow.models import Variable
import rail
from ascendmaterials.user_import.mappers.ascend_master_mapper_file_mapper import ascend_master_mapper_file
from ascendmaterials.user_import.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.timeoff_update_dag_id,
        description=f'Ascend_Child Workflow to add/remove timeoff type for Update/Rehire {config.instance}',
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
            no_task='log_locationswhereemployeetypeneedstobeconsidered'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_locationswhereemployeetypeneedstobeconsidered',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        # Merged: mapper_search_entries_1 + log_locationswhereemployeetypeneedstobeconsidered
        log_locationswhereemployeetypeneedstobeconsidered = rail.PythonOperator(
            task_id='log_locationswhereemployeetypeneedstobeconsidered',
            python_callable=lambda: next(
                (x['value'] for x in ascend_master_mapper_file
                 if x["type"] == "Employee Type Check" and x["location"] == "Timeoff Type"),
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
            python_callable=lambda dag_run: list(filter(lambda x: x["type"] == "Timeoff Type" and
                                                        x["location"] == dag_run.conf["location"] and
                                                        normalize_udf(x["udf"]) == normalize_udf(dag_run.conf.get("scheduledhours")) and
                                                        x["employee_type"] == get_employee_type(dag_run), ascend_master_mapper_file))
        )

        if_entry_col1_blank = rail.IfOperator(
            task_id='if_entry_col1_blank',
            test='''{{ result('mapper_search_entries_2') | is_falsy }}''',
            yes_task="send_reply",
            no_task="declare_list_1",
        )

        send_reply = rail.PythonOperator(
            task_id='send_reply',
            python_callable=lambda dag_run: f'Timeoff not assigned/updated as no timeoff is defined in mapper for {dag_run.conf["location"]} - {dag_run.conf["employeetype"]} - {dag_run.conf["scheduledhours"]}'
        )

        declare_list_1 = rail.SetVariableOperator(
            task_id='declare_list_1',
            append=False,
            name='assigned_timeoff_types',
            value=[]
        )

        get_todaysdate = rail.PythonOperator(
            task_id='get_todaysdate',
            python_callable=python_callable.split_todaysdate
        )

        get_dateofchanges = rail.PythonOperator(
            task_id='get_dateofchanges',
            python_callable=lambda dag_run: python_callable.get_datetime_obj(
                dag_run.conf["dateused"])
        )

        get_user_time_off_type_policy_summary_1 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_1',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        foreach_d_1 = rail.ForEachOperator(
            task_id='foreach_d_1',
            items="{{ result('get_user_time_off_type_policy_summary_1').policiesByTimeOffType | to_json }}",
            start_task='if_timeoff_allowed_1',
            end_task='foreach_d_13_end'
        )

        if_timeoff_allowed_1 = rail.IfOperator(
            task_id='if_timeoff_allowed_1',
            test='''{{ result('foreach_d_1').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="insert_to_list_1",
            no_task="foreach_d_13_end",
        )

        insert_to_list_1 = rail.SetVariableOperator(
            task_id='insert_to_list_1',
            append=True,
            name='{{ result("declare_list_1").name }}',
            value={
                "name": "{{ result('foreach_d_1').timeOffType.name }}",
                "uri": "{{ result('foreach_d_1').timeOffType.uri }}",
                "policyset": "{{ result('foreach_d_1').policySetSchedule }}"
            }
        )

        foreach_d_13_end = rail.EmptyOperator(
            task_id='foreach_d_13_end',
        )

        get_enabled_timeoff_types = rail.RepliconServiceOperator(
            task_id='get_enabled_timeoff_types',
            endpoint="/services/TimeOffService1.svc/GetEnabledTimeOffTypes",
            data=None
        )

        if_first_displaytext_present = rail.IfOperator(
            task_id='if_first_displaytext_present',
            test='''{{ result('get_enabled_timeoff_types')[0].displayText | is_truthy }}''',
            yes_task="log_final_set_timeoff_uris",
            no_task="catch_and_log_errors",
        )

        # Replaced: declare_list_2 + foreach_ascend_master_mapper_file_search_entries
        #           + insert_to_list_2 + accumulate_list_items_1 + foreach_mapper_end
        # accumulate_list_items_1 was an exact duplicate of insert_to_list_2 writing to an unused variable
        log_final_set_timeoff_uris = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris',
            python_callable=lambda: (
                lambda entries: [x['uri'] for x in entries if x['uri']] if entries and entries[0]['name'] else []
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

        get_user_time_off_type_policy_summary_2 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_2',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        foreach_declare_list = rail.ForEachOperator(
            task_id='foreach_declare_list',
            items=lambda: rail.get_dag_run_var('assigned_timeoff_types') or [],
            start_task='log_ifthetimeoff_typeisnotrequiredanymore',
            end_task='foreach_declare_list_9_26_end'
        )

        log_ifthetimeoff_typeisnotrequiredanymore = rail.PythonOperator(
            task_id='log_ifthetimeoff_typeisnotrequiredanymore',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('assigned_timeoff_types'), 'uri', rail.result(
                'foreach_declare_list')['uri']) if rail.get_dag_run_var('assigned_timeoff_types') else ""
        )

        if_ifthetimeoff_typeisnotrequiredanymore_27_blank = rail.IfOperator(
            task_id='if_ifthetimeoff_typeisnotrequiredanymore_27_blank',
            test='''{{ result('log_ifthetimeoff_typeisnotrequiredanymore') | is_falsy }}''',
            yes_task="get_balance_summary_for_account",
            no_task="foreach_declare_list_9_26_end",
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": '{{ dag_run.conf["useruri"] }}',
                    "timeOffTypeUri": "{{ result('foreach_declare_list').uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('get_todaysdate').year }}",
                    "month": "{{ result('get_todaysdate').month }}",
                    "day": "{{ result('get_todaysdate').day }}"
                }
            }
        )

        trigger_timeoff_policy30 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_policy30',
            retries=0,
            trigger_dag_id=config.timeoff_policy_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "parentjobid": '{{ dag_run.conf["parentjobid"] }}',
                "userloginname": '{{ dag_run.conf["userloginname"] }}',
                "useruri": '{{ dag_run.conf["useruri"] }}',
                "timeoffuri": "{{ result('foreach_declare_list').uri }}",
                "policyset": lambda: json.loads(rail.result('foreach_declare_list')['policyset']),
                "enddate": "{{ result('get_dateofchanges').day }}/{{ result('get_dateofchanges').month }}/{{ result('get_dateofchanges').year }}",
                "newschedulebalance": "{{ result('get_balance_summary_for_account').timeRemaining }}",
                "ascend_user_import_logs_lookuptable": '{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}'
            }
        )

        wait_timeoff_policy30 = rail.WaitForDagRunsSensor(
            task_id='wait_timeoff_policy30',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_policy30") }}'
        )

        foreach_declare_list_9_26_end = rail.EmptyOperator(
            task_id='foreach_declare_list_9_26_end',
        )

        foreach_d_2 = rail.ForEachOperator(
            task_id='foreach_d_2',
            items="{{ result('get_user_time_off_type_policy_summary_2').policiesByTimeOffType | to_json }}",
            start_task='if_timeoff_allowed_2',
            end_task='foreach_d_31_end'
        )

        if_timeoff_allowed_2 = rail.IfOperator(
            task_id='if_timeoff_allowed_2',
            test='''{{ result('foreach_d_2').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="log_timeoffisalreadyassigned",
            no_task="catch_and_log_errors",
        )

        log_timeoffisalreadyassigned = rail.PythonOperator(
            task_id='log_timeoffisalreadyassigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.get_dag_run_var('assigned_timeoff_types'), 'uri', rail.result(
                'foreach_d_2')['timeOffType']['uri']) if rail.get_dag_run_var('assigned_timeoff_types') else ""
        )

        if_thetimeoffisalreadyassigned_33_blank = rail.IfOperator(
            task_id='if_thetimeoffisalreadyassigned_33_blank',
            test='''{{ result('log_timeoffisalreadyassigned') | is_falsy }}''',
            yes_task="get_default_time_off_type_policy_schedule_for_user",
            no_task="if_thetimeoffisalreadyassigned_present",
        )

        # Removed: accumulate_list_items_2 — appended {"timeofftype": name} to assigned_timeoff_types
        # but entries have no 'uri' field so find_first_by_attr_and_get_attr(..., 'uri', ...) never matched them

        get_default_time_off_type_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": '{{ dag_run.conf["useruri"] }}',
                    "timeOffTypeUri": "{{ result('foreach_d_2').timeOffType.uri }}"
                }
            }
        )

        log_timeoff_policy_1 = rail.PythonOperator(
            task_id='log_timeoff_policy_1',
            python_callable=lambda: json.loads(
                json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user'))
                .replace('null', '"effective"')
                .replace('"script"', '"scriptTarget"')
            ) if rail.result('get_default_time_off_type_policy_schedule_for_user') and
            rail.result('get_default_time_off_type_policy_schedule_for_user')[0].get('policySet') else None
        )

        if_present_2 = rail.IfOperator(
            task_id='if_present_2',
            test='''{{ result('log_timeoff_policy_1') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_1",
            no_task="if_thetimeoffisalreadyassigned_present",
        )

        put_user_time_off_account_policy_set_schedule_1 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_1',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": rail.result('foreach_d_2')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policy_1')
            }
        )

        if_thetimeoffisalreadyassigned_present = rail.IfOperator(
            task_id='if_thetimeoffisalreadyassigned_present',
            test='''{{ result('log_timeoffisalreadyassigned') | is_truthy  and dag_run.conf["rehire"] == 'yes' }}''',
            yes_task="get_defaultpolicyfromgloballevel",
            no_task="catch_and_log_errors",
        )

        get_defaultpolicyfromgloballevel = rail.RepliconServiceOperator(
            task_id='get_defaultpolicyfromgloballevel',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffPolicySetScheduleForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('foreach_d_2').timeOffType.uri }}"
            }
        )

        log_timeoff_policy_2 = rail.PythonOperator(
            task_id='log_timeoff_policy_2',
            python_callable=lambda: rail.result('get_defaultpolicyfromgloballevel')[0].get('policySet')
            if rail.result('get_defaultpolicyfromgloballevel') else None
        )

        if_timeoff_policy_present = rail.IfOperator(
            task_id='if_timeoff_policy_present',
            test='''{{ result('log_timeoff_policy_2') | is_truthy }}''',
            yes_task="declare_list_3",
            no_task="log_timeoff_policies",
        )

        declare_list_3 = rail.SetVariableOperator(
            task_id='declare_list_3',
            append=False,
            name='policyschedules',
            value=[]
        )

        log_existing_policy = rail.PythonOperator(
            task_id='log_existing_policy',
            python_callable=lambda: json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_2')[
                                               'policiesByTimeOffType'], 'timeOffType.uri', rail.result('foreach_d_2')['timeOffType']['uri'], 'policySetSchedule', ""))
        )

        if_existing_policy_present = rail.IfOperator(
            task_id='if_existing_policy_present',
            test='''{{ result('log_existing_policy') | is_truthy }}''',
            yes_task="parse_json",
            no_task="foreach_response",
        )

        parse_json = rail.PythonOperator(
            task_id='parse_json',
            python_callable=lambda: json.loads(
                rail.result('log_existing_policy'))
        )

        foreach_document = rail.ForEachOperator(
            task_id='foreach_document',
            items=lambda: rail.result('parse_json'),
            start_task='log_effectivedateforcomparison',
            end_task='foreach_document_50_end'
        )

        log_effectivedateforcomparison = rail.PythonOperator(
            task_id='log_effectivedateforcomparison',
            python_callable=lambda: str(rail.result('foreach_document')['effectiveDate']['day']) + "/" +
            str(rail.result('foreach_document')['effectiveDate']['month']) + "/" +
            str(rail.result('foreach_document')['effectiveDate']['year'])
        )

        if_date_in_past = rail.IfOperator(
            task_id='if_date_in_past',
            test=lambda dag_run: datetime.strptime(rail.result('log_effectivedateforcomparison'), "%d/%m/%Y").isoformat(
            ) < datetime.strptime(dag_run.conf["dateused"], "%d/%m/%Y").isoformat(),
            yes_task="insert_to_list_3",
            no_task="foreach_document_50_end",
        )

        insert_to_list_3 = rail.SetVariableOperator(
            task_id='insert_to_list_3',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.result('foreach_document')['effectiveDate']['day'],
                    "month": rail.result('foreach_document')['effectiveDate']['month'],
                    "year": rail.result('foreach_document')['effectiveDate']['year']
                },
                "description": rail.result('foreach_document').get('description') or "effective",
                "policySet": rail.result('foreach_document')['policySet']
            }
        )

        foreach_document_50_end = rail.EmptyOperator(
            task_id='foreach_document_50_end',
        )

        foreach_d_31_end = rail.EmptyOperator(
            task_id='foreach_d_31_end',
        )

        foreach_response = rail.ForEachOperator(
            task_id='foreach_response',
            items="{{ result('get_defaultpolicyfromgloballevel') | to_json }}",
            start_task='get_required_effective_date',
            end_task='foreach_response_55_end'
        )

        # Merged: log_required_effective_date + get_required_effective_date
        get_required_effective_date = rail.PythonOperator(
            task_id='get_required_effective_date',
            python_callable=lambda dag_run: (
                lambda dt: {'year': dt.year, 'month': dt.month, 'day': dt.day}
            )(
                datetime.strptime(dag_run.conf["dateused"], "%d/%m/%Y") + timedelta(
                    days=rail.result('foreach_response')['startOffset']['offsetValue'] * 365
                )
            )
        )

        insert_to_list_4 = rail.SetVariableOperator(
            task_id='insert_to_list_4',
            append=True,
            name='{{ result("declare_list_3").name }}',
            value=lambda: {
                "effectiveDate": {
                    "day": rail.result('get_required_effective_date')['day'],
                    "month": rail.result('get_required_effective_date')['month'],
                    "year": rail.result('get_required_effective_date')['year']
                },
                "description": f"Effective on {rail.result('get_required_effective_date')['month']}/{rail.result('get_required_effective_date')['day']}/{rail.result('get_required_effective_date')['year']}",
                "policySet": rail.result('foreach_response')['policySet']
            }
        )

        foreach_response_55_end = rail.EmptyOperator(
            task_id='foreach_response_55_end',
        )

        log_timeoff_policies = rail.PythonOperator(
            task_id='log_timeoff_policies',
            python_callable=lambda: json.loads(
                json.dumps(rail.get_dag_run_var('policyschedules'))
                .replace('"script"', '"scriptTarget"')
            ) if rail.get_dag_run_var('policyschedules') else None
        )

        if_timeoff_policies_present = rail.IfOperator(
            task_id='if_timeoff_policies_present',
            test='''{{ result('log_timeoff_policies') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_2",
            no_task="catch_and_log_errors",
        )

        put_user_time_off_account_policy_set_schedule_2 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_2',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf["useruri"],
                    "timeOffTypeUri": rail.result('foreach_d_2')['timeOffType']['uri']
                },
                "policySetScheduleEntries": rail.result('log_timeoff_policies')
            }
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
                "action": "Timeoff Update",
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
        can_run_batch_task >> rail.Label('No') >> log_locationswhereemployeetypeneedstobeconsidered
        log_locationswhereemployeetypeneedstobeconsidered >> mapper_search_entries_2 >> if_entry_col1_blank
        if_entry_col1_blank >> rail.Label('Yes') >> send_reply >> catch_and_log_errors
        if_entry_col1_blank >> rail.Label('No') >> declare_list_1 >> get_todaysdate >> get_dateofchanges >> get_user_time_off_type_policy_summary_1 >> foreach_d_1 >> if_timeoff_allowed_1
        foreach_d_1 >> foreach_d_13_end
        if_timeoff_allowed_1 >> rail.Label('Yes') >> insert_to_list_1 >> foreach_d_13_end
        if_timeoff_allowed_1 >> rail.Label('No') >> foreach_d_13_end
        foreach_d_13_end >> get_enabled_timeoff_types >> if_first_displaytext_present
        if_first_displaytext_present >> rail.Label('Yes') >> log_final_set_timeoff_uris >> if_present_1
        if_present_1 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user >> get_user_time_off_type_policy_summary_2 >> foreach_declare_list >> log_ifthetimeoff_typeisnotrequiredanymore >> if_ifthetimeoff_typeisnotrequiredanymore_27_blank
        foreach_declare_list >> foreach_declare_list_9_26_end
        if_ifthetimeoff_typeisnotrequiredanymore_27_blank >> rail.Label('Yes') >> get_balance_summary_for_account >> trigger_timeoff_policy30 >> wait_timeoff_policy30 >> foreach_declare_list_9_26_end
        if_ifthetimeoff_typeisnotrequiredanymore_27_blank >> rail.Label('No') >> foreach_declare_list_9_26_end
        foreach_declare_list_9_26_end >> foreach_d_2 >> if_timeoff_allowed_2
        foreach_d_2 >> foreach_d_31_end >> catch_and_log_errors
        if_timeoff_allowed_2 >> rail.Label('Yes') >> log_timeoffisalreadyassigned >> if_thetimeoffisalreadyassigned_33_blank
        if_thetimeoffisalreadyassigned_33_blank >> rail.Label('Yes') >> get_default_time_off_type_policy_schedule_for_user >> log_timeoff_policy_1 >> if_present_2
        if_present_2 >> rail.Label('Yes') >> put_user_time_off_account_policy_set_schedule_1 >> if_thetimeoffisalreadyassigned_present
        if_present_2 >> rail.Label('No') >> if_thetimeoffisalreadyassigned_present
        if_thetimeoffisalreadyassigned_33_blank >> rail.Label('No') >> if_thetimeoffisalreadyassigned_present
        if_thetimeoffisalreadyassigned_present >> rail.Label('Yes') >> get_defaultpolicyfromgloballevel >> log_timeoff_policy_2 >> if_timeoff_policy_present
        if_timeoff_policy_present >> rail.Label('Yes') >> declare_list_3 >> log_existing_policy >> if_existing_policy_present
        if_existing_policy_present >> rail.Label('Yes') >> parse_json >> foreach_document >> log_effectivedateforcomparison >> if_date_in_past
        if_date_in_past >> rail.Label('Yes') >> insert_to_list_3 >> foreach_document_50_end
        if_date_in_past >> rail.Label('No') >> foreach_document_50_end
        foreach_document >> foreach_document_50_end >> foreach_response
        if_existing_policy_present >> rail.Label('No') >> foreach_response
        foreach_response >> get_required_effective_date >> insert_to_list_4 >> foreach_response_55_end
        foreach_response >> foreach_response_55_end >> log_timeoff_policies
        if_timeoff_policy_present >> rail.Label('No') >> log_timeoff_policies
        log_timeoff_policies >> if_timeoff_policies_present
        if_timeoff_policies_present >> rail.Label('Yes') >> put_user_time_off_account_policy_set_schedule_2 >> catch_and_log_errors
        if_timeoff_policies_present >> rail.Label('No') >> catch_and_log_errors
        if_thetimeoffisalreadyassigned_present >> rail.Label('No') >> catch_and_log_errors
        if_timeoff_allowed_2 >> rail.Label('No') >> catch_and_log_errors
        if_present_1 >> rail.Label('No') >> catch_and_log_errors
        if_first_displaytext_present >> rail.Label('No') >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
