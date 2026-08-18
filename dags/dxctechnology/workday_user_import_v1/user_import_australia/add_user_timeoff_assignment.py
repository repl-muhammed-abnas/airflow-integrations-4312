from datetime import timedelta
from itertools import chain
from json import dumps, loads
from pendulum import datetime
import rail
from airflow.models import Variable
from dxctechnology.workday_user_import_v1.user_import_global.utils import custom_methods as gbl_custom_methods  
from dxctechnology.workday_user_import_v1.user_import.common_utils.request_payload import get_todays_date_in_json, get_todays_minus_specified_days_date_in_json

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_add_user_timeoff_process_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.timeoff_process_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name_australia, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_timeoff_to_assign_from_mapper"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_timeoff_to_assign_from_mapper",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )


        def get_timeoff_to_assign_from_mapper_callable(dag_run):
            uri_to_use = dag_run.conf['ausjc'] if dag_run.conf['ausjc'] else dag_run.conf['industrial_instrument_classification']
            country = dag_run.conf['country']
            is_ia = dag_run.conf['is_ia']
            
            if not dag_run.conf['ausjc']:
                state = dag_run.conf['state']
                rail.set_result(key="not_ausjc_timeoff_sample", val=list(
                        filter(
                            lambda item2: item2['Type']=='Timeoff Sample' and item2['Source']==state,
                            config.DXC_WORKDAY_USER_SYNC_USER_MAPPER
                        )
                    )
                )

            return list(filter(lambda item: item['Type']=='Timeoff' and item['Function']=='Workday User Sync' and\
                                item['Country']==country and item['URI']==uri_to_use and item['personnelsubarea'] == is_ia,
                   config.DXC_WORKDAY_USER_SYNC_USER_MAPPER))


        get_timeoff_to_assign_from_mapper = rail.PythonOperator(
            task_id = "get_timeoff_to_assign_from_mapper",
            python_callable=get_timeoff_to_assign_from_mapper_callable
        )

        has_any_timeoff_to_assign = rail.IfOperator(
            task_id = "has_any_timeoff_to_assign",
            test=lambda: bool(rail.result("get_timeoff_to_assign_from_mapper")),
            yes_task='get_all_timeoff_types_from_replicon',
            no_task="catch_and_log_error"
        )

        get_all_timeoff_types_from_replicon = rail.RepliconServiceOperator(
            task_id = "get_all_timeoff_types_from_replicon",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        def get_timeoff_types_to_assign_callable(dag_run):
            replicon_timoff_types = rail.result("get_all_timeoff_types_from_replicon")
            mapper_timeoff_types = [record['Value'] for record in rail.result("get_timeoff_to_assign_from_mapper")]
            sample_timeoff_type = rail.result(
                'get_timeoff_to_assign_from_mapper', 'not_ausjc_timeoff_sample') if rail.result(
                'get_timeoff_to_assign_from_mapper', 'not_ausjc_timeoff_sample') else []
            timeoff_list = mapper_timeoff_types + ([sample_timeoff_type[0]['Value']] if sample_timeoff_type else [])

            rail.set_result(key="timeoff_list", val=timeoff_list)

            return_value = list(map(lambda timeoff: {
                'name': timeoff,
                'uri': rail.find_first_by_attr_and_get_attr(replicon_timoff_types, 'name', timeoff, 'uri')
            }, timeoff_list))
            rail.set_result(key="return_value", val=return_value)
            return [item for item in return_value if item['uri']]


        get_timeoff_types_to_assign = rail.PythonOperator(
            task_id ="get_timeoff_types_to_assign",
            python_callable=get_timeoff_types_to_assign_callable
        )

        can_assign_timeoff_to_user = rail.IfOperator(
            task_id = "can_assign_timeoff_to_user",
            test=lambda: bool(rail.result("get_timeoff_types_to_assign")),
            yes_task="assign_timeoff_type_to_new_user",
            no_task="catch_and_log_error"
        )

        assign_timeoff_type_to_new_user = rail.RepliconServiceOperator(
            task_id = "assign_timeoff_type_to_new_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run:{
                'userUri': dag_run.conf['user_uri'],
                'timeOffTypeUris': [item['uri'] for item in rail.result('get_timeoff_types_to_assign')]
            }
        )

        for_each_timeoff_assigned = rail.ForEachOperator(
            task_id = "for_each_timeoff_assigned",
            items=lambda: rail.result('get_timeoff_types_to_assign'),
            start_task="is_fte_less_than_one",
            end_task="end_for_each"
        )

        is_fte_less_than_one = rail.IfOperator(
            task_id = "is_fte_less_than_one",
            test = lambda dag_run: float(dag_run.conf['fte']) < 1,
            yes_task="is_fte_based_timeoff",
            no_task="is_time_off_name_contains_lsl_prorata_accrual"
        )

        def is_fte_based_timeoff_test():
            return_value = next(filter(lambda row: row['Type'] == "FTE Based Timeoff Calculation" and\
                                    row['Function'] == "Workday User Sync" and\
                                    row['Country'] == "Australia" and\
                                    row['Source'] == rail.result("for_each_timeoff_assigned")['name'], config.DXC_WORKDAY_USER_SYNC_USER_MAPPER), {}).get('Value', "No")
            return return_value == "Yes"

        is_fte_based_timeoff = rail.IfOperator(
            task_id = "is_fte_based_timeoff",
            test= is_fte_based_timeoff_test,
            yes_task="trigger_aus_personal_carers_leave_parttime_child",
            no_task="is_timeoff_lsl_prorata_accrual"
        )

        trigger_aus_personal_carers_leave_parttime_child = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_aus_personal_carers_leave_parttime_child",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_personal_carers_leave_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff_assigned')['uri'],
                "caller": "Add",
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff_assigned')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": gbl_custom_methods.get_todays_date_in_json()
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_trigger_timeoff_processing_run_id = rail.PythonOperator(
            task_id = "add_trigger_timeoff_processing_run_id",
            python_callable=lambda: [rail.result('trigger_timeoff_processing')] if not rail.result(
                'add_trigger_timeoff_processing_run_id') else rail.result(
                'add_trigger_timeoff_processing_run_id') + [rail.result('trigger_aus_personal_carers_leave_parttime_child')]
        )

        def is_timeoff_name_contains_lsl_prorata_accrual():
            return 'LSL Prorata Accrual' in rail.result('for_each_timeoff_assigned')['name']

        is_timeoff_lsl_prorata_accrual = rail.IfOperator(
            task_id = "is_timeoff_lsl_prorata_accrual",
            test=is_timeoff_name_contains_lsl_prorata_accrual,
            yes_task="get_lsl_secondary_timeoff_to_assign",
            no_task="is_timeoff_type_starts_with_aus_annual_leave"
        )

        get_lsl_secondary_timeoff_to_assign = rail.PythonOperator(
            task_id = "get_lsl_secondary_timeoff_to_assign",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoff_types_from_replicon'),
                                            'name', f'''[AUS] LSL Prorata {dag_run.conf['state']}''')
        )

        get_default_policy_for_lsl_secondary_timeoff = rail.RepliconServiceOperator(
            task_id="get_default_policy_for_lsl_secondary_timeoff",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('get_lsl_secondary_timeoff_to_assign').uri }}"
                }
            }
        )

        has_any_policy = rail.IfOperator(
            task_id= "has_any_policy",
            test= lambda: bool(rail.result('get_default_policy_for_lsl_secondary_timeoff') and rail.result('get_default_policy_for_lsl_secondary_timeoff')[0]['policySet']),
            yes_task="assign_timeoff",
            no_task="end_for_each"
        )

        def assign_timeoff_payload(dag_run, default_policy_task_id):
            return {
                "timeOffAccount": {
                    "userUri": dag_run.conf['user_uri'],
                    "timeOffTypeUri": rail.result("for_each_timeoff_assigned")['uri']
                },
                "policySetScheduleEntries": loads(dumps(rail.result(default_policy_task_id)
                    ).replace("null", "\"effective\""
                ).replace("\"script\"", "\"scriptTarget\""
                )) if rail.result(default_policy_task_id) else []
            }

        assign_timeoff = rail.RepliconServiceOperator(
            task_id = "assign_timeoff",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: assign_timeoff_payload(dag_run, 'get_default_policy_for_lsl_secondary_timeoff')
        )

        is_timeoff_type_starts_with_aus_annual_leave = rail.IfOperator(
            task_id = "is_timeoff_type_starts_with_aus_annual_leave",
            test = lambda dag_run: dag_run.conf['company_code'] == 'AUES' and\
                                    float(dag_run.conf['fte']) < 1 and\
                                     rail.result("for_each_timeoff_assigned")['name'].startswith("[AUS] Annual Leave"),
            yes_task="trigger_aus_annual_leave_processing_child",
            no_task="get_default_policy_schedule_for_user"
        )

        trigger_aus_annual_leave_processing_child = rail.TriggerDagRunForEachItemOperator(
            task_id = "trigger_aus_annual_leave_processing_child",
            items=[1],
            trigger_dag_id=config.workday_user_import_australia_users_aus_annual_leave_parttime_timeoff_assignment_child_dag,
            conf=lambda dag_run: {
                "timeoff_type_uri": rail.result('for_each_timeoff_assigned')['uri'],
                "caller": "Add",
                "current_timeoff_policies": null,
                "timeoff_type_name": rail.result('for_each_timeoff_assigned')['name'],
                "json_formatted_dates": {
                    "start_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "hire_date": dag_run.conf['json_formatted_dates']['hire_date'],
                    "schedule_change_date": get_todays_date_in_json(),
                    "schedule_change_date_today_minus_1":get_todays_minus_specified_days_date_in_json(1),
                    "continuous_service_date": null
                },
                "user_uri":  dag_run.conf['user_uri'],
                "user_log": dag_run.conf['user_log'],
                "emp_id": dag_run.conf['file_data']['emp_id'],
                "email_id": dag_run.conf['file_data']['email_id'],
                "Secondarytimeoffuri": rail.find_first_by_attr_and_get_attr(rail.result("get_all_timeoff_types_from_replicon"), 'name', '[AUS] Annual Leave (part-time)', 'uri'),
                "secondary_timeoff_name": "[AUS] Annual Leave (part-time)",
                "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0,
                "starting_balance_set_to_uri": dag_run.conf['starting_balance_set_to_uri'],
                "prevent_balance_overdraw_uri": dag_run.conf['prevent_balance_overdraw_uri']
            },
            retries= 0,
            execution_timeout = timedelta(days=1)
        )

        add_trigger_aus_annual_leave_processing_child_run_id = rail.PythonOperator(
            task_id = "add_trigger_aus_annual_leave_processing_child_run_id",
            python_callable=lambda: [rail.result('trigger_aus_annual_leave_processing_child')] if not rail.result(
                'add_trigger_aus_annual_leave_processing_child_run_id') else rail.result(
                'add_trigger_aus_annual_leave_processing_child_run_id') + [rail.result('trigger_aus_annual_leave_processing_child')]
        )

        get_default_policy_schedule_for_user = rail.RepliconServiceOperator(
            task_id="get_default_policy_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['user_uri'],
                    'timeOffTypeUri': rail.result('for_each_timeoff_assigned')['uri']
                }
            }
        )

        has_any_default_policy_to_assign = rail.IfOperator(
            task_id = "has_any_default_policy_to_assign",
            test=lambda: bool(rail.result('get_default_policy_schedule_for_user') and rail.result('get_default_policy_schedule_for_user')[0]['policySet']),
            yes_task="put_user_timeoff_account_policy_set_schedule_for_user"
        )

        put_user_timeoff_account_policy_set_schedule_for_user = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_account_policy_set_schedule_for_user",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: assign_timeoff_payload(dag_run, 'get_default_policy_schedule_for_user')
        )


        is_time_off_name_contains_lsl_prorata_accrual = rail.IfOperator(
            task_id = "is_time_off_name_contains_lsl_prorata_accrual",
            test=is_timeoff_name_contains_lsl_prorata_accrual,
            yes_task="get_lsl_secondary_timeoff_to_assign2",
            no_task="get_default_policy_schedule_for_user2"
        )

        get_lsl_secondary_timeoff_to_assign2 = rail.PythonOperator(
            task_id = "get_lsl_secondary_timeoff_to_assign2",
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_all_timeoff_types_from_replicon'),
                                            'name', f'''[AUS] LSL Prorata {dag_run.conf['state']}''')
        )

        get_default_policy_for_lsl_secondary_timeoff2 = rail.RepliconServiceOperator(
            task_id="get_default_policy_for_lsl_secondary_timeoff2",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.user_uri }}",
                    "timeOffTypeUri": "{{ result('get_lsl_secondary_timeoff_to_assign2').uri }}"
                }
            }
        )

        has_any_policy2 = rail.IfOperator(
            task_id= "has_any_policy2",
            test= lambda: bool(rail.result('get_default_policy_for_lsl_secondary_timeoff2') and rail.result('get_default_policy_for_lsl_secondary_timeoff2')[0]['policySet']),
            yes_task="assign_timeoff2",
            no_task="end_for_each"
        )

        assign_timeoff2 = rail.RepliconServiceOperator(
            task_id = "assign_timeoff2",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: assign_timeoff_payload(dag_run, 'get_default_policy_for_lsl_secondary_timeoff2')
        )

        get_default_policy_schedule_for_user2 = rail.RepliconServiceOperator(
            task_id="get_default_policy_schedule_for_user2",
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data=lambda dag_run: {
                'timeOffAccount': {
                    'userUri': dag_run.conf['user_uri'],
                    'timeOffTypeUri': rail.result('for_each_timeoff_assigned')['uri']
                }
            }
        )

        has_any_default_policy_to_assign2 = rail.IfOperator(
            task_id = "has_any_default_policy_to_assign2",
            test=lambda: bool(rail.result('get_default_policy_schedule_for_user2') and rail.result('get_default_policy_schedule_for_user2')[0]['policySet']),
            yes_task="put_user_timeoff_account_policy_set_schedule_for_user2"
        )

        put_user_timeoff_account_policy_set_schedule_for_user2 = rail.RepliconServiceOperator(
            task_id = "put_user_timeoff_account_policy_set_schedule_for_user2",
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: assign_timeoff_payload(dag_run, 'get_default_policy_schedule_for_user2')
        )

        end_for_each = rail.EmptyOperator(
            task_id = "end_for_each"
        )

        def get_dagrun_ids_to_wait_callabled():
            _dagrun_ids= []

            if rail.result('add_trigger_timeoff_processing_run_id'):
                _dagrun_ids.append(rail.result('add_trigger_timeoff_processing_run_id'))

            if rail.result('add_trigger_aus_annual_leave_processing_child_run_id'):
                _dagrun_ids.append(rail.result('add_trigger_aus_annual_leave_processing_child_run_id'))

            dagrun_ids = list(chain.from_iterable(
                item if isinstance(item, list) else [item]
                for sublist in _dagrun_ids
                for item in sublist
            ))

            return list(filter(None, dagrun_ids))

        get_dagrun_ids_to_wait = rail.PythonOperator(
            task_id = "get_dagrun_ids_to_wait",
            python_callable=get_dagrun_ids_to_wait_callabled
        )

        wait_for_completion = rail.WaitForDagRunsSensor(
            task_id = "wait_for_completion",
            dag_runs="{{ result('get_dagrun_ids_to_wait')}}",
            retries = 0,
            execution_timeout = timedelta(days=14)
        )

        # may get converted to catch_and_log_error
        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Update Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Update",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> get_timeoff_to_assign_from_mapper

        get_timeoff_to_assign_from_mapper >> has_any_timeoff_to_assign >> rail.Label("Yes") >> get_all_timeoff_types_from_replicon
        has_any_timeoff_to_assign >> rail.Label("No") >> catch_and_log_error

        end_for_each >> get_dagrun_ids_to_wait >> wait_for_completion >> catch_and_log_error

        get_all_timeoff_types_from_replicon >> get_timeoff_types_to_assign >> can_assign_timeoff_to_user >> rail.Label("No") >> catch_and_log_error
        can_assign_timeoff_to_user >> rail.Label("Yes") >> assign_timeoff_type_to_new_user >> for_each_timeoff_assigned >> end_for_each

        for_each_timeoff_assigned >> is_fte_less_than_one >> rail.Label("Yes") >> is_fte_based_timeoff >> rail.Label(
            "Yes") >> trigger_aus_personal_carers_leave_parttime_child >> add_trigger_timeoff_processing_run_id >> end_for_each
        is_fte_based_timeoff >> rail.Label("No") >> is_timeoff_lsl_prorata_accrual >> rail.Label("Yes") >> get_lsl_secondary_timeoff_to_assign
        get_lsl_secondary_timeoff_to_assign >> get_default_policy_for_lsl_secondary_timeoff >> has_any_policy >> rail.Label("No") >> end_for_each
        has_any_policy >> rail.Label("Yes") >> assign_timeoff >> end_for_each

        is_timeoff_lsl_prorata_accrual >> rail.Label("No") >> is_timeoff_type_starts_with_aus_annual_leave >> rail.Label(
            "Yes") >> trigger_aus_annual_leave_processing_child  >> add_trigger_aus_annual_leave_processing_child_run_id >> end_for_each
        is_timeoff_type_starts_with_aus_annual_leave >> rail.Label("No") >> get_default_policy_schedule_for_user\
              >> has_any_default_policy_to_assign >> rail.Label("Yes") >> put_user_timeoff_account_policy_set_schedule_for_user
        put_user_timeoff_account_policy_set_schedule_for_user >> end_for_each
        has_any_default_policy_to_assign >> rail.Label("No") >> end_for_each

        is_fte_less_than_one >> rail.Label("No") >> is_time_off_name_contains_lsl_prorata_accrual >> rail.Label(
            "Yes") >> get_lsl_secondary_timeoff_to_assign2 >> get_default_policy_for_lsl_secondary_timeoff2 >> has_any_policy2 >> rail.Label(
                "Yes")>> assign_timeoff2 >> end_for_each
        has_any_policy2 >> rail.Label("No") >> end_for_each

        is_time_off_name_contains_lsl_prorata_accrual >> rail.Label("No") >> get_default_policy_schedule_for_user2 \
            >> has_any_default_policy_to_assign2 >> rail.Label("Yes") >> put_user_timeoff_account_policy_set_schedule_for_user2 >> end_for_each
        has_any_default_policy_to_assign2 >> rail.Label("No") >> end_for_each

    return dag

rail.for_each_instance(create_dag)
