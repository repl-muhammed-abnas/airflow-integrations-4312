
from datetime import timedelta
import json
from airflow.models import Variable
import rail
from velaw.user_import.user_import_mapper import velaw_user_import_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_timeoff_assignment_for_update_users_v2_0_{config.instance}',
        description=f'VelawG3 Child_Timeoff assignment for update users V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='date_split_date_considered_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='date_split_date_considered_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        date_split_date_considered_3 = rail.EmptyOperator(
            task_id='date_split_date_considered_3',
        )

        def get_timeoff_types_uri(response):
            data = response.json()['d']
            return list(map(lambda x: {
                "timeofftypename": x['name'].lower(),
                "uri": x['uri']}, data))

        get_all_time_off_types_4 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_4',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            response_filter=get_timeoff_types_uri
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_5 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_5',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x[
                                                         "location"] == "All" and x["person_type"] == "All" and x["assignment_category"] == "All" and x["flsa"] == "All" and x["job_code"] == "All",
                                                         velaw_user_import_mapper)))
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_6 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_6',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All" and x[
                                                         "person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == "All" and x["flsa"] == "All" and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_7 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_7',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All" and x[
                                                         "person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus'] and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        def get_job_code(dag_run):
            if dag_run.conf['persontype'] == "Attorney":
                return dag_run.conf['jobcode'] if dag_run.conf['jobcode'] in ("PART", "OFCO") else "Excluding PART and OFCO"
            return dag_run.conf['jobcode'] if dag_run.conf['jobcode'] == "PARA" else "All excluding PARA"

        def get_timeoff_to_assign_combination_8(dag_run):
            return list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All"
                                                and x["person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == dag_run.conf['assignmentcategory']
                                                and x["flsa"] == dag_run.conf['flsastatus']
                                                and x["job_code"] == get_job_code(dag_run), velaw_user_import_mapper)))

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_8 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_8',
            python_callable=get_timeoff_to_assign_combination_8
        )

        def get_timeoff_to_assign_combination_9(dag_run):
            return list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode']
                        and x["location"] == dag_run.conf['location'] and x["person_type"] == dag_run.conf['persontype']
                        and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus']
                        and x["job_code"] == get_job_code(dag_run), velaw_user_import_mapper)))

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_9 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_9',
            python_callable=get_timeoff_to_assign_combination_9
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_10 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_10',
            python_callable=lambda dag_run: list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode']
                                                                         and x["location"] == dag_run.conf['location'] and x["person_type"] == dag_run.conf['persontype']
                                                                         and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus']
                                                                         and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_11 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_11',
            python_callable=lambda dag_run: list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type"
                                                                         and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == dag_run.conf['location']
                                                                         and x["person_type"] == "All" and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == "All" and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        def merge_all_timeoff_to_assign():
            timeoff_to_assign_list = []
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_5'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_5'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_6'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_6'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_7'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_7'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_8'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_8'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_9'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_9'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_10'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_10'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_11'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_11'))
            return list(map(lambda item: {
                "timeoffname": item['value_|_default_uri'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_4'), 'timeofftypename', item['value_|_default_uri'].strip().lower(), 'uri'),
                "status": "Yes" if item['employee_type'] else "No",
            }, timeoff_to_assign_list)) if timeoff_to_assign_list else []

        invoke_custom_ruby_code_12 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_12',
            python_callable=merge_all_timeoff_to_assign
        )

        log_time_off_typeto_assign_13 = rail.PythonOperator(
            task_id='log_time_off_typeto_assign_13',
            python_callable=lambda:  [x['timeoffuri']
                                      for x in rail.result('invoke_custom_ruby_code_12')]
        )

        if_log_time_off_typeto_assign_13_blank_14 = rail.IfOperator(
            task_id='if_log_time_off_typeto_assign_13_blank_14',
            test=lambda: not rail.result('log_time_off_typeto_assign_13'),
            yes_task="log_to_sumo",
            no_task="get_user_time_off_type_policy_summary_16",
        )

        get_user_time_off_type_policy_summary_16 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_16',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        invoke_custom_ruby_code_17 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_17',
            python_callable=lambda: list(map(lambda item: {
                "name": item['timeOffType']['name'],
                "enabled": item['isTimeOffAllowedAgainstThisTimeOffType'],
                "uri": item['timeOffType']['uri'],
                "policy": item['policySetSchedule'][0]['effectiveDate']['day'] if (item['policySetSchedule'] and item['policySetSchedule'][0]['effectiveDate']) else null
            }, rail.result('get_user_time_off_type_policy_summary_16')['policiesByTimeOffType']))
        )

        invoke_custom_ruby_code_19 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_19',
            python_callable=lambda: list(map(lambda item: {
                "name": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_17'), 'uri', item, 'name', null),
                "enabled": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_17'), 'uri', item, 'enabled'),
                "uri": item,
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_17'), 'uri', item, 'name') else "No"
            }, rail.result('log_time_off_typeto_assign_13')))
        )

        invoke_custom_ruby_code_20 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_20',
            python_callable=lambda: list(map(lambda item: {
                "name": item['name'],
                "enabled": item['enabled'],
                "uri": item['uri'],
                "status": "Yes" if rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_17'), 'uri', item, 'name') else "No",
                "policy": item['policy']
            }, rail.result('invoke_custom_ruby_code_17')))
        )

        foreach_output_21 = rail.ForEachOperator(
            task_id='foreach_output_21',
            items=lambda: rail.result('invoke_custom_ruby_code_20'),
            start_task='if_foreach_output_21_policy_present_22',
            end_task='foreach_output_21_end'
        )

        if_foreach_output_21_policy_present_22 = rail.IfOperator(
            task_id='if_foreach_output_21_policy_present_22',
            test=lambda: rail.result('foreach_output_21') and rail.result(
                'foreach_output_21')['policy'],
            yes_task="get_balance_summary_for_account_23",
            no_task="foreach_output_21_end",
        )

        get_balance_summary_for_account_23 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_23',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_output_21')['uri']
                },
                "asOfDate": {
                    "year": dag_run.conf['startdate'].split('/')[2],
                    "month": dag_run.conf['startdate'].split('/')[0],
                    "day": dag_run.conf['startdate'].split('/')[1]
                }
            }
        )

        trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024',
            retries=0,
            items=[0],
            trigger_dag_id=f'velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_output_21')['uri'],
                "policyset": json.loads(
                    json.dumps(rail.find_first_by_attr_and_get_attr(rail.result('get_user_time_off_type_policy_summary_16')['policiesByTimeOffType'],
                                                                    "timeOffType.uri", rail.result('foreach_output_21')['uri'], 'policySetSchedule'), ensure_ascii=False).replace("[[{", "[{").replace("}]]", "}]")),
                "newschedulebalance": rail.result('get_balance_summary_for_account_23')['timeRemaining'],
                "enddate": dag_run.conf['startdate'].split('/')[2] + '/' + dag_run.conf['startdate'].split('/')[0] + '/' + dag_run.conf['startdate'].split('/')[1],
                "startingbalancesettouri": dag_run.conf['startingbalancesettouri'],
                "preventbalanceoverdrawuri": dag_run.conf['preventbalanceoverdrawuri'],
                "loginname": dag_run.conf['loginname'],
                "enddateday": dag_run.conf['startdate'].split('/')[1],
                "enddatemonth": dag_run.conf['startdate'].split('/')[0],
                "enddateyear": dag_run.conf['startdate'].split('/')[2]
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024") }}'
        )

        foreach_output_21_end = rail.EmptyOperator(
            task_id='foreach_output_21_end'
        )

        assign_timeoffassignmentsfornewusers_25 = rail.RepliconServiceOperator(
            task_id='assign_timeoffassignmentsfornewusers_25',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_time_off_typeto_assign_13')
            }
        )

        trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26',
            retries=0,
            items="{{ result('invoke_custom_ruby_code_19') | to_json}}",
            trigger_dag_id=f'velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": dag_run.conf['useruri'],
                "timeofftypeuri": item['uri']
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26") }}'
        )

        log_checkifanytimeofftypesneedstobedisabled_31 = rail.PythonOperator(
            task_id='log_checkifanytimeofftypesneedstobedisabled_31',
            python_callable=lambda: [x['timeoffuri'] for x in rail.result(
                'invoke_custom_ruby_code_12') if x['status'] == "Yes"]
        )

        if_log_checkifanytimeofftypesneedstobedisabled_31_present_32 = rail.IfOperator(
            task_id='if_log_checkifanytimeofftypesneedstobedisabled_31_present_32',
            test=lambda: rail.result(
                'log_checkifanytimeofftypesneedstobedisabled_31'),
            yes_task="log_time_off_typeto_disable_33",
            no_task="log_to_sumo",
        )

        log_time_off_typeto_disable_33 = rail.PythonOperator(
            task_id='log_time_off_typeto_disable_33',
            python_callable=lambda: [x['timeoffuri'] for x in rail.result(
                'invoke_custom_ruby_code_12') if x['status'] == "No"]
        )

        if_log_time_off_typeto_disable_33_present_34 = rail.IfOperator(
            task_id='if_log_time_off_typeto_disable_33_present_34',
            test=lambda: rail.result('log_time_off_typeto_disable_33'),
            yes_task="assign_timeoffassignmentsfornewuserstodisabletimeofftypes_35",
            no_task="log_to_sumo",
        )

        assign_timeoffassignmentsfornewuserstodisabletimeofftypes_35 = rail.RepliconServiceOperator(
            task_id='assign_timeoffassignmentsfornewuserstodisabletimeofftypes_35',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_time_off_typeto_disable_33')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> date_split_date_considered_3
        date_split_date_considered_3 >> get_all_time_off_types_4 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_5 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_6 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_7 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_8 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_9 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_10 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_11 \
            >> invoke_custom_ruby_code_12 >> log_time_off_typeto_assign_13 >> if_log_time_off_typeto_assign_13_blank_14
        if_log_time_off_typeto_assign_13_blank_14 >> rail.Label(
            'Yes') >> log_to_sumo
        if_log_time_off_typeto_assign_13_blank_14 >> rail.Label(
            'No') >> get_user_time_off_type_policy_summary_16 >> invoke_custom_ruby_code_17 >> invoke_custom_ruby_code_19 \
            >> invoke_custom_ruby_code_20 >> foreach_output_21 >> if_foreach_output_21_policy_present_22
        if_foreach_output_21_policy_present_22 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_23 >> trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_024 >> foreach_output_21_end
        if_foreach_output_21_policy_present_22 >> rail.Label(
            'No') >> foreach_output_21_end
        foreach_output_21 >> foreach_output_21_end >> assign_timeoffassignmentsfornewusers_25 \
            >> trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_26 \
            >> log_checkifanytimeofftypesneedstobedisabled_31 >> if_log_checkifanytimeofftypesneedstobedisabled_31_present_32
        if_log_checkifanytimeofftypesneedstobedisabled_31_present_32 >> rail.Label(
            'Yes') >> log_time_off_typeto_disable_33 >> if_log_time_off_typeto_disable_33_present_34
        if_log_time_off_typeto_disable_33_present_34 >> rail.Label(
            'Yes') >> assign_timeoffassignmentsfornewuserstodisabletimeofftypes_35 >> log_to_sumo
        if_log_time_off_typeto_disable_33_present_34 >> rail.Label(
            'No') >> log_to_sumo
        if_log_checkifanytimeofftypesneedstobedisabled_31_present_32 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
