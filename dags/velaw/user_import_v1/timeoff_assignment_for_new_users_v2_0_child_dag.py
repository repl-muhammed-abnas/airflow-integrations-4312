
from datetime import timedelta
from airflow.models import Variable
import rail
from velaw.user_import_v1.user_import_mapper import velaw_user_import_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.timeoff_assignment_for_new_users_child_dag_id,
        description=f'VelawG3 Child_Timeoff assignment for new users V2.0 {config.instance}',
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
            no_task='get_all_time_off_types_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_time_off_types_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_timeoff_types_uri(response):
            data = response.json()['d']
            return list(map(lambda x: {
                "timeofftypename": x['name'].lower(),
                "uri": x['uri']}, data))

        get_all_time_off_types_3 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_3',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            response_filter=get_timeoff_types_uri
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_4 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_4',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x[
                                                         "location"] == "All" and x["person_type"] == "All" and x["assignment_category"] == "All" and x["flsa"] == "All" and x["job_code"] == "All",
                                                         velaw_user_import_mapper)))

        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_5 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_5',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All" and x[
                                                         "person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == "All" and x["flsa"] == "All" and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_6 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_6',
            python_callable=lambda dag_run: list(map(lambda x: x,
                                                     filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All" and x[
                                                         "person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus'] and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        def get_job_code(dag_run):
            if dag_run.conf['persontype'] == "Attorney":
                return dag_run.conf['jobcode'] if dag_run.conf['jobcode'] in ("PART", "OFCO") else "Excluding PART and OFCO"
            return dag_run.conf['jobcode'] if dag_run.conf['jobcode'] == "PARA" else "All excluding PARA"

        def get_timeoff_to_assign_combination_7(dag_run):
            return list(map(lambda x: x,
                            filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == "All"
                                   and x["person_type"] == dag_run.conf['persontype'] and x["assignment_category"] == dag_run.conf['assignmentcategory']
                                   and x["flsa"] == dag_run.conf['flsastatus']
                                   and x["job_code"] == get_job_code(dag_run), velaw_user_import_mapper)))

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_7 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_7',
            python_callable=get_timeoff_to_assign_combination_7
        )

        def get_timeoff_to_assign_combination_8(dag_run):
            return list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode']
                        and x["location"] == dag_run.conf['location'] and x["person_type"] == dag_run.conf['persontype']
                        and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus']
                        and x["job_code"] == get_job_code(dag_run), velaw_user_import_mapper)))

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_8 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_8',
            python_callable=get_timeoff_to_assign_combination_8
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_9 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_9',
            python_callable=lambda dag_run: list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type" and x["country_code"] == dag_run.conf['countryisocode']
                                                                         and x["location"] == dag_run.conf['location'] and x["person_type"] == dag_run.conf['persontype']
                                                                         and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == dag_run.conf['flsastatus']
                                                                         and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination7_10 = rail.PythonOperator(
            task_id='velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination7_10',
            python_callable=lambda dag_run: list(map(lambda x: x, filter(lambda x: x["mapper"] == "Yes" and x["type"] == "Timeoff Type"
                                                                         and x["country_code"] == dag_run.conf['countryisocode'] and x["location"] == dag_run.conf['location']
                                                                         and x["person_type"] == "All" and x["assignment_category"] == dag_run.conf['assignmentcategory'] and x["flsa"] == "All"
                                                                         and x["job_code"] == "All", velaw_user_import_mapper)))
        )

        def merge_all_timeoff_to_assign():
            timeoff_to_assign_list = []
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_4'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_4'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_5'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_5'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_6'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_6'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_7'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_7'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_8'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_8'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_9'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_9'))
            if rail.result('velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination7_10'):
                timeoff_to_assign_list.extend(rail.result(
                    'velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination7_10'))
            return list(map(lambda item: {
                "timeoffname": item['value_|_default_uri'],
                "timeoffuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_3'), 'timeofftypename', item['value_|_default_uri'].strip().lower(), 'uri'),
                "status": "Yes" if item['employee_type'] else "No",
            }, timeoff_to_assign_list)) if timeoff_to_assign_list else []

        invoke_custom_ruby_code_11 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_11',
            python_callable=merge_all_timeoff_to_assign
        )

        log_time_off_typeto_assign_12 = rail.PythonOperator(
            task_id='log_time_off_typeto_assign_12',
            python_callable=lambda:  [x['timeoffuri']
                                      for x in rail.result('invoke_custom_ruby_code_11')]
        )

        if_log_time_off_typeto_assign_12_blank_13 = rail.IfOperator(
            task_id='if_log_time_off_typeto_assign_12_blank_13',
            test='''{{ result('log_time_off_typeto_assign_12') | is_falsy }}''',
            yes_task="log_to_sumo",
            no_task="assign_timeoffassignmentsfornewusers_15",
        )

        assign_timeoffassignmentsfornewusers_15 = rail.RepliconServiceOperator(
            task_id='assign_timeoffassignmentsfornewusers_15',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_time_off_typeto_assign_12')
            }
        )

        trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17',
            retries=0,
            items=lambda: rail.result('log_time_off_typeto_assign_12'),
            trigger_dag_id=config.type_policy_schedule_for_user_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: {
                "useruri": dag_run.conf['useruri'],
                "timeofftypeuri": item
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17") }}'
        )

        log_checkiftimeoffistheretobedisabled_22 = rail.PythonOperator(
            task_id='log_checkiftimeoffistheretobedisabled_22',
            python_callable=lambda: [x['timeoffuri'] for x in rail.result(
                'invoke_custom_ruby_code_11') if x['status'] == "Yes"]
        )

        if_log_checkiftimeoffistheretobedisabled_22_present_23 = rail.IfOperator(
            task_id='if_log_checkiftimeoffistheretobedisabled_22_present_23',
            test='''{{ result('log_checkiftimeoffistheretobedisabled_22') | is_truthy }}''',
            yes_task="log_time_off_typeto_disable_24",
            no_task="log_to_sumo",
        )

        log_time_off_typeto_disable_24 = rail.PythonOperator(
            task_id='log_time_off_typeto_disable_24',
            python_callable=lambda: list({x['timeoffuri'] for x in rail.result(
                'invoke_custom_ruby_code_11') if x['status'] == "No"})
        )

        if_log_time_off_typeto_disable_24_present_25 = rail.IfOperator(
            task_id='if_log_time_off_typeto_disable_24_present_25',
            test='''{{ result('log_time_off_typeto_disable_24') | is_truthy }}''',
            yes_task="assign_timeoffassignmentsfornewuserstodisabletimeofftypes_26",
            no_task="log_to_sumo",
        )

        assign_timeoffassignmentsfornewuserstodisabletimeofftypes_26 = rail.RepliconServiceOperator(
            task_id='assign_timeoffassignmentsfornewuserstodisabletimeofftypes_26',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_time_off_typeto_disable_24')
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> get_all_time_off_types_3
        get_all_time_off_types_3 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination1_4 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination2_5 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination3_6 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination4_7 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination5_8 \
            >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination6_9 >> velaw_user_import_mapper_search_entries_timeoffto_assign_search_combination7_10 \
            >> invoke_custom_ruby_code_11 >> log_time_off_typeto_assign_12 >> if_log_time_off_typeto_assign_12_blank_13
        if_log_time_off_typeto_assign_12_blank_13 >> rail.Label(
            'Yes') >> log_to_sumo
        if_log_time_off_typeto_assign_12_blank_13 >> rail.Label(
            'No') >> assign_timeoffassignmentsfornewusers_15 >> trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_time_off_type_policy_schedule_for_user_17 >> log_checkiftimeoffistheretobedisabled_22 \
            >> if_log_checkiftimeoffistheretobedisabled_22_present_23
        if_log_checkiftimeoffistheretobedisabled_22_present_23 >> rail.Label(
            'Yes') >> log_time_off_typeto_disable_24 >> if_log_time_off_typeto_disable_24_present_25
        if_log_time_off_typeto_disable_24_present_25 >> rail.Label(
            'Yes') >> assign_timeoffassignmentsfornewuserstodisabletimeofftypes_26 >> log_to_sumo
        if_log_time_off_typeto_disable_24_present_25 >> rail.Label(
            'No') >> log_to_sumo
        if_log_checkiftimeoffistheretobedisabled_22_present_23 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
