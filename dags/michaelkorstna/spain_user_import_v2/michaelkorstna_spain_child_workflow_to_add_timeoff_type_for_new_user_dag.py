
from datetime import timedelta
import json
from airflow.models import Variable
import rail
from michaelkorstna.spain_user_import_v2.mappers.michael_kors_gmbh_user_sync_master_mapper_spain import michael_kors_gmbh_user_sync_master_mapper_spain
from michaelkorstna.spain_user_import_v2.utils import custom_methods
null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_spain_child_workflow_to_add_timeoff_type_for_new_user_child_{config.instance}_{config.version}',
        description=f'MichaelKorsTnA Spain_Child Workflow to add timeoff type for new user {config.instance}_{config.version}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='_adhoc_http_action_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='_adhoc_http_action_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        _adhoc_http_action_3 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_3',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        search_timeofftype_entries_from_mapper = rail.PythonOperator(
            task_id='search_timeofftype_entries_from_mapper',
            python_callable=lambda: list(filter(
                lambda entry: entry['country'] == 'Spain' and entry['type'] == 'Timeoff Type', michael_kors_gmbh_user_sync_master_mapper_spain))
        )

        if_first_displaytext_present_4 = rail.IfOperator(
            task_id='if_first_displaytext_present_4',
            test=lambda: rail.result('_adhoc_http_action_3') and rail.result(
                '_adhoc_http_action_3')[0]['displayText'],
            yes_task="get_list_of_timeoff_types_to_assign",
            no_task="catch_and_log_error",
        )

        def get_timeoffs_to_assign():
            all_timeoffs = rail.result('_adhoc_http_action_3')
            timeoffs_to_assign = [{
                        'name': item['name'],
                        'uri': item['uri']
                    } for item in list(filter(lambda timeoff: timeoff['name'].startswith('[ES]'),all_timeoffs))]
            timeoffs_from_mapper = rail.result(
                'search_timeofftype_entries_from_mapper')
            for to in timeoffs_from_mapper:
                timeoffs_to_assign.append({
                    'name': to['value'],
                    'uri': rail.find_first_by_attr_and_get_attr(all_timeoffs, 'name', to['value'], 'uri', '')
                })
            return {
                'timeoffstoassign': timeoffs_to_assign,
                'uris': list({timeoff['uri'] for timeoff in timeoffs_to_assign})
            }

        get_list_of_timeoff_types_to_assign = rail.PythonOperator(
            task_id='get_list_of_timeoff_types_to_assign',
            python_callable=get_timeoffs_to_assign
        )

        if_log_12_present_14 = rail.IfOperator(
            task_id='if_log_12_present_14',
            test=lambda: len(rail.result(
                'get_list_of_timeoff_types_to_assign')['uris']) > 0,
            yes_task="invoke_custom_ruby_code_15",
            no_task="catch_and_log_error",
        )

        invoke_custom_ruby_code_15 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_15',
            python_callable=lambda dag_run: custom_methods.get_date_object(
                dag_run.conf['startdate'])
        )

        put_time_off_type_assignments_for_user_16 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_16',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('get_list_of_timeoff_types_to_assign')['uris']
            }
        )

        create_list_of_child_to_wait = rail.SetVariableOperator(
            task_id='create_list_of_child_to_wait',
            name='childtowait',
            append=False,
            value=[]
        )

        foreach_declare_list_5_17 = rail.ForEachOperator(
            task_id='foreach_declare_list_5_17',
            items=lambda: rail.result('get_list_of_timeoff_types_to_assign')[
                'timeoffstoassign'],
            start_task='get_default_time_off_type_policy_schedule_for_user_20',
            end_task='foreach_declare_list_5_17_end'
        )

        get_default_time_off_type_policy_schedule_for_user_20 = rail.RepliconServiceOperator(
            task_id='get_default_time_off_type_policy_schedule_for_user_20',
            endpoint="/services/TimeOffPolicyService2.svc/GetDefaultTimeOffTypePolicyScheduleForUser",
            data={
                "timeOffAccount": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_declare_list_5_17').uri }}"
                }
            }
        )

        if_foreach_1_name_not_equals_to_esholidays_22 = rail.IfOperator(
            task_id='if_foreach_1_name_not_equals_to_esholidays_22',
            test='''{{ result('foreach_declare_list_5_17').name != '[ES] Holidays' }}''',
            yes_task="log_timeoff_policy_23",
            no_task="trigger_timeoff_type_proration_assignment_child",
        )

        log_timeoff_policy_23 = rail.PythonOperator(
            task_id='log_timeoff_policy_23',
            python_callable=lambda: (json.dumps(rail.result('get_default_time_off_type_policy_schedule_for_user_20'))).replace(
                'null', '\"effective\"').replace('\"script\"', '\"scriptTarget\"') if rail.result(
                'get_default_time_off_type_policy_schedule_for_user_20') and rail.result(
                'get_default_time_off_type_policy_schedule_for_user_20')[0]['policySet'] else ''
        )

        if_log_13_present_24 = rail.IfOperator(
            task_id='if_log_13_present_24',
            test='''{{ result('log_timeoff_policy_23') | is_truthy }}''',
            yes_task="put_user_time_off_account_policy_set_schedule_25",
            no_task="foreach_declare_list_5_17_end",
        )

        put_user_time_off_account_policy_set_schedule_25 = rail.RepliconServiceOperator(
            task_id='put_user_time_off_account_policy_set_schedule_25',
            endpoint="/services/TimeOffPolicyService2.svc/PutUserTimeOffAccountPolicySetSchedule",
            data=lambda dag_run: {
                "timeOffAccount": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_declare_list_5_17')['uri']
                },
                "policySetScheduleEntries": json.loads(rail.result('log_timeoff_policy_23'))
            }
        )

        trigger_timeoff_type_proration_assignment_child = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_type_proration_assignment_child',
            retries=0,
            trigger_dag_id=f'michaelkorstna_spain_user_import_timeoff_type_proration_assignment_child_{config.instance}_{config.version}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "callerjobid": dag_run.conf['callerjobid'],
                "userloginname": dag_run.conf['userloginname'],
                "useruri": dag_run.conf['useruri'],
                "startdate": dag_run.conf['startdate'],
                "type": dag_run.conf['type'],
                "timeoffuri": rail.result('foreach_declare_list_5_17')['uri'],
                "scheduledweeklyhours": float(dag_run.conf['scheduledweeklyhours']),
                "fullpart": dag_run.conf['fullpart'],
                "timeofftype": rail.result('foreach_declare_list_5_17')['name'],
                "cbabasedhours": dag_run.conf['cbabasedhours'],
                "accrualdays": dag_run.conf['accrualdays']
            }
        )

        insert_child_id_to_wait = rail.SetVariableOperator(
            task_id='insert_child_id_to_wait',
            name='childtowait',
            append=True,
            value="{{result('trigger_timeoff_type_proration_assignment_child')}}"
        )

        foreach_declare_list_5_17_end = rail.EmptyOperator(
            task_id='foreach_declare_list_5_17_end',
        )

        if_child_triggered = rail.IfOperator(
            task_id='if_child_triggered',
            test=lambda: len(rail.get_dag_run_var('childtowait')) > 0,
            yes_task='wait_for_timeoff_type_proration_assignment_child',
            no_task='catch_and_log_error'
        )

        wait_for_timeoff_type_proration_assignment_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_type_proration_assignment_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_child_id_to_wait").value | to_json }}'
        )

        catch_and_log_error = rail.PythonOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> _adhoc_http_action_3
        _adhoc_http_action_3 >> search_timeofftype_entries_from_mapper >> if_first_displaytext_present_4
        if_first_displaytext_present_4 >> rail.Label(
            'Yes') >> get_list_of_timeoff_types_to_assign >> if_log_12_present_14
        if_log_12_present_14 >> rail.Label(
            'Yes') >> invoke_custom_ruby_code_15 >> put_time_off_type_assignments_for_user_16 >> create_list_of_child_to_wait
        create_list_of_child_to_wait >> foreach_declare_list_5_17 >> get_default_time_off_type_policy_schedule_for_user_20
        get_default_time_off_type_policy_schedule_for_user_20 >> if_foreach_1_name_not_equals_to_esholidays_22
        if_foreach_1_name_not_equals_to_esholidays_22 >> rail.Label(
            'Yes') >> log_timeoff_policy_23 >> if_log_13_present_24
        if_log_13_present_24 >> rail.Label(
            'Yes') >> put_user_time_off_account_policy_set_schedule_25 >> foreach_declare_list_5_17_end
        if_log_13_present_24 >> rail.Label(
            'No') >> foreach_declare_list_5_17_end
        if_foreach_1_name_not_equals_to_esholidays_22 >> rail.Label(
            'No') >> trigger_timeoff_type_proration_assignment_child >> insert_child_id_to_wait >> foreach_declare_list_5_17_end
        foreach_declare_list_5_17 >> foreach_declare_list_5_17_end >> if_child_triggered
        if_child_triggered >> rail.Label(
            'Yes') >> wait_for_timeoff_type_proration_assignment_child >> catch_and_log_error
        if_child_triggered >> rail.Label('No') >> catch_and_log_error
        if_log_12_present_14 >> rail.Label('No') >> catch_and_log_error
        if_first_displaytext_present_4 >> rail.Label(
            'No') >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
