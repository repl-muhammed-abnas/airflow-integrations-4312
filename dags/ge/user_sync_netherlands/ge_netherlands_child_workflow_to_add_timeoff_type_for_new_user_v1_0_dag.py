
from datetime import timedelta
from ge.user_sync_netherlands.netherlands_master_mapper import netherlands_master_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ge_user_sync_netherlands_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
        description=f'GE_Netherlands_Child Workflow to add timeoff type for new user v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
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
            no_task='_adhoc_http_action_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='_adhoc_http_action_3',
            end_task='catch_52_52_52',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        _adhoc_http_action_3 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_3',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data=None
        )

        if_first_displaytext_present_4 = rail.IfOperator(
            task_id='if_first_displaytext_present_4',
            test='''{{ result('_adhoc_http_action_3') | is_truthy and result('_adhoc_http_action_3') | length > 0 }}''',
            yes_task="ge_netherlands_user_sync_master_mapper_search_entries_5",
            no_task="catch_52_52_52",
        )

        ge_netherlands_user_sync_master_mapper_search_entries_5 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_5',
            python_callable=lambda:  list(filter(
                lambda x: x['type'] == "Timeofftype", netherlands_master_mapper))
        )

        if_entry_col1_blank_6 = rail.IfOperator(
            task_id='if_entry_col1_blank_6',
            test='''{{ result('ge_netherlands_user_sync_master_mapper_search_entries_5') | is_falsy }}''',
            yes_task="catch_52_52_52",
            no_task="ge_netherlands_user_sync_master_mapper_search_entries_9",
        )

        ge_netherlands_user_sync_master_mapper_search_entries_9 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_9',
            python_callable=lambda dag_run:  list(filter(
                lambda x: x["type"] == "Restricted Timeoff Type assignment" and x['jobtype'] == dag_run.conf['jobtype'], netherlands_master_mapper))
        )

        def get_mapper_jobtype():
            derived_job_type = "Non Restricted Timeoff Type assignment"
            mapper_info = rail.result(
                'ge_netherlands_user_sync_master_mapper_search_entries_9')
            if mapper_info:
                jobinfo = mapper_info[0]['value']
                if jobinfo.lower() == 'yes':
                    derived_job_type = "Restricted Timeoff Type assignment"
                else:
                    derived_job_type = "Non Restricted Timeoff Type assignment"
            return derived_job_type

        log_required_search_criteria_10 = rail.PythonOperator(
            task_id='log_required_search_criteria_10',
            python_callable=get_mapper_jobtype
        )

        ge_netherlands_user_sync_master_mapper_search_entries_11 = rail.PythonOperator(
            task_id='ge_netherlands_user_sync_master_mapper_search_entries_11',
            python_callable=lambda:  list(filter(
                lambda x: x["type"] == "Timeofftype" and x['jobtype'] == rail.result('log_required_search_criteria_10'), netherlands_master_mapper))
        )

        def get_timeoff_type_to_assign(dag_run):
            timeoffstoassign = []
            mapper_timeoffs = rail.result(
                'ge_netherlands_user_sync_master_mapper_search_entries_11')
            for mapper in mapper_timeoffs:
                if mapper['legacy_payroll_id'] and mapper['legacy_payroll_id'] == dag_run.conf['legacypayrollid']:
                    timeoffstoassign.append({
                        "name": mapper['value'],
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result(
                            '_adhoc_http_action_3'), 'displayText', mapper['value'].strip(), 'uri')
                    })
                else:
                    timeoffstoassign.append({
                        "name": mapper['value'],
                        "uri": rail.find_first_by_attr_and_get_attr(rail.result(
                            '_adhoc_http_action_3'), 'displayText', mapper['value'].strip(), 'uri')
                    })
            return timeoffstoassign

        def get_unique_timeoff_uri(dag_run):
            timeoff_uri = []
            timeoffstoassign = []
            timeoff_types = get_timeoff_type_to_assign(dag_run)
            for to in timeoff_types:
                if to['uri'] and to['uri'] not in timeoff_uri:
                    timeoff_uri.append(to['uri'])
                    timeoffstoassign.append({
                        "name": to['name'],
                        "uri": to['uri']
                    })
            return {
                "timeoff_uri": timeoff_uri,
                "timeoffstoassign": timeoffstoassign
            }

        log_final_set_timeoff_uris_21 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_21',
            python_callable=get_unique_timeoff_uri
        )

        if_log_12_present_22 = rail.IfOperator(
            task_id='if_log_12_present_22',
            test='''{{ result('log_final_set_timeoff_uris_21').timeoff_uri | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_23",
            no_task="catch_52_52_52",
        )

        put_time_off_type_assignments_for_user_23 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_23',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_21')['timeoff_uri']
            }
        )

        trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16',
            retries=0,
            items=lambda: rail.result('log_final_set_timeoff_uris_21')[
                'timeoffstoassign'],
            trigger_dag_id=f'ge_netherlands_child_add_to_policy_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "userloginname": "{{ dag_run.conf.userloginname }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "uri": "{{ item.uri }}",
                "LegalEntity": "{{ dag_run.conf.legalentity }}",
                "name": "{{ item.name }}",
                "fullpart": "{{ dag_run.conf.fullpart }}",
                "startdate": "{{ dag_run.conf.startdate }}",
                "type": "Add",
                "scheduledweeklyhours": "{{ dag_run.conf.scheduledweeklyhours }}",
                "payrule": "{{ dag_run.conf.payrule }}",
                "legacypayrollid": "{{ dag_run.conf.legacypayrollid }}"
            }
        )

        wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16") }}'
        )

        catch_52_52_52 = rail.EmptyOperator(
            task_id='catch_52_52_52',
            trigger_rule='one_failed',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_52_52_52
        can_run_batch_task >> rail.Label('No') >> _adhoc_http_action_3
        _adhoc_http_action_3 >> if_first_displaytext_present_4
        if_first_displaytext_present_4 >> rail.Label(
            'Yes') >> ge_netherlands_user_sync_master_mapper_search_entries_5 >> if_entry_col1_blank_6
        if_entry_col1_blank_6 >> rail.Label(
            'Yes') >> catch_52_52_52
        if_entry_col1_blank_6 >> rail.Label('No') >> ge_netherlands_user_sync_master_mapper_search_entries_9 >> \
            log_required_search_criteria_10 >> ge_netherlands_user_sync_master_mapper_search_entries_11 >> log_final_set_timeoff_uris_21 >> \
            if_log_12_present_22
        if_log_12_present_22 >> rail.Label('No') >> catch_52_52_52
        if_log_12_present_22 >> rail.Label('Yes') >> put_time_off_type_assignments_for_user_23 >> \
            trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 >> \
            wait_for_completion_trigger_dag_run_ge_netherlands_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 >> catch_52_52_52
        if_first_displaytext_present_4 >> rail.Label(
            'No') >> catch_52_52_52 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
