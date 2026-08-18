
from datetime import timedelta
from ge_healthcare.user_sync_greece.greece_master_mapper import greece_master_mapper
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'gehealthcare_greece_child_workflow_to_add_timeoff_type_for_new_user_v1_0_{config.instance}',
        description=f'GE_Greece_Child Workflow to add timeoff type for new user v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
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
            end_task='add_timeoff_type_logs',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        _adhoc_http_action_3 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_3',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
        )

        if_first_displaytext_present_4 = rail.IfOperator(
            task_id='if_first_displaytext_present_4',
            test='''{{ result('_adhoc_http_action_3') | is_truthy and result('_adhoc_http_action_3') | length > 0 }}''',
            yes_task="ge_greece_user_sync_master_mapper_search_entries_5",
            no_task="add_timeoff_type_logs",
        )

        def get_entity_from_mapper(LegalEntity, to_type):
            emp_types = list(filter(
                lambda x: x['legal_entity'] == LegalEntity
                and x['type'] == to_type, greece_master_mapper))
            return [emp_type['value'] for emp_type in emp_types]

        ge_greece_user_sync_master_mapper_search_entries_5 = rail.PythonOperator(
            task_id='ge_greece_user_sync_master_mapper_search_entries_5',
            python_callable=lambda dag_run: get_entity_from_mapper(
                dag_run.conf['legalentity'], 'Timeoff types')
        )

        if_entry_col1_blank_6 = rail.IfOperator(
            task_id='if_entry_col1_blank_6',
            test='''{{ result('ge_greece_user_sync_master_mapper_search_entries_5') | is_falsy }}''',
            yes_task="add_timeoff_type_logs_7",
            no_task="log_final_set_timeoff_uris_13",
        )

        add_timeoff_type_logs_7 = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs_7',
            message="Timeoff not assigned/updated as no timeoff is defined in mapper for legal entity - {{ dag_run.conf.legalentity}}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "Timeoff not assigned/updated as no timeoff is defined in mapper for legal entity - {{ dag_run.conf.legalentity}}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": ""
            }
        )

        def get_final_set_timeoff_info():
            timeoff_uris = []
            for mapper_to_info in rail.result('ge_greece_user_sync_master_mapper_search_entries_5'):
                timeoff_uri = rail.find_first_by_attr_and_get_attr(rail.result(
                    '_adhoc_http_action_3'), 'displayText', mapper_to_info, 'uri')
                if timeoff_uri:
                    timeoff_uris.append(timeoff_uri)
            return timeoff_uris

        log_final_set_timeoff_uris_13 = rail.PythonOperator(
            task_id='log_final_set_timeoff_uris_13',
            python_callable=get_final_set_timeoff_info
        )

        if_log_12_present_14 = rail.IfOperator(
            task_id='if_log_12_present_14',
            test='''{{ result('log_final_set_timeoff_uris_13') | is_truthy }}''',
            yes_task="put_time_off_type_assignments_for_user_15",
            no_task="add_timeoff_type_logs",
        )

        put_time_off_type_assignments_for_user_15 = rail.RepliconServiceOperator(
            task_id='put_time_off_type_assignments_for_user_15',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('log_final_set_timeoff_uris_13')
            }
        )

        trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16',
            retries=0,
            items=lambda: rail.result('log_final_set_timeoff_uris_13'),
            trigger_dag_id=f'gehealthcare_greece_child_add_to_policy_new_user_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf={
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "useruri": "{{ dag_run.conf.useruri }}",
                "timeofftypeuri": "{{ item }}",
                "type": "Add"
            }
        )

        wait_for_completion_trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16") }}'
        )

        add_timeoff_type_logs = rail.WriteLogOperator(
            task_id='add_timeoff_type_logs',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "action": "{{ dag_run.conf.type }}",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "child_job_id": "{{ dag_run_ecid() }}",
                "OHRID": "{{ dag_run.conf.OHRID }}",
                "username": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> add_timeoff_type_logs
        can_run_batch_task >> rail.Label('No') >> _adhoc_http_action_3
        _adhoc_http_action_3 >> if_first_displaytext_present_4
        if_first_displaytext_present_4 >> rail.Label(
            'Yes') >> add_timeoff_type_logs
        if_first_displaytext_present_4 >> rail.Label(
            'Yes') >> ge_greece_user_sync_master_mapper_search_entries_5 >> if_entry_col1_blank_6
        if_entry_col1_blank_6 >> rail.Label(
            'Yes') >> add_timeoff_type_logs_7 >> add_timeoff_type_logs
        if_entry_col1_blank_6 >> rail.Label(
            'No') >> log_final_set_timeoff_uris_13 >> if_log_12_present_14
        if_log_12_present_14 >> rail.Label(
            'Yes') >> put_time_off_type_assignments_for_user_15 >> \
            trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 >> \
            wait_for_completion_trigger_dag_run_ge_greece_child_workflow_to_add_timeoff_policy_for_new_user_v1_16 >> add_timeoff_type_logs
        if_log_12_present_14 >> rail.Label(
            'No') >> add_timeoff_type_logs >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
