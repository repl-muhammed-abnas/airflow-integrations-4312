from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from daimlertrucks.liquidplanner_time_entry_sync.utils import request_payload
from daimlertrucks.liquidplanner_time_entry_sync.utils import python_callable_method

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_timeimport_for_each_user_child_{config.instance}',
        description=f'Live|DTNA_Time import_For each user time entries (Child) {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_time_entry_import_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_time_entry_import_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_time_entry_import_log = rail.CreateLogOperator(
            task_id='create_time_entry_import_log'
        )

        query_task_list = rail.QueryCollectionOperator(
            task_id='query_task_list',
            query="""SELECT * FROM non_empty_records WHERE userid = '{{ dag_run.conf.userid }}'""",
        )

        getusersby_i_d_25 = rail.RepliconServiceOperator(
            task_id='getusersby_i_d_25',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_users_by_id_payload
        )

        if_first_datatype_present_28 = rail.IfOperator(
            task_id='if_first_datatype_present_28',
            test="{{ result('getusersby_i_d_25').rows | is_truthy }}",
            yes_task="foreach_d_29",
            no_task="log_useruri_31",
        )

        foreach_d_29 = rail.ForEachOperator(
            task_id='foreach_d_29',
            items="{{ result('getusersby_i_d_25').rows | to_json }}",
            start_task='accumulate_list_items_30',
            end_task='foreach_d_29_end'
        )

        accumulate_list_items_30 = rail.SetVariableOperator(
            task_id='accumulate_list_items_30',
            name='userlist',
            append=True,
            value={
                "loginname": "{{ result('foreach_d_29').cells[0].textValue }}",
                "uri": "{{ result('foreach_d_29').cells[0].uri }}",
                "enabled": "{{ result('foreach_d_29').cells | find_first_by_attr_and_get_attr('dataType', 'urn:replicon:list-type:bool', 'textValue') }}"
            }
        )

        foreach_d_29_end = rail.EmptyOperator(
            task_id='foreach_d_29_end',
        )

        log_useruri_31 = rail.PythonOperator(
            task_id='log_useruri_31',
            # pylint: disable=line-too-long
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('accumulate_list_items_30')['value'], 'loginname',
                dag_run.conf['userid'], 'uri') if rail.result('accumulate_list_items_30') else null
        )

        log_userid_32 = rail.PythonOperator(
            task_id='log_userid_32',
            python_callable=lambda: rail.result('log_useruri_31').rsplit(
                ':', 1)[-1] if rail.result('log_useruri_31') else null

        )

        if_log_useruri_31_blank_false_34 = rail.IfOperator(
            task_id='if_log_useruri_31_blank_false_34',
            test='''{{ result('log_useruri_31') | is_falsy }}''',
            yes_task="dtna_time_entry_import_logs_add_entry_36",
            no_task="log_userstatus_37",
        )

        dtna_time_entry_import_logs_add_entry_36 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_36',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_task_list') }}",
            severity="Error",
            message="Time Entry not synced since user profile is not found in Replicon",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "Time Entry not synced since user profile is not found in Replicon",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        log_userstatus_37 = rail.PythonOperator(
            task_id='log_userstatus_37',
            # pylint: disable=line-too-long
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                rail.result('accumulate_list_items_30')['value'], 'loginname',
                dag_run.conf['userid'], 'enabled') if rail.result('accumulate_list_items_30') else 'False'
        )

        if_log_userstatus_37_equals_to_false_38 = rail.IfOperator(
            task_id='if_log_userstatus_37_equals_to_false_38',
            test='''{{ result('log_userstatus_37') == 'False' }}''',
            yes_task="dtna_time_entry_import_logs_add_entry_40",
            no_task="if_log_useruri_31_present_41",
        )

        dtna_time_entry_import_logs_add_entry_40 = rail.WriteLogOperator(
            task_id='dtna_time_entry_import_logs_add_entry_40',
            log="{{ result('create_time_entry_import_log') }}",
            items="{{ result('query_task_list') }}",
            message="Time Entry not synced since user profile is disabled",
            severity="Error",
            properties={
                "user_name": "{{ item.userid }}",
                "status": "Error",
                "reason": "Time Entry not synced since user profile is disabled",
                "entrydate": "{{ item.entrydate }}",
                "taskcode": "{{ item.taskname }}",
                "hoursworked": "{{ item.hoursworked }}"
            }
        )

        if_log_useruri_31_present_41 = rail.IfOperator(
            task_id='if_log_useruri_31_present_41',
            test='''{{ result('log_useruri_31') | is_truthy and result('log_userstatus_37') == 'True' }}''',
            yes_task="foreach_query_list_23_42",
            no_task="finish",
        )

        foreach_query_list_23_42 = rail.ForEachOperator(
            task_id='foreach_query_list_23_42',
            items="{{ result('query_task_list') }}",
            start_task='log_46',
            end_task='foreach_query_list_23_42_end'
        )

        log_46 = rail.PythonOperator(
            task_id='log_46',
            python_callable=lambda: rail.result(
                'foreach_query_list_23_42')['entrydate']
        )

        accumulate_list_items_47 = rail.SetVariableOperator(
            task_id='accumulate_list_items_47',
            name='entry_dates',
            append=True,
            value=lambda:  {
                "entrydate": datetime.strptime(rail.result("log_46"), "%m/%d/%Y").strftime("%m/%d/%Y")
            }
        )

        foreach_query_list_23_42_end = rail.EmptyOperator(
            task_id='foreach_query_list_23_42_end',
        )

        def max_min_dates():
            date_objects = [datetime.strptime(date['entrydate'], '%m/%d/%Y')
                            for date in rail.result('accumulate_list_items_47')['value']]
            return {
                'max_date': max(date_objects).strftime("%m/%d/%Y"),
                'min_date': min(date_objects).strftime("%m/%d/%Y")
            }

        get_max_min_date = rail.PythonOperator(
            task_id='get_max_min_date',
            python_callable=max_min_dates
        )

        trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50',
            retries=0,
            items=[0],
            trigger_dag_id=f'daimlertrucks_deletingexisitngtimeenteries_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "mindate": "{{ result('get_max_min_date').min_date }}",
                "maxdate": "{{ result('get_max_min_date').max_date }}",
                "useruri": "{{ result('log_useruri_31') }}",
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50") }}'
        )

        trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69',
            retries=0,
            items="{{ result('query_task_list') }}",
            trigger_dag_id=f'daimlertrucks_timeimport_puttimeentrieschild_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "useruri": rail.result('log_useruri_31'),
                "entrydateday": int(item['entrydate'].split("/")[1]),
                "entrydatemonth": int(item['entrydate'].split("/")[0]),
                "entrydateyear": int(item['entrydate'].split("/")[2]),
                "hoursworked": item['hoursworked'],
                "userid": item['userid'],
                "taskcode": item['taskname'],
                "entrydate_received": item['entrydate']
            }
        )

        wait_for_completion_trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69") }}'
        )

        gather_timesheets_to_submit_logs_from_put_entries = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timesheets_to_submit_logs_from_put_entries',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69") }}',
            dagrun_task_id='create_timesheets_to_submit_log',
            flatten=True
        )

        gather_timesheets_to_submit_logs_from_delete_entries = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timesheets_to_submit_logs_from_delete_entries',
            dag_runs='{{ result("trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50") }}',
            dagrun_task_id='create_timesheets_to_submit_log',
            flatten=True
        )

        format_timesheets_to_submit_logs = rail.PythonOperator(
            task_id='format_timesheets_to_submit_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=python_callable_method.do_format_timesheets_to_submit_logs
        )

        gather_time_entry_import_logs_from_put_entries = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_time_entry_import_logs_from_put_entries',
            dag_runs='{{ result("trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69") }}',
            dagrun_task_id='create_time_entry_import_log',
            flatten=True
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_time_entry_import_log

        create_time_entry_import_log >> query_task_list >> getusersby_i_d_25 >> if_first_datatype_present_28
        if_first_datatype_present_28 >> rail.Label(
            'Yes') >> foreach_d_29 >> accumulate_list_items_30 >> foreach_d_29_end
        foreach_d_29 >> foreach_d_29_end >> log_useruri_31
        if_first_datatype_present_28 >> rail.Label(
            'No') >> log_useruri_31 >> log_userid_32 >> if_log_useruri_31_blank_false_34
        if_log_useruri_31_blank_false_34 >> rail.Label(
            'Yes') >> dtna_time_entry_import_logs_add_entry_36 >> finish
        if_log_useruri_31_blank_false_34 >> rail.Label(
            'No') >> log_userstatus_37 >> if_log_userstatus_37_equals_to_false_38
        if_log_userstatus_37_equals_to_false_38 >> rail.Label(
            'Yes') >> dtna_time_entry_import_logs_add_entry_40 >> finish
        if_log_userstatus_37_equals_to_false_38 >> rail.Label(
            'No') >> if_log_useruri_31_present_41
        if_log_useruri_31_present_41 >> rail.Label(
            'Yes') >> foreach_query_list_23_42 >> log_46 >> accumulate_list_items_47 >> foreach_query_list_23_42_end
        foreach_query_list_23_42 >> foreach_query_list_23_42_end >> get_max_min_date \
            >> trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50 \
            >> wait_for_completion_trigger_dag_run_live_dtna_deleting_exisitng_time_enteries_child50 \
            >> trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69 \
            >> wait_for_completion_trigger_dag_run_live_dtna_time_import_put_time_entries_childasync_69 \
            >> gather_timesheets_to_submit_logs_from_put_entries >> gather_timesheets_to_submit_logs_from_delete_entries \
            >> format_timesheets_to_submit_logs >> gather_time_entry_import_logs_from_put_entries \
            >> finish

        if_log_useruri_31_present_41 >> rail.Label(
            'No') >> finish

        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
