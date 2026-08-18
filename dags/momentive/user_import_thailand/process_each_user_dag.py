from datetime import timedelta, datetime
from pendulum import now
from airflow.models import Variable
import rail
from momentive.user_import_thailand.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_process_each_user_dag_id,
        description=f'Momentive_thailand_user_sync_process_each_user_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_user,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='userlist',
            value=[]
        )

        # Recipe [28]-[34]: the search is wrapped in a try/catch that retries 3x @10s and,
        # on failure, logs an error and moves on to the next user. The retry is reproduced
        # here; the "move on" part is now inherent -- each user is its own DAG run, so one
        # user's failure cannot stop the others, and catch_and_log_error (trigger_rule
        # one_failed) writes the Error row against the master's jobid.
        #
        # An EMPTY result is NOT a failure: it means the user does not exist in Replicon,
        # which must fall through to the ADD branch. Do not gate this on get_task_state --
        # inner tasks of a BatchTaskRunOperator are not real TaskInstances, so their state
        # is not observable from inside the batch (that check only works in the master,
        # which has no batch wrapper).
        search_users_33 = rail.RepliconServiceOperator(
            task_id='search_users_33',
            endpoint="/services/UserListService1.svc/GetData",
            retries=3,
            retry_delay=timedelta(seconds=10),
            data=lambda dag_run: request_payload.get_user_by_search_payload(
                dag_run.conf['User_ID']),
            data_handler=lambda response: response['rows'] if response['rows'] else [
            ]
        )

        if_user_name_textvalue_present = rail.IfOperator(
            task_id='if_user_name_textvalue_present',
            test='''{{ result('search_users_33') | is_truthy }}''',
            yes_task="foreach_search_users_33",
            no_task="log_ifuserexistsuseruri_36",
        )

        foreach_search_users_33 = rail.ForEachOperator(
            task_id='foreach_search_users_33',
            items=lambda: rail.result('search_users_33'),
            start_task='insert_to_list',
            end_task='foreach_search_users_33_end'
        )

        insert_to_list = rail.SetVariableOperator(
            task_id='insert_to_list',
            append=True,
            name='{{ result("declare_list").name }}',
            value=lambda: python_callable.build_user_list_item()
        )

        foreach_search_users_33_end = rail.EmptyOperator(
            task_id='foreach_search_users_33_end',
        )

        # Recipe [36]/[37]: the matched user's uri. The department-group uri is resolved
        # once by the master (from its enabled-departments prefetch) and arrives in conf.
        log_ifuserexistsuseruri_36 = rail.PythonOperator(
            task_id='log_ifuserexistsuseruri_36',
            python_callable=lambda dag_run: {
                'useruri': rail.find_first_by_attr_and_get_attr(
                    rail.get_dag_run_var('userlist'), 'username',
                    dag_run.conf['User_ID'].lower(), 'useruri') if rail.get_dag_run_var('userlist') else null,
                'departmentgroupuri': dag_run.conf['departmentgroupuri'],
            }
        )

        if_log_ifuserexistsuseruri_36_present_41 = rail.IfOperator(
            task_id='if_log_ifuserexistsuseruri_36_present_41',
            test='''{{ result('log_ifuserexistsuseruri_36').useruri | is_truthy }}''',
            yes_task="log_enddatepresent_and_userstatus_42_43",
            no_task="if_active_equals_to_1_72",
        )

        log_enddatepresent_and_userstatus_42_43 = rail.PythonOperator(
            task_id='log_enddatepresent_and_userstatus_42_43',
            python_callable=lambda dag_run: {
                'enddatepresent': rail.find_first_by_attr_and_get_attr(
                    rail.get_dag_run_var('userlist'), 'username',
                    dag_run.conf['User_ID'].lower(), 'enddate', '') if rail.get_dag_run_var('userlist') else null,
                'userstatus': rail.find_first_by_attr_and_get_attr(
                    rail.get_dag_run_var('userlist'), 'username',
                    dag_run.conf['User_ID'].lower(), 'status', '') if rail.get_dag_run_var('userlist') else null
            }
        )

        if_log_userstatus_43_equals_to_false_44 = rail.IfOperator(
            task_id='if_log_userstatus_43_equals_to_false_44',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').userstatus | is_falsy }}''',
            yes_task="if_active_present_45",
            no_task="if_log_userstatus_43_equals_to_true_63",
        )

        if_active_present_45 = rail.IfOperator(
            task_id='if_active_present_45',
            test='''{{ dag_run.conf.Active | is_truthy and dag_run.conf.Active == '0' }}''',
            yes_task="if_log_enddatepresent_42_present_46",
            no_task="if_active_present_rehire_60",
        )

        if_log_enddatepresent_42_present_46 = rail.IfOperator(
            task_id='if_log_enddatepresent_42_present_46',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').enddatepresent | is_truthy }}''',
            yes_task="momentive_user_import_logs_add_entry_47",
            no_task="if_termination_date_is_present_49",
        )

        momentive_user_import_logs_add_entry_47 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_47',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User is already disabled in Replicon with end date"
            }
        )

        if_termination_date_is_present_49 = rail.IfOperator(
            task_id='if_termination_date_is_present_49',
            test='''{{ dag_run.conf.Termination_Date | is_truthy }}''',
            yes_task="if_to_date_to_time_equals_to_todayto_time_50",
            no_task="if_active_present_rehire_60",
        )

        if_to_date_to_time_equals_to_todayto_time_50 = rail.IfOperator(
            task_id='if_to_date_to_time_equals_to_todayto_time_50',
            test=lambda dag_run: bool(datetime.strptime(
                dag_run.conf['Termination_Date'], "%Y-%m-%d").date() >= now(tz=config.time_zone).date()),
            yes_task="log_split_dates",
            no_task="momentive_user_import_logs_add_entry_59",
        )

        def startdate_and_enddate_splitter(dag_run):
            startdate = rail.find_first_by_attr_and_get_attr(
                rail.get_dag_run_var('userlist'), 'username',
                dag_run.conf['User_ID'].lower(), 'startdate', '') if rail.get_dag_run_var('userlist') else null

            return {
                'start_date': startdate,
                'start_date_split': python_callable.split_date_string(startdate, 'int') if startdate else '',
                'end_date_split': python_callable.split_date_string(rail.result(
                    'log_enddatepresent_and_userstatus_42_43')['enddatepresent'], 'int') if rail.result(
                    'log_enddatepresent_and_userstatus_42_43')['enddatepresent'] else ''
            }

        log_split_dates = rail.PythonOperator(
            task_id='log_split_dates',
            python_callable=startdate_and_enddate_splitter
        )

        if_termination_date_less_than_startdate_54 = rail.IfOperator(
            task_id='if_termination_date_less_than_startdate_54',
            test=lambda dag_run: datetime.strptime(dag_run.conf['Termination_Date'], "%Y-%m-%d") < datetime.strptime(
                rail.result('log_split_dates')['start_date'], "%Y-%m-%d"),
            yes_task="momentive_user_import_logs_add_entry_55",
            no_task="trigger_disable_user_child_dag_57",
        )

        momentive_user_import_logs_add_entry_55 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_55',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User was already disabled in Replicon, end date was updated since end date received is in the past"
            }
        )

        momentive_user_import_logs_add_entry_59 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_59',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User not disabled since end date received is in the past"
            }
        )

        trigger_disable_user_child_dag_57 = rail.TriggerDagRunOperator(
            task_id='trigger_disable_user_child_dag_57',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.conf_payload(
                'disablewithenddate', dag_run)
        )

        wait_for_disable_user_child_dag_57 = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_user_child_dag_57',
            dag_runs='{{ result("trigger_disable_user_child_dag_57") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_present_rehire_60 = rail.IfOperator(
            task_id='if_active_present_rehire_60',
            test='''{{ dag_run.conf.Active | is_truthy and dag_run.conf.Active == '1' }}''',
            yes_task="trigger_user_sync_update_rehire_62",
            no_task="finish",
        )

        trigger_user_sync_update_rehire_62 = rail.TriggerDagRunOperator(
            task_id='trigger_user_sync_update_rehire_62',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.conf_payload('rehire', dag_run)
        )

        wait_for_user_sync_update_rehire_62 = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_sync_update_rehire_62',
            dag_runs='{{ result("trigger_user_sync_update_rehire_62") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_log_userstatus_43_equals_to_true_63 = rail.IfOperator(
            task_id='if_log_userstatus_43_equals_to_true_63',
            test='''{{ result('log_enddatepresent_and_userstatus_42_43').userstatus | is_truthy }}''',
            yes_task="if_active_present_64",
            no_task="finish",
        )

        if_active_present_64 = rail.IfOperator(
            task_id='if_active_present_64',
            test='''{{ dag_run.conf.Active | is_truthy and dag_run.conf.Active == '0' }}''',
            yes_task="trigger_dag_child_workflow_to_disable_user_65",
            no_task="if_active_present_66",
        )

        trigger_dag_child_workflow_to_disable_user_65 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_child_workflow_to_disable_user_65',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_disable_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.conf_payload('disable', dag_run)
        )

        wait_for_dag_child_workflow_to_disable_user_65 = rail.WaitForDagRunsSensor(
            task_id='wait_for_dag_child_workflow_to_disable_user_65',
            dag_runs='{{ result("trigger_dag_child_workflow_to_disable_user_65") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_present_66 = rail.IfOperator(
            task_id='if_active_present_66',
            test='''{{ dag_run.conf.Active | is_truthy and dag_run.conf.Active == '1' }}''',
            yes_task="trigger_user_sync_update_68",
            no_task="if_active_blank_69",
        )

        trigger_user_sync_update_68 = rail.TriggerDagRunOperator(
            task_id='trigger_user_sync_update_68',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_update_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.conf_payload('update', dag_run)
        )

        wait_for_user_sync_update_68 = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_sync_update_68',
            dag_runs='{{ result("trigger_user_sync_update_68") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_blank_69 = rail.IfOperator(
            task_id='if_active_blank_69',
            test='''{{ dag_run.conf.Active | is_falsy or dag_run.conf.Active == '-' }}''',
            yes_task="momentive_user_import_logs_add_entry_70",
            no_task="finish",
        )

        momentive_user_import_logs_add_entry_70 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_70',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Skipped",
                "details": "User status (Active) received blank value or \"-\""
            }
        )

        if_active_equals_to_1_72 = rail.IfOperator(
            task_id='if_active_equals_to_1_72',
            test='''{{ dag_run.conf.Active == '1' }}''',
            yes_task="trigger_user_sync_add_73",
            no_task="if_active_equals_to_0_74",
        )

        trigger_user_sync_add_73 = rail.TriggerDagRunOperator(
            task_id='trigger_user_sync_add_73',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_add_user_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.conf_payload('add', dag_run)
        )

        wait_for_user_sync_add_73 = rail.WaitForDagRunsSensor(
            task_id='wait_for_user_sync_add_73',
            dag_runs='{{ result("trigger_user_sync_add_73") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        if_active_equals_to_0_74 = rail.IfOperator(
            task_id='if_active_equals_to_0_74',
            test='''{{ dag_run.conf.Active == '0' or dag_run.conf.Active == '-' }}''',
            yes_task="momentive_user_import_logs_add_entry_75",
            no_task="finish",
        )

        momentive_user_import_logs_add_entry_75 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_75',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add",
                "status": "Skipped",
                "details": "User is  disabled in workday hence not added"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        # Any unhandled failure in this run is logged against the master's job id so it
        # appears in the master's import report instead of being lost with the run.
        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                'jobid': "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Add/Update",
                "status": "Error",
                "details": "Error while processing the user {{ get_error_message() }}"
            }
        )

        # ---- wiring ----
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> declare_list

        declare_list >> search_users_33 >> if_user_name_textvalue_present

        if_user_name_textvalue_present >> rail.Label('Yes') >> foreach_search_users_33 >> insert_to_list >> foreach_search_users_33_end
        foreach_search_users_33 >> foreach_search_users_33_end >> log_ifuserexistsuseruri_36
        if_user_name_textvalue_present >> rail.Label('No') >> log_ifuserexistsuseruri_36

        log_ifuserexistsuseruri_36 >> if_log_ifuserexistsuseruri_36_present_41

        # Existing user.
        if_log_ifuserexistsuseruri_36_present_41 >> rail.Label('Yes') >> log_enddatepresent_and_userstatus_42_43 >> if_log_userstatus_43_equals_to_false_44
        if_log_ifuserexistsuseruri_36_present_41 >> rail.Label('No') >> if_active_equals_to_1_72

        if_log_userstatus_43_equals_to_false_44 >> rail.Label('Yes') >> if_active_present_45
        if_log_userstatus_43_equals_to_false_44 >> rail.Label('No') >> if_log_userstatus_43_equals_to_true_63

        # Disabled in Replicon (status false) and Active=0 in the feed.
        if_active_present_45 >> rail.Label('Yes') >> if_log_enddatepresent_42_present_46
        if_active_present_45 >> rail.Label('No') >> if_active_present_rehire_60

        if_log_enddatepresent_42_present_46 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_47 >> if_active_present_rehire_60
        if_log_enddatepresent_42_present_46 >> rail.Label('No') >> if_termination_date_is_present_49

        if_termination_date_is_present_49 >> rail.Label('Yes') >> if_to_date_to_time_equals_to_todayto_time_50
        if_termination_date_is_present_49 >> rail.Label('No') >> if_active_present_rehire_60

        if_to_date_to_time_equals_to_todayto_time_50 >> rail.Label('Yes') >> log_split_dates >> if_termination_date_less_than_startdate_54
        if_to_date_to_time_equals_to_todayto_time_50 >> rail.Label('No') >> momentive_user_import_logs_add_entry_59 >> if_active_present_rehire_60

        if_termination_date_less_than_startdate_54 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_55 >> if_active_present_rehire_60
        if_termination_date_less_than_startdate_54 >> rail.Label('No') >> trigger_disable_user_child_dag_57 >> wait_for_disable_user_child_dag_57 >> if_active_present_rehire_60

        # Rehire: disabled in Replicon but Active=1 in the feed. Every status==false path
        # above converges here exactly as it did in the master loop; the Active values are
        # mutually exclusive, so a disable path can never also trigger the rehire.
        if_active_present_rehire_60 >> rail.Label('Yes') >> trigger_user_sync_update_rehire_62 >> wait_for_user_sync_update_rehire_62 >> finish
        if_active_present_rehire_60 >> rail.Label('No') >> finish

        # Enabled in Replicon (status true).
        if_log_userstatus_43_equals_to_true_63 >> rail.Label('Yes') >> if_active_present_64
        if_log_userstatus_43_equals_to_true_63 >> rail.Label('No') >> finish

        if_active_present_64 >> rail.Label('Yes') >> trigger_dag_child_workflow_to_disable_user_65 >> wait_for_dag_child_workflow_to_disable_user_65 >> if_active_present_66
        if_active_present_64 >> rail.Label('No') >> if_active_present_66

        if_active_present_66 >> rail.Label('Yes') >> trigger_user_sync_update_68 >> wait_for_user_sync_update_68 >> if_active_blank_69
        if_active_present_66 >> rail.Label('No') >> if_active_blank_69

        if_active_blank_69 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_70 >> finish
        if_active_blank_69 >> rail.Label('No') >> finish

        # New user (no uri found in Replicon).
        if_active_equals_to_1_72 >> rail.Label('Yes') >> trigger_user_sync_add_73 >> wait_for_user_sync_add_73 >> if_active_equals_to_0_74
        if_active_equals_to_1_72 >> rail.Label('No') >> if_active_equals_to_0_74

        if_active_equals_to_0_74 >> rail.Label('Yes') >> momentive_user_import_logs_add_entry_75 >> finish
        if_active_equals_to_0_74 >> rail.Label('No') >> finish

        finish >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
