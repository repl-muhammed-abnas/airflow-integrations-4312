from datetime import timedelta, datetime
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_japan.utils import request_payload, python_callable

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_japan_user_sync_child_disable_user_dag_id,
        description=f'Momentive_japan_user_sync_disable_user_child_{config.instance}',
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
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_my_actual_identity'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_my_actual_identity',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_my_actual_identity = rail.RepliconServiceOperator(
            task_id='get_my_actual_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity"
        )

        if_request_user_id_equals_to_loginname = rail.IfOperator(
            task_id='if_request_user_id_equals_to_loginname',
            test='''{{ dag_run.conf.userid == result('get_my_actual_identity').loginName }}''',
            yes_task="catch_and_log_error",
            no_task="if_active_equals_0",
        )

        if_active_equals_0 = rail.IfOperator(
            task_id='if_active_equals_0',
            test=lambda dag_run: bool(int(dag_run.conf['active']) == 0),
            yes_task="if_termination_date_to_date_lesser_or_equals_to_today",
            no_task="get_split_start_and_end_dates",
        )

        if_termination_date_to_date_lesser_or_equals_to_today = rail.IfOperator(
            task_id='if_termination_date_to_date_lesser_or_equals_to_today',
            test=lambda dag_run: bool(datetime.strptime(dag_run.conf['terminationdate'], "%Y-%m-%d") <= datetime.now()),
            yes_task="disable_login",
            no_task="get_split_start_and_end_dates",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )
     
        get_split_start_and_end_dates = rail.PythonOperator(
            task_id="get_split_start_and_end_dates",
            python_callable=lambda dag_run: {
                "startdate_split": python_callable.split_date_string(dag_run.conf['hiredate'],'int'),
                "enddate_split": python_callable.split_date_string(dag_run.conf['terminationdate'],'int') if dag_run.conf['terminationdate'] else '',
                "enddate_plus_one_day": python_callable.split_date_string((datetime.strptime(
                    dag_run.conf['terminationdate'], "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d"), 'int') if dag_run.conf['terminationdate'] else ''
            }
        )

        if_termination_date_present = rail.IfOperator(
            task_id='if_termination_date_present',
            test='''{{ dag_run.conf.terminationdate | is_truthy }}''',
            yes_task="get_0hrs_office_schedule_uri",
            no_task="momentive_user_import_logs_add_entry_31",
        )

        get_0hrs_office_schedule_uri = rail.RepliconServiceOperator(
            task_id='get_0hrs_office_schedule_uri',
            endpoint="/services/OfficeScheduleService1.svc/GetAllOfficeSchedules",
            data_handler=lambda response: {
                '0hrs_schedule': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', "0 hrs. Schedule", 'uri')
            }
        )

        if_0hrs_schedule_present = rail.IfOperator(
            task_id='if_0hrs_schedule_present',
            test=lambda: rail.result('get_0hrs_office_schedule_uri')['0hrs_schedule'] is not None,
            yes_task="update_office_schedule",
            no_task="update_employment_date_rangeforenddate"
        )

        update_office_schedule = rail.RepliconServiceOperator(
            task_id='update_office_schedule',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.update_office_schedule_payload
        )

        update_employment_date_rangeforenddate = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": rail.result('get_split_start_and_end_dates')['startdate_split'],
                    "endDate": rail.result('get_split_start_and_end_dates')['enddate_split'],
                    "relativeDateRangeUri": None,
                    "relativeDateRangeAsOfDate": None
                }
            }
        )

        create_payout_trigger_list = rail.SetVariableOperator(
            task_id='create_payout_trigger_list',
            name='payouttriggeredlist',
            append=False,
            value=[]
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts_time_offbalanceeventscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offbalanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        get_all_scripts_time_offvalidationscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offvalidationscripts',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        foreach_d_21 = rail.ForEachOperator(
            task_id='foreach_d_21',
            items=lambda: rail.result('get_user_time_off_type_policy_summary')[
                'policiesByTimeOffType'],
            start_task='if_21_istimeoffallowedagainstthistimeofftype_is_true_22',
            end_task='foreach_d_21_end'
        )

        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 = rail.IfOperator(
            task_id='if_21_istimeoffallowedagainstthistimeofftype_is_true_22',
            test='''{{ result('foreach_d_21').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account",
            no_task="foreach_d_21_end",
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_21')['timeOffType']['uri']
                },
                "asOfDate": rail.result('get_split_start_and_end_dates')['enddate_split']
            }
        )

        if_first_description_present_16 = rail.IfOperator(
            task_id='if_first_description_present_16',
            test='''{{ result('foreach_d_21').policySetSchedule | is_truthy and result('foreach_d_21').policySetSchedule[0].description | is_truthy }}''',
            yes_task="trigger_dag_run_momentive_put_remaining_balance_for_payout",
            no_task="foreach_d_21_end",
        )

        trigger_dag_run_momentive_put_remaining_balance_for_payout = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_momentive_put_remaining_balance_for_payout',
            retries=0,
            trigger_dag_id=config.momentive_japan_user_sync_child_put_remaining_balance_for_payout_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "timeoffuri": rail.result('foreach_d_21')['timeOffType']['uri'],
                "useruri": dag_run.conf['useruri'],
                "terminationdate": str(rail.result('get_split_start_and_end_dates')['enddate_split']['day']).zfill(2) + "/" + str(rail.result('get_split_start_and_end_dates')['enddate_split']['month']).zfill(2) + "/" + str(rail.result('get_split_start_and_end_dates')['enddate_split']['year']),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_offbalanceeventscripts'), 'displayText', "Starting Balance Set To", 'uri', ''),
                "balance": int(rail.result('get_balance_summary_for_account')['timeRemaining']) if rail.result('get_balance_summary_for_account')['timeRemaining'] else 0
            }
        )

        insert_childid_to_payout_list = rail.SetVariableOperator(
            task_id='insert_childid_to_payout_list',
            name="{{ result('create_payout_trigger_list').name }}",
            append=True,
            value="{{ result('trigger_dag_run_momentive_put_remaining_balance_for_payout') }}"
        )

        wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_payout_child_ids") }}'
        )

        foreach_d_21_end = rail.EmptyOperator(
            task_id='foreach_d_21_end',
        )

        get_payout_child_ids = rail.PythonOperator(
            task_id='get_payout_child_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('payouttriggeredlist')] if rail.get_dag_run_var('payouttriggeredlist') else []
        )

        gather_payout_results = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_payout_results',
            dag_runs='{{ result("get_payout_child_ids") }}',
            dagrun_task_id='final_response_from_dag',
            flatten=True
        )

        momentive_user_import_logs_add_entry_29 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_29',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Error" if [e for e in (rail.result('gather_payout_results') or []) if e] else "Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['userid'],
                "username": dag_run.conf['firstname'] + "|" + dag_run.conf['lastname'],
                "action": "Disable user",
                "status": "Error" if [e for e in (rail.result('gather_payout_results') or []) if e] else "Success",
                "details": ";".join([e for e in (rail.result('gather_payout_results') or []) if e] + [
                    "User profile disabled successfully with end date"]),
                "childjobid": get_dagrun_ecid(dag_run),
            }
        )

        momentive_user_import_logs_add_entry_31 = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_31',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + "|" + "{{ dag_run.conf.lastname }}",
                "action": "Disable user",
                "status": "Success",
                "details": "User profile disabled successfully however no end date was received",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}",
                "action": "Disable user",
                "status": "Error",
                "details": "Error processing Disabling user - {{get_error_message()}}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> get_my_actual_identity

        get_my_actual_identity >> if_request_user_id_equals_to_loginname >> rail.Label(
            'Yes') >> catch_and_log_error

        if_request_user_id_equals_to_loginname >> rail.Label(
            'No') >> if_active_equals_0 
        
        if_active_equals_0 >> rail.Label(
            'Yes') >> if_termination_date_to_date_lesser_or_equals_to_today
        if_active_equals_0 >> rail.Label(
            'No') >> get_split_start_and_end_dates

        if_termination_date_to_date_lesser_or_equals_to_today >> rail.Label(
            'No') >> get_split_start_and_end_dates
        if_termination_date_to_date_lesser_or_equals_to_today >> rail.Label(
            'Yes') >> disable_login >> get_split_start_and_end_dates >> if_termination_date_present
        
        if_termination_date_present >> rail.Label(
            'No') >> momentive_user_import_logs_add_entry_31 >> catch_and_log_error
        
        if_termination_date_present >> rail.Label(
            'Yes') >> get_0hrs_office_schedule_uri >> if_0hrs_schedule_present
        if_termination_date_present >> rail.Label(
            'No') >> momentive_user_import_logs_add_entry_31 >> catch_and_log_error
        
        if_0hrs_schedule_present >> rail.Label(
            'Yes') >> update_office_schedule >> update_employment_date_rangeforenddate
        
        if_0hrs_schedule_present >> rail.Label(
            'No') >> update_employment_date_rangeforenddate
        
        update_employment_date_rangeforenddate >> get_user_time_off_type_policy_summary >> get_all_scripts_time_offbalanceeventscripts \
            >> get_all_scripts_time_offvalidationscripts >> create_payout_trigger_list >> foreach_d_21 >> if_21_istimeoffallowedagainstthistimeofftype_is_true_22

        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 >> rail.Label(
            'No') >> foreach_d_21_end
        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 >> rail.Label(
            'Yes') >> get_balance_summary_for_account >> if_first_description_present_16

        if_first_description_present_16 >> rail.Label('No') >> foreach_d_21_end
        # Loop body ends after accumulating the triggered payout id. The wait must NOT be
        # inside the foreach: a WaitForDagRunsSensor in the loop body breaks iteration after
        # item 0, so only the first time-off type was paid out. Mirror the master DAG pattern
        # (trigger + accumulate inside the loop; wait/gather AFTER the loop) so every type is
        # visited - matching the recipe's FOREACH (#20) which pays out all time-off types.
        if_first_description_present_16 >> rail.Label('Yes') >> trigger_dag_run_momentive_put_remaining_balance_for_payout \
            >> insert_childid_to_payout_list >> foreach_d_21_end

        foreach_d_21 >> foreach_d_21_end >> get_payout_child_ids \
            >> wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout \
            >> gather_payout_results >> momentive_user_import_logs_add_entry_29 >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
