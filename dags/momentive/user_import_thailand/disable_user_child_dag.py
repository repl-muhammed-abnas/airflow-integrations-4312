# pylint: disable=line-too-long
from datetime import timedelta, datetime
from pendulum import now
from airflow.models import Variable
import rail
from momentive.user_import_thailand.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.momentive_thailand_user_sync_child_disable_user_dag_id,
        description=f'Momentive_thailand_user_sync_disable_user_child_{config.instance}',
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

        # Recipe [2]: GetMyActualUserIdentity (used to prevent the integration user from disabling itself).
        get_my_actual_identity = rail.RepliconServiceOperator(
            task_id='get_my_actual_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity"
        )

        # Recipe [3]/[4]: if the incoming User_ID is the calling (integration) user, stop without action.
        if_request_user_id_equals_to_loginname = rail.IfOperator(
            task_id='if_request_user_id_equals_to_loginname',
            test='''{{ dag_run.conf.User_ID == result('get_my_actual_identity').loginName }}''',
            yes_task="catch_and_log_error",
            no_task="if_active_equals_0",
        )

        # Recipe [5]: Active == '0'.
        if_active_equals_0 = rail.IfOperator(
            task_id='if_active_equals_0',
            test=lambda dag_run: dag_run.conf['Active'] == '0',
            yes_task="if_termination_date_to_date_lesser_or_equals_to_today",
            no_task="get_split_start_and_end_dates",
        )

        # Recipe [6]: Termination_Date <= today (equals today OR earlier).
        if_termination_date_to_date_lesser_or_equals_to_today = rail.IfOperator(
            task_id='if_termination_date_to_date_lesser_or_equals_to_today',
            test=lambda dag_run: bool(dag_run.conf.get('Termination_Date')) and datetime.strptime(dag_run.conf['Termination_Date'], "%Y-%m-%d").date() <= now(tz=config.time_zone).date(),
            yes_task="disable_login",
            no_task="get_split_start_and_end_dates",
        )

        # Recipe [7]: DisableLogin.
        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        # Recipe [8]/[10]: split Hire_Date (start) and Termination_Date (end) into year/month/day.
        get_split_start_and_end_dates = rail.PythonOperator(
            task_id="get_split_start_and_end_dates",
            python_callable=lambda dag_run: {
                "startdate_split": python_callable.split_date_string(dag_run.conf['Hire_Date'], 'int') if dag_run.conf.get('Hire_Date') else null,
                "enddate_split": python_callable.split_date_string(dag_run.conf['Termination_Date'], 'int') if dag_run.conf.get('Termination_Date') else null,
            }
        )

        # Recipe [9]: Termination_Date present.
        if_termination_date_present = rail.IfOperator(
            task_id='if_termination_date_present',
            test='''{{ dag_run.conf.Termination_Date | is_truthy }}''',
            yes_task="update_employment_date_rangeforenddate",
            no_task="momentive_user_import_logs_add_entry_no_enddate",
        )

        # Recipe [11]: UpdateEmploymentDateRange with start (hire) and end (termination) dates.
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

        # Recipe [12]: GetUserTimeOffTypePolicySummary.
        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        # Recipe [13]: balance event scripts (to resolve "Starting Balance Set To").
        get_all_scripts_time_offbalanceeventscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offbalanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
        )

        # Recipe [14]: validation scripts.
        get_all_scripts_time_offvalidationscripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts_time_offvalidationscripts',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
        )

        # Recipe [15]: for each time-off-type policy of the user.
        foreach_d_21 = rail.ForEachOperator(
            task_id='foreach_d_21',
            items=lambda: rail.result('get_user_time_off_type_policy_summary')[
                'policiesByTimeOffType'],
            start_task='if_21_istimeoffallowedagainstthistimeofftype_is_true_22',
            end_task='foreach_d_21_end'
        )

        # Recipe [16]: only time-off types that allow time off.
        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 = rail.IfOperator(
            task_id='if_21_istimeoffallowedagainstthistimeofftype_is_true_22',
            test='''{{ result('foreach_d_21').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account",
            no_task="foreach_d_21_end",
        )

        # Recipe [17]: balance remaining as of the termination date.
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

        # Recipe [18]: only when a policy set schedule (with description) exists.
        if_first_description_present_16 = rail.IfOperator(
            task_id='if_first_description_present_16',
            test=lambda: bool(
                (rail.result('foreach_d_21').get('policySetSchedule') or [{}])[0].get('description')),
            yes_task="trigger_dag_run_momentive_put_remaining_balance_for_payout",
            no_task="foreach_d_21_end",
        )

        # Recipe [20]: hand the remaining balance to the put-remaining-balance child (flow 1145868).
        trigger_dag_run_momentive_put_remaining_balance_for_payout = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_momentive_put_remaining_balance_for_payout',
            retries=0,
            trigger_dag_id=config.momentive_thailand_user_sync_child_put_remaining_balance_for_payout_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "timeoffuri": rail.result('foreach_d_21')['timeOffType']['uri'],
                "useruri": dag_run.conf['useruri'],
                "terminationdate": str(rail.result('get_split_start_and_end_dates')['enddate_split']['day']) + "/" + str(rail.result('get_split_start_and_end_dates')['enddate_split']['month']) + "/" + str(rail.result('get_split_start_and_end_dates')['enddate_split']['year']),
                "startingbalancesettouri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_time_offbalanceeventscripts'), 'displayText', "Starting Balance Set To", 'uri', ''),
                "balance": int(rail.result('get_balance_summary_for_account')['timeRemaining']) if rail.result('get_balance_summary_for_account')['timeRemaining'] else 0,
                "parentjobid": dag_run.conf['parentjobid'],
                "user_import_logs": dag_run.conf['user_import_logs'],
                "User_ID": dag_run.conf['User_ID'],
                "First_Name": dag_run.conf['First_Name'],
                "Last_Name": dag_run.conf['Last_Name'],
            }
        )

        wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("get_payout_child_ids") }}'
        )

        foreach_d_21_end = rail.EmptyOperator(
            task_id='foreach_d_21_end',
        )

        # Recipe [23]: accumulate each triggered payout child so the success log can
        # surface their errors (col5 escalates to Error when any payout failed).
        create_payout_trigger_list = rail.SetVariableOperator(
            task_id='create_payout_trigger_list',
            append=False,
            name='payouttriggeredlist',
            value=[]
        )

        insert_childid_to_payout_list = rail.SetVariableOperator(
            task_id='insert_childid_to_payout_list',
            append=True,
            name="{{ result('create_payout_trigger_list').name }}",
            value="{{ result('trigger_dag_run_momentive_put_remaining_balance_for_payout') }}"
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

        # Recipe [23]: success log when an end date was processed; status escalates to
        # Error and details surface the reasons when any payout child reported an error.
        momentive_user_import_logs_add_entry_success = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_success',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity=lambda: "Error" if [e for e in (rail.result('gather_payout_results') or []) if e] else "Success",
            properties=lambda dag_run: {
                "jobid": dag_run.conf['parentjobid'],
                "userid": dag_run.conf['User_ID'],
                "username": dag_run.conf['First_Name'] + " " + dag_run.conf['Last_Name'],
                "action": "Disable user",
                "status": "Error" if [e for e in (rail.result('gather_payout_results') or []) if e] else "Success",
                "details": ";".join([e for e in (rail.result('gather_payout_results') or []) if e] + [
                    "User profile disabled successfully with end date"]),
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
            }
        )

        # Recipe [25]: success log when no end date was received.
        momentive_user_import_logs_add_entry_no_enddate = rail.WriteLogOperator(
            task_id='momentive_user_import_logs_add_entry_no_enddate',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            severity="Success",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Success",
                "details": "User profile disabled successfully however no end date was received",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        # Recipe [27]: error log (catch).
        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.user_import_logs }}",
            message="na",
            trigger_rule='one_failed',
            severity="Error",
            properties={
                "jobid": "{{ dag_run.conf.parentjobid }}",
                "userid": "{{ dag_run.conf.User_ID }}",
                "username": "{{ dag_run.conf.First_Name }} {{ dag_run.conf.Last_Name }}",
                "action": "Disable user",
                "status": "Error",
                "details": "Error processing Disabling user - {{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}",
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_my_actual_identity

        get_my_actual_identity >> if_request_user_id_equals_to_loginname
        if_request_user_id_equals_to_loginname >> rail.Label('Yes') >> catch_and_log_error
        if_request_user_id_equals_to_loginname >> rail.Label('No') >> if_active_equals_0

        if_active_equals_0 >> rail.Label('Yes') >> if_termination_date_to_date_lesser_or_equals_to_today
        if_active_equals_0 >> rail.Label('No') >> get_split_start_and_end_dates

        if_termination_date_to_date_lesser_or_equals_to_today >> rail.Label('No') >> get_split_start_and_end_dates
        if_termination_date_to_date_lesser_or_equals_to_today >> rail.Label('Yes') >> disable_login >> get_split_start_and_end_dates

        get_split_start_and_end_dates >> if_termination_date_present

        if_termination_date_present >> rail.Label('No') >> momentive_user_import_logs_add_entry_no_enddate >> catch_and_log_error
        if_termination_date_present >> rail.Label('Yes') >> update_employment_date_rangeforenddate

        update_employment_date_rangeforenddate >> get_user_time_off_type_policy_summary >> get_all_scripts_time_offbalanceeventscripts \
            >> get_all_scripts_time_offvalidationscripts >> create_payout_trigger_list >> foreach_d_21 >> if_21_istimeoffallowedagainstthistimeofftype_is_true_22

        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 >> rail.Label('No') >> foreach_d_21_end
        if_21_istimeoffallowedagainstthistimeofftype_is_true_22 >> rail.Label('Yes') >> get_balance_summary_for_account >> if_first_description_present_16

        if_first_description_present_16 >> rail.Label('No') >> foreach_d_21_end
        if_first_description_present_16 >> rail.Label('Yes') >> trigger_dag_run_momentive_put_remaining_balance_for_payout \
            >> insert_childid_to_payout_list >> foreach_d_21_end

        foreach_d_21 >> foreach_d_21_end >> get_payout_child_ids \
            >> wait_for_completion_trigger_dag_run_momentive_put_remaining_balance_for_payout \
            >> gather_payout_results >> momentive_user_import_logs_add_entry_success >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
