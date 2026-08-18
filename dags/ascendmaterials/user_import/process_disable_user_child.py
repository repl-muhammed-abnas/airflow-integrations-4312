from datetime import timedelta
import json
from airflow.models import Variable
import rail
from ascendmaterials.user_import.utils import python_callable


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.disable_user_dag_id,
        description=f'Ascend_Child_Workflow to disable user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
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
            no_task='if_enddate_blank'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_enddate_blank',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_enddate_blank = rail.IfOperator(
            task_id='if_enddate_blank',
            test='''{{ dag_run.conf["enddate"] | is_falsy }}''',
            yes_task="log_entry_1",
            no_task="disable_login",
        )

        def get_details(dag_run):
            if dag_run.conf["enddate"]:
                if "/" in dag_run.conf["enddate"]:
                    return ""
                return "End Date is not in the predefined format"
            return "End date is not present"

        log_entry_1 = rail.WriteLogOperator(
            task_id='log_entry_1',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('userloginname', ''),
                "username": dag_run.conf.get('firstname', '') + " " + dag_run.conf.get('lastname', ''),
                "status": "Skipped",
                "action": "Disable",
                "details": get_details(dag_run)
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        log_start_date = rail.PythonOperator(
            task_id='log_start_date',
            python_callable=python_callable.get_datetime_obj,
            op_args=['{{ dag_run.conf["startdate"] }}', "%m/%d/%Y"]
        )

        log_end_date_day = rail.PythonOperator(
            task_id='log_end_date_day',
            python_callable=python_callable.get_datetime_obj,
            op_args=['{{ dag_run.conf["enddate"] }}', "%m/%d/%Y"]
        )

        update_employment_date_rangeforenddate = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}',
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('log_start_date').year }}",
                        "month": "{{ result('log_start_date').month }}",
                        "day": "{{ result('log_start_date').day }}"
                    },
                    "endDate": {
                        "year": "{{ result('log_end_date_day').year }}",
                        "month": "{{ result('log_end_date_day').month }}",
                        "day": "{{ result('log_end_date_day').day }}"
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_time_off_type_policy_summary = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": '{{ dag_run.conf["useruri"] }}'
            }
        )

        foreach_d = rail.ForEachOperator(
            task_id='foreach_d',
            items="{{ result('get_user_time_off_type_policy_summary').policiesByTimeOffType | to_json }}",
            start_task='if_timeoff_allowed',
            end_task='foreach_d_15_end'
        )

        if_timeoff_allowed = rail.IfOperator(
            task_id='if_timeoff_allowed',
            test='''{{ result('foreach_d').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account",
            no_task="foreach_d_15_end",
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": '{{ dag_run.conf["useruri"] }}',
                    "timeOffTypeUri": "{{ result('foreach_d').timeOffType.uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('log_end_date_day').year }}",
                    "month": "{{ result('log_end_date_day').month }}",
                    "day": "{{ result('log_end_date_day').day }}"
                }
            }
        )

        log_policy_set_schedule = rail.PythonOperator(
            task_id='log_policy_set_schedule',
            python_callable=lambda:  rail.result(
                'foreach_d')['policySetSchedule']
        )

        if_policy_set_schedule_present = rail.IfOperator(
            task_id='if_policy_set_schedule_present',
            test='''{{ result('log_policy_set_schedule') | is_truthy }}''',
            yes_task="trigger_timeoff_policy21",
            no_task="foreach_d_15_end",
        )

        trigger_timeoff_policy21 = rail.TriggerDagRunOperator(
            task_id='trigger_timeoff_policy21',
            retries=0,
            trigger_dag_id=config.timeoff_policy_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf["parentjobid"],
                "userloginname": dag_run.conf["userloginname"],
                "useruri": dag_run.conf["useruri"],
                "enddate": dag_run.conf["enddate"],
                "timeoffuri": rail.result('foreach_d')['timeOffType']['uri'],
                "policyset": json.loads(json.dumps(rail.result('log_policy_set_schedule'))),
                "newschedulebalance": rail.result('get_balance_summary_for_account')['timeRemaining'],
                "ascend_user_import_logs_lookuptable": dag_run.conf["ascend_user_import_logs_lookuptable"]
            }
        )

        foreach_d_15_end = rail.EmptyOperator(
            task_id='foreach_d_15_end',
            trigger_rule='none_failed',
        )

        wait_timeoff_policy21 = rail.WaitForDagRunsSensor(
            task_id='wait_timeoff_policy21',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_policy21") }}'
        )

        log_entry_2 = rail.WriteLogOperator(
            task_id='log_entry_2',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            message="na",
            severity="Success",
            properties=lambda dag_run: {
                "userloginname": dag_run.conf.get('userloginname', ''),
                "username": dag_run.conf.get('firstname', '') + " " + dag_run.conf.get('lastname', ''),
                "status": "Success",
                "action": "Disable",
                "details": "User profile disabled successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["ascend_user_import_logs_lookuptable"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": dag_run.conf.get('firstname', '') + " " + dag_run.conf.get('lastname', ''),
                "userloginname": dag_run.conf.get('userloginname', ''),
                "status": "Error",
                "action": "Disable",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> if_enddate_blank
        if_enddate_blank >> rail.Label(
            'Yes') >> log_entry_1 >> catch_and_log_errors
        if_enddate_blank >> rail.Label(
            'No') >> disable_login >> log_start_date >> log_end_date_day >> update_employment_date_rangeforenddate >> get_user_time_off_type_policy_summary >> foreach_d >> if_timeoff_allowed
        if_timeoff_allowed >> rail.Label(
            'Yes') >> get_balance_summary_for_account >> log_policy_set_schedule >> if_policy_set_schedule_present
        if_policy_set_schedule_present >> rail.Label(
            'Yes') >> trigger_timeoff_policy21 >> foreach_d_15_end
        if_policy_set_schedule_present >> rail.Label(
            'No') >> foreach_d_15_end
        if_timeoff_allowed >> rail.Label(
            'No') >> foreach_d_15_end
        foreach_d >> foreach_d_15_end >> wait_timeoff_policy21 >> log_entry_2 >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
