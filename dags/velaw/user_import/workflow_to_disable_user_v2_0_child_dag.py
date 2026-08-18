
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'velaw_user_import_velawg3_child_workflow_to_disable_user_v2_0_{config.instance}',
        description=f'Velawg3_Child_Workflow to disable user_V2.0 {config.instance}',
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
            no_task='velaw_user_disable_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='velaw_user_disable_logs',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        velaw_user_disable_logs = rail.CreateLogOperator(
            task_id='velaw_user_disable_logs',
        )

        is_integrationuser = rail.IfOperator(
            task_id='is_integrationuser',
            test="{{ dag_run.conf.actualuserlogin == dag_run.conf.userloginname }}",
            yes_task='write_exception_integrationuser',
            no_task='if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3'
        )

        write_exception_integrationuser = rail.WriteLogOperator(
            task_id='write_exception_integrationuser',
            log="{{ result('velaw_user_disable_logs') }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "username": dag_run.conf['firstname'] + ' ' + dag_run.conf['lastname'],
                "loginname": dag_run.conf['userloginname'],
                "employeeid": dag_run.conf['employeeid'],
                "action": "disable",
                "status": "Skipped",
                "details": "Used for user integration and Country ISO Code not set to \"US\" or \"GB\"" if dag_run.conf['emplid'] == "NA" else "Used for user integration"
            }
        )

        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3 = rail.IfOperator(
            task_id='if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%m/%d/%Y") > datetime.strptime(dag_run.conf['enddate'], "%m/%d/%Y"),
            yes_task="velaw_user_import_logs_add_entry_4",
            no_task="date_split_end_date_6",
        )

        velaw_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_4',
            log="{{ result('velaw_user_disable_logs') }}",
            message="na",
            severity="Skipped",
            properties={
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }} ",
                "importaction": "disable",
                "employeeid": "{{ dag_run.conf.emplid }}",
                "status": "Skipped",
                "details": "User's start date ({{ dag_run.conf.startdate }}) is in future",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        date_split_end_date_6 = rail.EmptyOperator(
            task_id='date_split_end_date_6',
        )

        date_split_start_date_7 = rail.EmptyOperator(
            task_id='date_split_start_date_7',
        )

        disable_login_8 = rail.RepliconServiceOperator(
            task_id='disable_login_8',
            endpoint="/services/SecurityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        update_employment_date_rangeforenddate_9 = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate_9',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": {
                        "year": dag_run.conf['startdate'].split('/')[2],
                        "month": dag_run.conf['startdate'].split('/')[0],
                        "day":  dag_run.conf['startdate'].split('/')[1]
                    },
                    "endDate": {
                        "year": dag_run.conf['enddate'].split('/')[2],
                        "month": dag_run.conf['enddate'].split('/')[0],
                        "day":  dag_run.conf['enddate'].split('/')[1]
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_time_off_type_policy_summary_10 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_10',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        foreach_d_11 = rail.ForEachOperator(
            task_id='foreach_d_11',
            items="{{ result('get_user_time_off_type_policy_summary_10').policiesByTimeOffType | to_json }}",
            start_task='if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12',
            end_task='foreach_d_11_end'
        )

        if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12 = rail.IfOperator(
            task_id='if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12',
            test='''{{ result('foreach_d_11').isTimeOffAllowedAgainstThisTimeOffType | is_truthy and result('foreach_d_11').policySetSchedule[0].effectiveDate.day | is_truthy }}''',
            yes_task="get_balance_summary_for_account_13",
            no_task="foreach_d_11_end",
        )

        get_balance_summary_for_account_13 = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account_13',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_d_11')['timeOffType']['uri']
                },
                "asOfDate": {
                    "year": dag_run.conf['enddate'].split('/')[2],
                    "month": dag_run.conf['enddate'].split('/')[0],
                    "day":  dag_run.conf['enddate'].split('/')[1]
                }
            }
        )

        trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014',
            retries=0,
            items=[0],
            trigger_dag_id=f'velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_0_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri": dag_run.conf['useruri'],
                "timeoffuri": rail.result('foreach_d_11')['timeOffType']['uri'],
                "policyset": rail.result('foreach_d_11')['policySetSchedule'],
                "newschedulebalance": rail.result('get_balance_summary_for_account_13')['timeRemaining'],
                "enddate": dag_run.conf['enddate'].split('/')[1] + "/" + dag_run.conf['enddate'].split('/')[0] + "/" + dag_run.conf['enddate'].split('/')[2],
                "startingbalancesettouri": dag_run.conf['startingbalancesettouri'],
                "preventbalanceoverdrawuri": dag_run.conf['preventbalanceoverdrawuri'],
                "loginname": rail.result('get_balance_summary_for_account_13')['account']['user']['loginName'],
                "enddateday": dag_run.conf['enddate'].split('/')[1],
                "enddatemonth": dag_run.conf['enddate'].split('/')[0],
                "enddateyear": dag_run.conf['enddate'].split('/')[2]
            }
        )

        wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014") }}'
        )

        foreach_d_11_end = rail.EmptyOperator(
            task_id='foreach_d_11_end',
        )

        velaw_user_import_logs_add_entry_15 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_15',
            log="{{ result('velaw_user_disable_logs') }}",
            message="na",
            severity="Success",
            properties={
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }} ",
                "importaction": "disable",
                "employeeid": "{{ dag_run.conf.emplid }}",
                "status": "Success",
                "details": "User profile disabled successfully",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        velaw_user_import_logs_add_entry_17 = rail.WriteLogOperator(
            task_id='velaw_user_import_logs_add_entry_17',
            log="{{ result('velaw_user_disable_logs') }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties={
                "username": "{{ dag_run.conf.username }}",
                "loginname": "{{ dag_run.conf.userloginname }} ",
                "importaction": "disable",
                "employeeid": "{{ dag_run.conf.emplid }}",
                "status": "Error",
                "childjobid": "{{ dag_run_ecid() }}",
                "details": "{{ get_error_message() }}",
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> velaw_user_disable_logs >> is_integrationuser
        is_integrationuser >> rail.Label(
            'Yes') >> write_exception_integrationuser >> velaw_user_import_logs_add_entry_17
        is_integrationuser >> rail.Label(
            'No') >> if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3

        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3 >> rail.Label(
            'Yes') >> velaw_user_import_logs_add_entry_4 >> velaw_user_import_logs_add_entry_17
        if_startdate_to_date_greater_than_dataworkato_servicereceive_requestrequestenddateto_date_3 >> rail.Label(
            'No') >> date_split_end_date_6 >> date_split_start_date_7 >> disable_login_8 >> update_employment_date_rangeforenddate_9 \
            >> get_user_time_off_type_policy_summary_10 >> foreach_d_11 >> if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12
        if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12 >> rail.Label(
            'Yes') >> get_balance_summary_for_account_13 >> trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014 \
            >> wait_for_completion_trigger_dag_run_velaw_user_import_velawg3_child_for_timeoff_policy_update_on_each_time_off_type_for_no_accrual_v2_014 >> foreach_d_11_end
        if_foreach_d_11_istimeoffallowedagainstthistimeofftype_is_true_12 >> rail.Label(
            'No') >> foreach_d_11_end
        foreach_d_11 >> foreach_d_11_end >> velaw_user_import_logs_add_entry_15 >> velaw_user_import_logs_add_entry_17 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
