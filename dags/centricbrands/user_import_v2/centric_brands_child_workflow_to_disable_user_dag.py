
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'centric_brands_child_workflow_to_disable_user_{config.instance}_v2',
        description=f'Centric_Brands_Child_Workflow to disable user {config.instance}_v2',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
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
            no_task='if_userloginname_equals_admin'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_userloginname_equals_admin',
            end_task='catch_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_userloginname_equals_admin = rail.IfOperator(
            task_id='if_userloginname_equals_admin',
            test='''{{ dag_run.conf.userloginname == 'admin' }}''',
            yes_task="catch_log_error",
            no_task="if_enddate_not_present",
        )

        if_enddate_not_present = rail.IfOperator(
            task_id='if_enddate_not_present',
            test=lambda dag_run: not (
                dag_run.conf['enddate']) and '/' not in dag_run.conf['enddate'],
            yes_task="log_end_date_not_in_prescribed_format",
            no_task="get_startdate_object",
        )

        log_end_date_not_in_prescribed_format = rail.WriteLogOperator(
            task_id='log_end_date_not_in_prescribed_format',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity="Skipped",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['userloginname'],
                "empid": dag_run.conf['empid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": 'Skipped',
                "details": "End Date is not in the prescribed format",
                "jobid": dag_run.conf['parentjobid'],
                "childjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        def get_date_object(datestring):
            dateobj = datetime.strptime(datestring, '%m/%d/%Y')
            return {
                'day': dateobj.day,
                'month': dateobj.month,
                'year': dateobj.year,
                'datestring': datestring
            }

        get_startdate_object = rail.PythonOperator(
            task_id='get_startdate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['startdate'])
        )

        get_enddate_object = rail.PythonOperator(
            task_id='get_enddate_object',
            python_callable=lambda dag_run: get_date_object(
                dag_run.conf['enddate'])
        )

        update_employment_date_rangeforenddate = rail.RepliconServiceOperator(
            task_id='update_employment_date_rangeforenddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "dateRange": {
                    "startDate": {
                        "year": "{{ result('get_startdate_object').year }}",
                        "month": "{{ result('get_startdate_object').month }}",
                        "day": "{{ result('get_startdate_object').day }}"
                    },
                    "endDate": {
                        "year": "{{ result('get_enddate_object').year }}",
                        "month": "{{ result('get_enddate_object').month }}",
                        "day": "{{ result('get_enddate_object').day }}"
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
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_startingbalance_setto_uri = rail.RepliconServiceOperator(
            task_id='get_startingbalance_setto_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_prevent_balance_overdraw_uri = rail.RepliconServiceOperator(
            task_id='get_prevent_balance_overdraw_uri',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        declare_child_triggered_list = rail.SetVariableOperator(
            task_id='declare_child_triggered_list',
            name='childtriggered',
            append=False,
            value=[]
        )

        foreach_policy_by_timeoff_type = rail.ForEachOperator(
            task_id='foreach_policy_by_timeoff_type',
            items="{{ result('get_user_time_off_type_policy_summary').policiesByTimeOffType | to_json}}",
            start_task='if_timeoffallowed_againstthis_timeofftype_is_true',
            end_task='foreach_policy_by_timeoff_type_end'
        )

        if_timeoffallowed_againstthis_timeofftype_is_true = rail.IfOperator(
            task_id='if_timeoffallowed_againstthis_timeofftype_is_true',
            test='''{{ result('foreach_policy_by_timeoff_type').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}''',
            yes_task="get_balance_summary_for_account",
            no_task="foreach_policy_by_timeoff_type_end",
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data={
                "account": {
                    "userUri": "{{ dag_run.conf.useruri }}",
                    "timeOffTypeUri": "{{ result('foreach_policy_by_timeoff_type').timeOffType.uri }}"
                },
                "asOfDate": {
                    "year": "{{ result('get_enddate_object').year }}",
                    "month": "{{ result('get_enddate_object').month }}",
                    "day": "{{ result('get_enddate_object').day }}"
                }
            }
        )

        if_policy_set_schedule_present = rail.IfOperator(
            task_id='if_policy_set_schedule_present',
            test=lambda: bool(rail.result('foreach_policy_by_timeoff_type')[
                              'policySetSchedule']),
            yes_task="trigger_child_to_put_0_balance",
            no_task="foreach_policy_by_timeoff_type_end",
        )

        trigger_child_to_put_0_balance = rail.TriggerDagRunOperator(
            task_id='trigger_child_to_put_0_balance',
            retries=0,
            trigger_dag_id=f'centricbrands_user_import_put_0_balance_{config.instance}_v2',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "timeoffuri": rail.result('foreach_policy_by_timeoff_type')['timeOffType']['uri'],
                "useruri": dag_run.conf['useruri'],
                "terminationdate": str(rail.result('get_enddate_object')['month']) + "/" + str(rail.result('get_enddate_object')['day']) + "/" +
                    str(rail.result('get_enddate_object')['year']),
                "startingbalancesettouri": rail.result('get_startingbalance_setto_uri'),
                "preventbalanceoverdrawuri": rail.result('get_prevent_balance_overdraw_uri'),
                "balance": rail.result('get_balance_summary_for_account')['timeRemaining']
            }
        )

        insert_child_to_triggered_list = rail.SetVariableOperator(
            task_id='insert_child_to_triggered_list',
            name="{{result('declare_child_triggered_list').name}}",
            append=True,
            value="{{result('trigger_child_to_put_0_balance')}}"
        )

        foreach_policy_by_timeoff_type_end = rail.EmptyOperator(
            task_id='foreach_policy_by_timeoff_type_end',
        )

        if_child_triggered = rail.IfOperator(
            task_id='if_child_triggered',
            test=lambda: bool(rail.get_dag_run_var('childtriggered')),
            yes_task='wait_for_child_to_put_0_balance',
            no_task='log_user_disabled_successfully'
        )

        wait_for_child_to_put_0_balance = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_to_put_0_balance',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("insert_child_to_triggered_list").value | to_json}}'
        )

        gather_results_from_put_0_balance_child = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_results_from_put_0_balance_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('insert_child_to_triggered_list').value | to_json}}",
            dagrun_task_id='catch_error_and_return_response',
            flatten=True
        )

        log_user_disabled_successfully = rail.WriteLogOperator(
            task_id='log_user_disabled_successfully',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            message="na",
            severity=lambda: "Error" if rail.result(
                'gather_results_from_put_0_balance_child') else "Success",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['userloginname'],
                "empid": dag_run.conf['empid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": "Error" if rail.result('gather_results_from_put_0_balance_child') else "Success",
                "details": rail.smartjoin_by_delim((( ','.join(rail.result('gather_results_from_put_0_balance_child')) if rail.result(
                    'gather_results_from_put_0_balance_child') else '') + "," + "User profile disabled successfully").split(','),';'),
                "jobid": dag_run.conf['parentjobid'],
                "childjobid": dag_run.conf['childjobid'] + '|' + rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        catch_log_error = rail.WriteLogOperator(
            task_id='catch_log_error',
            log="{{ dag_run.conf.userimportlogslookuptable }}",
            trigger_rule='one_failed',
            message="na",
            severity="Error",
            properties=lambda dag_run: {
                "loginname": dag_run.conf['userloginname'],
                "empid": dag_run.conf['empid'],
                "email": dag_run.conf['email'],
                "isloginenabled": dag_run.conf['isloginenabled'],
                "status": "Error",
                "details": "Error processing Disabling user - " + rail.render_template("{{get_error_message()}}"),
                "jobid": dag_run.conf['parentjobid'],
                "childjobid": dag_run.conf['childjobid'] + '|' + rail.render_template("{{ dag_run_ecid() }}"),
                "department|location|team": '||'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_log_error
        can_run_batch_task >> rail.Label('No') >> if_userloginname_equals_admin
        if_userloginname_equals_admin >> rail.Label('Yes') >> catch_log_error
        if_userloginname_equals_admin >> rail.Label(
            'No') >> if_enddate_not_present
        if_enddate_not_present >> rail.Label(
            'Yes') >> log_end_date_not_in_prescribed_format >> catch_log_error
        if_enddate_not_present >> rail.Label(
            'No') >> get_startdate_object >> get_enddate_object >> update_employment_date_rangeforenddate
        update_employment_date_rangeforenddate >> get_user_time_off_type_policy_summary >> get_startingbalance_setto_uri >> get_prevent_balance_overdraw_uri
        get_prevent_balance_overdraw_uri >> declare_child_triggered_list >> foreach_policy_by_timeoff_type >> if_timeoffallowed_againstthis_timeofftype_is_true
        if_timeoffallowed_againstthis_timeofftype_is_true >> rail.Label(
            'Yes') >> get_balance_summary_for_account >> if_policy_set_schedule_present
        if_policy_set_schedule_present >> rail.Label(
            'Yes') >> trigger_child_to_put_0_balance >> insert_child_to_triggered_list >> foreach_policy_by_timeoff_type_end
        if_policy_set_schedule_present >> rail.Label(
            'No') >> foreach_policy_by_timeoff_type_end
        if_timeoffallowed_againstthis_timeofftype_is_true >> rail.Label(
            'No') >> foreach_policy_by_timeoff_type_end
        foreach_policy_by_timeoff_type >> foreach_policy_by_timeoff_type_end >> if_child_triggered >> rail.Label(
            'Yes') >> wait_for_child_to_put_0_balance
        wait_for_child_to_put_0_balance >> gather_results_from_put_0_balance_child >> log_user_disabled_successfully >> catch_log_error >> log_to_sumo
        if_child_triggered >> rail.Label(
            'No') >> log_user_disabled_successfully
    return dag


rail.for_each_instance(create_dag)
