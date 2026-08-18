
from datetime import timedelta
import rail
from rail.lib.ecid import get_dagrun_ecid
from impervainc.user_sync.utils import python_callable

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_usersync_disable_user,
        description=f'impervainc disable user child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        if_username_equal_to_tempadmin = rail.IfOperator(
            task_id='if_username_equal_to_tempadmin',
            test=lambda dag_run: dag_run.conf['Username'] == 'tempadmin1',
            yes_task="imperva_user_import_logs_add_entry_4",
            no_task="disable_login"
        )

        imperva_user_import_logs_add_entry_4 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_4',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Skipped",
                "reason": "User profile used by Integration.",
                "action": "Disable User",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            }
        )

        if_termination_date_present = rail.IfOperator(
            task_id='if_termination_date_present',
            test="{{dag_run.conf.termination_date | is_truthy}}",
            yes_task="update_employeement_date_range",
            no_task="imperva_user_import_logs_add_entry_24"
        )

        update_employeement_date_range = rail.RepliconServiceOperator(
            task_id='update_employeement_date_range',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": python_callable.get_originalhiredate(dag_run),
                    "endDate": python_callable.get_termination_date(dag_run)['terminationdate']
                }
            }
        )

        get_user_time_off_type_policy_summary_11 = rail.RepliconServiceOperator(
            task_id='get_user_time_off_type_policy_summary_11',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts_timeOff_balance_eventscript = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_balance_eventscript',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
        )

        get_all_scripts_timeOff_validation_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
        )

        variable_trigger_child_dag_ids = rail.SetVariableOperator(
            task_id='variable_trigger_child_dag_ids',
            append=False,
            name='trigger_child_dag_ids',
            value=[]
        )

        foreach_timeofftype_policies = rail.ForEachOperator(
            task_id='foreach_timeofftype_policies',
            items="{{result('get_user_time_off_type_policy_summary_11').policiesByTimeOffType | to_json}}",
            start_task='if_timeoff_type_allowed',
            end_task='foreach_timeofftype_policies_end'
        )

        if_timeoff_type_allowed = rail.IfOperator(
            task_id='if_timeoff_type_allowed',
            test="{{result('foreach_timeofftype_policies').isTimeOffAllowedAgainstThisTimeOffType | is_truthy}}",
            yes_task="get_balance_summary_for_account",
            no_task="foreach_timeofftype_policies_end"
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('foreach_timeofftype_policies')['timeOffType']['uri']
                },
                "asOfDate": python_callable.get_termination_date(dag_run)['terminationdate']
            }
        )

        if_discription_present = rail.IfOperator(
            task_id='if_discription_present',
            test="{{result('foreach_timeofftype_policies').policySetSchedule[0].description | is_truthy}}",
            yes_task="trigger_imperva_put_remaining_balance_for_payout",
            no_task="foreach_timeofftype_policies_end"
        )

        trigger_imperva_put_remaining_balance_for_payout = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_put_remaining_balance_for_payout',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_put_remaining_balance_for_payout,
            conf=lambda dag_run: {
                "timeoffuri":rail.result('foreach_timeofftype_policies')['timeOffType']['uri'],
                "useruri":dag_run.conf['useruri'],
                "terminationdate":python_callable.get_termination_date(dag_run)['m_d_y'],
                "startingbalancesettouri":rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_scripts_timeOff_balance_eventscript'),
                    "displayText", "Starting Balance Set To", "uri"),
                "preventbalanceoverdrawuri":rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_scripts_timeOff_validation_script'),
                    "displayText", "Prevent balance overdraw", "uri"),
                "balance":rail.result('get_balance_summary_for_account')['timeRemaining'] \
                    if rail.result('get_balance_summary_for_account')['timeRemaining'] else 0,
                "user_sync_log": dag_run.conf['user_sync_log'],
                "supervisor_sync_log": dag_run.conf['supervisor_sync_log'],
                "parentjobid": dag_run.conf['parentjobid'],
                "Username": dag_run.conf['Username'],
                "Employee_ID": dag_run.conf['Employee_ID'],
                "Work_Address_Country": dag_run.conf['Work_Address_Country'],
            }
        )

        insert_to_trigger_child_dag_ids = rail.SetVariableOperator(
            task_id='insert_to_trigger_child_dag_ids',
            append=True,
            name='{{ result("variable_trigger_child_dag_ids").name }}',
            value="{{result('trigger_imperva_put_remaining_balance_for_payout')}}"
        )

        foreach_timeofftype_policies_end = rail.EmptyOperator(
            task_id='foreach_timeofftype_policies_end'
        )

        get_variable_trigger_child_dag_ids = rail.GetVariableOperator(
            task_id='get_variable_trigger_child_dag_ids',
            name='{{ result("variable_trigger_child_dag_ids").name }}'
        )

        wait_for_variable_trigger_child_dag_ids = rail.WaitForDagRunsSensor(
            task_id='wait_for_variable_trigger_child_dag_ids',
            dag_runs='{{ result("get_variable_trigger_child_dag_ids").value | to_json }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        imperva_user_import_logs_add_entry_22 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_22',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Success",
                "reason": "User profile disabled successfully",
                "action": "Disable User",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        imperva_user_import_logs_add_entry_24 = rail.WriteLogOperator(
            task_id='imperva_user_import_logs_add_entry_24',
            message="na",
            log= "{{dag_run.conf.user_sync_log}}",
            severity="Success",
            properties=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "childjobid": get_dagrun_ecid(dag_run),
                "loginname": dag_run.conf['Username'],
                "employeeid": dag_run.conf['Employee_ID'],
                "status": "Warning",
                "reason": "User Disabled, but end and time off policies not adjusted in the termination date is blank.",
                "action": "Disable User",
                "country": dag_run.conf['Work_Address_Country']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.user_sync_log}}",
            message="Error | {{ get_error_message() }}",
            severity="Error",
            properties= {
                "parentjobid": "{{dag_run.conf.parentjobid}}",
                "childjobid": "{{ dag_run_ecid() }}",
                "loginname": "{{dag_run.conf.Username}}",
                "employeeid": "{{dag_run.conf.Employee_ID}}",
                "status": "Error",
                "reason": "{{get_error_message()}}",
                "action": "Disable user",
                "country": "{{dag_run.conf.Work_Address_Country}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        if_username_equal_to_tempadmin >> rail.Label("Yes") >> imperva_user_import_logs_add_entry_4 >> catch_and_log_errors
        if_username_equal_to_tempadmin >> rail.Label("No") >> disable_login >> if_termination_date_present >> rail.Label("Yes") >> \
        update_employeement_date_range >> get_user_time_off_type_policy_summary_11 >> get_all_scripts_timeOff_balance_eventscript >> \
        get_all_scripts_timeOff_validation_script >> variable_trigger_child_dag_ids >> foreach_timeofftype_policies >> if_timeoff_type_allowed >> rail.Label(
            "Yes") >> get_balance_summary_for_account >> if_discription_present >> rail.Label(
            "Yes") >> trigger_imperva_put_remaining_balance_for_payout >> \
        insert_to_trigger_child_dag_ids >> foreach_timeofftype_policies_end
        if_discription_present >> rail.Label("No") >> foreach_timeofftype_policies_end
        if_timeoff_type_allowed >> rail.Label("No") >> foreach_timeofftype_policies_end
        foreach_timeofftype_policies >> foreach_timeofftype_policies_end >> get_variable_trigger_child_dag_ids >> wait_for_variable_trigger_child_dag_ids >>\
        imperva_user_import_logs_add_entry_22 >> catch_and_log_errors
        if_termination_date_present >> rail.Label("No") >> imperva_user_import_logs_add_entry_24 >> catch_and_log_errors
        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
