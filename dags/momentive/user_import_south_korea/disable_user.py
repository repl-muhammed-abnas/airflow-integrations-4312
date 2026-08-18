from datetime import timedelta
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid
from momentive.user_import_south_korea.utils import request_payload, python_callable

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'momentive_userimport_disable_user_child_{config.instance}',
        description=f'momentive_userimport_disable_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.disable_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_actual_user_identity'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_actual_user_identity',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_actual_user_identity = rail.RepliconServiceOperator(
            task_id='get_actual_user_identity',
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity",
            data_handler=lambda response : response['loginName']
        )

        if_userid_equals_identity_loginname = rail.IfOperator(
            task_id='if_userid_equals_identity_loginname',
            test='''{{ dag_run.conf.userid == result("get_actual_user_identity") }}''',
            yes_task="catch_and_log_error",
            no_task="if_active_equals_0",
        )

        if_active_equals_0 = rail.IfOperator(
            task_id='if_active_equals_0',
            test='''{{ dag_run.conf.active == '0' }}''',
            yes_task="validate_termination_date",
            no_task="if_termination_date_present",
        )

        validate_termination_date = rail.PythonOperator(
            task_id = "validate_termination_date",
            python_callable=python_callable.validate_terminationdate
        )

        check_termination_date = rail.IfOperator(
            task_id='check_termination_date',
            test=lambda: bool(rail.result('validate_termination_date')),
            yes_task="disable_login",
            no_task="if_termination_date_present",
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        if_termination_date_present = rail.IfOperator(
            task_id='if_termination_date_present',
            test=lambda dag_run: bool(dag_run.conf['terminationdate']),
            yes_task="update_employment_daterange_for_enddate",
            no_task="log_user_import",
        )

        update_employment_daterange_for_enddate = rail.RepliconServiceOperator(
            task_id='update_employment_daterange_for_enddate',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data = request_payload.update_emp_date_for_disableuser
        )

        get_user_timeofftype_policysummary = rail.RepliconServiceOperator(
            task_id='get_user_timeofftype_policysummary',
            endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_scripts = rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_all_scriptsfor_time_off_validation_script_administration_service1 = rail.RepliconServiceOperator(
            task_id='get_all_scriptsfor_time_off_validation_script_administration_service1',
            endpoint="/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts"
        )

        foreach_policiesby_timeofftype = rail.ForEachOperator(
            task_id='foreach_policiesby_timeofftype',
            items="{{ result('get_user_timeofftype_policysummary').policiesByTimeOffType }}",
            start_task='is_isTimeOffAllowedAgainstThisTimeOffType_true',
            end_task='foreach_policiesby_timeofftype_end'
        )

        is_isTimeOffAllowedAgainstThisTimeOffType_true = rail.IfOperator(
            task_id='is_isTimeOffAllowedAgainstThisTimeOffType_true',
            test="{{ result('foreach_policiesby_timeofftype').isTimeOffAllowedAgainstThisTimeOffType | is_truthy }}",
            yes_task="get_balance_summary_foraccount",
            no_task="foreach_policiesby_timeofftype_end",
        )

        get_balance_summary_foraccount = rail.RepliconServiceOperator(
            task_id='get_balance_summary_foraccount',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=request_payload.get_balancesummary_foraccount,
            data_handler=lambda response: float(response['timeRemaining']) if response.get(
                'timeRemaining', '') else 0
        )

        if_description_is_present = rail.IfOperator(
            task_id='if_description_is_present',
            test="{{ result('foreach_policiesby_timeofftype').policySetSchedule | is_truthy and \
                result('foreach_policiesby_timeofftype').policySetSchedule[0].description | is_truthy}}",
            yes_task="if_timeofftype_contains_annual_leave",
            no_task="foreach_policiesby_timeofftype_end",
        )

        if_timeofftype_contains_annual_leave = rail.IfOperator(
            task_id='if_timeofftype_contains_annual_leave',
            test=lambda : bool(rail.result('foreach_policiesby_timeofftype')) and (
                'annual leave' in rail.result('foreach_policiesby_timeofftype')['timeOffType']['displayText'].lower()),
            yes_task="put_remaining_balance_for_payout",
            no_task="put_remaining_balance_for_payout_as_0",
        )

        put_remaining_balance_for_payout = rail.TriggerDagRunOperator(
            task_id='put_remaining_balance_for_payout',
            trigger_dag_id=f'momentive_userimport_put_0_balance_for_payout_child_{config.instance}',
            conf=request_payload.put_remainig_balance_for_payout_parameter_annual,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_put_remaining_balance_for_payout = rail.WaitForDagRunsSensor(
            task_id='wait_for_put_remaining_balance_for_payout',
            dag_runs='{{ result("put_remaining_balance_for_payout") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        put_remaining_balance_for_payout_as_0 = rail.TriggerDagRunOperator(
            task_id='put_remaining_balance_for_payout_as_0',
            trigger_dag_id=f'momentive_userimport_put_0_balance_for_payout_child_{config.instance}',
            conf=request_payload.put_remainig_balance_for_payout_parameter_0,
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        wait_for_put_remaining_balance_for_payout_as_0 = rail.WaitForDagRunsSensor(
            task_id='wait_for_put_remaining_balance_for_payout_as_0',
            dag_runs='{{ result("put_remaining_balance_for_payout_as_0") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        foreach_policiesby_timeofftype_end = rail.EmptyOperator(
            task_id='foreach_policiesby_timeofftype_end'
        )

        log_user_import_with_enddate = rail.WriteLogOperator(
            task_id="log_user_import_with_enddate",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            severity="Success",
            properties=request_payload.log_user_disable_payload
        )

        log_user_import = rail.WriteLogOperator(
            task_id="log_user_import",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            severity="Success",
            properties=request_payload.log_user_disable_payload
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            trigger_rule='one_failed',
            message="Error",
            severity="Error",
            properties={
                "userid": "{{ dag_run.conf.userid }}",
                "username": "{{ dag_run.conf.firstname }}" + " " + "{{ dag_run.conf.lastname }}",
                "action": "Disable user",
                "status": "Error",
                'details': "{{ get_error_message() }}",
                'country':''
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_actual_user_identity

        get_actual_user_identity >> if_userid_equals_identity_loginname

        if_userid_equals_identity_loginname >> rail.Label('Yes') >> catch_and_log_error
        if_userid_equals_identity_loginname >> rail.Label('No') >> if_active_equals_0

        if_active_equals_0 >> rail.Label('Yes') >> validate_termination_date >> check_termination_date
        if_active_equals_0 >> rail.Label('No') >> if_termination_date_present

        check_termination_date >> rail.Label('Yes') >> disable_login >> if_termination_date_present
        check_termination_date >> rail.Label('No') >> if_termination_date_present

        if_termination_date_present >> rail.Label('Yes') >> update_employment_daterange_for_enddate >> get_user_timeofftype_policysummary >> \
        get_all_scripts >> get_all_scriptsfor_time_off_validation_script_administration_service1 >> \
            foreach_policiesby_timeofftype >> is_isTimeOffAllowedAgainstThisTimeOffType_true
        if_termination_date_present >> rail.Label('No') >> log_user_import >> catch_and_log_error

        is_isTimeOffAllowedAgainstThisTimeOffType_true >> rail.Label('Yes') >> get_balance_summary_foraccount >> if_description_is_present
        is_isTimeOffAllowedAgainstThisTimeOffType_true >> rail.Label('No') >> foreach_policiesby_timeofftype_end

        if_description_is_present >> rail.Label('Yes') >> if_timeofftype_contains_annual_leave
        if_description_is_present >> rail.Label('No') >> foreach_policiesby_timeofftype_end

        if_timeofftype_contains_annual_leave >> rail.Label('Yes') >> put_remaining_balance_for_payout >> wait_for_put_remaining_balance_for_payout >> \
            foreach_policiesby_timeofftype_end
        if_timeofftype_contains_annual_leave >> rail.Label('No') >> put_remaining_balance_for_payout_as_0 >> wait_for_put_remaining_balance_for_payout_as_0 >> \
            foreach_policiesby_timeofftype_end

        foreach_policiesby_timeofftype_end >> log_user_import_with_enddate >> catch_and_log_error

        catch_and_log_error >> log_to_sumo


    return dag

rail.for_each_instance(create_dag)
