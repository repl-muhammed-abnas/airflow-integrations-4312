from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_workflow_timeoff_type_for_disable_user_dag_id,
        description=f'Assured Partners User Import Workflow for Timeoff Type for disabled users Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_get_pto1_timeoff_type_names_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_get_pto1_timeoff_type_names_list',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_get_pto1_timeoff_type_names_list = rail.PythonOperator(
            task_id='log_get_pto1_timeoff_type_names_list',
            python_callable=lambda: list(
                map(lambda x: x['time_off_type_name'], config.TO_PTO1_MAPPER))
        )

        log_timeoff_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='log_timeoff_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        log_starting_balance_script_from_timeoffbalanceeventscripts = rail.RepliconServiceOperator(
            task_id='log_starting_balance_script_from_timeoffbalanceeventscripts',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri')
        )

        dag_run_wait_list = rail.SetVariableOperator(
            task_id='dag_run_wait_list',
            name='wait_list',
            append=False,
            value=[]
        )

        foreach_timeoff_type_assignments_for_user = rail.ForEachOperator(
            task_id='foreach_timeoff_type_assignments_for_user',
            items=lambda: rail.result('log_timeoff_type_assignments_for_user'),
            start_task='if_timeoff_type_name_not_equals_sick_pay_p',
            end_task='foreach_timeoff_type_assignments_for_user_end'
        )

        if_timeoff_type_name_not_equals_sick_pay_p = rail.IfOperator(
            task_id='if_timeoff_type_name_not_equals_sick_pay_p',
            test=lambda: rail.result('foreach_timeoff_type_assignments_for_user')[
                'displayText'] != "Sick Pay-P",
            yes_task="if_timeoff_type_name_in_pto1_timeoff_names_list",
            no_task="foreach_timeoff_type_assignments_for_user_end",
        )

        if_timeoff_type_name_in_pto1_timeoff_names_list = rail.IfOperator(
            task_id='if_timeoff_type_name_in_pto1_timeoff_names_list',
            test=lambda: rail.result('foreach_timeoff_type_assignments_for_user')[
                'name'] in rail.result('log_get_pto1_timeoff_type_names_list'),
            yes_task="log_get_user_timeofftype_balance_summary",
            no_task="foreach_timeoff_type_assignments_for_user_end",
        )

        log_get_user_timeofftype_balance_summary = rail.RepliconServiceOperator(
            task_id='log_get_user_timeofftype_balance_summary',
            endpoint="/services/TimeOffService1.svc/GetUserTimeOffTypeBalanceSummary",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUri": rail.result('foreach_timeoff_type_assignments_for_user')['uri'],
                "asOfDate": python_callable.get_split_date(dag_run.conf['TerminationDate'], 'int')
            }
        )

        log_balance_minutes_and_seconds_converted_to_balance_hours = rail.PythonOperator(
            task_id='log_balance_minutes_and_seconds_converted_to_balance_hours',
            python_callable=lambda: {
                'minutes_to_hours': ((float(rail.result('log_get_user_timeofftype_balance_summary')['timeRemaining']['calendarDayDuration']['minutes']) / 60) if int(rail.result('log_get_user_timeofftype_balance_summary')['timeRemaining']['calendarDayDuration']['minutes']) > 0 else 0) if rail.result('log_get_user_timeofftype_balance_summary') else 0,
                'seconds_to_hours': ((float(rail.result('log_get_user_timeofftype_balance_summary')['timeRemaining']['calendarDayDuration']['seconds']) / 3600) if int(rail.result('log_get_user_timeofftype_balance_summary')['timeRemaining']['calendarDayDuration']['seconds']) > 0 else 0) if rail.result('log_get_user_timeofftype_balance_summary') else 0
            }
        )

        trigger_dag_run_assured_partners_child_remove_future_timeoff_policies_transfer_termination = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_assured_partners_child_remove_future_timeoff_policies_transfer_termination',
            retries=0,
            trigger_dag_id=config.child_remove_future_timeoff_policies_transfer_termination_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "parentjobid": dag_run.conf['parentjobid'],
                "useruri":  dag_run.conf['useruri'],
                "employeenumber":  dag_run.conf['EmplID_Login'],
                "firstname":  dag_run.conf['FirstName'],
                "lastname":  dag_run.conf['LastName'],
                "startdate":  dag_run.conf['ServiceDate'],
                "timeoffuri": rail.result('foreach_timeoff_type_assignments_for_user')['uri'],
                "timeofftypename":  rail.result('foreach_timeoff_type_assignments_for_user')['name'],
                "schedulename":  dag_run.conf['Schedule'],
                "type": "terminate",
                "previousstartdate": dag_run.conf['previousstartdate'],
                "previousbalance": (float(rail.result('log_get_user_timeofftype_balance_summary')['timeRemaining']['calendarDayDuration']['hours']) + rail.result(
                    'log_balance_minutes_and_seconds_converted_to_balance_hours')['minutes_to_hours'] + rail.result('log_balance_minutes_and_seconds_converted_to_balance_hours')['seconds_to_hours']) if rail.result(
                    'log_get_user_timeofftype_balance_summary') else 0,
                "enddate": dag_run.conf['TerminationDate'],
                "starting_balance_set_to_uri": rail.result('log_starting_balance_script_from_timeoffbalanceeventscripts'),
                "previousptoname": rail.result('log_get_user_timeofftype_balance_summary')['timeOffType']['name'],
                "pto_1":  dag_run.conf['PTO_1'],
                "estatus":  dag_run.conf['EEStatus'],
                "illness":  dag_run.conf['Illness'],
                "integration_run_date": dag_run.conf['integration_run_date'],
                "ChangeEffectiveDate":  dag_run.conf['ChangeEffectiveDate']
            }
        )

        add_dag_run_to_wait_list = rail.SetVariableOperator(
            task_id='add_dag_run_to_wait_list',
            name='wait_list',
            append=True,
            value="{{result('trigger_dag_run_assured_partners_child_remove_future_timeoff_policies_transfer_termination')}}"
        )

        foreach_timeoff_type_assignments_for_user_end = rail.EmptyOperator(
            task_id='foreach_timeoff_type_assignments_for_user_end'
        )

        child_dag_ids = rail.PythonOperator(
            task_id='child_dag_ids',
            python_callable=lambda: [
                int(item) for item in rail.get_dag_run_var('wait_list')] if rail.get_dag_run_var('wait_list') else []
        )

        wait_for_completion_trigger_assured_partners_child_remove_future_timeoff_policies_transfer_termination = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_assured_partners_child_remove_future_timeoff_policies_transfer_termination',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{result('child_dag_ids') | to_json}}"
        )

        gather_response_from_dag_runs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_response_from_dag_runs',
            dag_runs="{{result('child_dag_ids') | to_json}}",
            dagrun_task_id='final_response_from_dag',
            execution_timeout=timedelta(
                hours=config.gather_response_from_dag_runs_timeout_hours),
            flatten=True
        )

        catch_and_log_error = rail.PythonOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            python_callable=lambda: rail.render_template(
                "Error in Workflow Timeoff Type for Disabled User : {{get_error_message()}}")
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.result(
                "catch_and_log_error") or rail.result("gather_response_from_dag_runs")
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> log_get_pto1_timeoff_type_names_list

        log_get_pto1_timeoff_type_names_list >> log_timeoff_type_assignments_for_user >> log_starting_balance_script_from_timeoffbalanceeventscripts \
            >> dag_run_wait_list >> foreach_timeoff_type_assignments_for_user

        foreach_timeoff_type_assignments_for_user >> if_timeoff_type_name_not_equals_sick_pay_p

        if_timeoff_type_name_not_equals_sick_pay_p >> rail.Label(
            'No') >> foreach_timeoff_type_assignments_for_user_end
        if_timeoff_type_name_not_equals_sick_pay_p >> rail.Label(
            'Yes') >> if_timeoff_type_name_in_pto1_timeoff_names_list

        if_timeoff_type_name_in_pto1_timeoff_names_list >> rail.Label(
            'No') >> foreach_timeoff_type_assignments_for_user_end
        if_timeoff_type_name_in_pto1_timeoff_names_list >> rail.Label('Yes') >> log_get_user_timeofftype_balance_summary \
            >> log_balance_minutes_and_seconds_converted_to_balance_hours >> trigger_dag_run_assured_partners_child_remove_future_timeoff_policies_transfer_termination \
            >> add_dag_run_to_wait_list >> foreach_timeoff_type_assignments_for_user_end

        foreach_timeoff_type_assignments_for_user >> foreach_timeoff_type_assignments_for_user_end >> child_dag_ids >> wait_for_completion_trigger_assured_partners_child_remove_future_timeoff_policies_transfer_termination \
            >> gather_response_from_dag_runs >> catch_and_log_error

        catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
