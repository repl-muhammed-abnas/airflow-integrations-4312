from datetime import timedelta
from airflow.models import Variable
import rail
from assuredpartnersinc.user_import_v4.utils import python_callable, request_payload

null = None


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_loa_logic_dag_id,
        description=f'Assured Partners User Import LOA Logic child{config.instance}',
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
            no_task='response_from_dag_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='response_from_dag_variable',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        response_from_dag_variable = rail.SetVariableOperator(
            task_id="response_from_dag_variable",
            name='response_from_dag',
            append=False,
            value=""
        )

        if_request_timesheettemplate_equals_to_no_5 = rail.IfOperator(
            task_id='if_request_timesheettemplate_equals_to_no_5',
            test='''{{ dag_run.conf.timesheettemplate == 'no' }}''',
            yes_task="assign_no_timesheet_period_6",
            no_task="if_request_previous_ee_status_equals_to_a_9",
        )

        assign_no_timesheet_period_6 = rail.RepliconServiceOperator(
            task_id='assign_no_timesheet_period_6',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.payload_for_assigning_no_timesheet_template
        )

        set_response_from_dag_7 = rail.SetVariableOperator(
            task_id="set_response_from_dag_7",
            name='response_from_dag',
            append=False,
            value="Success"
        )

        if_request_previous_ee_status_equals_to_a_9 = rail.IfOperator(
            task_id='if_request_previous_ee_status_equals_to_a_9',
            test='''{{ dag_run.conf.previous_ee_status == 'A'  and dag_run.conf.new_ee_status == 'L' }}''',
            yes_task="if_request_currenttimesheetperiod_present_10",
            no_task="if_request_previous_ee_status_equals_to_l_17",
        )

        if_request_currenttimesheetperiod_present_10 = rail.IfOperator(
            task_id='if_request_currenttimesheetperiod_present_10',
            test='''{{ dag_run.conf.currenttimesheetperiod | is_truthy }}''',
            yes_task="assign_no_timesheet_period_11",
            no_task="get_assigned_policy_sets_for_user_12",
        )

        assign_no_timesheet_period_11 = rail.RepliconServiceOperator(
            task_id='assign_no_timesheet_period_11',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.payload_for_assigning_no_timesheet_template
        )

        get_assigned_policy_sets_for_user_12 = rail.RepliconServiceOperator(
            task_id='get_assigned_policy_sets_for_user_12',
            endpoint="/services/PolicySetService1.svc/GetAssignedPolicySetsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'policyUri', 'urn:replicon:policy:time-off', 'policySet.uri', '')
        )

        if_log_checkiftimeofftemplateisassigned_13_present_14 = rail.IfOperator(
            task_id='if_log_checkiftimeofftemplateisassigned_13_present_14',
            test='''{{ result('get_assigned_policy_sets_for_user_12') | is_truthy }}''',
            yes_task="remove_policy_set_assignment_from_user_time_offtemplate_15",
            no_task="set_response_from_dag_16",
        )

        remove_policy_set_assignment_from_user_time_offtemplate_15 = rail.RepliconServiceOperator(
            task_id='remove_policy_set_assignment_from_user_time_offtemplate_15',
            endpoint="/services/PolicySetService1.svc/RemovePolicySetAssignmentFromUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_assigned_policy_sets_for_user_12') }}"
            }
        )

        set_response_from_dag_16 = rail.SetVariableOperator(
            task_id="set_response_from_dag_16",
            name='response_from_dag',
            append=False,
            value="call time off transfer"
        )

        if_request_previous_ee_status_equals_to_l_17 = rail.IfOperator(
            task_id='if_request_previous_ee_status_equals_to_l_17',
            test='''{{ dag_run.conf.previous_ee_status == 'L'  and dag_run.conf.new_ee_status == 'A' }}''',
            yes_task="if_request_currenttimesheetperiod_blank_18",
            no_task="catch_and_log_error",
        )

        if_request_currenttimesheetperiod_blank_18 = rail.IfOperator(
            task_id='if_request_currenttimesheetperiod_blank_18',
            test='''{{ dag_run.conf.currenttimesheetperiod | is_falsy  and dag_run.conf.timesheettemplate | is_truthy }}''',
            yes_task="assign_timesheetperiod_weeklystartingon_monday_19",
            no_task="if_request_currenttimesheetperiod_present_20",
        )

        assign_timesheetperiod_weeklystartingon_monday_19 = rail.RepliconServiceOperator(
            task_id='assign_timesheetperiod_weeklystartingon_monday_19',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=lambda dag_run: {
                "user": {
                    "uri": dag_run.conf['useruri']
                },
                "modifications": {
                    "timesheetPeriodScheduleToApply": {
                        "userTimesheetPeriodScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementTimesheetPeriodSchedule": [],
                        "updateTimesheetPeriodScheduleOverDateRange": {
                            "replacementTimesheetPeriodScheduleEntries": [{
                                "timesheetPeriod": {
                                    "uri": null,
                                    "name": "Weekly starting on Sunday"
                                },
                                "effectiveDate": python_callable.get_split_date(
                                    dag_run.conf['loaend'], 'int') if dag_run.conf['loaend'] else python_callable.get_split_date(dag_run.conf['integration_run_date'], 'int')
                            }]
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        if_request_currenttimesheetperiod_present_20 = rail.IfOperator(
            task_id='if_request_currenttimesheetperiod_present_20',
            test='''{{ dag_run.conf.currenttimesheetperiod | is_truthy  and dag_run.conf.timesheettemplate | is_falsy }}''',
            yes_task="assign_no_timesheet_period_21",
            no_task="get_all_policy_sets_22",
        )

        assign_no_timesheet_period_21 = rail.RepliconServiceOperator(
            task_id='assign_no_timesheet_period_21',
            endpoint="/services/ImportService1.svc/ApplyUserModifications3",
            data=request_payload.payload_for_assigning_no_timesheet_template
        )

        get_all_policy_sets_22 = rail.RepliconServiceOperator(
            task_id='get_all_policy_sets_22',
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', dag_run.conf['timeofftemplate'], 'uri')
        )

        assign_policy_set_to_user_time_offtemplate_24 = rail.RepliconServiceOperator(
            task_id='assign_policy_set_to_user_time_offtemplate_24',
            endpoint="/services/PolicySetService1.svc/AssignPolicySetToUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "policySetUri": "{{ result('get_all_policy_sets_22') }}"
            }
        )

        set_response_from_dag_25 = rail.SetVariableOperator(
            task_id="set_response_from_dag_25",
            name='response_from_dag',
            append=False,
            value="assign policy"
        )

        catch_and_log_error = rail.SetVariableOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            name='response_from_dag',
            append=True,
            value="Error in LOA Logic dag: {{get_error_message()}}"
        )

        final_response_from_dag = rail.PythonOperator(
            task_id='final_response_from_dag',
            trigger_rule='all_done',
            python_callable=lambda: rail.get_dag_run_var("response_from_dag")
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> response_from_dag_variable

        response_from_dag_variable >> if_request_timesheettemplate_equals_to_no_5

        if_request_timesheettemplate_equals_to_no_5 >> rail.Label(
            'Yes') >> assign_no_timesheet_period_6 >> set_response_from_dag_7 >> catch_and_log_error
        if_request_timesheettemplate_equals_to_no_5 >> rail.Label(
            'No') >> if_request_previous_ee_status_equals_to_a_9

        if_request_previous_ee_status_equals_to_a_9 >> rail.Label(
            'No') >> if_request_previous_ee_status_equals_to_l_17
        if_request_previous_ee_status_equals_to_a_9 >> rail.Label(
            'Yes') >> if_request_currenttimesheetperiod_present_10

        if_request_currenttimesheetperiod_present_10 >> rail.Label(
            'Yes') >> assign_no_timesheet_period_11 >> get_assigned_policy_sets_for_user_12
        if_request_currenttimesheetperiod_present_10 >> rail.Label(
            'No') >> get_assigned_policy_sets_for_user_12

        get_assigned_policy_sets_for_user_12 >> if_log_checkiftimeofftemplateisassigned_13_present_14

        if_log_checkiftimeofftemplateisassigned_13_present_14 >> rail.Label(
            'Yes') >> remove_policy_set_assignment_from_user_time_offtemplate_15 >> set_response_from_dag_16
        if_log_checkiftimeofftemplateisassigned_13_present_14 >> rail.Label(
            'No') >> set_response_from_dag_16

        set_response_from_dag_16 >> catch_and_log_error

        if_request_previous_ee_status_equals_to_l_17 >> rail.Label(
            'No') >> catch_and_log_error
        if_request_previous_ee_status_equals_to_l_17 >> rail.Label(
            'Yes') >> if_request_currenttimesheetperiod_blank_18

        if_request_currenttimesheetperiod_blank_18 >> rail.Label(
            'Yes') >> assign_timesheetperiod_weeklystartingon_monday_19 >> if_request_currenttimesheetperiod_present_20
        if_request_currenttimesheetperiod_blank_18 >> rail.Label(
            'No') >> if_request_currenttimesheetperiod_present_20

        if_request_currenttimesheetperiod_present_20 >> rail.Label(
            'Yes') >> assign_no_timesheet_period_21 >> get_all_policy_sets_22
        if_request_currenttimesheetperiod_present_20 >> rail.Label(
            'No') >> get_all_policy_sets_22

        get_all_policy_sets_22 >> assign_policy_set_to_user_time_offtemplate_24 >> set_response_from_dag_25 >> catch_and_log_error >> final_response_from_dag

    return dag


rail.for_each_instance(create_dag)
