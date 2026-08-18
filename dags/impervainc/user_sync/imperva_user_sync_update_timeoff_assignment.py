
from datetime import timedelta
from pendulum import now
import rail
from impervainc.user_sync.utils import python_callable, response_filter, request_payload

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.imperva_user_sync_update_timeoff_assignment,
        description=f'impervainc user sync update timeoff assignment child dag {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        get_time_off_type_assignments_for_user = rail.RepliconServiceOperator(
            task_id='get_time_off_type_assignments_for_user',
            endpoint="/services/TimeOffService1.svc/GetTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        get_all_timeoff_types = rail.RepliconServiceOperator(
            task_id="get_all_timeoff_types",
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
        )

        get_all_timeoff_uris = rail.PythonOperator(
            task_id='get_all_timeoff_uris',
            python_callable=lambda: [rec['uri'] for rec in rail.result('get_all_timeoff_types')]
        )

        get_bulktimeoff_details = rail.RepliconServiceOperator(
            task_id='get_bulktimeoff_details',
            endpoint='/services/TimeOffService1.svc/BulkGetTimeOffTypeDetails',
            data=lambda: {
                "timeOffTypeUris": rail.result('get_all_timeoff_uris')
            }
        )

        variable_previous_balance_data = rail.SetVariableOperator(
            task_id='variable_previous_balance_data',
            append=False,
            name='timeoffnameswithpreviousbalance',
            value=[]
        )

        for_each_time_off_type_assignments = rail.ForEachOperator(
            task_id='for_each_time_off_type_assignments',
            items="{{ result('get_time_off_type_assignments_for_user') | to_json }}",
            start_task = 'get_balance_summary_for_account',
            end_task = 'for_each_time_off_type_assignments_end'
        )

        get_balance_summary_for_account = rail.RepliconServiceOperator(
            task_id='get_balance_summary_for_account',
            endpoint="/services/TimeOffService2.svc/GetBalanceSummaryForAccount",
            data=lambda dag_run: {
                "account": {
                    "userUri": dag_run.conf['useruri'],
                    "timeOffTypeUri": rail.result('for_each_time_off_type_assignments')['uri']
                },
                "asOfDate": {
                    "year":now().year,
                    "month":now().month,
                    "day":now().day
                }
            }
        )

        add_items_to_variable_data= rail.SetVariableOperator(
            task_id='add_items_to_variable_data',
            append=True,
            name='{{ result("variable_previous_balance_data").name }}',
            value=response_filter.add_variable_to_list
        )

        for_each_time_off_type_assignments_end = rail.EmptyOperator(
            task_id = 'for_each_time_off_type_assignments_end'
        )

        get_variable_list_data = rail.GetVariableOperator(
            task_id = 'get_variable_list_data',
            name= '{{ result("variable_previous_balance_data").name }}'
        )

        create_timeoff_names_with_uri = rail.PythonOperator(
            task_id='create_timeoff_names_with_uri',
            python_callable=lambda dag_run: python_callable.create_timeoff_names_with_uri(
                rail.result('get_bulktimeoff_details'),
                rail.result('get_all_timeoff_types'),
                dag_run.conf['Country_ISO_Code']
            )
        )

        assign_timeofftypes = rail.RepliconServiceOperator(
            task_id='assign_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "timeOffTypeUris": rail.result('create_timeoff_names_with_uri')['finaluris']
            }
        )

        get_all_scripts_timeOff_balance_eventscript = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_balance_eventscript',
            endpoint='/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Starting Balance Set To', 'uri', '')
        )

        get_all_scripts_timeOff_validation_script = rail.RepliconServiceOperator(
            task_id='get_all_scripts_timeOff_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Prevent balance overdraw', 'uri', '')
        )

        # pylint: disable=unnecessary-lambda
        trigger_timeoff_names_with_uris = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_timeoff_names_with_uris',
            retries=0,
            items='{{ result("create_timeoff_names_with_uri").nameswithuri | to_json }}',
            trigger_dag_id=config.imperva_user_sync_update_rehire_time_off_type_child,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run: request_payload.get_timeoffnameswithuri_payload(item, dag_run)
        )

        wait_for_timeoff_names_with_uris = rail.WaitForDagRunsSensor(
            task_id='wait_for_timeoff_names_with_uris',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_timeoff_names_with_uris") }}'
        )

        for_each_timeoff_names_with_previousbalance = rail.ForEachOperator(
            task_id='for_each_timeoff_names_with_previousbalance',
            items="{{ result('get_variable_list_data').value | to_json }}",
            start_task = 'get_timeoff_previously_assigned',
            end_task = 'for_each_timeoff_names_with_previousbalance_end'
        )

        get_timeoff_previously_assigned = rail.PythonOperator(
            task_id='get_timeoff_previously_assigned',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('create_timeoff_names_with_uri')['nameswithuri'],
                "uri", rail.result('for_each_timeoff_names_with_previousbalance')['uri'], "uri")
        )

        if_rehire_update_equals = rail.IfOperator(
            task_id='if_rehire_update_equals',
            test=lambda dag_run: bool(rail.result('get_timeoff_previously_assigned') and dag_run.conf['rehire_update'] == 'update'),
            yes_task="trigger_imperva_put_remaining_balance_for_payout",
            no_task="for_each_timeoff_names_with_previousbalance_end"
        )

        trigger_imperva_put_remaining_balance_for_payout = rail.TriggerDagRunOperator(
            task_id='trigger_imperva_put_remaining_balance_for_payout',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.imperva_put_remaining_balance_for_payout,
            conf=lambda dag_run: {
                "timeoffuri":rail.result('for_each_timeoff_names_with_previousbalance')['uri'],
                "useruri":dag_run.conf['useruri'],
                "terminationdate":now().strftime("%m/%d/%Y"),
                "startingbalancesettouri":rail.result('get_all_scripts_timeOff_balance_eventscript'),
                "preventbalanceoverdrawuri":rail.result('get_all_scripts_timeOff_validation_script'),
                "balance":rail.result('for_each_timeoff_names_with_previousbalance')['balance'] \
                    if rail.result('for_each_timeoff_names_with_previousbalance')['balance'] else 0,
                "user_sync_log": dag_run.conf['user_sync_log'],
                "supervisor_sync_log": dag_run.conf['supervisor_sync_log'],
                "parentjobid": dag_run.conf['parentjobid'],
                "Username": dag_run.conf['Username'],
                "Employee_ID": dag_run.conf['Employee_ID'],
                "Work_Address_Country": dag_run.conf['Work_Address_Country'],
            }
        )

        wait_for_imperva_put_remaining_balance_for_payout = rail.WaitForDagRunsSensor(
            task_id='wait_for_imperva_put_remaining_balance_for_payout',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_imperva_put_remaining_balance_for_payout") }}'
        )

        for_each_timeoff_names_with_previousbalance_end = rail.EmptyOperator(
            task_id = 'for_each_timeoff_names_with_previousbalance_end'
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        get_time_off_type_assignments_for_user >> get_all_timeoff_types >> get_all_timeoff_uris >> get_bulktimeoff_details >> \
        variable_previous_balance_data >> for_each_time_off_type_assignments >> get_balance_summary_for_account >> \
        add_items_to_variable_data >> for_each_time_off_type_assignments_end
        for_each_time_off_type_assignments >> for_each_time_off_type_assignments_end >> get_variable_list_data >> create_timeoff_names_with_uri >> \
        assign_timeofftypes >> get_all_scripts_timeOff_balance_eventscript >> get_all_scripts_timeOff_validation_script >> \
        trigger_timeoff_names_with_uris >> wait_for_timeoff_names_with_uris >> for_each_timeoff_names_with_previousbalance >> \
        get_timeoff_previously_assigned >> if_rehire_update_equals >> rail.Label("Yes") >> trigger_imperva_put_remaining_balance_for_payout >> \
        wait_for_imperva_put_remaining_balance_for_payout >> for_each_timeoff_names_with_previousbalance_end
        if_rehire_update_equals >> rail.Label("Yes") >> for_each_timeoff_names_with_previousbalance_end
        for_each_timeoff_names_with_previousbalance >> for_each_timeoff_names_with_previousbalance_end >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
