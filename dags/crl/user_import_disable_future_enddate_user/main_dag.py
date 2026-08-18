from datetime import timedelta
from dateutil.parser import parse
from pendulum import datetime
import rail

DATE_FORMAT = "%m/%d/%Y"


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.disable_user_master_dagid,
        description='CRL User Import Disable User Master',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.pacific_timezone),
        schedule_interval=config.disable_user_master_dag_interval,
        max_active_runs=config.disable_user_master_dag_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config',
            extra_config=config
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report_generation',
            report_params={
                'reportParameters': [
                    {
                        'reportUri': "{{ result('get_report_details').uri }}",
                        'filterValues': [],
                        'outputFormatUri': 'urn:replicon:report-output-format-option:csv'
                    }
                ]
            }
        )

        is_report_failed = rail.IfOperator(
            task_id='is_report_failed',
            test="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].error | is_truthy }}",
            yes_task='fail_report_generation',
            no_task='report_has_data'
        )

        fail_report_generation = rail.FailOperator(
            task_id='fail_report_generation',
            message="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].error }}"
        )

        report_has_data = rail.IfOperator(
            task_id='report_has_data',
            test="{{ result('run_report_generation.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data'
        )

        fail_no_report_data = rail.FailOperator(
            task_id='fail_no_report_data',
            message='No Data for Contractors'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report_generation.get_report_result').reportGenerationResults[0].payload }}"
        )

        report_data_collection = rail.CreateCollectionOperator(
            task_id='report_data_collection',
            source="{{ result('load_report_data') }}",
            name='userdata',
            columns={
                'User Name': 'user',
                'User Status': 'status',
                'User Uri': 'uri',
                'User End Date': 'enddate',
                'Day Diff': 'daydiff',
                'Employee Type':'employee_type',
                'Location Full Path': 'location_full_path',
                'Employee Status': 'emp_status'
            }
        )

        query_users_to_disable = rail.QueryCollectionOperator(
            task_id='query_users_to_disable',
            query=f"""SELECT * FROM userdata WHERE NULLIF(enddate,"") IS NOT NULL
                    AND daydiff < 0 AND status = 'Enabled'  and user !='{config.INTEGRATION_USERNAME}'"""
        )

        users_to_disable = rail.IfOperator(
            task_id='users_to_disable',
            test="{{ result('query_users_to_disable', 'length') > 0 }}",
            yes_task='get_timeoff_balance_event_script_uri',
            no_task='finish'
        )

        get_timeoff_balance_event_script_uri = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_event_script_uri',
            endpoint="/services/TimeOffBalanceEventScriptAdministrationService1.svc/GetActiveScripts",
            data_handler=lambda response: {
                "starting_balance_script_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Starting Balance Set To', 'uri')
            }
        )

        get_timeoff_balance_validation_script = rail.RepliconServiceOperator(
            task_id='get_timeoff_balance_validation_script',
            endpoint='/services/TimeOffValidationScriptAdministrationService1.svc/GetAllScripts',
            data_handler=lambda response:{
                 "prevent_balance_overdraw_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Prevent balance overdraw', 'uri')
            }
        )

        get_all_policy_sets = rail.RepliconServiceOperator(
            task_id="get_all_policy_sets",
            endpoint="/services/PolicySetService1.svc/GetAllPolicySets",
            data_handler=lambda response: {
                'timeoff': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', 'Time Off', 'uri', ''),
            }
        )

        disable_user_child = rail.TriggerDagRunForEachItemOperator(
            task_id='disable_user_child',
            retries=0,
            items="{{ result('query_users_to_disable') }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.disable_future_enddate_user_child_dagid,
            conf=lambda item: {
                'user': item['user'],
                'useruri': item['uri'],
                'end_date': parse(item['enddate']).strftime(DATE_FORMAT),
                'location': item['location_full_path'].split(' / ')[0] if item['location_full_path'] else None,
                'employee_type': item['employee_type'] if item['employee_type'] else None,
                'action': 'disable',
                'starting_balance_script_uri': rail.result('get_timeoff_balance_event_script_uri')['starting_balance_script_uri'],
                'prevent_balance_overdraw_uri': rail.result('get_timeoff_balance_validation_script')['prevent_balance_overdraw_uri'],
                'emp_status': item['emp_status'],
                'timeoff_policy_set_uri': rail.result('get_all_policy_sets')['timeoff']
            }
        )

        wait_for_disable_user_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_user_child',
            dag_runs='{{ result("disable_user_child") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_disable_user_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disable_user_errors',
            dag_runs="{{ result('disable_user_child') }}",
            dagrun_task_id='catch_disable_user_error',
            flatten=True
        )

        is_disable_user_error = rail.IfOperator(
            task_id='is_disable_user_error',
            test="{{ result('gather_disable_user_errors') | map_to_attr('useruri') | length > 0 }}",
            yes_task='fail_disable_user_error',
            no_task='finish'
        )

        fail_disable_user_error = rail.FailOperator(
            task_id='fail_disable_user_error',
            message='Errors noticed while disabling few users'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_report_details >> run_report_group_entry
        run_report_group_exit >> is_report_failed

        is_report_failed >> rail.Label(
            'Yes') >> fail_report_generation

        is_report_failed >> rail.Label(
            'No') >> report_has_data

        report_has_data >> rail.Label(
            'Yes') >> load_report_data >> report_data_collection >> query_users_to_disable >> users_to_disable

        users_to_disable >> rail.Label(
            'Yes') >> get_timeoff_balance_event_script_uri >> get_timeoff_balance_validation_script

        get_timeoff_balance_validation_script >> get_all_policy_sets >> disable_user_child
        
        disable_user_child >> wait_for_disable_user_child >> gather_disable_user_errors

        gather_disable_user_errors >> is_disable_user_error

        is_disable_user_error >> rail.Label(
            'Yes') >> fail_disable_user_error

        is_disable_user_error >> rail.Label(
            'No') >> finish

        users_to_disable >> rail.Label(
            'No') >> finish

        report_has_data >> rail.Label(
            'No') >> fail_no_report_data
    return dag


rail.for_each_instance(create_main_dag)
