from datetime import timedelta
from datetime import datetime as py_datetime
import rail
from airflow.models import Variable
from pendulum import datetime


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/replicon_datastore/config.py


null = None


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"oxfordfinancial_replicon_datastore_master_dag_{config.instance}",
        description=f"Oxfordfinancial Replicon To DataStore - Final Version {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=1
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_master_data_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_master_data_details',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_master_data_details = rail.RepliconReportDetailsOperator(
            task_id='get_master_data_details',
            report_name=config.master_data_report_name
        )

        run_master_data_report = rail.run_report2(
            group_id='run_master_data_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_master_data_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        is_masterdata_report = rail.IfOperator(
            task_id='is_masterdata_report',
            test="{{ result('run_master_data_report.get_report_result', 'has_data') | is_truthy }}",
            yes_task='load_master_data_to_csv',
            no_task='dagrun_log_to_sumo'
        )

        load_master_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_master_data_to_csv",
            document='{{ result("run_master_data_report.get_report_result").reportGenerationResults[0].payload }}',
            headers=['User Name', 'Service Name', 'Service Code', 'Client Name', 'Hours Worked', 'Projects', 'Tasks',
                     'Entry Date', 'useruri', 'ServiceURI', 'ClientURI', 'Comments', 'Middle Name', 'Timesheet Period',
                     'Time Off Hrs', 'Time Off Type', 'timesheeturi']
        )

        create_master_data_collection = rail.CreateCollectionOperator(
            task_id='create_master_data_collection',
            source="{{ result('load_master_data_to_csv') }}",
            name='masterdata',
            columns={
                'User Name': 'username',
                'Service Name': 'servicename',
                'Service Code': 'servicecode',
                'Client Name': 'clientname',
                'Hours Worked': 'hoursworked',
                'Projects': 'projects',
                'Tasks': 'tasks',
                'Entry Date': 'entrydate',
                'useruri': 'useruri',
                'ServiceURI': 'serviceuri',
                'ClientURI': 'clienturi',
                'Comments': 'comments',
                'Middle Name': 'middlename',
                'Timesheet Period': 'timesheetperiod',
                'Time Off Hrs': 'timeoffhrs',
                'Time Off Type': 'timeofftype',
                'timesheeturi': 'timesheeturi'
            }
        )

        query_distinct_timesheet_uri = rail.QueryCollectionOperator(
            task_id='query_distinct_timesheet_uri',
            query="""SELECT DISTINCT timesheeturi FROM masterdata"""
        )

        get_client_data_details = rail.RepliconReportDetailsOperator(
            task_id='get_client_data_details',
            report_name=config.client_data_report_name
        )

        run_client_data_report = rail.run_report2(
            group_id='run_client_data_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_client_data_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_client_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_client_data_to_csv",
            document='{{ result("run_client_data_report.get_report_result").reportGenerationResults[0].payload }}',
            headers=['Client Name', 'Salesforce_ID',
                     'Household_Firm_ID', 'clienturi']
        )

        get_service_data_details = rail.RepliconReportDetailsOperator(
            task_id='get_service_data_details',
            report_name=config.service_data_report_name
        )

        run_service_data_report = rail.run_report2(
            group_id='run_service_data_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_service_data_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_service_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_service_data_to_csv",
            document='{{ result("run_service_data_report.get_report_result").reportGenerationResults[0].payload }}',
            headers=['Service Name', 'Salesforce_ID',
                     'Service Name2', 'serviceuri']
        )

        get_user_data_details = rail.RepliconReportDetailsOperator(
            task_id='get_user_data_details',
            report_name=config.user_data_report_name
        )

        run_user_data_report = rail.run_report2(
            group_id='run_user_data_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_user_data_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_user_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_user_data_to_csv",
            document='{{ result("run_user_data_report.get_report_result").reportGenerationResults[0].payload }}',
            headers=['User Name', 'Salesforce_ID', 'Department Groups (Current)',
                     'uri', 'Middle Name', 'Initials', 'User First Name',
                     'User Last Name']
        )

        create_entry_id_variable = rail.SetVariableOperator(
            task_id='create_entry_id_variable',
            name='entryid',
            value=lambda: int(Variable.get(
                config.time_entry_id_var_name, default_var=0))
        )

        for_each_timesheet_uri = rail.ForEachOperator(
            task_id='for_each_timesheet_uri',
            items="{{ result('query_distinct_timesheet_uri') }}",
            start_task='query_values_timesheet_uri',
            end_task='for_each_timesheet_uri_end'
        )

        query_values_timesheet_uri = rail.QueryCollectionOperator(
            task_id='query_values_timesheet_uri',
            query="""SELECT * FROM masterdata WHERE timesheeturi == :filter_timesheeturi""",
            query_params={
                'filter_timesheeturi': "{{ result('for_each_timesheet_uri').timesheeturi }}"
            }
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_timeentry_id_variable = rail.GetVariableOperator(
            task_id='get_timeentry_id_variable',
            name='entryid'
        )

        def get_process_append_time_entries():
            queried_records = rail.load_all_records(
                rail.result('query_values_timesheet_uri'))
            service_data = rail.load_all_records(
                rail.result('load_service_data_to_csv'))
            user_data = rail.load_all_records(
                rail.result('load_user_data_to_csv'))
            client_data = rail.load_all_records(
                rail.result('load_client_data_to_csv'))

            def get_userdata_fields(user_data, useruri):
                filtered_user_data = [
                    x for x in user_data if x['uri'] == useruri] if useruri else []
                return {
                    'usercurrentdepartment': rail.smartjoin_by_delim(
                        [x['Department Groups (Current)'] for x in filtered_user_data], ' ') if filtered_user_data else '',
                    'usersfid': rail.smartjoin_by_delim(
                        [x['Salesforce_ID'] for x in filtered_user_data], ' ') if filtered_user_data else '',
                    'userinitials': rail.smartjoin_by_delim(
                        [x['Initials'] for x in filtered_user_data], ' ') if filtered_user_data else '',
                    'middlename': rail.smartjoin_by_delim(
                        [x['Middle Name'] for x in filtered_user_data], ' ') if filtered_user_data else '',
                    'firstname': rail.smartjoin_by_delim(
                        [x['User First Name'] for x in filtered_user_data], ' ') if filtered_user_data else '',
                    'lastname': rail.smartjoin_by_delim(
                        [x['User Last Name'] for x in filtered_user_data], ' ') if filtered_user_data else ''
                }

            def get_service_data_fields(service_data, serviceuri):
                servicenamebasedonuri = rail.smartjoin_by_delim(
                    [x['Service Name2'] for x in service_data if x[
                        'serviceuri'] == serviceuri], ' ') if serviceuri else ''
                servicecodesfidbasedonuri = rail.smartjoin_by_delim(
                    [x['Salesforce_ID'] for x in service_data if x[
                        'serviceuri'] == serviceuri], ' ') if serviceuri else ''
                return {
                    'servicenamebasedonuri': servicenamebasedonuri,
                    'servicecodesfidbasedonuri': servicecodesfidbasedonuri,
                    'oxfordcompanyid': ('2' if 'TCO' in servicenamebasedonuri else '1') if servicenamebasedonuri else '',
                    'oxfordcompanyname': ('Trust Company Of Oxford' if 'TCO' in servicenamebasedonuri else 'Oxford Financial Group, Ltd'
                                          ) if servicenamebasedonuri else ''
                }

            def get_client_data_fields(client_data, clienturi):
                filtered_client_data = [
                    x for x in client_data if x['clienturi'] == clienturi] if clienturi else []
                return {
                    'clienthouseholdid': rail.smartjoin_by_delim(
                        [x['Household_Firm_ID'] for x in filtered_client_data], ' ') if filtered_client_data else '',
                    'householdfirmid': rail.smartjoin_by_delim(
                        [x['Household_Firm_ID'] for x in filtered_client_data], ' ') if filtered_client_data else '',
                    'clienthouseholdidlength': len(rail.smartjoin_by_delim(
                        [x['Household_Firm_ID'] for x in filtered_client_data], ' ')) if filtered_client_data else null,
                    'advisorcontactfid': rail.smartjoin_by_delim(
                        [x['Salesforce_ID'] for x in filtered_client_data], ' ') if filtered_client_data else '',
                }

            timesheet_data = list(map(lambda item: {
                **dict(item.items()),
                **{
                    'timeentryid': rail.result('get_timeentry_id_variable')['value']
                },
                **dict(get_userdata_fields(user_data, item['useruri']).items()),
                **dict(get_service_data_fields(service_data, item['serviceuri']).items()),
                **dict(get_client_data_fields(client_data, item['clienturi']).items())
            }, queried_records))
            return {
                'timesheeturi': rail.result('for_each_timesheet_uri')['timesheeturi'],
                'log': rail.result('create_log'),
                'timesheetdata': timesheet_data
            }
        process_append_time_entries = rail.TriggerDagRunOperator(
            task_id='process_append_time_entries',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'oxfordfinancial_replicon_datastore_append_timeentries_{config.instance}',
            conf=get_process_append_time_entries
        )

        update_child_dag_runs = rail.SetVariableOperator(
            task_id='update_child_dag_runs',
            name='child_dag_runs',
            append=True,
            value="{{ result('process_append_time_entries') }}"
        )

        update_child_log_value = rail.SetVariableOperator(
            task_id='update_child_log_value',
            name='child_logs',
            append=True,
            value="{{ result('create_log') }}"
        )

        update_entry_id_value = rail.SetVariableOperator(
            task_id='update_entry_id_value',
            name='entryid',
            value=lambda: rail.result('get_timeentry_id_variable')['value'] + rail.result(
                'query_values_timesheet_uri', 'length')
        )

        for_each_timesheet_uri_end = rail.EmptyOperator(
            task_id='for_each_timesheet_uri_end'
        )

        wait_for_process_append_time_entries = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_append_time_entries',
            dag_runs="{{ result('update_child_dag_runs').value | to_json }}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        get_timeentry_id_variable_final = rail.GetVariableOperator(
            task_id='get_timeentry_id_variable_final',
            name='entryid'
        )

        update_entry_id_variable = rail.PythonOperator(
            task_id='update_entry_id_variable',
            python_callable=lambda: Variable.set(config.time_entry_id_var_name, int(rail.result(
                'get_timeentry_id_variable_final')['value']))
        )

        process_timeentry_dynamic_email = rail.TriggerDagRunOperator(
            task_id='process_timeentry_dynamic_email',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'oxfordfinancial_replicon_datastore_timeentry_dynamic_email_{config.instance}',
            conf=lambda: {
                'child_logs': [x for x in rail.result('update_child_log_value')['value'] if x],
                'date': py_datetime.now().strftime('%m%d%Y')
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> dagrun_log_to_sumo

        can_run_batch_task >> rail.Label(
            'No') >> get_master_data_details >> run_master_data_report >> is_masterdata_report

        is_masterdata_report >> rail.Label(
            'Yes') >> load_master_data_to_csv >> create_master_data_collection >> query_distinct_timesheet_uri >> \
            get_client_data_details >> run_client_data_report >> load_client_data_to_csv >> get_service_data_details >> \
            run_service_data_report >> load_service_data_to_csv >> get_user_data_details >> \
            run_user_data_report >> load_user_data_to_csv >> create_entry_id_variable >> for_each_timesheet_uri

        for_each_timesheet_uri >> query_values_timesheet_uri >> create_log >> get_timeentry_id_variable >> process_append_time_entries >> \
            update_child_dag_runs >> update_child_log_value >> update_entry_id_value >> for_each_timesheet_uri_end
        for_each_timesheet_uri >> for_each_timesheet_uri_end
        for_each_timesheet_uri_end >> wait_for_process_append_time_entries >> get_timeentry_id_variable_final >> \
            update_entry_id_variable >> process_timeentry_dynamic_email >> dagrun_log_to_sumo

        is_masterdata_report >> rail.Label(
            'No') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
