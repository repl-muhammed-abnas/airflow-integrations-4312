from datetime import timedelta, datetime
from os import path
import rail


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/oxfordfinancial/user_import/config.py


# pylint: disable=too-many-statements
def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'oxfordfinancial_user_import_master_dag_{config.instance}',
        description=f'New/updated file in directory on SFTP server will Create/update/disable User Profile {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=30),
        max_active_runs=config.master_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        def get_dagrun_start_time(start_time):
            return datetime.fromisoformat(start_time).strftime('%m%d%YT%H%M%S')
        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=get_dagrun_start_time,
            op_args=['{{ dag_run.start_date }}']
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='archive_file',
            no_task='delete_this_dagrun'
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/{{ result('new_file_sensor') | file_base }}_{{ result('get_time_for_file') }}.csv"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_file') }}",
            headers=['SF_18_Digit_ID', 'Full_Name', 'Initials', 'Title', 'Department',
                     'Email', 'Status', 'Employee_Type', 'Supervisor', 'Start_Date',
                     'End_Date', 'First_Name', 'Middle_Name', 'Last_Name', 'Active_Directory_Login',
                     'SF_15_Digit_ID', 'IsDeleted', 'Last_Modified_Date', 'Date_File_Created']
        )

        get_enabled_users_details = rail.RepliconReportDetailsOperator(
            task_id='get_enabled_users_details',
            report_name=config.enabled_users_report_name
        )

        run_enabled_users_report = rail.run_report2(
            group_id='run_enabled_users_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_enabled_users_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        load_users_data_to_csv = rail.LoadCSVFileOperator(
            task_id="load_users_data_to_csv",
            document='{{ result("run_enabled_users_report.get_report_result").reportGenerationResults[0].payload }}'
        )

        get_project_resource_permissionset = rail.RepliconServiceOperator(
            task_id='get_project_resource_permissionset',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets',
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Project Resource with Reports', 'uri', '')
        )

        get_required_user_custom_fields = rail.RepliconServiceOperator(
            task_id='get_required_user_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:user"},
            data_handler=lambda response: {
                'sfid_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Salesforce_ID', 'uri', ''),
                'initials_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Initials', 'uri', ''),
                'title_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Title', 'uri', ''),
                'middle_name_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Middle Name', 'uri', '')
            }
        )

        get_required_holiday_calendar = rail.RepliconServiceOperator(
            task_id='get_required_holiday_calendar',
            endpoint='/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            data={"objectUri": "urn:replicon:object-type:user"},
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'United States', 'uri', '')
        )

        get_enabled_locations = rail.RepliconServiceOperator(
            task_id='get_enabled_locations',
            endpoint='/services/LocationService1.svc/GetEnabledLocations'
        )

        def get_process_add_update_users(item):
            replicon_users_data = rail.load_all_records(
                rail.result('load_users_data_to_csv'))
            user_uri = ''
            current_department = ''
            if replicon_users_data:
                user_uri = rail.find_first_by_attr_and_get_attr(replicon_users_data, 'Salesforce_ID',
                                                                item['SF_18_Digit_ID'], 'uri', '')
                current_department = rail.find_first_by_attr_and_get_attr(replicon_users_data, 'Salesforce_ID',
                                                                          item[
                                                                              'SF_18_Digit_ID'], 'Department Groups (Current)',
                                                                          '')
            return {
                **dict(item.items()),
                **dict(rail.result('get_required_user_custom_fields').items()),
                **{
                    'permission_set_uri': rail.result('get_project_resource_permissionset'),
                    'holiday_calendar_uri': rail.result('get_required_holiday_calendar'),
                    'useruri': user_uri,
                    'current_department': current_department,
                    'location_uri': rail.find_first_by_attr_and_get_attr(rail.result(
                        'get_enabled_locations'), 'displayText', item['Department'], 'uri', ''),
                    'action': 'add_update'
                }
            }
        process_add_update_users = rail.TriggerDagRunForEachItemOperator(
            task_id='process_add_update_users',
            retries=0,
            items="{{ result('parse_csv') }}",
            trigger_dag_id=f'oxfordfinancial_user_import_process_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=get_process_add_update_users
        )

        wait_for_add_update_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_add_update_users',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_add_update_users") }}'
        )

        gather_addupdateusers_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_addupdateusers_logs',
            dag_runs="{{ result('process_add_update_users') }}",
            dagrun_task_id='create_addupdateuser_log',
            flatten=True
        )

        def get_process_disable_users():
            disable_users = []
            feed_file_data = rail.load_all_records(rail.result('parse_csv'))
            replicon_user_data = rail.load_all_records(
                rail.result('load_users_data_to_csv'))
            for item in replicon_user_data:
                should_disable = not bool(rail.find_first_by_attr_and_get_attr(feed_file_data,
                                                                               'SF_18_Digit_ID',
                                                                               item['Salesforce_ID'],
                                                                               'Full_Name', ''))
                if should_disable:
                    disable_users.append({
                        **{k: v for k, v in item.items() if k in ('uri', 'Salesforce_ID')},
                        **{
                            'action': 'disable'
                        }
                    })
            return disable_users
        get_disable_users_conf = rail.PythonOperator(
            task_id='get_disable_users_conf',
            python_callable=get_process_disable_users
        )

        process_disable_users = rail.TriggerDagRunForEachItemOperator(
            task_id='process_disable_users',
            retries=0,
            items=lambda: rail.result('get_disable_users_conf'),
            trigger_dag_id=f'oxfordfinancial_user_import_process_users_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: dict(item.items())
        )

        wait_for_disable_users = rail.WaitForDagRunsSensor(
            task_id='wait_for_disable_users',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_disable_users") }}'
        )

        gather_disableusers_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_disableusers_logs',
            dag_runs="{{ result('process_disable_users') }}",
            dagrun_task_id='create_disableuser_log',
            flatten=True
        )

        def get_logs():
            logs = []
            gather_addupdateusers_logs = rail.result(
                'gather_addupdateusers_logs')
            if gather_addupdateusers_logs:
                logs.extend(gather_addupdateusers_logs)
            gather_disableusers_logs = rail.result('gather_disableusers_logs')
            if gather_disableusers_logs:
                logs.extend(gather_disableusers_logs)
            return logs
        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            trigger_dag_id=f'oxfordfinancial_user_import_child_log_{config.instance}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda: {
                "filename": path.split(rail.result('new_file_sensor'))[1],
                "logs": get_logs()
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='dagrun_log_to_sumo'
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            extra_info={
                'Filename': "{{ result('new_file_sensor') | file_base }}"
            }
        )

        new_file_sensor >> download_file >> get_time_for_file

        get_time_for_file >> rail.Label(
            'Always') >> was_new_file_found
        was_new_file_found >> rail.Label(
            'Yes') >> archive_file
        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun
        get_time_for_file >> parse_csv >> get_enabled_users_details >> run_enabled_users_report >> \
            load_users_data_to_csv >> get_project_resource_permissionset >> get_required_user_custom_fields >> \
            get_required_holiday_calendar >> get_enabled_locations >> process_add_update_users >> \
            wait_for_add_update_users >> gather_addupdateusers_logs >> get_disable_users_conf >> \
            process_disable_users >> wait_for_disable_users >> gather_disableusers_logs >> \
            process_log_generation >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found

        check_if_new_file_found >> rail.Label(
            'Yes') >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
