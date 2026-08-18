
from datetime import datetime, timedelta
from pendulum import datetime as dt
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_timesheet_export_master_{config.instance}',
        description=f'Live|DTNA_DataMart_Export_ENG_Timesheet Export_V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date= dt(2022, 4, 1, tz=config.schedule_time_zone),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        dir_checkifthereisanyprocessedrecords_2 = rail.SFTPListFilesOperator(
            task_id='dir_checkifthereisanyprocessedrecords_2',
            paths=[config.sftp_processing_directory]
        )

        if_has_processed_files = rail.IfOperator(
            task_id='if_has_processed_files',
            test=lambda: bool(rail.result(
                'dir_checkifthereisanyprocessedrecords_2').values()),
            yes_task='foreach_dir_checkifthereisanyprocessedrecords_2_3',
            no_task='get_all_reports'
        )

        foreach_dir_checkifthereisanyprocessedrecords_2_3 = rail.ForEachOperator(
            task_id='foreach_dir_checkifthereisanyprocessedrecords_2_3',
            items=lambda: list(rail.result(
                'dir_checkifthereisanyprocessedrecords_2').values())[0],
            start_task='remove_4',
            end_task='foreach_dir_checkifthereisanyprocessedrecords_2_3_end'
        )

        remove_4 = rail.SFTPDeleteFileOperator(
            task_id='remove_4',
            existing_filename=config.sftp_processing_directory + '/' +
            "{{ result('foreach_dir_checkifthereisanyprocessedrecords_2_3').name }}"
        )

        foreach_dir_checkifthereisanyprocessedrecords_2_3_end = rail.EmptyOperator(
            task_id='foreach_dir_checkifthereisanyprocessedrecords_2_3_end',
        )

        get_all_reports = rail.RepliconServiceOperator(
            task_id='get_all_reports',
            endpoint='/services/ReportService1.svc/GetAllReports'
        )

        invoke_custom_ruby_code_5 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_5',
            python_callable=lambda: {
                "today": datetime.utcnow().strftime("%Y%m%d"),
                "startdate": Variable.get(config.startdate_test_var_name, default_var='') or (datetime.utcnow()-timedelta(days=50)).strftime("%m/%d/%Y"),
                "enddate":  Variable.get(config.enddate_test_var_name, default_var='') or datetime.utcnow().strftime("%m/%d/%Y")
            }
        )

        get_report_details_timesheet_period = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_timesheet_period',
            report_name='**Get the Timesheet Period Details****'
        )

        trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process6 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process6',
            retries=0,
            trigger_dag_id=f'daimlertrucks_datamart_eng_timesheet_export_costcenter_process_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            wait_for_completion=True,
            conf={
                "type": "Datamart Export - Get Timesheet Period Details - DTNA ENG - Timesheet",
                "report_name": '**Get the Timesheet Period Details****',
                "domain": "ALL"
            }
        )

        gather_report_filter_timesheet = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_report_filter_timesheet',
            dagrun_task_id='log_final_list',
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process6") }}',
            flatten=True
        )

        getthe_timesheet_period_details_8 = rail.RepliconServiceOperator(
            task_id='getthe_timesheet_period_details_8',
            endpoint="/services/reportservice1.svc/CreateReportGenerationBatch",
            data=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '**Get the Timesheet Period Details****', 'uri'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": null
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": rail.result('invoke_custom_ruby_code_5')['startdate']
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                                "value": rail.result('invoke_custom_ruby_code_5')['startdate']
                            },
                        ] + rail.result('gather_report_filter_timesheet'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        batch_management_9 = rail.batch_execution(
            group_id='execute_batch_management_9',
            creation_task_id='getthe_timesheet_period_details_8',
        )

        get_report_batch_results_10 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_10',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri':  "{{ result('getthe_timesheet_period_details_8') }}"
            },
            target='artifact'
        )

        parse_csv_11 = rail.LoadCSVFileOperator(
            task_id='parse_csv_11',
            document="{{ (result('get_report_batch_results_10') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        log_get_start_dateofthetimesheet_12 = rail.PythonOperator(
            task_id='log_get_start_dateofthetimesheet_12',
            python_callable=lambda:  rail.load_all_records(rail.result('parse_csv_11'))[
                0]['Timesheet Start Date']
        )

        getthe_timesheet_period_details_13 = rail.RepliconServiceOperator(
            task_id='getthe_timesheet_period_details_13',
            endpoint="/services/ReportService1.svc/GenerateReport",
            data=lambda: {
                "reportUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '**Get the Timesheet Period Details****', 'uri'),
                "filterValues": [
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                        "value": null
                    },
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                        "value": rail.result('invoke_custom_ruby_code_5')['enddate']
                    },
                    {
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_period')['filterConfiguration']['enabledFilters'], 'displayText', "EntryDateFilter", 'uri'),
                        "value": rail.result('invoke_custom_ruby_code_5')['enddate']
                    },

                ] + rail.result('gather_report_filter_timesheet'),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        parse_csv_14 = rail.LoadCSVFileOperator(
            task_id='parse_csv_14',
            document="{{ result('getthe_timesheet_period_details_13').payload  }}"
        )

        log_get_end_dateofthetimesheet_15 = rail.PythonOperator(
            task_id='log_get_end_dateofthetimesheet_15',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_14'))[0]['Timesheet End Date']
        )

        get_report_details_timesheet_data = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_timesheet_data',
            report_name='**ENG_DataMart Timesheet Export-Timesheet Data****'
        )

        trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process16 = rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process16',
            retries=0,
            trigger_dag_id=f'daimlertrucks_datamart_eng_timesheet_export_costcenter_process_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            wait_for_completion=True,
            conf={
                "type": "Datamart Export - DTNA ENG - Timesheet",
                "report_name": '**ENG_DataMart Timesheet Export-Timesheet Data****',
                "domain": "DTNA ENG"
            }
        )

        gather_report_filter_timesheet_data = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_report_filter_timesheet_data',
            dagrun_task_id='log_final_list',
            dag_runs='{{ result("trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process16") }}',
            flatten=True
        )

        create_report_generation_batch_17 = rail.RepliconServiceOperator(
            task_id='create_report_generation_batch_17',
            endpoint="/services/reportservice1.svc/CreateReportGenerationBatch",
            data=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '**ENG_DataMart Timesheet Export-Timesheet Data****', 'uri'),
                        "filterValues": [
                            {
                                "reportFilterUri":  rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_data')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri'),
                                "value": null
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_data')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri'),
                                "value": rail.result('log_get_start_dateofthetimesheet_12')
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_data')['filterConfiguration']['enabledFilters'], 'displayText', "TimesheetPeriodFilter", 'uri'),
                                "value": rail.result('log_get_end_dateofthetimesheet_15')
                            },
                            {
                                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_timesheet_data')['filterConfiguration']['enabledFilters'], 'displayText', "ApprovalStatusFilter", 'uri'),
                                "value": "2"
                            },

                        ] + rail.result('gather_report_filter_timesheet_data'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        batch_management_19 = rail.batch_execution(
            group_id='execute_batch_management_19',
            creation_task_id='create_report_generation_batch_17',
        )

        get_report_batch_results_21 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_21',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri':  "{{result('create_report_generation_batch_17')}}"
            },
            target='artifact'
        )

        load_csv_create_list_from_csv_22_22 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_22_22",
            document="{{ (result('get_report_batch_results_21') | load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_create_list_from_csv_22_22 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_22_22',
            source="{{ result('load_csv_create_list_from_csv_22_22') }}",
            name="initial_data",
            # headers :  Approval Status,workerfirstname,workerlastname,clientworkerid,Login Name,Timesheet Number,periodbegindate,periodenddate,workdate,projectcode,projectname,costcenternumber,taskid,taskname,Hours Worked,weeklyhours,User Supervisor Name (Current),hiringmanagerid,projecttype,workertype,timesheeturi
            columns={
                'Approval Status': 'status',
                'workerfirstname': 'workerfirstname',
                'workerlastname': 'workerlastname',
                'clientworkerid': 'clientworkerid',
                'Login Name': 'loginname',
                'Timesheet Number': 'timesheetnumber',
                'periodbegindate': 'periodbegindate',
                'periodenddate': 'periodenddate',
                'workdate': 'workdate',
                'projectcode': 'projectcode',
                'projectname': 'projectname',
                'costcenternumber': 'costcenternumber',
                'taskid': 'taskid',
                'taskname': 'taskname',
                'Hours Worked': 'hoursworked',
                'weeklyhours': 'weeklyhours',
                'User Supervisor Name (Current)': 'approverid',
                'hiringmanagerid': 'hiringmanagerid',
                'projecttype': 'projecttype',
                'workertype': 'workertype',
                'timesheeturi': 'timesheeturi'
            }
        )

        query_list_23 = rail.QueryCollectionOperator(
            task_id='query_list_23',
            query="""SELECT * FROM  initial_data""",
        )

        accumulate_list_items_24 = rail.SetVariableOperator(
            task_id='accumulate_list_items_24',
            name='Data Header',
            append=True,
            value={
                "status": "Status",
                "clientworkerid": "Client Worker ID",
                "loginname": "Replicon Login Name",
                "timesheetnumber": "Timesheet Number",
                "periodbegindate": "Period Begin Date",
                "periodenddate": "Period End Date",
                "workdate": "Work Date",
                "projectcode": "Project Code",
                "projectname": "Project Name",
                "costcenternumber": "Cost Center Number",
                "taskid": "Task ID",
                "taskname": "Task Name",
                "hoursworked": "Hours Worked",
                "weeklyhours": "Weekly Hours",
                "approverid": "Approver ID",
                "hiringmanagerid": "Hiring Manager ID",
                "projecttype": "Project Type",
                "workertype": "Worker Type",
                "workerfirstname": "Worker First Name",
                "workerlastname": "Worker Last Name"
            }
        )

        create_csv_lines_25 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_25',
            source="{{ [] }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type'],
            row=[
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.loginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}"
            ]
        )

        upload_26 = rail.SFTPUploadFileOperator(
            task_id='upload_26',
            content="{{ result('create_csv_lines_25') }}",
            remote_filepath=config.sftp_processing_directory +
            "/Processing_Replicon_TimesheetEngr_Download_{{ result('invoke_custom_ruby_code_5').today }}.csv",
        )

        accumulate_list_items_27 = rail.SetVariableOperator(
            task_id='accumulate_list_items_27',
            name='Data Header',
            append=True,
            value={
                "status": "Status",
                "clientworkerid": "Client Worker ID",
                "loginname": "Replicon Login Name",
                "timesheetnumber": "Timesheet Number",
                "periodbegindate": "Period Begin Date",
                "periodenddate": "Period End Date",
                "workdate": "Work Date",
                "projectcode": "Project Code",
                "projectname": "Project Name",
                "costcenternumber": "Cost Center Number",
                "taskid": "Task ID",
                "taskname": "Task Name",
                "hoursworked": "Hours Worked",
                "weeklyhours": "Weekly Hours",
                "approverid": "Approver ID",
                "hiringmanagerid": "Hiring Manager ID",
                "projecttype": "Project Type",
                "workertype": "Worker Type",
                "reason": "Reason",
                "workerlastname": "Worker Last Name",
                "workerfirstname": "Worker First Name"
            }
        )

        create_csv_lines_28 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_28',
            source="{{ [] }}",
            header=['Status',
                    'Worker First Name',
                    'Worker Last Name',
                    'Client Worker ID',
                    'Replicon Login Name',
                    'Timesheet Number',
                    'Period Begin Date',
                    'Period End Date',
                    'Work Date',
                    'Project Code',
                    'Project Name',
                    'Cost Center Number',
                    'Task ID',
                    'Task Name',
                    'Hours Worked',
                    'Weekly Hours',
                    'Approver ID',
                    'Hiring Manager ID',
                    'Project Type',
                    'Worker Type',
                    'Reason'],
            row=[
                "{{ item.status }}",
                "{{ item.workerfirstname }}",
                "{{ item.workerlastname }}",
                "{{ item.clientworkerid }}",
                "{{ item.loginname }}",
                "{{ item.timesheetnumber }}",
                "{{ item.periodbegindate }}",
                "{{ item.periodenddate }}",
                "{{ item.workdate }}",
                "{{ item.projectcode }}",
                "{{ item.projectname }}",
                "{{ item.costcenternumber }}",
                "{{ item.taskid }}",
                "{{ item.taskname }}",
                "{{ item.hoursworked }}",
                "{{ item.weeklyhours }}",
                "{{ item.approverid }}",
                "{{ item.hiringmanagerid }}",
                "{{ item.projecttype }}",
                "{{ item.workertype }}",
                "{{ item.reason }}"
            ],
        )

        upload_29 = rail.SFTPUploadFileOperator(
            task_id='upload_29',
            content="{{ result('create_csv_lines_28') }}",
            remote_filepath=config.sftp_processing_directory +
            "/Processing_Replicon_TimesheetEngr_RejectedRecords_{{ result('invoke_custom_ruby_code_5').today }}.csv",
        )

        if_query_list_23_rows_greater_than_0_30 = rail.IfOperator(
            task_id='if_query_list_23_rows_greater_than_0_30',
            test='''{{ result('query_list_23','length') > 0 }}''',
            yes_task="trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33",
            no_task="log_to_sumo",
        )

        batch_size = 1000
        trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33',
            retries=0,
            items="{{ result('query_list_23') }}",
            batch_size=batch_size,
            trigger_dag_id=f'daimlertrucks_datamart_eng_timesheet_export_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item, index: {
                'items': item,
                "todaydateinmmddyyyy": datetime.utcnow().strftime("%m/%d/%Y"),
                "todayinmmddyyyyforname": rail.result('invoke_custom_ruby_code_5')['today'],
                "last": ((index+1)*batch_size) >= rail.result('query_list_23', 'length'),
                "path": config.sftp_processing_directory,
                "supervisor_report_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_reports'), 'displayText', '***Supervisor Details Template***', 'uri'),
            }
        )

        wait_for_completion_trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33") }}'
        )

        gather_csv_artifacts_from_children = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_csv_artifacts_from_children',
            dagrun_task_id='log_csv_artifact_for_master_30',
            dag_runs='{{ result("trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33") }}',
            flatten=True
        )

        def merge_csv_artifacts():
            """Merge all child CSV artifacts preserving selective quoting"""
            from rail.lib.artifact import existing_artifact, new_artifact

            csv_artifacts = rail.result('gather_csv_artifacts_from_children')
            if not csv_artifacts:
                return None

            merged_lines = []
            header_written = False

            for artifact_name in csv_artifacts:
                if not artifact_name:
                    continue

                with existing_artifact(artifact_name, mode='r', encoding='utf-8') as csv_artifact:
                    content = csv_artifact.file.read()
                    lines = content.splitlines()  # Use splitlines() to avoid empty trailing lines

                    if not header_written:
                        merged_lines.extend(lines)
                        header_written = True
                    else:
                        merged_lines.extend(lines[1:] if len(lines) > 1 else [])

            merged_content = '\n'.join(merged_lines)

            with new_artifact(mode='w', encoding='utf-8') as output_artifact:
                output_artifact.file.write(merged_content)
                output_artifact.set_attribute('type', 'csv')
                return output_artifact.name

        merge_csv_artifacts_34 = rail.PythonOperator(
            task_id='merge_csv_artifacts_34',
            python_callable=merge_csv_artifacts
        )

        upload_merged_csv_35 = rail.SFTPUploadFileOperator(
            task_id='upload_merged_csv_35',
            content="{{ result('merge_csv_artifacts_34') }}",
            remote_filepath=config.sftp_processing_directory + "/Processing_Replicon_TimesheetEngr_Download_{{ result('invoke_custom_ruby_code_5')['today'] }}.csv"
        )

        trigger_dag_run_sftp_file_move_process = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_sftp_file_move_process',
            retries=0,
            items=[1],
            batch_size=batch_size,
            trigger_dag_id=f'daimlertrucks_datamart_eng_timesheet_export_sftp_process_file_child_{config.instance}',
            execution_timeout=timedelta(days=14),
        )

        wait_for_completion_trigger_dag_run_sftp_file_move_process = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_sftp_file_move_process',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_sftp_file_move_process") }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        dir_checkifthereisanyprocessedrecords_2 >> if_has_processed_files
        if_has_processed_files >> rail.Label(
            'yes') >> foreach_dir_checkifthereisanyprocessedrecords_2_3 >> remove_4 >> foreach_dir_checkifthereisanyprocessedrecords_2_3_end
        foreach_dir_checkifthereisanyprocessedrecords_2_3 >> foreach_dir_checkifthereisanyprocessedrecords_2_3_end >> get_all_reports
        if_has_processed_files >> rail.Label('no') >> get_all_reports
        get_all_reports >> invoke_custom_ruby_code_5 >> get_report_details_timesheet_period >> trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process6 >> gather_report_filter_timesheet >> getthe_timesheet_period_details_8 >> batch_management_9[0] >> batch_management_9[
            1] >> get_report_batch_results_10 >> parse_csv_11 >> log_get_start_dateofthetimesheet_12 >> getthe_timesheet_period_details_13 >> parse_csv_14 >> log_get_end_dateofthetimesheet_15 >> get_report_details_timesheet_data >> trigger_dag_run_live_dtna_get_the_list_of_active_costcenters_to_process16 >> gather_report_filter_timesheet_data >> create_report_generation_batch_17 >> batch_management_19[0] >> batch_management_19[1] >> get_report_batch_results_21 >> load_csv_create_list_from_csv_22_22 >> create_collection_create_list_from_csv_22_22 >> query_list_23 >> accumulate_list_items_24 >> create_csv_lines_25 >> upload_26 >> accumulate_list_items_27 >> create_csv_lines_28 >> upload_29 >> if_query_list_23_rows_greater_than_0_30
        if_query_list_23_rows_greater_than_0_30 >> rail.Label(
            'Yes') >> trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33 >> wait_for_completion_trigger_dag_run_live_child_of_dtna_timesheet_datamart_eng_call_v3_0async_33 >> gather_csv_artifacts_from_children >> merge_csv_artifacts_34 >> upload_merged_csv_35 >> trigger_dag_run_sftp_file_move_process >> wait_for_completion_trigger_dag_run_sftp_file_move_process >> log_to_sumo
        if_query_list_23_rows_greater_than_0_30 >> rail.Label(
            'No') >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
