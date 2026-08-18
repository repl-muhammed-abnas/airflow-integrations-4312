
from datetime import timedelta, datetime
import chardet
import pendulum
from airflow.models import Variable
from rail.lib.artifact import existing_artifact
import rail
null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalxchange_timeoff_import_intercontinentalexchange_timeoffimport_master_{config.instance}',
        description=f' IntercontinentalExchange_timeoffimport_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        get_time_for_file = rail.PythonOperator(
            task_id='get_time_for_file',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_today_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_today_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_today_4 = rail.PythonOperator(
            task_id='log_today_4',
            python_callable=lambda: pendulum.now(
                config.pacific_timezone).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        has_input_filename_ends_with_csv = rail.IfOperator(
            task_id="has_input_filename_ends_with_csv",
            test='{{ result("new_file_sensor").split(".")[-1] | lower == "csv" | lower if result("new_file_sensor") else False }}',
            yes_task="download_file_8",
            no_task="send_mail_5",
        )

        send_mail_5 = rail.EmailOperator(
            task_id='send_mail_5',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon timeoff import - skipped {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p>Hi Team,<br /> <br /> Replicon timeoff import for  {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} skipped, since the file format is incorrect </p>
            <p> File name : {{ result('new_file_sensor') | file_name }} </p>
            <p>Please send the correct input file in csv file format.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params=None,
        )

        rename_archivetheinputfile_6 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_6',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        download_file_8 = rail.SFTPDownloadFileOperator(
            task_id='download_file_8',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        def find_file_encoding_callable(task_id):
            feed_file = rail.result(task_id)
            with existing_artifact(feed_file) as ff:
                return chardet.detect_all(ff.file.read())

        find_file_encoding = rail.PythonOperator(
            task_id = "find_file_encoding",
            python_callable=find_file_encoding_callable,
            op_args=[download_file_8.task_id]
        )

        parse_input_csv_9 = rail.LoadCSVFileOperator(
            task_id="parse_input_csv_9",
            document="{{result('download_file_8')}}",
            encoding="{{ result('find_file_encoding')[0].encoding}}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        get_all_object_extension_field_bindings_14 = rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_bindings_14',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings",
            data={
                "bindingContextUri": "urn:replicon:object-type:time-off"
            }
        )

        get_all_columns_15 = rail.RepliconServiceOperator(
            task_id='get_all_columns_15',
            endpoint="/services/TimeOffListService1.svc/GetAllColumns",
            data={}
        )

        get_all_filter_definitions_16 = rail.RepliconServiceOperator(
            task_id='get_all_filter_definitions_16',
            endpoint="/services/TimeOffListService1.svc/GetAllFilterDefinitions",
            data={}
        )

        get_all_time_off_types_18 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_18',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data={}
        )

        get_report_details_19 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_19',
            report_name=config.user_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details_19').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='if_generate_report_payload_starts_with_nodata_19',
            no_task='log_to_sumo'
        )

        if_generate_report_payload_starts_with_nodata_19 = rail.IfOperator(
            task_id='if_generate_report_payload_starts_with_nodata_19',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="log_to_sumo",
            no_task="if_generate_report_payload_not_starts_with_column_19",
        )

        if_generate_report_payload_not_starts_with_column_19 = rail.IfOperator(
            task_id='if_generate_report_payload_not_starts_with_column_19',
            # pylint: disable=line-too-long
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('User Name,Employee ID,userUri,User Start Date')}}",
            yes_task="load_report_data_19",
            no_task="log_to_sumo",
        )

        load_report_data_19 = rail.LoadCSVFileOperator(
            task_id='load_report_data_19',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection_24 = rail.CreateCollectionOperator(
            task_id='create_user_collection_24',
            name='user_info_from_report',
            source="{{ result('load_report_data_19') }}",
            columns={
                'User Name': 'username',
                'Employee ID': 'employeeid',
                'userUri': 'useruri',
                'User Start Date': 'userstartdate'}
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def do_format_csv_line():
            csv_lines = []
            raw_input_csv_lines = get_data_from_document(
                rail.result('parse_input_csv_9'))
            report_data = get_data_from_document(
                rail.result('create_user_collection_24'))
            for item in raw_input_csv_lines:
                employee_id = item["EMPLOYEE_ID"].strip(
                ) if item["EMPLOYEE_ID"] else ""
                csv_lines.append({
                    "EMPLOYEE_ID": employee_id,
                    "ENTRY_ID": item["ENTRY_ID"].strip() if item["ENTRY_ID"] else "",
                    "NAME": item["NAME"].strip() if item["NAME"] else "",
                    "LEAVE_START_DT": item["LEAVE_START_DT"].strip() if item["LEAVE_START_DT"] else "",
                    "LEAVE_END_DT": item["LEAVE_END_DT"].strip() if item["LEAVE_END_DT"] else "",
                    "MODIFIED_DATE": item["MODIFIED_DATE"].strip() if item["MODIFIED_DATE"] else "",
                    "TIME_OFF_TYPE": item["TIME_OFF_TYPE"].strip() if item["TIME_OFF_TYPE"] else "",
                    "UNIT": item["UNIT"].strip() if item["UNIT"] else "",
                    "DAY_HOURS": item["DAY_HOURS"].strip() if item["DAY_HOURS"] else "",
                    "STATUS": item["STATUS"].strip() if item["STATUS"] else "",
                    "START_DATE_DURATION": item["START_DATE_DURATION"].strip() if item["START_DATE_DURATION"] else "",
                    "END_DATE_DURATION": item["END_DATE_DURATION"].strip() if item["END_DATE_DURATION"] else "",
                    "USER_URI": rail.find_first_by_attr_and_get_attr(report_data, 'employeeid', employee_id, 'useruri'),
                    "USER_START_DATE": rail.find_first_by_attr_and_get_attr(report_data, 'employeeid', employee_id, 'userstartdate')
                })

            return csv_lines

        create_csv_lines_23 = rail.PythonOperator(
            task_id='create_csv_lines_23',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: do_format_csv_line()
        )

        if_parse_csv_9_lines_less_than_1_10 = rail.IfOperator(
            task_id='if_parse_csv_9_lines_less_than_1_10',
            test='''{{ result('create_csv_lines_23') | length < 1 }}''',
            yes_task="send_mail_11",
            no_task="load_csv_create_list_from_csv_24",
        )

        send_mail_11 = rail.EmailOperator(
            task_id='send_mail_11',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | Replicon timeoff import - skipped {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p>Hi Team,<br /> <br /> The timeoff import for  {{ get_company_key() }} skipped, since there is no data in the input file</p>
            <p> File name : {{ result('new_file_sensor') | file_name }} </p>
            <br /> Regards,<br /> Deltek Inc</p> ''',
            params=None,
        )

        load_csv_create_list_from_csv_24 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_24",
            document="{{ result('create_csv_lines_23') }}}",
        )

        create_collection_create_list_from_csv_24 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_24',
            source="{{ result('create_csv_lines_23') | to_json }}",
            name="input_with_useruri",
            columns={
                'EMPLOYEE_ID': 'EMPLOYEE_ID',
                'ENTRY_ID': 'ENTRY_ID',
                'NAME': 'NAME',
                'LEAVE_START_DT': 'LEAVE_START_DT',
                'LEAVE_END_DT': 'LEAVE_END_DT',
                'MODIFIED_DATE': 'MODIFIED_DATE',
                'TIME_OFF_TYPE': 'TIME_OFF_TYPE',
                'UNIT': 'UNIT',
                'DAY_HOURS': 'DAY_HOURS',
                'STATUS': 'STATUS',
                'START_DATE_DURATION': 'START_DATE_DURATION',
                'END_DATE_DURATION': 'END_DATE_DURATION',
                'USER_URI': 'USER_URI',
                'USER_START_DATE': 'USER_START_DATE'
            }
        )

        query_list_recordstoprocess_25 = rail.QueryCollectionOperator(
            task_id='query_list_recordstoprocess_25',
            query="""SELECT * FROM  input_with_useruri WHERE  input_with_useruri.USER_URI != '' AND input_with_useruri.USER_URI IS NOT NULL AND  input_with_useruri.ENTRY_ID != '' AND input_with_useruri.ENTRY_ID IS NOT NULL AND  input_with_useruri.EMPLOYEE_ID != '' AND input_with_useruri.EMPLOYEE_ID IS NOT NULL AND input_with_useruri.LEAVE_START_DT != '' AND input_with_useruri.LEAVE_START_DT != '' AND input_with_useruri.LEAVE_START_DT IS NOT NULL AND input_with_useruri.LEAVE_END_DT != '' AND input_with_useruri.LEAVE_END_DT IS NOT NULL AND input_with_useruri.TIME_OFF_TYPE != '' AND input_with_useruri.TIME_OFF_TYPE IS NOT NULL AND  input_with_useruri.STATUS != '' AND input_with_useruri.STATUS IS NOT NULL AND  input_with_useruri.DAY_HOURS != '' AND input_with_useruri.DAY_HOURS IS NOT NULL AND ( LOWER(input_with_useruri.TIME_OFF_TYPE) = 'regular' OR  LOWER(input_with_useruri.TIME_OFF_TYPE) = 'extended' )""",
        )

        query_list_skippedrecordduetousernotbeingavailableintheinstance_26 = rail.QueryCollectionOperator(
            task_id='query_list_skippedrecordduetousernotbeingavailableintheinstance_26',
            query="""SELECT * FROM  input_with_useruri WHERE  input_with_useruri.USER_URI = '' OR input_with_useruri.USER_URI IS NULL""",
        )

        query_list_recordsto_ignore_27 = rail.QueryCollectionOperator(
            task_id='query_list_recordsto_ignore_27',
            query="""SELECT * FROM  input_with_useruri WHERE  input_with_useruri.ENTRY_ID = '' OR input_with_useruri.ENTRY_ID IS NULL OR  input_with_useruri.EMPLOYEE_ID = '' OR input_with_useruri.EMPLOYEE_ID IS NULL OR  input_with_useruri.LEAVE_START_DT = '' OR input_with_useruri.LEAVE_START_DT IS NULL OR  input_with_useruri.LEAVE_END_DT = '' OR input_with_useruri.LEAVE_END_DT IS NULL OR  input_with_useruri.TIME_OFF_TYPE = '' OR input_with_useruri.TIME_OFF_TYPE IS NULL OR  input_with_useruri.STATUS = '' OR input_with_useruri.STATUS IS NULL OR  input_with_useruri.DAY_HOURS = '' OR input_with_useruri.DAY_HOURS IS NULL OR ( LOWER(input_with_useruri.TIME_OFF_TYPE) != 'regular' AND  LOWER(input_with_useruri.TIME_OFF_TYPE) != 'extended' )""",
        )

        if_query_list_skippedrecordduetousernotbeingavailableintheinstance_26_rows_greater_than_0_31 = rail.IfOperator(
            task_id='if_query_list_skippedrecordduetousernotbeingavailableintheinstance_26_rows_greater_than_0_31',
            test='''{{ result('query_list_skippedrecordduetousernotbeingavailableintheinstance_26', 'length') > 0 }}''',
            yes_task="intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_32",
            no_task="if_query_list_recordsto_ignore_27_rows_greater_than_0_35",
        )

        intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_32 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_32',
            items="{{ result('query_list_skippedrecordduetousernotbeingavailableintheinstance_26') }}",
            message='Created Successfully',
            properties={
                "employee_id": "{{ item.EMPLOYEE_ID }}",
                'entry_id': "{{ item.ENTRY_ID }}",
                'leave_start_dt': "{{ item.LEAVE_START_DT }}",
                'leave_end_dt': "{{ item.LEAVE_END_DT }}",
                'employee_name': "{{ item.NAME }}",
                'approval_status': "{{ item.STATUS }}",
                'status': "Skipped",
                'description': "Provided employee id is not available/disabled in Replicon",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_query_list_recordsto_ignore_27_rows_greater_than_0_35 = rail.IfOperator(
            task_id='if_query_list_recordsto_ignore_27_rows_greater_than_0_35',
            test='''{{ result('query_list_recordsto_ignore_27', 'length') > 0 }}''',
            yes_task="intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_36",
            no_task="if_query_list_recordstoprocess_25_rows_greater_than_0_38",
        )

        intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_36 = rail.WriteLogOperator(
            task_id='intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_36',
            items="{{ result('query_list_recordsto_ignore_27') }}",
            message='Created Successfully',
            properties={
                "employee_id": "{{ item.EMPLOYEE_ID }}",
                'entry_id': "{{ item.ENTRY_ID }}",
                'leave_start_dt': "{{ item.LEAVE_START_DT }}",
                'leave_end_dt': "{{ item.LEAVE_END_DT }}",
                'employee_name': "{{ item.NAME }}",
                'approval_status': "{{ item.STATUS }}",
                'status': "Skipped",
                'description': "One or more mandatory field values are missing or incorrect.",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        if_query_list_recordstoprocess_25_rows_greater_than_0_38 = rail.IfOperator(
            task_id='if_query_list_recordstoprocess_25_rows_greater_than_0_38',
            test='''{{ result('query_list_recordstoprocess_25', 'length') > 0 }}''',
            yes_task="query_list_getunique_entryid_39",
            no_task="create_csv_lines_45",
        )

        query_list_getunique_entryid_39 = rail.QueryCollectionOperator(
            task_id='query_list_getunique_entryid_39',
            query="""SELECT DISTINCT input_with_useruri.ENTRY_ID FROM  input_with_useruri WHERE  input_with_useruri.USER_URI != '' AND input_with_useruri.USER_URI IS NOT NULL  AND  input_with_useruri.ENTRY_ID != '' AND input_with_useruri.ENTRY_ID IS NOT NULL AND  input_with_useruri.EMPLOYEE_ID != '' AND input_with_useruri.EMPLOYEE_ID IS NOT NULL AND input_with_useruri.LEAVE_START_DT != '' AND input_with_useruri.LEAVE_START_DT IS NOT NULL AND input_with_useruri.LEAVE_END_DT != '' AND input_with_useruri.LEAVE_END_DT IS NOT NULL AND input_with_useruri.TIME_OFF_TYPE != '' AND input_with_useruri.TIME_OFF_TYPE IS NOT NULL AND  input_with_useruri.STATUS != '' AND input_with_useruri.STATUS IS NOT NULL AND input_with_useruri.DAY_HOURS != '' AND input_with_useruri.DAY_HOURS IS NOT NULL AND ( LOWER(input_with_useruri.TIME_OFF_TYPE) == 'regular' OR  LOWER(input_with_useruri.TIME_OFF_TYPE) == 'extended' )""",
        )

        declare_list_dag_runs_39 = rail.SetVariableOperator(
            task_id='declare_list_dag_runs_39',
            name='timeoff_process_dag_runs',
            value=[]
        )

        foreach_query_list_getunique_entryid_39_40 = rail.ForEachOperator(
            task_id='foreach_query_list_getunique_entryid_39_40',
            items="{{ result('query_list_getunique_entryid_39') }}",
            start_task='query_list_getrecordbyentryid_41',
            end_task='foreach_query_list_getunique_entryid_39_40_end'
        )

        query_list_getrecordbyentryid_41 = rail.QueryCollectionOperator(
            task_id='query_list_getrecordbyentryid_41',
            query="""SELECT * FROM input_with_useruri WHERE input_with_useruri.ENTRY_ID = '{{ result('foreach_query_list_getunique_entryid_39_40').ENTRY_ID }}'  AND input_with_useruri.ENTRY_ID IS NOT NULL AND  input_with_useruri.EMPLOYEE_ID != '' AND input_with_useruri.EMPLOYEE_ID IS NOT NULL AND input_with_useruri.LEAVE_START_DT != '' AND input_with_useruri.LEAVE_START_DT IS NOT NULL AND input_with_useruri.LEAVE_END_DT != '' AND input_with_useruri.LEAVE_END_DT IS NOT NULL AND input_with_useruri.TIME_OFF_TYPE != '' AND input_with_useruri.TIME_OFF_TYPE IS NOT NULL AND  input_with_useruri.STATUS != '' AND input_with_useruri.STATUS IS NOT NULL AND input_with_useruri.DAY_HOURS != '' AND input_with_useruri.DAY_HOURS IS NOT NULL AND ( LOWER(input_with_useruri.TIME_OFF_TYPE) == 'regular' OR  LOWER(input_with_useruri.TIME_OFF_TYPE) == 'extended' )""",
        )

        if_query_list_getrecordbyentryid_41_rows_greater_than_1_42 = rail.IfOperator(
            task_id='if_query_list_getrecordbyentryid_41_rows_greater_than_1_42',
            test='''{{ result('query_list_getrecordbyentryid_41', 'length') > 1 }}''',
            yes_task="trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0async_43",
            no_task="trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0async_45",
        )

        def get_numbers_of_days(item):
            numbers_of_days = 0
            start_date = datetime.strptime(
                item['LEAVE_START_DT'], '%Y%m%d') if item['LEAVE_START_DT'] else None
            end_date = datetime.strptime(
                item['LEAVE_END_DT'], '%Y%m%d') if item['LEAVE_END_DT'] else None
            if start_date and end_date:
                numbers_of_days = (end_date - start_date).days
            return numbers_of_days

        def get_timeoff_records():
            records_by_entity = get_data_from_document(
                rail.result('query_list_getrecordbyentryid_41'))
            to_records = []
            for entity in records_by_entity:
                to_records.append({
                    "employee_id": entity['EMPLOYEE_ID'],
                    "entry_id": entity['ENTRY_ID'],
                    "name": entity['NAME'],
                    "leave_start_date": entity['LEAVE_START_DT'],
                    "leave_end_date": entity['LEAVE_END_DT'],
                    "modified_date": entity['MODIFIED_DATE'],
                    "timeoff_type": entity['TIME_OFF_TYPE'],
                    "unit": entity['UNIT'],
                    "day_hours": entity['DAY_HOURS'],
                    "status": entity['STATUS'],
                    "start_date_duration": entity['START_DATE_DURATION'],
                    "end_date_duration": entity['END_DATE_DURATION'],
                    "daydiff": get_numbers_of_days(entity),
                    "useruri": entity['USER_URI']
                })
            time_entry_oef_collection = rail.result(
                'get_all_object_extension_field_bindings_14')
            timeoff_collection = rail.result('get_all_time_off_types_18')
            oef_filter_collection = rail.result(
                'get_all_filter_definitions_16')
            oef_column_collection = rail.result('get_all_columns_15')
            return {
                "timeoffs": to_records,
                "timeentryidoef_uri": rail.find_first_by_attr_and_get_attr(time_entry_oef_collection, 'displayText', 'bookingid', 'uri'),
                "regulartimeoff_typeuri": rail.find_first_by_attr_and_get_attr(timeoff_collection, 'displayText', 'Regular Time Off', 'uri'),
                "extended_timeoff_typeuri": rail.find_first_by_attr_and_get_attr(timeoff_collection, 'displayText', 'Extended Time Off', 'uri'),
                "OefFilterDefinitionUri": rail.find_first_by_attr_and_get_attr(oef_filter_collection, 'name', 'bookingid', 'uri'),
                "OefColumndefinitionUri": rail.find_first_by_attr_and_get_attr(oef_column_collection, 'displayText', 'bookingid', 'uri'),
                "user_startdate": records_by_entity[0]['USER_START_DATE']
            }

        trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0async_43 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0async_43',
            retries=0,
            items=[-1],
            trigger_dag_id=f'intercontinentalxchange_timeoff_import_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=get_timeoff_records,
        )

        trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0async_45 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0async_45',
            retries=0,
            items="{{ result('query_list_getrecordbyentryid_41') }}",
            trigger_dag_id=f'intercontinentalxchange_timeoff_import_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda item: {
                "employee_id": item['EMPLOYEE_ID'],
                "entry_id": item['ENTRY_ID'],
                "name": item['NAME'],
                "leave_start_date": item['LEAVE_START_DT'],
                "leave_end_date": item['LEAVE_END_DT'],
                "modified_date": item['MODIFIED_DATE'],
                "timeoff_type": item['TIME_OFF_TYPE'],
                "unit": item['UNIT'],
                "day_hours": item['DAY_HOURS'],
                "status": item['STATUS'],
                "start_date_duration": item['START_DATE_DURATION'],
                "end_date_duration": item['END_DATE_DURATION'],
                "daydiff": get_numbers_of_days(item),
                "user_startdate": item['USER_START_DATE'],
                "useruri": item['USER_URI'],
                "timeentryidoef_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_object_extension_field_bindings_14'), 'displayText', 'bookingid', 'uri'),
                "regulartimeoff_typeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_18'), 'displayText', 'Regular Time Off', 'uri'),
                "extended_timeoff_typeuri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_time_off_types_18'), 'displayText', 'Extended Time Off', 'uri'),
                "OefFilterDefinitionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_filter_definitions_16'), 'name', 'bookingid', 'uri'),
                "OefColumndefinitionUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_columns_15'), 'displayText', 'bookingid', 'uri'),
            }
        )

        insert_to_timeoff_dag_run_list_45 = rail.SetVariableOperator(
            task_id='insert_to_timeoff_dag_run_list_45',
            append=True,
            name='{{ result("declare_list_dag_runs_39").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0async_43") or result("trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0async_45"))[0]}}'
        )

        foreach_query_list_getunique_entryid_39_40_end = rail.EmptyOperator(
            task_id='foreach_query_list_getunique_entryid_39_40_end',
        )

        is_timeoff_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_timeoff_trigger_runs_avaialbale',
            test='''{{ result('insert_to_timeoff_dag_run_list_45') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_v2_0async_45",
            no_task="create_csv_lines_45",
        )

        wait_for_completion_trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_v2_0async_45 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_v2_0async_45',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_timeoff_dag_run_list_45").value | to_json }}'
        )

        create_csv_lines_45 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_45',
            source="{{ get_master_log() }}",
            header=['EMPLOYEE_ID',
                    'ENTRY_ID',
                    'LEAVE_START_DT',
                    'LEAVE_END_DT',
                    'USER_NAME',
                    'APPROVAL STATUS',
                    'STATUS',
                    'DESCRIPTION',
                    'JOBID'],
            row=lambda item: [ item['properties']['employee_id'],
                item['properties']['entry_id'],
                (datetime.strptime(item['properties']['leave_start_dt'], "%Y%m%d")).strftime("%Y-%m-%d"),
               (datetime.strptime(item['properties']['leave_end_dt'], "%Y%m%d")).strftime("%Y-%m-%d"),
                item['properties']['employee_name'],
                item['properties']['approval_status'],
                item['properties']['status'],
                item['properties']['description'],
                item['properties']['childjobid']]
        )

        def file_upload_failed(context):
            subject = '''{{ get_company_key() }} | Time off import to Replicon - Uploading Logs to SFTP failed  - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} '''
            body = '''<p>Hi Team,<br /> <br /> The 'Time off import to Replicon job for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} has been completed.however,
            the log upload to sftp has failed. Attached is the log file for reference.</p>
            <p>For any queries, Please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br /> Deltek Inc.</p> '''
            email = rail.EmailOperator(
                task_id='send_timeoff_import_data_on_sftp_failure_email',
                to=config.tenant_email,
                subject=subject,
                html_content=body,
                files=[
                    ("{{ result('create_csv_lines_45') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_uploadlogs_45 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadlogs_45',
            content="{{ result('create_csv_lines_45') }}",
            remote_filepath=config.log_filepath +
            "/logs_{{ dag_run_ecid() }}_{{ result('new_file_sensor') | file_name }}",
            on_failure_callback=file_upload_failed
        )

        get_logged_errors_45 = rail.FilterLogEntriesOperator(
            task_id='get_logged_errors_45',
            severity='Error',
        )

        get_logged_exception_45 = rail.FilterLogEntriesOperator(
            task_id='get_logged_exception_45',
            severity='Exception',
        )

        def get_subject_line():
            import_completion_message = "completed succesfully"
            has_error_message = rail.render_template(
                '{{result("get_logged_errors_45", key="length") > 0}}')
            has_exception_message = rail.render_template(
                '{{result("get_logged_exception_45", key="length") > 0}}')
            if has_error_message == 'True':
                import_completion_message = "completed with errors"
            elif has_exception_message == 'True':
                import_completion_message = "completed with exceptions"
            return import_completion_message

        email_subject_line_45 = rail.PythonOperator(
            task_id='email_subject_line_45',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: get_subject_line()
        )

        send_mail_45 = rail.EmailOperator(
            task_id='send_mail_45',
            to=config.tenant_email,
            bcc="{%- if result('get_logged_errors_45', key='length') > 0 -%}\
                    "+config.alert_email+"\
                {%- else -%}\
                    "+config.internal_logs_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() }} | Time off import to Replicon is {{ result("email_subject_line_45") }} - {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />  Time off import to Replicon is {{result("email_subject_line_45")}} on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }}. Please find the  log file details below for reference: <br /> <br />
            File path: {{params.log_file_path}} <br />
            File name: logs_{{ dag_run_ecid() }}_{{ result('new_file_sensor') |file_base }}.csv<br />
            <br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'log_file_path': config.log_filepath},
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_fail_dag = rail.IfOperator(
            task_id="can_fail_dag",
            test='{{ get_error_message() | is_truthy }}',
            yes_task="fail_dagrun",
        )

        fail_dagrun = rail.FailOperator(
            task_id="fail_dagrun",
            message='{{ get_error_message() }}'
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_today_4 >> has_input_filename_ends_with_csv
        has_input_filename_ends_with_csv >> rail.Label(
            'No') >> send_mail_5 >> rename_archivetheinputfile_6 >> log_to_sumo
        has_input_filename_ends_with_csv >> rail.Label('Yes') >> download_file_8 >> find_file_encoding >> parse_input_csv_9 >> \
            archive_file >> get_all_object_extension_field_bindings_14 >> \
            get_all_columns_15 >> get_all_filter_definitions_16 >> get_all_time_off_types_18 >> \
            get_report_details_19 >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label('No') >> log_to_sumo
        report_has_data >> rail.Label(
            'Yes') >> if_generate_report_payload_starts_with_nodata_19
        if_generate_report_payload_starts_with_nodata_19 >> rail.Label(
            'No') >> log_to_sumo
        if_generate_report_payload_starts_with_nodata_19 >> rail.Label(
            'Yes') >> if_generate_report_payload_not_starts_with_column_19
        if_generate_report_payload_not_starts_with_column_19 >> rail.Label(
            'No') >> log_to_sumo
        if_generate_report_payload_not_starts_with_column_19 >> rail.Label('Yes') >> load_report_data_19 >> \
            create_user_collection_24 >> create_csv_lines_23 >> if_parse_csv_9_lines_less_than_1_10
        if_parse_csv_9_lines_less_than_1_10 >> rail.Label(
            'No') >> send_mail_11 >> log_to_sumo
        if_parse_csv_9_lines_less_than_1_10 >> rail.Label('Yes') >> load_csv_create_list_from_csv_24 >> create_collection_create_list_from_csv_24 >> \
            query_list_recordstoprocess_25 >> query_list_skippedrecordduetousernotbeingavailableintheinstance_26 >> query_list_recordsto_ignore_27 >> \
            if_query_list_skippedrecordduetousernotbeingavailableintheinstance_26_rows_greater_than_0_31
        if_query_list_skippedrecordduetousernotbeingavailableintheinstance_26_rows_greater_than_0_31 >> rail.Label(
            'No') >> if_query_list_recordsto_ignore_27_rows_greater_than_0_35
        if_query_list_skippedrecordduetousernotbeingavailableintheinstance_26_rows_greater_than_0_31 >> rail.Label('Yes') >> intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_32 >> \
            if_query_list_recordsto_ignore_27_rows_greater_than_0_35
        if_query_list_recordsto_ignore_27_rows_greater_than_0_35 >> rail.Label(
            'No') >> intercontinentalexchange_timeoff_import_logs_add_batch_of_entries_36 >> if_query_list_recordstoprocess_25_rows_greater_than_0_38
        if_query_list_recordsto_ignore_27_rows_greater_than_0_35 >> rail.Label(
            'Yes') >> if_query_list_recordstoprocess_25_rows_greater_than_0_38
        if_query_list_recordstoprocess_25_rows_greater_than_0_38 >> rail.Label(
            'Yes') >> query_list_getunique_entryid_39 >> declare_list_dag_runs_39 >> foreach_query_list_getunique_entryid_39_40
        if_query_list_recordstoprocess_25_rows_greater_than_0_38 >> rail.Label(
            'No') >> create_csv_lines_45
        foreach_query_list_getunique_entryid_39_40 >> query_list_getrecordbyentryid_41 >> if_query_list_getrecordbyentryid_41_rows_greater_than_1_42
        if_query_list_getrecordbyentryid_41_rows_greater_than_1_42 >> rail.Label(
            'Yes') >> trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_mutliplerecords_v2_0async_43 >> \
            insert_to_timeoff_dag_run_list_45
        if_query_list_getrecordbyentryid_41_rows_greater_than_1_42 >> rail.Label('No') >> trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_singlerecord_v2_0async_45 >> \
            insert_to_timeoff_dag_run_list_45 >> foreach_query_list_getunique_entryid_39_40_end
        foreach_query_list_getunique_entryid_39_40 >> foreach_query_list_getunique_entryid_39_40_end >> is_timeoff_trigger_runs_avaialbale
        is_timeoff_trigger_runs_avaialbale >> rail.Label('Yes') >> \
            wait_for_completion_trigger_dag_run_live_intercontinentalexchange_timeoff_import_child_v2_0async_45 >> \
            create_csv_lines_45 >> upload_uploadlogs_45 >> get_logged_errors_45 >> \
            get_logged_exception_45 >> email_subject_line_45 >> send_mail_45 >> log_to_sumo >> can_fail_dag >> rail.Label('Yes') >> fail_dagrun
        is_timeoff_trigger_runs_avaialbale >> rail.Label(
            'No') >> create_csv_lines_45

    return dag


rail.for_each_instance(create_dag)
