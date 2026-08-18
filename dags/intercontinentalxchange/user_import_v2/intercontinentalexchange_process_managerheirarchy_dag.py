
from datetime import timedelta, datetime
import hashlib
import pendulum
from airflow.models import Variable
from rail.lib.ecid import get_dagrun_ecid
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_process_managerheirarchy_v2_{config.instance}',
        description=f'intercontinentalexchange_Process_ManagerHeirarchy v2.0 {config.instance}',
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
            path=config.manage_hierarhy_input_filepath,
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
            no_task='has_input_filename_ends_with_csv'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_input_filename_ends_with_csv',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_input_filename_ends_with_csv = rail.IfOperator(
            task_id="has_input_filename_ends_with_csv",
            test='{{ result("new_file_sensor").split(".")[-1] | lower == "csv" | lower if result("new_file_sensor") else False }}',
            yes_task="download_2",
            no_task="archive_incorrect_file",
        )

        archive_incorrect_file = rail.SFTPMoveFileOperator(
            task_id='archive_incorrect_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        send_mail_for_incorrect_file = rail.EmailOperator(
            task_id='send_mail_for_incorrect_file',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon Manager Hierarchy import - skipped {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> Replicon Manager Hierarchy import for {{get_company_key()}}, created on {{ current_time_in_specified_tz("US/Pacific", "%Y-%m-%dT%H:%M:%S") }} skipped, since the file format is incorrect </p>
            <p> File name : {{ result('new_file_sensor') | file_name }} </p>
            <p>Please send the correct input file in csv file format.<br />
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>''',
            params=None,
        )

        download_2 = rail.SFTPDownloadFileOperator(
            task_id='download_2',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        rename_archivetheinputfile_3 = rail.SFTPMoveFileOperator(
            task_id='rename_archivetheinputfile_3',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.manage_hierarhy_archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        parse_csv_4 = rail.LoadCSVFileOperator(
            task_id="parse_csv_4",
            document="{{result('download_2')}}"
        )

        def get_formated_user_row(item):
            item_val_md5 = hashlib.md5(
                ('_'.join(val if val else '' for val in item.values())).encode())
            user_md5 = item_val_md5.hexdigest()
            item_val_md5 = hashlib.md5((
                item["PERSON_NUMBER"]+"_" +
                item["EFFECTIVE_DATE"]+"_" +
                item['MGR_HRCHY_F']).encode()).hexdigest()
            return {
                "pernsonnumber": item["PERSON_NUMBER"].strip() if item["PERSON_NUMBER"] else "",
                "name": item["EFFECTIVE_DATE"].strip() if item["EFFECTIVE_DATE"] else "",
                "MGR_HRCHY": item["MGR_HRCHY_F"].strip() if item["MGR_HRCHY_F"] else "",
                "md5": user_md5
            }.values()

        create_csv_lines_5 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_5',
            source="{{ result('parse_csv_4') }}",
            header=['pernsonnumber',
                    'name',
                    'MGR_HRCHY',
                    'md5'],
            row=get_formated_user_row,
        )

        download_6 = rail.SFTPDownloadFileOperator(
            task_id='download_6',
            remote_filepath=config.manage_hierarhy_referance_filepath +
            "/ICE_Replicon_MgrHrchy_Reference.csv"
        )

        load_csv_create_list_from_csv_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_7",
            document="{{ result('create_csv_lines_5') }}",
        )

        create_collection_create_list_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_7',
            source="{{ result('load_csv_create_list_from_csv_7') }}",
            name="inputfilewithmd5",
            columns={
                'pernsonnumber': 'person_number',
                'name': 'effectivedate',
                'MGR_HRCHY': 'MGR_HRCHY',
                'md5': 'md5'
            }
        )

        query_list_inputfilerecords_8 = rail.QueryCollectionOperator(
            task_id='query_list_inputfilerecords_8',
            query="""SELECT * FROM  inputfilewithmd5""",
        )

        if_query_list_inputfilerecords_greater_than_8 = rail.IfOperator(
            task_id='if_query_list_inputfilerecords_greater_than_8',
            test='{{ result("query_list_inputfilerecords_8", "length") > 0 }}',
            yes_task="load_csv_create_list_from_csv_9",
            no_task="finish",
        )

        load_csv_create_list_from_csv_9 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_9",
            document="{{ result('download_6') }}",
        )

        create_collection_create_list_from_csv_9 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_9',
            source="{{ result('load_csv_create_list_from_csv_9') }}",
            name="referencefilewithmd5",
            columns={
                'pernsonnumber': 'person_number',
                'name': 'effectivedate',
                'MGR_HRCHY': 'MGR_HRCHY',
                'md5': 'md5'
            }
        )

        query_list_referencefilerecords_10 = rail.QueryCollectionOperator(
            task_id='query_list_referencefilerecords_10',
            query="""SELECT * FROM  referencefilewithmd5""",
        )

        declare_list_11 = rail.SetVariableOperator(
            task_id='declare_list_11',
            append=False,
            name='importlogger',
            value=[]
        )

        query_list_identify_unchangedrecords_12 = rail.QueryCollectionOperator(
            task_id='query_list_identify_unchangedrecords_12',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        if_query_list_identify_unchangedrecords_12_rows_greater_than_0_13 = rail.IfOperator(
            task_id='if_query_list_identify_unchangedrecords_12_rows_greater_than_0_13',
            test='{{ result("query_list_identify_unchangedrecords_12", "length") > 0 }}',
            yes_task="insert_to_list_14",
            no_task="query_list_identify_changedrecords_15",
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_ignore_list(dag_run, task_name):
            ignore_list = []
            unchanged_records = get_data_from_document(rail.result(task_name))
            for unchanged_user in unchanged_records:
                ignore_list.append({
                    "empid": unchanged_user['person_number'],
                    "action": "pre-check",
                    "status": "Ignored",
                    "details": "No changes in user manager hierarchy",
                    "jobid": get_dagrun_ecid(dag_run),
                    "effectivedate": unchanged_user['effectivedate']
                })
            return ignore_list

        insert_to_list_14 = rail.PythonOperator(
            task_id='insert_to_list_14',
            python_callable=lambda dag_run: get_ignore_list(
                dag_run, 'query_list_identify_unchangedrecords_12')
        )

        query_list_identify_changedrecords_15 = rail.QueryCollectionOperator(
            task_id='query_list_identify_changedrecords_15',
            query="""SELECT * FROM  inputfilewithmd5 WHERE  inputfilewithmd5.md5 NOT IN (SELECT  referencefilewithmd5.md5 FROM  referencefilewithmd5)""",
        )

        create_list_16 = rail.CreateCollectionOperator(
            task_id='create_list_16',
            source="{{ result('query_list_identify_changedrecords_15') }}",
            name="changedrecordslist",
        )

        query_list_changedrecordswithout_mandatoryfields_17 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswithout_mandatoryfields_17',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.person_number= "" OR  changedrecordslist.effectivedate= "" OR  changedrecordslist.MGR_HRCHY= "" OR  changedrecordslist.person_number IS NULL OR  changedrecordslist.effectivedate IS NULL OR  changedrecordslist.MGR_HRCHY IS NULL OR  changedrecordslist.MGR_HRCHY= "-")""",
        )

        if_query_list_changedrecordswithout_mandatoryfields_17_rows_greater_than_0_18 = rail.IfOperator(
            task_id='if_query_list_changedrecordswithout_mandatoryfields_17_rows_greater_than_0_18',
            test='{{ result("query_list_changedrecordswithout_mandatoryfields_17", "length") > 0 }}',
            yes_task="insert_to_list_19",
            no_task="log_formatteddateandtime_20",
        )

        def get_mandatory_missing_list(dag_run, task_name):
            ignore_list = []
            mandatory_missing_records = get_data_from_document(
                rail.result(task_name))
            for errored_user in mandatory_missing_records:
                ignore_list.append({
                    "empid": errored_user['person_number'],
                    "action": "pre-check",
                    "status": "Ignored",
                    "details": "One or more mandatory fields are blank/invalid",
                    "jobid": get_dagrun_ecid(dag_run),
                    "effectivedate": errored_user['effectivedate']
                })
            return ignore_list

        insert_to_list_19 = rail.PythonOperator(
            task_id='insert_to_list_19',
            python_callable=lambda dag_run: get_mandatory_missing_list(
                dag_run, 'query_list_changedrecordswithout_mandatoryfields_17')
        )

        log_formatteddateandtime_20 = rail.PythonOperator(
            task_id='log_formatteddateandtime_20',
            python_callable=lambda: "{{ current_time('%m/%d/%Y') }}"
        )

        query_list_changedrecordswith_mandatoryfields_21 = rail.QueryCollectionOperator(
            task_id='query_list_changedrecordswith_mandatoryfields_21',
            query="""SELECT * FROM  changedrecordslist WHERE ( changedrecordslist.person_number != "" AND  changedrecordslist.effectivedate != "" AND  changedrecordslist.MGR_HRCHY!= "" AND   changedrecordslist.person_number IS NOT NULL AND  changedrecordslist.effectivedate IS NOT NULL AND  changedrecordslist.MGR_HRCHY IS NOT NULL AND  changedrecordslist.MGR_HRCHY != "-")""",
        )

        create_list_changedrecordswith_mandatoryfields_22 = rail.CreateCollectionOperator(
            task_id='create_list_changedrecordswith_mandatoryfields_22',
            source="{{ result('query_list_changedrecordswith_mandatoryfields_21') }}",
            name="changedrecords",
        )

        if_query_list_changedrecordswith_mandatoryfields_21_rows_greater_than_0_23 = rail.IfOperator(
            task_id='if_query_list_changedrecordswith_mandatoryfields_21_rows_greater_than_0_23',
            test='{{ result("query_list_changedrecordswith_mandatoryfields_21", "length") > 0 }}',
            yes_task="get_report_details_24",
            no_task="invoke_custom_ruby_code_50",
        )

        get_report_details_24 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_24',
            report_name=config.user_managerhierarchy_report,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details_24').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data_24 = rail.IfOperator(
            task_id="report_has_data_24",
            test="{{ result('run_report.get_report_result','has_data')}}",
            yes_task='if_generate_report_24_payload_starts_with_nodata_25',
            no_task='finish'
        )

        if_generate_report_24_payload_starts_with_nodata_25 = rail.IfOperator(
            task_id='if_generate_report_24_payload_starts_with_nodata_25',
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('No Data') }}",
            yes_task="stop_26",
            no_task="if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27",
        )

        stop_26 = rail.FailOperator(
            task_id='stop_26',
            message='''No Data in the base report'''
        )

        if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27 = rail.IfOperator(
            task_id='if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27',
            # pylint: disable=line-too-long
            test="{{ result('run_report.get_report_result').reportGenerationResults[0].payload | starts_with('Employee ID,User Name,Manager Hierarchy (Current) (Full Path),UserUri')}}",
            yes_task="parse_csv_29",
            no_task="stop_28",
        )

        parse_csv_29 = rail.LoadCSVFileOperator(
            task_id='parse_csv_29',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_csv_lines_30 = rail.CreateCollectionOperator(
            task_id='create_csv_lines_30',
            name='currentassignmentlist',
            source="{{ result('parse_csv_29') }}",
            columns={
                'Employee ID': 'person_number',
                'User Name': 'name',
                'Manager Hierarchy (Current) (Full Path)': 'MGR_HRCHY',
                'UserUri': 'useruri'
            }
        )

        stop_28 = rail.FailOperator(
            task_id='stop_28',
            message='''Base report column order doesn't match'''
        )

        query_list_32 = rail.QueryCollectionOperator(
            task_id='query_list_32',
            query="""SELECT * FROM  currentassignmentlist""",
        )

        def get_service_full_path(center_collection):
            center_names = [a['textValue'] for a in center_collection]
            return rail.smartjoin_by_delim(center_names, "|")

        def get_service_parent(center_collection, props_value):
            center_names = [a[props_value] for a in center_collection]
            if center_names and len(center_names) <= 1:
                return center_names[0] if len(center_names) == 1 else None
            first_two_records = rail.smartjoin_by_delim(
                center_names, "|").split('|')[:2]
            return first_two_records[-1]

        def get_filtered_groups_data(response):
            data = response.json()['d']['rows']
            groups_info = list(map(lambda item: {
                "fullpath": get_service_full_path(item['cells'][0]['cellCollection']),
                "uri": item['cells'][1]['uri'],
                "name": item['cells'][1].get('textValue'),
                "parent": get_service_parent(item['cells'][0]['cellCollection'], 'textValue'),
                "parenturi": get_service_parent(item['cells'][0]['cellCollection'], 'uri'),
            }, data))

            return groups_info if groups_info else []

        service_center_list_service1svc_get_data_33 = rail.RepliconServiceOperator(
            task_id='service_center_list_service1svc_get_data_33',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                    "page": "1",
                    "pagesize": "10000",
                    "columnUris": [
                        "urn:replicon:service-center-list-column:full-path",
                        "urn:replicon:service-center-list-column:service-center"
                    ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        def is_hierarchy_one_present(changed_user):
            hierarchy_one_name = changed_user['MGR_HRCHY'].split(
                '|')[-1] if "|" in changed_user['MGR_HRCHY'] else changed_user['MGR_HRCHY']
            hierarchy_one = rail.find_first_by_attr_and_get_attr(rail.result(
                'service_center_list_service1svc_get_data_33'), "name", hierarchy_one_name, 'name')
            hierarhy_one_present = "Yes" if hierarchy_one else "No"
            return hierarhy_one_present

        def is_hierarchy_present(changed_user, hierarhy_number):
            hierarhy_one_present = "No"
            hierarchy_name_arr = changed_user['MGR_HRCHY'].split(
                '|') if "|" in changed_user['MGR_HRCHY'] else []
            if len(hierarchy_name_arr) >= hierarhy_number:
                hierarchy_name = hierarchy_name_arr[-hierarhy_number]
                existing_hierarchy_name = rail.find_first_by_attr_and_get_attr(rail.result(
                    'service_center_list_service1svc_get_data_33'), "name", hierarchy_name, 'name')
                hierarhy_one_present = "Yes" if existing_hierarchy_name else "No"
            return hierarhy_one_present

        def get_input_file(task_name):
            input_list = []
            changed_records = get_data_from_document(rail.result(task_name))
            for changed_user in changed_records:
                changed_user_fullpath = changed_user['MGR_HRCHY'].split(
                    '|') if "|" in changed_user['MGR_HRCHY'] else [changed_user['MGR_HRCHY']]
                full_path_len = len(changed_user_fullpath)
                print("changed_user_fullpath", changed_user_fullpath)
                input_list.append(
                    {
                        "fullpath": changed_user['MGR_HRCHY'],
                        "present": "Yes" if rail.find_first_by_attr_and_get_attr(rail.result('service_center_list_service1svc_get_data_33'), 'fullpath', changed_user['MGR_HRCHY'], 'fullpath') else False,
                        "heirarchyone": changed_user_fullpath[:full_path_len][-1] if full_path_len >= 1 else None,
                        "heirarchyonepresent": is_hierarchy_one_present(changed_user),
                        "heirarchytwo": changed_user_fullpath[:full_path_len][-2] if full_path_len >= 2 else None,
                        "heirarchytwopresent": is_hierarchy_present(changed_user, 2),
                        "heirarchythree": changed_user_fullpath[:full_path_len][-3] if full_path_len >= 3 else None,
                        "heirarchythreepresent": is_hierarchy_present(changed_user, 3),
                        "heirarchyfour": changed_user_fullpath[:full_path_len][-4] if full_path_len >= 4 else None,
                        "heirarchyfourpresent": is_hierarchy_present(changed_user, 4),
                        "heirarchyfive": changed_user_fullpath[:full_path_len][-5] if full_path_len >= 5 else None,
                        "heirarchyfivepresent": is_hierarchy_present(changed_user, 5),
                        "heirarchysix": changed_user_fullpath[:full_path_len][-6] if full_path_len >= 6 else None,
                        "heirarchysixpresent": is_hierarchy_present(changed_user, 6),
                        "heirarchyseven": changed_user_fullpath[:full_path_len][-7] if full_path_len >= 7 else None,
                        "heirarchysevenpresent": is_hierarchy_present(changed_user, 7),
                    }
                )
            return input_list

        invoke_custom_ruby_code_35 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_35',
            python_callable=lambda: get_input_file(
                'query_list_changedrecordswith_mandatoryfields_21')
        )

        def get_current_hierarchy(current_assignments, changed_user):
            current_hierarchy_info = rail.find_first_by_attr_and_get_attr(
                current_assignments, 'person_number', changed_user['person_number'], 'MGR_HRCHY')
            current_hierarchy = current_hierarchy_info.replace(
                " / ", "|") if current_hierarchy_info else None
            return current_hierarchy

        def get_process_list(current_assigment_task, changed_record_task):
            process_list = []
            current_assignments = get_data_from_document(
                rail.result(current_assigment_task))
            changed_records = get_data_from_document(
                rail.result(changed_record_task))

            for changed_user in changed_records:
                current_hierarchy = get_current_hierarchy(
                    current_assignments, changed_user)
                process_list.append(
                    {
                        "user": rail.find_first_by_attr_and_get_attr(
                            current_assignments, 'person_number', changed_user['person_number'], 'name'),
                        "useruri": rail.find_first_by_attr_and_get_attr(
                            current_assignments, 'person_number', changed_user['person_number'], 'useruri'),
                        "currentvalue": current_hierarchy,
                        "recivedvalue": changed_user['MGR_HRCHY'],
                        "changerequired": "no" if current_hierarchy == changed_user['MGR_HRCHY'] else "yes",
                        "effiectivedate": changed_user['effectivedate'],
                        "effectivedatedate": datetime.strptime(
                            changed_user['effectivedate'], '%Y%m%d').day,
                        "effectivedatemonth": datetime.strptime(
                            changed_user['effectivedate'], '%Y%m%d').month,
                        "effectivedateyear": datetime.strptime(
                            changed_user['effectivedate'], '%Y%m%d').year,
                        "recivedvalueuri": rail.find_first_by_attr_and_get_attr(
                            rail.result('service_center_list_service1svc_get_data_33'), 'fullpath', changed_user['MGR_HRCHY'], 'uri'),
                    }
                )

            return process_list

        invoke_custom_ruby_code_36 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_36',
            python_callable=lambda: get_process_list(
                'query_list_32', 'query_list_changedrecordswith_mandatoryfields_21')
        )

        create_list_37 = rail.CreateCollectionOperator(
            task_id='create_list_37',
            source="{{ result('invoke_custom_ruby_code_36') | to_json }}",
            name="processlist",
        )

        query_list_39 = rail.QueryCollectionOperator(
            task_id='query_list_39',
            query="""SELECT * from  processlist WHERE processlist.useruri IS NULL OR processlist.useruri = ''""",
        )

        if_query_list_39_rows_greater_than_0_40 = rail.IfOperator(
            task_id='if_query_list_39_rows_greater_than_0_40',
            test='{{ result("query_list_39", "length") > 0 }}',
            yes_task="insert_to_list_41",
            no_task="foreach_output_42",
        )

        def get_missing_uri_list(dag_run, task_name):
            ignore_list = []
            mandatory_missing_records = get_data_from_document(
                rail.result(task_name))
            for errored_user in mandatory_missing_records:
                ignore_list.append({
                    "empid": errored_user['user'],
                    "action": "pre-check",
                    "status": "Exception",
                    "details": "User is not available/enabled in Replicon",
                    "jobid": get_dagrun_ecid(dag_run),
                    "effectivedate": errored_user['effiectivedate']
                })
            return ignore_list

        insert_to_list_41 = rail.PythonOperator(
            task_id='insert_to_list_41',
            python_callable=lambda dag_run: get_missing_uri_list(
                dag_run, 'query_list_39')
        )

        foreach_output_42 = rail.ForEachOperator(
            task_id='foreach_output_42',
            items="{{ result('invoke_custom_ruby_code_36') | to_json }}",
            start_task='if_foreach_output_42_useruri_present_43',
            end_task='foreach_output_42_end'
        )

        if_foreach_output_42_useruri_present_43 = rail.IfOperator(
            task_id='if_foreach_output_42_useruri_present_43',
            test='''{{ result('foreach_output_42').useruri | is_truthy  and result('foreach_output_42').changerequired == 'yes'  and result('foreach_output_42').recivedvalueuri | is_truthy }}''',
            yes_task="updateservice_center_schedule_45",
            no_task="if_foreach_output_42_useruri_present_47",
        )

        updateservice_center_schedule_45 = rail.RepliconServiceOperator(
            task_id='updateservice_center_schedule_45',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ result('foreach_output_42').useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "serviceCenterScheduleToApply": {
                        "userServiceCenterScheduleModificationOptionUri": "urn:replicon:schedule-modification-option:update-schedule-over-date-range",
                        "replacementServiceCenterSchedule": [],
                        "updateServiceCenterScheduleOverDateRange": {
                            "replacementServiceCenterScheduleEntries": [
                                {
                                    "serviceCenter": {
                                        "uri": "{{ result('foreach_output_42').recivedvalueuri }}",
                                        "parentUri": null,
                                        "name": null
                                    },
                                    "effectiveDate": {
                                        "year": "{{ result('foreach_output_42').effectivedateyear }}",
                                        "month": "{{ result('foreach_output_42').effectivedatemonth }}",
                                        "day": "{{ result('foreach_output_42').effectivedatedate }}"
                                    }
                                }
                            ],
                            "endDate": null
                        }
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        insert_to_list_46 = rail.SetVariableOperator(
            task_id='insert_to_list_46',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "empid": "{{ result('foreach_output_42').user }}",
                "action": "update",
                "status": "Success",
                "details": "Manager Hierarchy updated",
                "jobid": "{{ dag_run_ecid() }}",
                "childjob": null,
                "effectivedate": "{{ result('foreach_output_42').effiectivedate }}"
            }
        )

        if_foreach_output_42_useruri_present_47 = rail.IfOperator(
            task_id='if_foreach_output_42_useruri_present_47',
            test='''{{ result('foreach_output_42').useruri | is_truthy }}''',
            yes_task="insert_to_list_47",
            no_task="foreach_output_42_end",
        )

        insert_to_list_47 = rail.SetVariableOperator(
            task_id='insert_to_list_47',
            append=True,
            name='{{ result("declare_list_11").name }}',
            value={
                "empid": "{{ result('foreach_output_42').user }}",
                "action": "pre-check",
                "status": "Ignored",
                "details": "Manager Hierarchy ignored",
                "jobid": "{{ dag_run_ecid() }}",
                "childjob": null,
                "effectivedate": "{{ result('foreach_output_42').effiectivedate }}"
            }
        )

        foreach_output_42_end = rail.EmptyOperator(
            task_id='foreach_output_42_end',
        )

        def get_merge_log_list():
            ignore_list1 = rail.result('insert_to_list_14')
            log_list1 = ignore_list1 if ignore_list1 else []
            ignore_listt2 = rail.result('insert_to_list_19')
            log_list2 = ignore_listt2 if ignore_listt2 else []
            ignore_listt3 = rail.result('insert_to_list_41')
            log_list3 = ignore_listt3 if ignore_listt3 else []
            process_list = rail.get_dag_run_var(
                rail.result('declare_list_11')['name'])
            log_list4 = process_list if process_list else []
            return [*log_list1, *log_list2, *log_list3, *log_list4]

        invoke_custom_ruby_code_50 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_50',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda: get_merge_log_list()
        )

        if_output_loggers_greater_than_0_51 = rail.IfOperator(
            task_id='if_output_loggers_greater_than_0_51',
            test='''{{ result('invoke_custom_ruby_code_50') | length > 0 }}''',
            yes_task="create_csv_lines_52",
            no_task="rename_archivethereferncefile_63",
        )

        create_csv_lines_52 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_52',
            source="{{ result('invoke_custom_ruby_code_50') | to_json }}",
            header=['employeeid',
                    'effectivedate',
                    'action',
                    'status',
                    'details',
                    'jobid'],
            row=[
                "{{ item.empid }}",
                "{{ item.effectivedate }}",
                "{{ item.action }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}"
            ],
        )

        upload_54 = rail.SFTPUploadFileOperator(
            task_id='upload_54',
            content='''{{ result('create_csv_lines_52') }}''',
            remote_filepath=config.manage_hierarhy_log_filepath + '/' +
            '''{{ dag_run_ecid() }}_ManagerhierarchyLogs_{{ result('get_time_for_file') }}.csv''',
        )

        def has_error_in_logs():
            hierarchy_one = rail.find_first_by_attr_and_get_attr(rail.result(
                'invoke_custom_ruby_code_50'), "status", 'Error', 'status')
            return bool(hierarchy_one)

        if_wherestatuserror_presentcompletedwitherrorsnil_present_58 = rail.IfOperator(
            task_id='if_wherestatuserror_presentcompletedwitherrorsnil_present_58',
            test=has_error_in_logs,
            yes_task="send_mail_59",
            no_task="send_mail_61",
        )

        send_mail_59 = rail.EmailOperator(
            task_id='send_mail_59',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Manager Hierarchy import completed with errors - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon Manager heirarchy import is completed with errors on {{ current_time() }}. Please find the  log file details below for reference: <br /> <br />
            File name: /IntercontinentalExchangeafmig/Manager Hierarchy/Logs/ {{ dag_run_ecid() }}_ManagerhierarchyLogs_{{ result('get_time_for_file') }}.csv<br />
            </p>''',
            params=None,
        )

        send_mail_61 = rail.EmailOperator(
            task_id='send_mail_61',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Manager Hierarchy import completed successfully - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon Manager heirarchy import is completed successfully on {{ current_time() }}. Please find the  log file details below for reference: <br /> <br />
            File path: {{params.log_file_path}} <br />
            File name: {{ dag_run_ecid() }}_ManagerhierarchyLogs_{{ result('get_time_for_file') }}.csv<br />
            </p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'log_file_path': config.manage_hierarhy_log_filepath}
        )

        rename_archivethereferncefile_63 = rail.SFTPMoveFileOperator(
            task_id='rename_archivethereferncefile_63',
            existing_filename=config.manage_hierarhy_referance_filepath +
            '/' + "ICE_Replicon_MgrHrchy_Reference.csv",
            new_filename=config.manage_hierarhy_archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-') }}_Old_ICE_Replicon_MgrHrchy_Reference.csv"
        )

        create_csv_lines_new_referance_rawdata_64 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_new_referance_rawdata_64',
            source="{{ result('create_collection_create_list_from_csv_7') }}",
            header=['pernsonnumber',
                    'name',
                    'MGR_HRCHY',
                    'md5'],
            row=[
                "{{ item.person_number }}",
                "{{ item.effectivedate }}",
                "{{ item.MGR_HRCHY }}",
                "{{ item.md5 }}"
            ],
        )

        upload_uploadnewreference_64 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadnewreference_64',
            content="{{ result('create_csv_lines_new_referance_rawdata_64') }}",
            remote_filepath=config.manage_hierarhy_referance_filepath +
            '/' +
            "ICE_Replicon_MgrHrchy_Reference.csv",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        new_file_sensor >> get_time_for_file >> was_new_file_found
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> has_input_filename_ends_with_csv
        has_input_filename_ends_with_csv >> rail.Label(
            'No') >> archive_incorrect_file >> send_mail_for_incorrect_file >> finish
        has_input_filename_ends_with_csv >> rail.Label('Yes') >>\
            download_2 >> rename_archivetheinputfile_3 >> parse_csv_4 >> create_csv_lines_5 >> download_6 >> load_csv_create_list_from_csv_7 >> create_collection_create_list_from_csv_7 >>\
            query_list_inputfilerecords_8 >> if_query_list_inputfilerecords_greater_than_8
        if_query_list_inputfilerecords_greater_than_8 >> rail.Label(
            'No') >> finish
        if_query_list_inputfilerecords_greater_than_8 >> rail.Label('Yes') >> load_csv_create_list_from_csv_9 >> create_collection_create_list_from_csv_9 >> query_list_referencefilerecords_10 >>\
            declare_list_11 >> query_list_identify_unchangedrecords_12 >> if_query_list_identify_unchangedrecords_12_rows_greater_than_0_13
        if_query_list_identify_unchangedrecords_12_rows_greater_than_0_13 >> rail.Label(
            'Yes') >> insert_to_list_14 >> query_list_identify_changedrecords_15
        if_query_list_identify_unchangedrecords_12_rows_greater_than_0_13 >> rail.Label(
            'No') >> query_list_identify_changedrecords_15 >> create_list_16 >> query_list_changedrecordswithout_mandatoryfields_17 >>\
            if_query_list_changedrecordswithout_mandatoryfields_17_rows_greater_than_0_18
        if_query_list_changedrecordswithout_mandatoryfields_17_rows_greater_than_0_18 >> rail.Label(
            'Yes') >> insert_to_list_19 >> log_formatteddateandtime_20
        if_query_list_changedrecordswithout_mandatoryfields_17_rows_greater_than_0_18 >> rail.Label(
            'No') >> log_formatteddateandtime_20 >> query_list_changedrecordswith_mandatoryfields_21 >> create_list_changedrecordswith_mandatoryfields_22 >>\
            if_query_list_changedrecordswith_mandatoryfields_21_rows_greater_than_0_23
        if_query_list_changedrecordswith_mandatoryfields_21_rows_greater_than_0_23 >> rail.Label('No') >>\
            invoke_custom_ruby_code_50
        if_query_list_changedrecordswith_mandatoryfields_21_rows_greater_than_0_23 >> rail.Label(
            'Yes') >> get_report_details_24 >> run_report_group_entry
        run_report_group_exit >> report_has_data_24
        report_has_data_24 >> rail.Label(
            'Yes') >> if_generate_report_24_payload_starts_with_nodata_25
        report_has_data_24 >> rail.Label('No') >> finish
        if_generate_report_24_payload_starts_with_nodata_25 >> rail.Label(
            'Yes') >> stop_26
        if_generate_report_24_payload_starts_with_nodata_25 >> rail.Label(
            'No') >> if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27
        if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27 >> rail.Label(
            'Yes') >> stop_28
        if_generate_report_24_payload_not_starts_with_employeeidusernamemanagerhierarchycurrentfullpathuseruri_27 >> rail.Label(
            'No') >> parse_csv_29 >> create_csv_lines_30 >> query_list_32 >> service_center_list_service1svc_get_data_33 >>\
            invoke_custom_ruby_code_35 >> invoke_custom_ruby_code_36 >> create_list_37 >> query_list_39 >> if_query_list_39_rows_greater_than_0_40
        if_query_list_39_rows_greater_than_0_40 >> rail.Label(
            'Yes') >> insert_to_list_41 >> foreach_output_42
        if_query_list_39_rows_greater_than_0_40 >> rail.Label(
            'No') >> foreach_output_42 >> if_foreach_output_42_useruri_present_43
        if_foreach_output_42_useruri_present_43 >> rail.Label(
            'Yes') >> updateservice_center_schedule_45 >> insert_to_list_46 >> foreach_output_42_end
        if_foreach_output_42_useruri_present_43 >> rail.Label(
            'No') >> if_foreach_output_42_useruri_present_47
        if_foreach_output_42_useruri_present_47 >> rail.Label(
            'Yes') >> insert_to_list_47 >> foreach_output_42_end
        if_foreach_output_42_useruri_present_47 >> rail.Label(
            'No') >> foreach_output_42_end
        foreach_output_42 >> foreach_output_42_end >> invoke_custom_ruby_code_50 >> if_output_loggers_greater_than_0_51
        if_output_loggers_greater_than_0_51 >> rail.Label(
            'Yes') >> create_csv_lines_52 >> upload_54 >> if_wherestatuserror_presentcompletedwitherrorsnil_present_58
        if_wherestatuserror_presentcompletedwitherrorsnil_present_58 >> rail.Label(
            'Yes') >> send_mail_59 >> rename_archivethereferncefile_63
        if_wherestatuserror_presentcompletedwitherrorsnil_present_58 >> rail.Label(
            'No') >> send_mail_61 >> rename_archivethereferncefile_63
        if_output_loggers_greater_than_0_51 >> rail.Label(
            'No') >> rename_archivethereferncefile_63 >> create_csv_lines_new_referance_rawdata_64 >> \
            upload_uploadnewreference_64 >> finish

    return dag


rail.for_each_instance(create_dag)
