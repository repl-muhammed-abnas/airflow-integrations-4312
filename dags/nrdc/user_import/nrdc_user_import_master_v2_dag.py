
from datetime import datetime, timedelta, timezone
import itertools
from nrdc.user_import.utils import custom_method
from airflow.models import Variable
import rail
from rail.lib.ecid import get_dagrun_ecid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nrdc_userimport_master_v2_{config.instance}',
        description=f'NRDC_User import_Master_v2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id2,
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
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='can_run_batch_task',
            no_task='delete_this_dagrun'
        )

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
            python_callable=lambda: datetime.now(
                timezone.utc).strftime('%m_%d_%Y_T%H_%M_%S')
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        has_input_filename_ends_with_csv = rail.IfOperator(
            task_id="has_input_filename_ends_with_csv",
            test='{{ result("new_file_sensor").split(".")[-1] == "csv" if result("new_file_sensor") else False }}',
            yes_task="download_file",
            no_task="send_mail_7",
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename="{{ result('new_file_sensor') }}",
            new_filename=config.archive_filepath +
            "/Old_raw_input_{{ dag_run_ecid() | replace(':', '-') }}_{{ result('new_file_sensor') | \
                file_name }}_{{ result('get_time_for_file') }}"
        )

        send_mail_7 = rail.EmailOperator(
            task_id='send_mail_7',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import has been skipped - {{ current_time_in_specified_tz() }} ''',
            # pylint: disable=line-too-long
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The User Import job has been skipped, since the file {{ result('new_file_sensor') }} is not in .csv file format. Please correct the file name and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        stop_8 = rail.EmptyOperator(
            task_id='stop_8',

        )

        parse_input_csv = rail.LoadCSVFileOperator(
            task_id="parse_input_csv",
            document="{{result('download_file')}}"
        )

        create_csv_lines_rawdata_14_1 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_rawdata_14_1',
            source="{{ result('parse_input_csv') }}",
            header=['displayname',
                    'firstname',
                    'lastname',
                    'emailaddress',
                    'empid',
                    'empnumber',
                    'whencreated',
                    'whenchanged',
                    'office',
                    'logonname',
                    'accountstatus',
                    'department',
                    'memberof',
                    'title',
                    'md5'],
            row=custom_method.get_formated_user_row
        )

        create_input_list_collection = rail.CreateCollectionOperator(
            task_id='create_input_list_collection',
            name="inputlist",
            source="{{result('create_csv_lines_rawdata_14_1')}}",
            columns=['displayname',
                     'firstname',
                     'lastname',
                     'emailaddress',
                     'empid',
                     'empnumber',
                     'whencreated',
                     'whenchanged',
                     'office',
                     'logonname',
                     'accountstatus',
                     'department',
                     'memberof',
                     'title',
                     'md5']
        )

        get_all_imput_records = rail.QueryCollectionOperator(
            task_id='get_all_imput_records',
            query="SELECT * FROM inputlist",
        )

        input_has_any_data = rail.IfOperator(
            task_id='input_has_any_data',
            test='{{ result("get_all_imput_records", "length") < 1 }}',
            yes_task="send_mail_12",
            no_task="create_csv_lines_rawdata_14",
        )

        send_mail_12 = rail.EmailOperator(
            task_id='send_mail_12',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import - no records in file - {{ current_time_in_specified_tz() }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
                        <p>Hi ,</p>
                        <p>The User import is completed on{{ result('log_today_4') }}. There were no records in the file {{ result('new_file_sensor') }} to be processed.</p>
                        <p>For any queries, please contact our support team at https://support.deltek.com</p>
                        <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        create_csv_lines_rawdata_14 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_rawdata_14',
            source="{{ result('parse_input_csv') }}",
            header=['displayname',
                    'firstname',
                    'lastname',
                    'emailaddress',
                    'empid',
                    'empnumber',
                    'whencreated',
                    'whenchanged',
                    'office',
                    'logonname',
                    'accountstatus',
                    'department',
                    'memberof',
                    'title',
                    'md5'],
            row=custom_method.get_formated_user_row
        )

        if_query_list_changedrecords_16_rows_greater_than_0_17 = rail.IfOperator(
            task_id='if_query_list_changedrecords_16_rows_greater_than_0_17',
            test="{{ result('get_all_imput_records','length') > 0 }}",
            yes_task="load_csv_create_list_from_csv_emails_20",
            no_task="if_query_list_changedrecords_16_rows_less_than_1_113",
        )

        load_csv_create_list_from_csv_emails_20 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_emails_20",
            document="{{result('download_file')}}",
            headers=['displayname',
                     'firstname',
                     'lastname',
                     'emailaddress',
                     'empid',
                     'empnumber',
                     'whencreated',
                     'whenchanged',
                     'office',
                     'logonname',
                     'accountstatus',
                     'department',
                     'memberof',
                     'title',
                     'md5']
        )

        create_collection_create_list_from_csv_emails_20 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_emails_20',
            source="{{ result('load_csv_create_list_from_csv_emails_20') }}",
            name="emails",
            # todo update this map from actual csv header for key name
            columns={
                'email': 'email'
            }
        )

        query_list_combinedlistwithallcolumns_21 = rail.QueryCollectionOperator(
            task_id='query_list_combinedlistwithallcolumns_21',
            query="SELECT DISTINCT inputlist.*, emails.email FROM inputlist LEFT JOIN emails ON inputlist.emailaddress=emails.email",
        )

        def page_handler(request, result):
            if len(result['rows']) > 0:
                request['page'] += 1
                return request
            return None

        def all_result_data_handler(result):
            flaten_rows = list(itertools.chain(
                *list(map(lambda x: x['rows'], result))))
            return list(map(lambda row: {
                'username': row['cells'][0]['textValue'] if 'textValue' in row['cells'][0] else None,
                'employeeid': row['cells'][2]['textValue'] if 'textValue' in row['cells'][2] else None,
                'email': row['cells'][3]['textValue'] if 'textValue' in row['cells'][3] else None,
                'loginname': row['cells'][4]['textValue'],
                'useruri': row['cells'][4]['uri']

            }, flaten_rows))

        gettheuserdetailsreference_22 = rail.RepliconServicePageOperator(
            task_id="gettheuserdetailsreference_22",
            endpoint="/services/UserListService1.svc/GetData",
            data={
                "page": 1,
                "pagesize": 10000,
                "columnUris": [
                    "urn:replicon:user-list-column:user",
                    "urn:replicon:user-list-column:start-date",
                    "urn:replicon:user-list-column:employee-id",
                    "urn:replicon:user-list-column:email-address",
                    "urn:replicon:user-list-column:login-name"
                ],
                "sort": [],
                "filterExpression": null
            },
            page_handler=page_handler,
            all_result_data_handler=all_result_data_handler
        )

        get_all_reports_23 = rail.RepliconServiceOperator(
            task_id='get_all_reports_23',
            endpoint="/services/ReportService1.svc/GetAllReports",
        )

        create_csv_lines_25 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_25',
            source=lambda: rail.result('gettheuserdetailsreference_22'),
            header=['username',
                    'employeeid',
                    'email',
                    'loginname',
                    'useruri'],
            row=["{{item.username}}", "{{ item.employeeid }}", "{{ item.email }}",
                 "{{ item.loginname }}", "{{ item.useruri }}"],
        )

        create_collection_create_list_from_csv_26 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_26',
            source=lambda: rail.result('create_csv_lines_25'),
            name="allusers"
        )

        query_list_27 = rail.QueryCollectionOperator(
            task_id='query_list_27',
            query="""SELECT * FROM  allusers""",
        )

        declare_list_28 = rail.SetVariableOperator(
            task_id='declare_list_28',
            append=False,
            name='Locations',
            value=[]
        )

        get_enabled_locations_29 = rail.RepliconServiceOperator(
            task_id='get_enabled_locations_29',
            endpoint="/services/LocationService1.svc/GetEnabledLocations",
        )

        def get_location_list():
            all_locations = rail.result('get_enabled_locations_29')
            location_info = []
            for location in all_locations:
                location_info.append({
                    "name": location['displayText'],
                    "uri": location['uri']
                })
            return location_info

        location_list_info = rail.PythonOperator(
            task_id='location_list_info',
            python_callable=get_location_list
        )

        query_list_userstobe_updated_32 = rail.QueryCollectionOperator(
            task_id='query_list_userstobe_updated_32',
            query="SELECT * FROM inputlist WHERE LOWER( inputlist.emailaddress) IN (SELECT DISTINCT LOWER( allusers.email) FROM  allusers)"
        )

        query_list_userstobe_created_33 = rail.QueryCollectionOperator(
            task_id='query_list_userstobe_created_33',
            query="SELECT * FROM inputlist WHERE LOWER( inputlist.emailaddress) NOT IN (SELECT DISTINCT LOWER( allusers.email) FROM  allusers)",
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.user_report_name,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_report_details').uri}}",
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
            yes_task='load_report_data',
            no_task='stop_37'
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('run_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_user_collection = rail.CreateCollectionOperator(
            task_id='create_user_collection',
            name='userdata',
            source="{{ result('load_report_data') }}",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Email Notification': 'emailnotification',
                'ueruri': 'useruri',
                'User Status': 'userstatus',
                'Type': 'type'}
        )

        query_userdata = rail.QueryCollectionOperator(
            task_id='query_userdata',
            query='SELECT * FROM userdata'
        )

        query_report_user_has_data = rail.IfOperator(
            task_id="query_report_user_has_data",
            test="{{ result('query_userdata','length') > 0 }}",
            yes_task='query_user_has_data',
            no_task='stop_37'
        )

        query_user_has_data = rail.IfOperator(
            task_id="query_user_has_data",
            test="{{ result('query_list_userstobe_updated_32','length') > 0 }}",
            yes_task='foreach_query_list_userstobe_updated_32_39',
            no_task='log_listsize_80'
        )

        stop_37 = rail.FailOperator(
            task_id='stop_37',
            message="Error occurred while generating report"
        )

        foreach_query_list_userstobe_updated_32_39 = rail.ForEachOperator(
            task_id='foreach_query_list_userstobe_updated_32_39',
            items="{{ result('query_list_userstobe_updated_32') }}",
            start_task='declare_list_update_dag_runs',
            end_task='foreach_query_list_userstobe_updated_32_39_end'
        )

        declare_list_update_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_update_dag_runs',
            name='user_process_update_dag_runs',
            value=[]
        )

        declare_variable_40 = rail.SetVariableOperator(
            task_id='declare_variable_40',
            append=False,
            name='currentusercount',
            value=0
        )

        declare_variable_41 = rail.SetVariableOperator(
            task_id='declare_variable_41',
            append=False,
            name='currentuseruri',
            value=None
        )

        declare_variable_42 = rail.SetVariableOperator(
            task_id='declare_variable_42',
            append=False,
            name='currentuserlogin',
            value=None
        )

        declare_variable_43 = rail.SetVariableOperator(
            task_id='declare_variable_43',
            append=False,
            name='currentusetype',
            value=None
        )

        declare_variable_44 = rail.SetVariableOperator(
            task_id='declare_variable_44',
            append=False,
            name='location_update',
            value=None
        )

        log_loginname_45 = rail.PythonOperator(
            task_id='log_loginname_45',
            python_callable=lambda:  '''"=_('data.foreach.foreach_83dddc26_39.logonname').split("@").first"'''
        )

        if_foreach_83dddc26_39_logonname_present_46 = rail.IfOperator(
            task_id='if_foreach_83dddc26_39_logonname_present_46',
            test="{{ result('foreach_query_list_userstobe_updated_32_39').logonname | is_truthy }}",
            yes_task="log_location_uritoassign_47",
            no_task="query_list_togettheusercurrentcountifenabled_55",
        )

        def get_existing_location_uri():
            logon_name = rail.result('foreach_query_list_userstobe_updated_32_39')[
                'logonname']
            location_list = rail.result('location_list_info')
            location_info = list(
                filter(lambda x: x['name'] and x['name'].lower() == logon_name.lower(), location_list))
            location_uri = location_info[0]['uri'] if location_info else None
            return location_uri

        log_location_uritoassign_47 = rail.PythonOperator(
            task_id='log_location_uritoassign_47',
            # pylint: disable=line-too-long
            python_callable=get_existing_location_uri
        )

        def is_logon_name_present_for_update():
            logon_name = rail.result('foreach_query_list_userstobe_updated_32_39')[
                'logonname']
            location_list = rail.result('location_list_info')
            location_info = list(
                filter(lambda x: x['name'] and x['name'].lower() == logon_name.lower(), location_list))
            location_uri = location_info[0]['uri'] if location_info else None
            return bool(location_uri)

        if_log_location_uritoassign_47_blank_48 = rail.IfOperator(
            task_id='if_log_location_uritoassign_47_blank_48',
            test=is_logon_name_present_for_update,
            yes_task="update_variable_54",
            no_task="log_loginname_49",
        )

        log_loginname_49 = rail.PythonOperator(
            task_id='log_loginname_49',
            # pylint: disable=line-too-long
            python_callable=lambda:  '''"=_('data.foreach.foreach_83dddc26_39.logonname').present? ? _('data.foreach.foreach_83dddc26_39.logonname').downcase : nil"'''
        )

        create_new_draft_location_50 = rail.RepliconServiceOperator(
            task_id='create_new_draft_location_50',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
            data={
                "parentLocationUri": null
            }
        )

        def get_location_update_request():
            location_draft_uri = rail.result('create_new_draft_location_50')
            user_to_updated = rail.result(
                'foreach_query_list_userstobe_updated_32_39')
            return {
                "locationUri": location_draft_uri,
                "name": user_to_updated['logonname']
            }

        update_name_location_51 = rail.RepliconServiceOperator(
            task_id='update_name_location_51',
            endpoint="/services/LocationService1.svc/UpdateName",
            data=get_location_update_request
        )

        publish_draft_location_52 = rail.RepliconServiceOperator(
            task_id='publish_draft_location_52',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_location_50') }}"
            }
        )

        log_location_uritoassign_53 = rail.PythonOperator(
            task_id='log_location_uritoassign_53',
            python_callable=lambda:  rail.result(
                'publish_draft_location_52')['uri']
        )

        def get_location_update_uri():
            existing_location_uri = rail.result('log_location_uritoassign_47')
            new_location_uri = rail.result('log_location_uritoassign_53')
            return existing_location_uri or new_location_uri

        update_variable_54 = rail.SetVariableOperator(
            task_id='update_variable_54',
            append=False,
            name='{{ result("declare_variable_44").name }}',
            value=get_location_update_uri
        )

        query_list_togettheusercurrentcountifenabled_55 = rail.QueryCollectionOperator(
            task_id='query_list_togettheusercurrentcountifenabled_55',
            # pylint: disable=line-too-long
            query="SELECT * FROM  userdata WHERE LOWER( userdata.emailnotification)='{{ result('foreach_query_list_userstobe_updated_32_39').emailaddress }}' AND  userdata.userstatus='Enabled'",
        )

        if_query_list_togettheusercurrentcountifenabled_55_rows_greater_than_0_56 = rail.IfOperator(
            task_id='if_query_list_togettheusercurrentcountifenabled_55_rows_greater_than_0_56',
            test="{{ result('query_list_togettheusercurrentcountifenabled_55','length') > 0 }}",
            yes_task="update_variable_57",
            no_task="if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61",
        )

        update_variable_57 = rail.SetVariableOperator(
            task_id='update_variable_57',
            append=False,
            name='{{ result("declare_variable_40").name }}',
            value="{{ result('query_list_togettheusercurrentcountifenabled_55', key='length')}}"
        )

        def get_data_from_document(document):
            with rail.lib.readers.get_data_reader(document) as reader:
                return list(reader)

        def get_user_informations(category, task_name):
            user_profiles = get_data_from_document(rail.result(task_name))
            return [item[category] for item in user_profiles]

        update_variable_58 = rail.SetVariableOperator(
            task_id='update_variable_58',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=lambda: get_user_informations(
                'useruri', 'query_list_togettheusercurrentcountifenabled_55')
        )

        update_variable_59 = rail.SetVariableOperator(
            task_id='update_variable_59',
            append=False,
            name='{{ result("declare_variable_42").name }}',
            value=lambda: get_user_informations(
                'loginname', 'query_list_togettheusercurrentcountifenabled_55')
        )

        update_variable_60 = rail.SetVariableOperator(
            task_id='update_variable_60',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value=lambda: get_user_informations(
                'type', 'query_list_togettheusercurrentcountifenabled_55')
        )

        if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61 = rail.IfOperator(
            task_id='if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61',
            test="{{ result('query_list_togettheusercurrentcountifenabled_55','length') == 0 }}",
            yes_task="query_list_togettheusercurrentcountifdisabled_62",
            no_task="if_declare_variable_40_value_equals_to_1_68",
        )

        query_list_togettheusercurrentcountifdisabled_62 = rail.QueryCollectionOperator(
            task_id='query_list_togettheusercurrentcountifdisabled_62',
            # pylint: disable=line-too-long
            query="SELECT * FROM  userdata WHERE LOWER(userdata.emailnotification)='{{ result('foreach_query_list_userstobe_updated_32_39').emailaddress }}' AND  userdata.userstatus='Disabled'",
        )

        if_query_list_togettheusercurrentcountifdisabled_62_rows_greater_than_0_63 = rail.IfOperator(
            task_id='if_query_list_togettheusercurrentcountifdisabled_62_rows_greater_than_0_63',
            test="{{ result('query_list_togettheusercurrentcountifdisabled_62','length') > 0 }}",
            yes_task="update_variable_64",
            no_task="if_declare_variable_40_value_equals_to_1_68",
        )

        update_variable_64 = rail.SetVariableOperator(
            task_id='update_variable_64',
            append=False,
            name='{{ result("declare_variable_40").name }}',
            value="{{ result('query_list_togettheusercurrentcountifdisabled_62', key='length')}}"
        )

        update_variable_65 = rail.SetVariableOperator(
            task_id='update_variable_65',
            append=False,
            name='{{ result("declare_variable_41").name }}',
            value=lambda: get_user_informations(
                'useruri', 'query_list_togettheusercurrentcountifdisabled_62')
        )

        update_variable_66 = rail.SetVariableOperator(
            task_id='update_variable_66',
            append=False,
            name='{{ result("declare_variable_42").name }}',
            value=lambda: get_user_informations(
                'loginname', 'query_list_togettheusercurrentcountifdisabled_62')
        )

        update_variable_67 = rail.SetVariableOperator(
            task_id='update_variable_67',
            append=False,
            name='{{ result("declare_variable_43").name }}',
            value=lambda: get_user_informations(
                'type', 'query_list_togettheusercurrentcountifdisabled_62')
        )

        def is_current_user_1():
            current_user_count = rail.get_dag_run_var(
                rail.result('declare_variable_40')['name'])
            return bool(current_user_count == 1)

        if_declare_variable_40_value_equals_to_1_68 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_1_68',
            test=is_current_user_1,
            yes_task="get_c3_c4_update_data",
            no_task="if_declare_variable_40_value_equals_to_5_70",
        )

        def get_c3_c4_data(manager):
            c3_c4_data = []
            c3_c4_data.append(
                {
                    "firstname": rail.result('foreach_query_list_userstobe_updated_32_39')['firstname'],
                    "lastname": rail.result('foreach_query_list_userstobe_updated_32_39')['lastname'],
                    "emailaddress": rail.result('foreach_query_list_userstobe_updated_32_39')['emailaddress'],
                    "empnumber": rail.result('foreach_query_list_userstobe_updated_32_39')['empnumber'],
                    "whencreated": rail.result('foreach_query_list_userstobe_updated_32_39')['whencreated'],
                    "office": rail.result('foreach_query_list_userstobe_updated_32_39')['office'],
                    "logonname": rail.result('foreach_query_list_userstobe_updated_32_39')['logonname'],
                    "accountstatus": rail.result('foreach_query_list_userstobe_updated_32_39')['accountstatus'],
                    "department": rail.result('foreach_query_list_userstobe_updated_32_39')['department'],
                    "memberof": rail.result('foreach_query_list_userstobe_updated_32_39')['memberof'],
                    "manager": manager,
                    "title": rail.result('foreach_query_list_userstobe_updated_32_39')['title'],
                    "currentprofilecount": int(rail.get_dag_run_var(rail.result('declare_variable_40')['name'])),
                    "useruris": rail.get_dag_run_var(rail.result('declare_variable_41')['name']),
                    "employeeid": rail.result('foreach_query_list_userstobe_updated_32_39')['empid'],
                    "locationuri": rail.get_dag_run_var(rail.result('declare_variable_44')['name']),
                    "loginnames": rail.get_dag_run_var(rail.result('declare_variable_42')['name']),
                    "displayname": rail.result('foreach_query_list_userstobe_updated_32_39')['displayname'],
                    "currenttype": rail.get_dag_run_var(rail.result('declare_variable_43')['name'])
                }
            )

            return c3_c4_data

        get_c3_c4_update_data = rail.PythonOperator(
            task_id='get_c3_c4_update_data',
            python_callable=lambda: get_c3_c4_data("nosupervisor")
        )

        trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_69 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_69',
            retries=0,
            items=lambda: get_c3_c4_data("nosupervisor"),
            trigger_dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['emailaddress'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "manager": "nosupervisor",
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "useruris": item['useruris'],
                "employeeid": item['employeeid'],
                "locationuri": item['locationuri'],
                "loginnames": item['loginnames'],
                "displayname": item['displayname'],
                "currenttype": item['currenttype'],
            }
        )

        if_declare_variable_40_value_equals_to_5_70 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_5_70',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_40')['name']) == 5,
            yes_task="trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_71",
            no_task="if_declare_variable_40_value_equals_to_6_72",
        )

        trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_71 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_71',
            retries=0,
            items=lambda: get_c3_c4_data("na"),
            trigger_dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['emailaddress'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "manager": "manager",
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "useruris": item['useruris'],
                "employeeid": item['employeeid'],
                "locationuri": item['locationuri'],
                "loginnames": item['loginnames'],
                "displayname": item['displayname'],
                "currenttype": item['currenttype'],
            }
        )

        if_declare_variable_40_value_equals_to_6_72 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_6_72',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_40')['name']) == 6,
            yes_task="trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_73",
            no_task="if_declare_variable_40_value_equals_to_2_74",
        )

        trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_73 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_73',
            retries=0,
            items=lambda: get_c3_c4_data("na"),
            trigger_dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['emailaddress'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "manager": "manager",
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "useruris": item['useruris'],
                "employeeid": item['employeeid'],
                "locationuri": item['locationuri'],
                "loginnames": item['loginnames'],
                "displayname": item['displayname'],
                "currenttype": item['currenttype'],
            }
        )

        if_declare_variable_40_value_equals_to_2_74 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_2_74',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_40')['name']) == 2,
            yes_task="trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_75",
            no_task="if_declare_variable_40_value_equals_to_7_76",
        )

        trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_75 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_75',
            retries=0,
            items=lambda: get_c3_c4_data("na"),
            trigger_dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['emailaddress'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "manager": "manager",
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "useruris": item['useruris'],
                "employeeid": item['employeeid'],
                "locationuri": item['locationuri'],
                "loginnames": item['loginnames'],
                "displayname": item['displayname'],
                "currenttype": item['currenttype'],
            }
        )

        if_declare_variable_40_value_equals_to_7_76 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_7_76',
            test=lambda: rail.get_dag_run_var(
                rail.result('declare_variable_40')['name']) == 7,
            yes_task="trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_77",
            no_task="if_declare_variable_40_value_equals_to_3_78",
        )

        trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_77 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_77',
            retries=0,
            items=lambda: get_c3_c4_data("na"),
            trigger_dag_id=f'nrdc_updating_c3_c4_values_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "emailaddress": item['emailaddress'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "manager": "manager",
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "useruris": item['useruris'],
                "employeeid": item['employeeid'],
                "locationuri": item['locationuri'],
                "loginnames": item['loginnames'],
                "displayname": item['displayname'],
                "currenttype": item['currenttype'],
            }
        )

        if_declare_variable_40_value_equals_to_3_78 = rail.IfOperator(
            task_id='if_declare_variable_40_value_equals_to_3_78',
            # test="{{ result('declare_variable_40').value == 3  or result('declare_variable_40').value == 4 }}",
            # pylint: disable=line-too-long
            test=lambda: rail.get_dag_run_var(rail.result('declare_variable_40')[
                                              'name']) == 3 or rail.get_dag_run_var(rail.result('declare_variable_40')['name']) == 4,
            yes_task="nrdc_user_import_logs_add_entry_79",
            no_task="foreach_query_list_userstobe_updated_32_39_end",
        )

        nrdc_user_import_logs_add_entry_79 = rail.WriteLogOperator(
            task_id='nrdc_user_import_logs_add_entry_79',
            message="User profile not updated",
            severity="Failed",
            properties={
                # pylint: disable=line-too-long
                "user": "{{ result('foreach_query_list_userstobe_updated_32_39').firstname }} {{ result('foreach_query_list_userstobe_updated_32_39').lastname }}",
                "status": "Failed",
                "details": "User profile not updated as invalid number of user profiles were found",
                "action": "Update",
                "jobId": "{{ dag_run_ecid() }}"
            }
        )

        insert_to_user_update_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_user_update_dag_run_list',
            append=True,
            name='{{ result("declare_list_update_dag_runs").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_69") or result("trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_71") or result("trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_73") or result("trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_75") or result("trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_77"))[0]}}'
        )

        foreach_query_list_userstobe_updated_32_39_end = rail.EmptyOperator(
            task_id='foreach_query_list_userstobe_updated_32_39_end',
        )

        is_c3c4_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_c3c4_trigger_runs_avaialbale',
            test='''{{ result('insert_to_user_update_dag_run_list') | is_truthy }}''',
            yes_task="wait_for_completion_trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync",
            no_task="log_listsize_80",
        )

        wait_for_completion_trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_update_dag_run_list").value | to_json }}'
        )

        log_listsize_80 = rail.PythonOperator(
            task_id='log_listsize_80',
            python_callable=lambda:  '''30'''
        )

        pause_81 = rail.EmptyOperator(
            task_id='pause_81',
        )

        if_users_available_to_create_82 = rail.IfOperator(
            task_id='if_users_available_to_create_82',
            test="{{ result('query_list_userstobe_created_33','length') > 0 }}",
            yes_task="foreach_query_list_userstobe_created_33_82",
            no_task="log_listsize_106",
        )

        foreach_query_list_userstobe_created_33_82 = rail.ForEachOperator(
            task_id='foreach_query_list_userstobe_created_33_82',
            items="{{ result('query_list_userstobe_created_33') }}",
            start_task='declare_list_dag_runs',
            end_task='foreach_query_list_userstobe_created_33_82_end'
        )

        declare_list_dag_runs = rail.SetVariableOperator(
            task_id='declare_list_dag_runs',
            name='user_process_dag_runs',
            value=[]
        )

        declare_variable_83 = rail.SetVariableOperator(
            task_id='declare_variable_83',
            append=False,
            name='location_create',
            value=None
        )

        def get_location_uri():
            logon_name = rail.result('foreach_query_list_userstobe_created_33_82')[
                'logonname']
            location_list = rail.result('location_list_info')
            location_info = list(
                filter(lambda x: x['name'] == logon_name, location_list))
            location_uri = location_info[0]['uri'] if location_info else None
            return location_uri

        log_location_uritoassign_85 = rail.PythonOperator(
            task_id='log_location_uritoassign_85',
            python_callable=get_location_uri
        )

        if_log_location_uritoassign_85_blank_86 = rail.IfOperator(
            task_id='if_log_location_uritoassign_85_blank_86',
            test="{{ result('log_location_uritoassign_85') | is_truthy }}",
            yes_task="update_variable_91",
            no_task="create_new_draft_location_87",
        )

        create_new_draft_location_87 = rail.RepliconServiceOperator(
            task_id='create_new_draft_location_87',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
            data={
                "parentLocationUri": null
            }
        )

        update_name_location_88 = rail.RepliconServiceOperator(
            task_id='update_name_location_88',
            endpoint="/services/LocationService1.svc/UpdateName",
            data={
                "locationUri": "{{ result('create_new_draft_location_87') }}",
                "name": "{{ result('foreach_query_list_userstobe_created_33_82').logonname }}"
            }
        )

        publish_draft_location_89 = rail.RepliconServiceOperator(
            task_id='publish_draft_location_89',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_location_87') }}"
            }
        )

        log_location_uritoassign_90 = rail.PythonOperator(
            task_id='log_location_uritoassign_90',
            python_callable=lambda:  rail.result(
                'publish_draft_location_89')['uri']
        )

        def get_existing_or_new_location():
            existing_location_uri = rail.result('log_location_uritoassign_85')
            new_location_uri = rail.result('log_location_uritoassign_90')
            return existing_location_uri or new_location_uri

        update_variable_91 = rail.SetVariableOperator(
            task_id='update_variable_91',
            append=False,
            name='{{ result("declare_variable_83").name }}',
            value=get_existing_or_new_location
        )

        def is_memberof_contains_c3_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C3' in member_of and 'C4' not in member_of and 'Delegate' not in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_c3_only_c3_92 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_c3_only_c3_92',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c3_only,
            yes_task="get_add_user_data",
            no_task="if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94",
        )

        def get_add_user_details(c3_c4_option, manager):
            add_user_data = []
            empid_from_input = rail.result(
                'foreach_query_list_userstobe_created_33_82')['empid']
            empid_from_empid = empid_from_input if empid_from_input and '-' not in empid_from_input else None
            logon_name = rail.result('foreach_query_list_userstobe_created_33_82')[
                'logonname'].split('.')[0]
            derived_empid = empid_from_empid if empid_from_empid else logon_name
            location_uri = rail.get_dag_run_var(
                rail.result('declare_variable_83')['name'])
            add_user_data.append(
                {
                    "firstname": rail.result('foreach_query_list_userstobe_created_33_82')['firstname'],
                    "lastname": rail.result('foreach_query_list_userstobe_created_33_82')['lastname'],
                    "displayname": rail.result('foreach_query_list_userstobe_created_33_82')['displayname'],
                    "emailaddress": rail.result('foreach_query_list_userstobe_created_33_82')['emailaddress'],
                    "empid": derived_empid,
                    "empnumber": rail.result('foreach_query_list_userstobe_created_33_82')['empnumber'],
                    "whencreated": rail.result('foreach_query_list_userstobe_created_33_82')['whencreated'],
                    "office": rail.result('foreach_query_list_userstobe_created_33_82')['office'],
                    "logonname": rail.result('foreach_query_list_userstobe_created_33_82')['logonname'],
                    "accountstatus": rail.result('foreach_query_list_userstobe_created_33_82')['accountstatus'],
                    "department": rail.result('foreach_query_list_userstobe_created_33_82')['department'],
                    "memberof": rail.result('foreach_query_list_userstobe_created_33_82')['memberof'],
                    "Manager": manager,
                    "title": rail.result('foreach_query_list_userstobe_created_33_82')['title'],
                    "currentprofilecount": 1,
                    "c4orc3_present": c3_c4_option,
                    "locationuri": location_uri
                }
            )

            return add_user_data

        get_add_user_data = rail.PythonOperator(
            task_id='get_add_user_data',
            python_callable=lambda: get_add_user_details("C3 Only", "zshankaf")
        )

        trigger_dag_run_live_nrdc_add_user_v2async_93 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_93',
            retries=0,
            items=lambda: get_add_user_details("C3 Only", "zshankaf"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_c4_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C4' in member_of and 'C3' not in member_of and 'Delegate' not in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c4_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_95",
            no_task="if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_95 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_95',
            retries=0,
            items=lambda: get_add_user_details("C4", "ZAkhter"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_delegate_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'Delegate' in member_of and 'C3' not in member_of and 'C4' not in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96',
            # pylint: disable=line-too-long
            test=is_memberof_contains_delegate_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_97",
            no_task="if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_97 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_97',
            retries=0,
            items=lambda: get_add_user_details(
                "Delegate only", "nosupervisor"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_c3c4_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C3' in member_of and 'C4' in member_of and 'Delegate' not in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c3c4_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_99",
            no_task="if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_99 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_99',
            retries=0,
            items=lambda: get_add_user_details(
                "C4 and C3", "ZAkhter|zshankaf"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_c4_delegate_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C3' not in member_of and 'C4' in member_of and 'Delegate' in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c4_delegate_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_101",
            no_task="if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_101 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_101',
            retries=0,
            items=lambda: get_add_user_details(
                "Delegate and 1", "ZAkhter|nosupervisor"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_c3_delegate_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C3' in member_of and 'C4' not in member_of and 'Delegate' in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c3_delegate_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_103",
            no_task="if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_103 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_103',
            retries=0,
            items=lambda: get_add_user_details(
                "C3 and Delegate", "zshankaf|nosupervisor"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        def is_memberof_contains_c3_c4_delegate_only():
            member_of = rail.result('foreach_query_list_userstobe_created_33_82')[
                'memberof']
            if member_of and 'C3' in member_of and 'C4' in member_of and 'Delegate' in member_of:
                return True
            return False

        if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104 = rail.IfOperator(
            task_id='if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104',
            # pylint: disable=line-too-long
            test=is_memberof_contains_c3_c4_delegate_only,
            yes_task="trigger_dag_run_live_nrdc_add_user_v2async_105",
            no_task="foreach_query_list_userstobe_created_33_82_end",
        )

        trigger_dag_run_live_nrdc_add_user_v2async_105 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_live_nrdc_add_user_v2async_105',
            retries=0,
            items=lambda: get_add_user_details(
                "Delegate and all", "ZAkhter|zshankaf"),
            trigger_dag_id=f'nrdc_add_user_v2_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda dag_run, item: {
                "firstname": item['firstname'],
                "lastname": item['lastname'],
                "displayname": item['displayname'],
                "emailaddress": item['emailaddress'],
                "empid": item['empid'],
                "empnumber": item['empnumber'],
                "whencreated": item['whencreated'],
                "office": item['office'],
                "logonname": item['logonname'],
                "accountstatus": item['accountstatus'],
                "department": item['department'],
                "memberof": item['memberof'],
                "Manager": item['Manager'],
                "title": item['title'],
                "currentprofilecount": item['currentprofilecount'],
                "c4orc3_present": item['c4orc3_present'],
                "locationuri": item['locationuri'],
                "parentjobid": get_dagrun_ecid(dag_run)
            }
        )

        insert_to_user_dag_run_list = rail.SetVariableOperator(
            task_id='insert_to_user_dag_run_list',
            append=True,
            name='{{ result("declare_list_dag_runs").name }}',
            # pylint: disable=line-too-long
            value='{{(result("trigger_dag_run_live_nrdc_add_user_v2async_93") or result("trigger_dag_run_live_nrdc_add_user_v2async_105") or result("trigger_dag_run_live_nrdc_add_user_v2async_103") or result("trigger_dag_run_live_nrdc_add_user_v2async_101") or result("trigger_dag_run_live_nrdc_add_user_v2async_99") or result("trigger_dag_run_live_nrdc_add_user_v2async_97") or result("trigger_dag_run_live_nrdc_add_user_v2async_95"))[0]}}'
        )

        foreach_query_list_userstobe_created_33_82_end = rail.EmptyOperator(
            task_id='foreach_query_list_userstobe_created_33_82_end',
        )

        def has_adduser_triggers():
            decvar = rail.result("declare_list_dag_runs")
            setvar = rail.result("insert_to_user_dag_run_list")
            return bool(decvar and setvar)

        is_adduser_trigger_runs_avaialbale = rail.IfOperator(
            task_id='is_adduser_trigger_runs_avaialbale',
            test=has_adduser_triggers,
            yes_task="wait_for_completion_trigger_dag_run_live_nrdc_add_user_v2async",
            no_task="log_listsize_106",
        )

        wait_for_completion_trigger_dag_run_live_nrdc_add_user_v2async = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_live_nrdc_add_user_v2async',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("insert_to_user_dag_run_list").value | to_json }}'
        )

        log_listsize_106 = rail.PythonOperator(
            task_id='log_listsize_106',
            python_callable=lambda:  '''70'''
        )

        pause_107 = rail.EmptyOperator(
            task_id='pause_107',
        )

        nrdc_user_import_logs_search_entries_108 = rail.FilterLogEntriesOperator(
            task_id='nrdc_user_import_logs_search_entries_108',
            properties={'status': 'Error'}
        )

        if_nrdc_user_import_logs_search_entries_108_entries_greater_than_0_109 = rail.IfOperator(
            task_id='if_nrdc_user_import_logs_search_entries_108_entries_greater_than_0_109',
            test="{{ result('get_all_imput_records','length') > 0 }}",
            yes_task="create_csv_lines_110",
            no_task="log_to_sumo",
        )

        create_csv_lines_110 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_110',
            source="{{ get_master_log() }}",
            header=['JobId',
                    'User',
                    'Action',
                    'Status',
                    'Details'],
            row=[
                '{{ item.properties | attr_or_default("jobId", "") }}',
                '{{ item.properties | attr_or_default("user", "") }}',
                '{{ item.properties | attr_or_default("action", "")}}',
                '{{ item.properties | attr_or_default("status", "") }}',
                '{{ item.properties | attr_or_default("details", "") }}']
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_lines_110')}}",
            output_file_name="log_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | \
                file_name }}",
            expires_in_seconds=7*24*60*60
        )

        # filter log for errors
        get_errored_logs = rail.FilterLogEntriesOperator(
            task_id ="get_errored_logs",
            properties={
                "status": "Error"
            }
        )

        upload_uploadlogs_112 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadlogs_112',
            content="{{ result('create_csv_lines_110') }}",
            sftp_conn_id=config.sftp_conn_id,
            remote_filepath=config.log_filepath +
            "/log_{{ result('get_time_for_file') }}_{{ result('new_file_sensor') | \
                file_name }}"
        )

        if_query_list_changedrecords_16_rows_less_than_1_113 = rail.IfOperator(
            task_id='if_query_list_changedrecords_16_rows_less_than_1_113',
            test='{{ result("get_all_imput_records", "length") < 1 }}',
            yes_task="send_mail_114",
            no_task="send_import_complete_email",
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', 'length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | User Import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%d%m%YT%H%M%S") }}',
            html_content="email_import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        send_mail_114 = rail.EmailOperator(
            task_id='send_mail_114',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | User import - no records changed in file - {{ current_time_in_specified_tz() }} ''',
            html_content='''<p><strong><em>This is a automated mail, please don't reply</em></strong></p>
            <p>Hi ,</p>
            <p>The User import is completed on{{ result('log_today_4') }}. There were no changes in the existing records in the file {{ result('new_file_sensor') }} to be processed hence the user add/update was skipped.</p>
            <p>For any queries, please contact our support team at https://support.deltek.com</p>
            <p>Thanks, <br />Deltek Inc.</p> ''',
            params=None,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        new_file_sensor >> was_new_file_found
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
        was_new_file_found >> rail.Label('Yes') >> can_run_batch_task
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> log_today_4 >> get_time_for_file >> has_input_filename_ends_with_csv
        has_input_filename_ends_with_csv >> rail.Label(
            'No') >> send_mail_7 >> stop_8 >> log_to_sumo
        has_input_filename_ends_with_csv >> rail.Label(
            'Yes') >> download_file >> archive_file >> parse_input_csv >> create_csv_lines_rawdata_14_1 >> \
            create_input_list_collection >> get_all_imput_records >> input_has_any_data
        input_has_any_data >> rail.Label('No') >> send_mail_12 >> log_to_sumo
        input_has_any_data >> rail.Label(
            'Yes') >> create_csv_lines_rawdata_14 >> if_query_list_changedrecords_16_rows_greater_than_0_17
        if_query_list_changedrecords_16_rows_greater_than_0_17 >> rail.Label(
            'Yes') >> load_csv_create_list_from_csv_emails_20 >> \
            create_collection_create_list_from_csv_emails_20 >> query_list_combinedlistwithallcolumns_21 >> \
            gettheuserdetailsreference_22 >> get_all_reports_23 >> create_csv_lines_25 >> create_collection_create_list_from_csv_26 >> \
            query_list_27 >> declare_list_28 >> get_enabled_locations_29 >> location_list_info >> \
            query_list_userstobe_updated_32 >> query_list_userstobe_created_33 >> \
            get_report_details >> run_report_group_entry
        run_report_group_exit >> report_has_data
        report_has_data >> rail.Label("No") >> stop_37 >> log_to_sumo
        report_has_data >> rail.Label(
            "Yes") >> load_report_data >> create_user_collection >> query_userdata >> query_report_user_has_data
        query_report_user_has_data >> rail.Label('Yes') >> query_user_has_data
        query_report_user_has_data >> rail.Label(
            'No') >> stop_37 >> log_to_sumo
        query_user_has_data >> rail.Label(
            'No') >> log_listsize_80
        query_user_has_data >> rail.Label(
            'Yes') >> foreach_query_list_userstobe_updated_32_39 >> declare_list_update_dag_runs >> declare_variable_40 >> declare_variable_41 >> \
            declare_variable_42 >> declare_variable_43 >> declare_variable_44 >> log_loginname_45 >> if_foreach_83dddc26_39_logonname_present_46
        if_foreach_83dddc26_39_logonname_present_46 >> rail.Label(
            'Yes') >> log_location_uritoassign_47 >> if_log_location_uritoassign_47_blank_48
        if_log_location_uritoassign_47_blank_48 >> rail.Label(
            'No') >> log_loginname_49 >> create_new_draft_location_50 >> update_name_location_51 >> \
            publish_draft_location_52 >> log_location_uritoassign_53 >> update_variable_54
        if_log_location_uritoassign_47_blank_48 >> rail.Label(
            'Yes') >> update_variable_54 >> query_list_togettheusercurrentcountifenabled_55
        if_foreach_83dddc26_39_logonname_present_46 >> rail.Label(
            'No') >> query_list_togettheusercurrentcountifenabled_55 >> if_query_list_togettheusercurrentcountifenabled_55_rows_greater_than_0_56
        if_query_list_togettheusercurrentcountifenabled_55_rows_greater_than_0_56 >> rail.Label(
            'Yes') >> update_variable_57 >> update_variable_58 >> update_variable_59 >> update_variable_60 >> \
            if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61
        if_query_list_togettheusercurrentcountifenabled_55_rows_greater_than_0_56 >> rail.Label(
            'No') >> if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61
        if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61 >> rail.Label(
            'Yes') >> query_list_togettheusercurrentcountifdisabled_62 >> if_query_list_togettheusercurrentcountifdisabled_62_rows_greater_than_0_63
        if_query_list_togettheusercurrentcountifdisabled_62_rows_greater_than_0_63 >> rail.Label(
            'Yes') >> update_variable_64 >> update_variable_65 >> update_variable_66 >> update_variable_67 >> if_declare_variable_40_value_equals_to_1_68
        if_query_list_togettheusercurrentcountifdisabled_62_rows_greater_than_0_63 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_1_68
        if_query_list_togettheusercurrentcountifenabled_55_rows_equals_to_0_61 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_1_68
        if_declare_variable_40_value_equals_to_1_68 >> rail.Label(
            'Yes') >> get_c3_c4_update_data >> trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_69 >> insert_to_user_update_dag_run_list >> \
            foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_1_68 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_5_70
        if_declare_variable_40_value_equals_to_5_70 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_71 >> \
            insert_to_user_update_dag_run_list >> foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_5_70 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_6_72
        if_declare_variable_40_value_equals_to_6_72 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_73 >> \
            insert_to_user_update_dag_run_list >> foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_6_72 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_2_74
        if_declare_variable_40_value_equals_to_2_74 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_75 >> \
            insert_to_user_update_dag_run_list >> foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_2_74 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_7_76
        if_declare_variable_40_value_equals_to_7_76 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync_77 >> \
            insert_to_user_update_dag_run_list >> foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_7_76 >> rail.Label(
            'No') >> if_declare_variable_40_value_equals_to_3_78
        if_declare_variable_40_value_equals_to_3_78 >> rail.Label(
            'Yes') >> nrdc_user_import_logs_add_entry_79 >> foreach_query_list_userstobe_updated_32_39_end
        if_declare_variable_40_value_equals_to_3_78 >> rail.Label(
            'No') >> foreach_query_list_userstobe_updated_32_39_end
        foreach_query_list_userstobe_updated_32_39 >> foreach_query_list_userstobe_updated_32_39_end >> is_c3c4_trigger_runs_avaialbale
        is_c3c4_trigger_runs_avaialbale >> rail.Label('Yes') >> \
            wait_for_completion_trigger_dag_run_live_nrdc_updating_c3_c4_valuesasync >> log_listsize_80 >> pause_81 >> \
            if_users_available_to_create_82
        if_users_available_to_create_82 >> rail.Label('No') >> log_listsize_106
        if_users_available_to_create_82 >> rail.Label('Yes') >> foreach_query_list_userstobe_created_33_82 >> \
            declare_list_dag_runs >> declare_variable_83 >> log_location_uritoassign_85 >> \
                if_log_location_uritoassign_85_blank_86
        is_c3c4_trigger_runs_avaialbale >> rail.Label('No') >> log_listsize_80
        if_log_location_uritoassign_85_blank_86 >> rail.Label(
            'No') >> create_new_draft_location_87 >> update_name_location_88 >> publish_draft_location_89 >> log_location_uritoassign_90 >> update_variable_91
        if_log_location_uritoassign_85_blank_86 >> rail.Label(
            'Yes') >> update_variable_91 >> if_foreach_80150ea2_82_memberof_contains_c3_only_c3_92
        if_foreach_80150ea2_82_memberof_contains_c3_only_c3_92 >> rail.Label(
            'Yes') >> get_add_user_data >> trigger_dag_run_live_nrdc_add_user_v2async_93 >> \
            insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_c3_only_c3_92 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94
        if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_95 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_c4_only_c4_94 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96
        if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_97 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_delegate_only_delegate_96 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98
        if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_99 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_c4_c4_c3_98 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100
        if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_101 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_c4_delegateand_c4_100 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102
        if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_103 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_not_contains_c4_delegateand_c3_102 >> rail.Label(
            'No') >> if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104
        if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104 >> rail.Label(
            'Yes') >> trigger_dag_run_live_nrdc_add_user_v2async_105 >> insert_to_user_dag_run_list >> foreach_query_list_userstobe_created_33_82_end
        if_foreach_80150ea2_82_memberof_contains_c4_delegate_c3and_c4_104 >> rail.Label(
            'No') >> foreach_query_list_userstobe_created_33_82_end
        foreach_query_list_userstobe_created_33_82 >> foreach_query_list_userstobe_created_33_82_end >> is_adduser_trigger_runs_avaialbale
        is_adduser_trigger_runs_avaialbale >> rail.Label(
            'Yes') >> wait_for_completion_trigger_dag_run_live_nrdc_add_user_v2async >> log_listsize_106 >> pause_107 >> \
            nrdc_user_import_logs_search_entries_108 >> if_nrdc_user_import_logs_search_entries_108_entries_greater_than_0_109
        is_adduser_trigger_runs_avaialbale >> rail.Label(
            'No') >> log_listsize_106
        if_nrdc_user_import_logs_search_entries_108_entries_greater_than_0_109 >> rail.Label(
            'Yes') >> create_csv_lines_110 >> generate_downloadable_link >> get_errored_logs \
                >> upload_uploadlogs_112 >> if_query_list_changedrecords_16_rows_less_than_1_113
        if_nrdc_user_import_logs_search_entries_108_entries_greater_than_0_109 >> rail.Label(
            'No') >> log_to_sumo
        if_query_list_changedrecords_16_rows_greater_than_0_17 >> rail.Label(
            'No') >> if_query_list_changedrecords_16_rows_less_than_1_113
        if_query_list_changedrecords_16_rows_less_than_1_113 >> rail.Label(
            'Yes') >> send_mail_114 >> log_to_sumo
        if_query_list_changedrecords_16_rows_less_than_1_113 >> rail.Label(
            'No') >>  send_import_complete_email >>log_to_sumo

    return dag


rail.for_each_instance(create_dag)
