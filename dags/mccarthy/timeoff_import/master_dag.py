
from datetime import timedelta
import csv
import itertools
from inflection import ordinalize
import rail
from rail.lib.ecid import get_dagrun_ecid
from rail.lib.artifact import existing_artifact, is_artifact_name
from mccarthy.timeoff_import.task.generate_report_batch import report_batch


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'mccarthy_timeoff_import_time_off_policy_import_master_v2_{config.instance}',
        description=f'Time_Off_Policy_Import_V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.schedule_interval),
        max_active_runs=config.max_active_runs,
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
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout),
        )

        is_csv = rail.IfOperator(
            task_id='is_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" and result("new_file_sensor") | matches("McCarthy_Time_Off_Upload") }}',
            yes_task='download_6',
            no_task='send_mail_3',
        )

        send_mail_3 = rail.EmailOperator(
            task_id='send_mail_3',
            to=config.tenant_email,
            bcc=config.internal_logs_email,  # config.alert_email on error fixme
            subject='''{{ get_company_key() }} | Replicon time off policy import - Incorrect File Format  ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon time-off import automation is skipped on {{ current_time_in_specified_tz("America/Denver","%Y-%m-%dT%H:%M:%S.%f%z") }} as the file "{{ result("new_file_sensor") | file_name }}" is not in .csv format or file name is not correct. <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        rename_4 = rail.SFTPMoveFileOperator(
            task_id='rename_4',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ current_time('%Y%m%dT%H%M%S') }}_{{ result('new_file_sensor') | file_name }}"
        )

        download_6 = rail.SFTPDownloadFileOperator(
            task_id='download_6',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ (get_task_state("new_file_sensor") == "success") and (result("new_file_sensor") | matches("McCarthy_Time_Off_Upload")) }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ current_time('%Y%m%dT%H%M%S') }}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        def get_csv_data_headers_mapped(document, headers):
            mapped_list = []
            if is_artifact_name(document):

                with existing_artifact(document, mode="r", encoding='utf-8') as artifact:
                    reader = csv.DictReader(
                        artifact.file, delimiter=',', fieldnames=headers)
                    mapped_list = [
                        row for row in reader if row['loginname'] != 'Login Name']
            return mapped_list

        parse_csv_7 = rail.PythonOperator(
            task_id='parse_csv_7',
            python_callable=lambda: get_csv_data_headers_mapped(
                rail.result('download_6'), ['loginname',
                                            'addusertimeoff',
                                            'asofdate',
                                            'allowedhours',
                                            'accruetype',
                                            'yearlyentitlement',
                                            'onweek',
                                            'proration',
                                            'resettype',
                                            'resetonmonth',
                                            'ondayofmonth',
                                            'resetrule',
                                            'resetamount'
                                            ])
        )

        create_csv_lines_12 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_12',
            source="{{ result('parse_csv_7') | to_json}}",
            header=['loginname',
                    'addusertimeoff',
                    'asofdate',
                    'allowedhours',
                    'accruetype',
                    'yearlyentitlement',
                    'onweek',
                    'proration',
                    'resettype',
                    'resetonmonth',
                    'ondayofmonth',
                    'resetrule',
                    'resetamount'],
            row=[
                "{{ item['loginname'] }}",
                "{{ item['addusertimeoff'] }}",
                "{{ item['asofdate'] }}",
                "{{ item['allowedhours'] }}",
                "{{ item['accruetype'] }}",
                "{{ item['yearlyentitlement'] }}",
                "{{ item['onweek'] }}",
                "{{ item['proration'] }}",
                "{{ item['resettype'] }}",
                "{{ item['resetonmonth'] }}",
                "{{ item['ondayofmonth'] }}",
                "{{ item['resetrule'] }}",
                "{{ item['resetamount'] }}"
            ],
        )

        load_csv_create_list_from_csv_13 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_13",
            document="{{ result('create_csv_lines_12') }}",
        )

        create_collection_create_list_from_csv_13 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_13',
            source="{{ result('load_csv_create_list_from_csv_13') }}",
            name="inputfiledata",
            columns={
                'loginname': 'loginname',
                'addusertimeoff': 'addusertimeoff',
                'asofdate': 'asofdate',
                'allowedhours': 'allowedhours',
                'accruetype': 'accruetype',
                'yearlyentitlement': 'yearlyentitlement',
                'onweek': 'onweek',
                'proration': 'proration',
                'resettype': 'resettype',
                'resetonmonth': 'resetonmonth',
                'ondayofmonth': 'ondayofmonth',
                'resetrule': 'resetrule',
                'resetamount': 'resetamount'
            }
        )

        if_parse_csv_7_lines_less_than_1_8 = rail.IfOperator(
            task_id='if_parse_csv_7_lines_less_than_1_8',
            test='''{{ result('create_collection_create_list_from_csv_13', 'length') == 0 }}''',
            yes_task="send_mail_9",
            no_task="create_timeoff_import_logs"
        )

        send_mail_9 = rail.EmailOperator(
            task_id='send_mail_9',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} |Time Off Import Skipped On {{ current_time_in_specified_tz("America/Denver","%Y-%m-%dT%H:%M:%S.%f%z") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The time off Import is skipped, since the file - '{{ result("new_file_sensor") | file_name }}' uploaded does not have any records. Please correct the file and place a new file for processing.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None
        )

        create_timeoff_import_logs = rail.CreateLogOperator(
            task_id='create_timeoff_import_logs'
        )

        query_list_checkformandatoryfields_14 = rail.QueryCollectionOperator(
            task_id='query_list_checkformandatoryfields_14',
            query="""SELECT * FROM inputfiledata WHERE NULLIF(loginname,'') IS NULL OR NULLIF(addusertimeoff,'') IS NULL OR NULLIF(asofdate,'') IS NULL"""
        )

        if_query_list_checkformandatoryfields_14_rows_greater_than_0_15 = rail.IfOperator(
            task_id='if_query_list_checkformandatoryfields_14_rows_greater_than_0_15',
            test='''{{ result('query_list_checkformandatoryfields_14', 'length') > 0 }}''',
            yes_task="time_off_import_logs_add_batch_of_entries_16",
            no_task="query_list_datato_processed_17",
        )

        time_off_import_logs_add_batch_of_entries_16 = rail.WriteLogOperator(
            task_id='time_off_import_logs_add_batch_of_entries_16',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_checkformandatoryfields_14') }}",
            message="User with {{ item.loginname }} is skipped as one or more mandatory fields are not present",
            severity="Info",
            properties={
                "loginname": "{{ item.loginname }}",
                "timeofftype": "{{ item.addusertimeoff }}",
                "status": "Skipped",
                "details": "User with {{ item.loginname }} is skipped as one or more mandatory fields are not present",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": ""
            }
        )

        query_list_datato_processed_17 = rail.QueryCollectionOperator(
            task_id='query_list_datato_processed_17',
            name='datato_processed',
            query="""SELECT * FROM inputfiledata WHERE NULLIF(loginname,'') IS NOT NULL AND NULLIF(addusertimeoff,'') IS NOT NULL AND NULLIF(asofdate,'') IS NOT NULL""",
        )

        if_query_list_datato_processed_17_rows_greater_than_0_18 = rail.IfOperator(
            task_id='if_query_list_datato_processed_17_rows_greater_than_0_18',
            test='''{{ result('query_list_datato_processed_17', 'length') > 0 }}''',
            yes_task="get_report_details",
            no_task="finish",
        )

        get_report_details, create_collection_from_report_data, fail_no_report_data, fail_column_order_mismatch = report_batch(
            config)

        query_user_to_processed = rail.QueryCollectionOperator(
            task_id='query_user_to_processed',
            name='user_to_processed',
            query="""SELECT * FROM datato_processed WHERE loginname IN (SELECT DISTINCT loginname FROM RepliconBalanceData)"""
        )

        query_user_not_to_processed = rail.QueryCollectionOperator(
            task_id='query_user_not_to_processed',
            name='user_not_to_processed',
            query="""SELECT * FROM datato_processed WHERE loginname NOT IN (SELECT DISTINCT loginname FROM RepliconBalanceData)"""
        )

        query_replicon_balance_data_for_user = rail.QueryCollectionOperator(
            task_id='query_replicon_balance_data_for_user',
            name='replicon_balanace_data_to_processed',
            query="""SELECT * FROM RepliconBalanceData WHERE loginname IN (SELECT DISTINCT loginname FROM user_to_processed)"""
        )

        def get_cummilative_data():
            data = rail.load_all_records(
                rail.result('query_user_to_processed'))
            user_data = rail.load_all_records(
                rail.result('query_replicon_balance_data_for_user'))

            def get_processingrequired(loginname, addusertimeoff, allowedhours):
                allowed_hours = allowedhours if allowedhours else '0'
                if next(x['useruri'] for x in user_data if loginname == x['loginname']):
                    if next((x['timeofftypeuri'] for x in user_data if loginname == x['loginname'] and addusertimeoff == x['timeofftype']), null):
                        if (next(float(str(x['timeoffbalance']).replace(",","")) for x in user_data if loginname == x['loginname'] and addusertimeoff == x['timeofftype'])) == float(allowed_hours):
                            return "No"
                        return "Yes"
                    return "No"
                return "No"

            def get_log(loginname, addusertimeoff, allowedhours):
                allowed_hours = allowedhours if allowedhours else '0'
                if next(x['useruri'] for x in user_data if loginname == x['loginname']):
                    if next((x['timeofftypeuri'] for x in user_data if loginname == x['loginname'] and addusertimeoff == x['timeofftype']), null):
                        if (next(float(str(x['timeoffbalance']).replace(",","")) for x in user_data if loginname == x['loginname'] and addusertimeoff == x['timeofftype'])) == float(allowed_hours):
                            return "Proposed balance and current balance is same"
                        return "Yes"
                    return "Timeoff Type is not found in Replicon"
                return "User is not found or disabled in Replicon"

            return list(map(lambda item: {
                "loginname": item['loginname'],
                "addusertimeoff": item['addusertimeoff'],
                "asofdate": item['asofdate'],
                "allowedhours": item['allowedhours'],
                "accruetype": item['accruetype'],
                "entitlementtype": item['yearlyentitlement'],
                "onweek": item['onweek'],
                "proration": item['proration'],
                "resettype": item['resettype'],
                "resetonmonth": item['resetonmonth'],
                "ondayofmonth": item['ondayofmonth'],
                "resetrule": item['resetrule'],
                "resetamount": item['resetamount'],
                "useruri": next((x['useruri'] for x in user_data if item['loginname'] == x['loginname']), null),
                "status": next((x['status'] for x in user_data if item['loginname'] == x['loginname']), null),
                "timeoffuri": next((x['timeofftypeuri'] for x in user_data if item['loginname'] == x['loginname'] and item['addusertimeoff'] == x['timeofftype']), null),
                "timeoffuriassigned": next((x['timeofftypeuri'] for x in user_data if item['loginname'] == x['loginname'] and item['addusertimeoff'] == x['timeofftype']), null),
                "existingbalance": next((x['timeoffbalance'] for x in user_data if item['loginname'] == x['loginname'] and item['addusertimeoff'] == x['timeofftype']), null),
                "processingrequired": get_processingrequired(item['loginname'], item['addusertimeoff'], item['allowedhours']),
                "log": get_log(item['loginname'], item['addusertimeoff'], item['allowedhours'])
            }, data))

        build_cummilative_data = rail.PythonOperator(
            task_id="build_cummilative_data",
            python_callable=get_cummilative_data
        )

        create_csv_lines_cummilated_data_for_processing_30 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_cummilated_data_for_processing_30',
            source="{{ result('build_cummilative_data') | to_json }}",
            header=['loginname',
                    'addusertimeoff',
                    'asofdate',
                    'allowedhours',
                    'accruetype',
                    'entitlementtype',
                    'onweek',
                    'proration',
                    'resettype',
                    'resetonmonth',
                    'ondayofmonth',
                    'resetrule',
                    'resetamount',
                    'useruri',
                    'status',
                    'timeoffuri',
                    'timeoffuriassigned',
                    'existingbalance',
                    'processingrequired',
                    'log'],
            row=[
                "{{ item.loginname }}",
                "{{ item.addusertimeoff }}",
                "{{ item.asofdate }}",
                "{{ item.allowedhours }}",
                "{{ item.accruetype }}",
                "{{ item.entitlementtype }}",
                "{{ item.onweek }}",
                "{{ item.proration }}",
                "{{ item.resettype }}",
                "{{ item.resetonmonth }}",
                "{{ item.ondayofmonth }}",
                "{{ item.resetrule }}",
                "{{ item.resetamount }}",
                "{{ item.useruri }}",
                "{{ item.status }}",
                "{{ item.timeoffuri }}",
                "{{ item.timeoffuriassigned }}",
                "{{ item.existingbalance }}",
                "{{ item.processingrequired }}",
                "{{ item.log }}"
            ],
        )

        load_csv_create_list_from_csv_31 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_31",
            document="{{ result('create_csv_lines_cummilated_data_for_processing_30') }}",
        )

        create_collection_create_list_from_csv_31 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_31',
            source="{{ result('load_csv_create_list_from_csv_31') }}",
            name="datatoprocess"
        )

        query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32 = rail.QueryCollectionOperator(
            task_id='query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32',
            query="""SELECT * FROM datatoprocess WHERE processingrequired='No'""",
        )

        if_query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32_rows_greater_than_0_33 = rail.IfOperator(
            task_id='if_query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32_rows_greater_than_0_33',
            test='''{{ result('query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32', 'length') > 0 or result('query_user_not_to_processed', 'length') > 0}}''',
            yes_task="time_off_import_logs_add_batch_of_entries_34",
            no_task="query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35",
        )

        time_off_import_logs_add_batch_of_entries_34 = rail.WriteLogOperator(
            task_id='time_off_import_logs_add_batch_of_entries_34',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32') }}",
            message="{{ item.log }}",
            severity="Exception",
            properties={
                "loginname": "{{ item.loginname }}",
                "timeofftype": "{{ item.addusertimeoff }}",
                "status": "Exception",
                "details": "{{ item.log }}",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": ""
            }
        )

        time_off_import_logs_add_batch_of_entries_34_1 = rail.WriteLogOperator(
            task_id='time_off_import_logs_add_batch_of_entries_34_1',
            log="{{ result('create_timeoff_import_logs') }}",
            items="{{ result('query_user_not_to_processed') }}",
            message="User is not found or disabled in Replicon",
            severity="Exception",
            properties={
                "loginname": "{{ item.loginname }}",
                "timeofftype": "{{ item.addusertimeoff }}",
                "status": "Exception",
                "details": "User is not found or disabled in Replicon",
                "jobid": "{{ dag_run_ecid() }}",
                "childjobid": ""
            }
        )

        query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35 = rail.QueryCollectionOperator(
            task_id='query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35',
            query="""SELECT * FROM datatoprocess WHERE processingrequired='Yes'""",
        )

        def get_ordinalize_value(ondayofmonth):
            if not ondayofmonth:
                return null
            return '1st' if '.' in ondayofmonth or int(ondayofmonth) < 0 else ordinalize(int(ondayofmonth))

        trigger_dag_run_add_update_timeoff_import_policy_37 = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_add_update_timeoff_import_policy_37',
            items="{{ result('query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35') }}",
            trigger_dag_id=f'mccarthy_timeoff_import_mccarthy_add_update_timeoff_policy_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "loginname": item['loginname'],
                "useruri": item['useruri'],
                "timeoffuri": item['timeoffuri'],
                "timeofftype": item['addusertimeoff'],
                "effectivedate": item['asofdate'],
                "allowedhours": item['allowedhours'],
                "accruetype": item['accruetype'],
                "yearlyentitlement": item['entitlementtype'],
                "onweek": item['onweek'].lower() if item['onweek'] else null,
                "proration": item['proration'],
                "resettype": item['resettype'],
                "resetonmonth": item['resetonmonth'].lower() if item['resetonmonth'] else null,
                "ondayofmonth": get_ordinalize_value(item['ondayofmonth']),
                "resettypeforlimitationrule": item['resetrule'],
                "resetamount": item['resetamount'],
                "resettypeuri": "urn:replicon:time-off-policy-reset-option:reset-balance-to-specific-value" if item['resetrule'] and item['resetrule'] == "Set To" else "urn:replicon:time-off-policy-reset-option:carry-over-previous-balance-with-limit" if item['resetrule'] and item['resetrule'] != "Set To" else null,
                "jobid": get_dagrun_ecid(rail.get_current_context()['dag_run'])
            },
            parallel_count=config.trigger_parallel_dagrun_count
        )

        get_child_task_ids = rail.PythonOperator(
            task_id='get_child_task_ids',
            python_callable=lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'trigger_dag_run_add_update_timeoff_import_policy_37_{x+1}'), range(config.trigger_parallel_dagrun_count))))),
            show_return_value_in_logs=False
        )

        gather_timeoff_import_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_timeoff_import_child_logs',
            dag_runs="{{ result('get_child_task_ids') }}",
            dagrun_task_id='create_timeoff_import_child_logs',
            flatten=True
        )

        def do_format_logs():
            def load_records(log_artifact):
                try:
                    logs = rail.load_all_records(log_artifact)
                    return logs
                except:  # pylint: disable=bare-except
                    return []

            log_artifacts = []
            if rail.result('create_timeoff_import_logs'):
                log_artifacts.append(rail.result('create_timeoff_import_logs'))

            if rail.result('gather_timeoff_import_child_logs'):
                log_artifacts.extend(rail.result(
                    'gather_timeoff_import_child_logs'))

            log_records = []

            if log_artifacts:
                for log in log_artifacts:
                    each_log_records = load_records(log)
                    if each_log_records:
                        log_records.extend(each_log_records)

            return list(map(lambda x: {
                **{k: v for k, v in x['properties'].items() if k != 'email'},
                **{
                    'jobid': x['ecid']
                }}, log_records))

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=do_format_logs
        )

        get_success_logs = rail.PythonOperator(
            task_id='get_success_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Success', rail.result('format_logs')))), 'length')
        )

        get_error_logs = rail.PythonOperator(
            task_id='get_error_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Error', rail.result('format_logs')))), 'length')
        )

        get_exception_logs = rail.PythonOperator(
            task_id='get_exception_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == 'Exception', rail.result('format_logs')))), 'length')
        )

        create_csv_lines_from_logs = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_from_logs',
            source="{{ result('format_logs') | to_json }}",
            header=['loginname',
                    'timeofftype',
                    'status',
                    'details',
                    'jobid',
                    'childjobid'
                    ],
            row=[
                "{{ item.loginname }}",
                "{{ item.timeofftype }}",
                "{{ item.status }}",
                "{{ item.details }}",
                "{{ item.jobid }}",
                "{{ item.childjobid }}"
            ]
        )

        def file_upload_failed(context):
            subject = '{{ get_company_key() }} | Replicon Timeoff import - Uploading Logs to SFTP failed {{ current_time_in_specified_tz("America/Denver","%Y-%m-%dT%H:%M:%S.%f%z") }}'
            email = rail.EmailOperator(
                task_id='send_time_data_to_sftp_failure_email',
                to=config.tenant_email,
                bcc=config.alert_email,
                subject=subject,
                html_content='''<p>Hi Team,<br/> <br/> The Replicon user sync for Companykey {{ get_company_key() }}  instance, hosted on  User name Properties , created on Job created at Properties  has been completed for file "{{ result('new_file_sensor') | file_name }}", however, the log upload to sftp has failed. Attached is the log file for reference.</p>
<ul>
<li>Recipe ID: {{ params.dag_id }} </li>
<li>Job ID: {{ dag_run_ecid() }} </li>
</ul>
<p>Please find the attached logs which was to be sent to intended recipients and debug the issue related to sftp upload.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
                params={
                    'dag_id': f'mccarthy_timeoff_import_time_off_policy_import_master_v2_{config.instance}'
                },
                files=[
                    ("{{ result('create_csv_lines_from_logs') }}")
                ]
            )
            email.render_template_fields(context)
            email.execute(context)

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('create_csv_lines_from_logs') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_name }}',
            on_failure_callback=file_upload_failed
        )

        send_email_on_successful_upload = rail.EmailOperator(
            task_id='send_email_on_successful_upload',
            to=config.tenant_email,
            bcc="{%- if result('get_error_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon timeoff import - " }} \
                {%- if result("get_error_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("get_exception_logs", key="length") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time_in_specified_tz("America/Denver","%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content='templates/emails/import_complete.html',
            params={'log_filepath': config.log_filepath},
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        new_file_sensor >> is_csv >> rail.Label(
            "No") >> send_mail_3 >> rename_4 >> finish

        is_csv >> rail.Label("Yes") >> download_6 >> was_new_file_found
        was_new_file_found >> rail.Label("Yes") >> archive_file >> parse_csv_7 >> create_csv_lines_12 \
            >> load_csv_create_list_from_csv_13 >> create_collection_create_list_from_csv_13 >> if_parse_csv_7_lines_less_than_1_8
        was_new_file_found >> rail.Label("No") >> delete_this_dagrun >> finish

        if_parse_csv_7_lines_less_than_1_8 >> rail.Label(
            'Yes') >> send_mail_9 >> finish
        if_parse_csv_7_lines_less_than_1_8 >> rail.Label(
            'No') >> create_timeoff_import_logs >> query_list_checkformandatoryfields_14 >> if_query_list_checkformandatoryfields_14_rows_greater_than_0_15
        if_query_list_checkformandatoryfields_14_rows_greater_than_0_15 >> rail.Label(
            'Yes') >> time_off_import_logs_add_batch_of_entries_16 >> query_list_datato_processed_17
        if_query_list_checkformandatoryfields_14_rows_greater_than_0_15 >> rail.Label(
            'No') >> query_list_datato_processed_17 >> if_query_list_datato_processed_17_rows_greater_than_0_18
        if_query_list_datato_processed_17_rows_greater_than_0_18 >> rail.Label(
            'Yes') >> get_report_details

        fail_no_report_data >> finish
        fail_column_order_mismatch >> finish

        create_collection_from_report_data >> query_user_to_processed >> query_user_not_to_processed >> query_replicon_balance_data_for_user \
            >> build_cummilative_data >> create_csv_lines_cummilated_data_for_processing_30 \
            >> load_csv_create_list_from_csv_31 >> create_collection_create_list_from_csv_31 >> query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32 \
            >> if_query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32_rows_greater_than_0_33
        if_query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32_rows_greater_than_0_33 >> rail.Label(
            'Yes') >> time_off_import_logs_add_batch_of_entries_34 >> time_off_import_logs_add_batch_of_entries_34_1 >> query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35
        if_query_list_checkforusersnotavailableorwithdisabledstatusortimeoffnotpresent_32_rows_greater_than_0_33 >> rail.Label(
            'No') >> query_list_checkforuserswithvalidbalanceandassignedtimeofftypes_35 >> trigger_dag_run_add_update_timeoff_import_policy_37 \
            >> get_child_task_ids >> gather_timeoff_import_child_logs \
            >> format_logs >> get_success_logs >> get_error_logs >> get_exception_logs >> create_csv_lines_from_logs \
            >> upload_logs_to_sftp >> send_email_on_successful_upload >> finish
        if_query_list_datato_processed_17_rows_greater_than_0_18 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
