from datetime import datetime
import itertools
import json

import rail


def get_gsap_task(config, header):
    with rail.TaskGroup(group_id='gsap_task', prefix_group_id=False) as gsap_task:

        gsap_task_start = rail.EmptyOperator(
            task_id='gsap_task_start'
        )

        has_gsap_userdata = rail.IfOperator(
            task_id='has_gsap_userdata',
            test=lambda: rail.result('query_list_gsap_userdata', 'length') > 0,
            yes_task="query_list_uniqueusers_for_gsap",
            no_task="gsap_task_finish",
        )

        query_list_uniqueusers_for_gsap = rail.QueryCollectionOperator(
            task_id='query_list_uniqueusers_for_gsap',
            query='''SELECT DISTINCT useruri FROM query_list_gsap_userdata''',
        )

        getkeyvalue_for_dxc_po_rate_gsap = rail.RepliconServiceCallForEachItemOperator(
            task_id='getkeyvalue_for_dxc_po_rate_gsap',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            items='{{ result("query_list_uniqueusers_for_gsap") }}',
            flatten=True,
            data={
                "keyNamespace": "CWF_workorderdetails",
                "key": "{{item.useruri}}"
            },
            all_result_data_handler=lambda data: list(
                map(lambda item:  {'key': item['key'], 'jsonValue': json.loads(item['jsonValue'])},
                    filter(lambda item: item, data))),
        )

        def get_work_order_id_for_po(item):
            return next(reversed(list(filter(lambda x: x['WorkerID'] == item['employeeid'] and
                                             datetime.strptime(x['WOStartDate'], config.output_date_format) <=
                                             datetime.strptime(item['entrydate'], config.input_date_format) and
                                             datetime.strptime(x['WOEndDate'], config.output_date_format) >= datetime.strptime(
                item['entrydate'], config.input_date_format),
                list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('getkeyvalue_for_dxc_po_rate_gsap')))
                     )))), {}).get('WorkOrderID', '')

        def get_uom_for_po(item):
            return next(reversed(list(filter(lambda x: x['WorkerID'] == item['employeeid'] and
                                             datetime.strptime(x['WOStartDate'], config.output_date_format) <=
                                             datetime.strptime(item['entrydate'], config.input_date_format) and
                                             datetime.strptime(x['WOEndDate'], config.output_date_format) >= datetime.strptime(
                item['entrydate'], config.input_date_format),
                list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('getkeyvalue_for_dxc_po_rate_gsap')))
                     )))), {}).get('RateUnit', '')

        create_csv_purchase_order_gsap = rail.WriteCSVFileOperator(
            task_id='create_csv_purchase_order_gsap',
            source="{{ result('query_list_gsap_userdata') }}",
            header=header,
            row=lambda item:  {
                'Work_Order_Id': get_work_order_id_for_po(item),
                'Last_Name': item['userlastname'],
                'First_Name': item['userfirstname'],
                'Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Week_Start_Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Cost_Center_Code': item['costcenter'],
                'Task_Code': '799' if item['attendancetypecode'] == "799" else "Hours Worked - Billable",
                'Rate_Category_Code': "ST",
                'UOM': get_uom_for_po(item),
                'Sat_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 5 else 0,
                'Sun_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 6 else 0,
                'Mon_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 0 else 0,
                'Tue_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 1 else 0,
                'Wed_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 2 else 0,
                'Thu_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 3 else 0,
                'Fri_Hrs': item['totalhrs'] if datetime.strptime(item['entrydate'], config.input_date_format).weekday() == 4 else 0,
                '[c] CATW':  "Yes" if item['companycodecode'] == "COMPASS" else "No",

            }.values()
        )

        load_csv_purchase_order_gsap = rail.LoadCSVFileOperator(
            task_id="load_csv_purchase_order_gsap",
            document="{{ result('create_csv_purchase_order_gsap') }}",
        )

        create_finaldata_collection_gsap = rail.CreateCollectionOperator(
            task_id='create_finaldata_collection_gsap',
            source="{{ result('load_csv_purchase_order_gsap') }}",
            name="finaldata_gsap",
        )

        query_list_getuniquedatacombinations_gsap = rail.QueryCollectionOperator(
            task_id='query_list_getuniquedatacombinations_gsap',
            query='''SELECT  work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,
                    SUM( CAST(sat_hrs as DECIMAL) ) as sat_hrs,
                    SUM( CAST(sun_hrs as DECIMAL) ) as sun_hrs,
                    SUM( CAST(mon_hrs as DECIMAL) ) as mon_hrs,
                    SUM( CAST(tue_hrs as DECIMAL) ) as tue_hrs,
                    SUM( CAST(wed_hrs as DECIMAL) ) as wed_hrs,
                    SUM( CAST(thu_hrs as DECIMAL) ) as thu_hrs,
                    SUM( CAST(fri_hrs as DECIMAL) ) as fri_hrs,
                    _c__CATW as catw
                    FROM finaldata_gsap
                    GROUP BY work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,_c__CATW
                    ''',
        )

        compose_finaldata_csv_gsap = rail.WriteCSVFileOperator(
            task_id='compose_finaldata_csv_gsap',
            source="{{ result('query_list_getuniquedatacombinations_gsap') }}",
            header=header,
            row=lambda item: item.values(),
        )

        log_message_filename_gsap = rail.PythonOperator(
            task_id='log_message_filename_gsap',
            python_callable=lambda: f"RepTS_GSAP_AU_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
        )

        encrypt_finaldata_csv_gsap = rail.PGPEncryptionOperator(
            task_id='encrypt_finaldata_csv_gsap',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('compose_finaldata_csv_gsap') }}",
        )

        upload_file_to_sftp_gsap = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp_gsap',
            content="{{ result('encrypt_finaldata_csv_gsap') }}",
            remote_filepath=config.field_glass_output_filepath +
            '/{{ result("log_message_filename_gsap") }}.pgp',
        )

        send_mail_timedata_file_export_failed = rail.EmailOperator(
            task_id='send_mail_timedata_file_export_failed',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='''{{get_company_key()}} | Compass Time data export automation (GSAP) - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}''',
            # pylint: disable=line-too-long
            html_content='''<p>Hi Team,<br /> <br /> The Replicon CWF time extract for Fieldglass GSAP has been completed at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}},
                        however the file upload to sftp has failed with error.</p>
                        <ul>
                        <li>Recipe ID: {{ dag_run.dag_id}}</li>
                            <li>Job ID: {{ ecid() }}</li>
                            <li>Instance: {{ get_company_key() }}</li>
                            <li>File Name:  {{ result("log_message_filename_gsap") }}.pgp</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + '''</li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp.
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.field_glass_output_filepath
            },
            files=[
                ('{{ result("log_message_filename_gsap") }}.pgp',
                 "{{ result('encrypt_finaldata_csv_gsap') }}"
                 ),
            ]
        )

        send_export_complete_mail_gsap = rail.EmailOperator(
            task_id='send_export_complete_mail_gsap',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }} | Replicon CWF time extract for Fieldglass (GSAP) - Completed Successfully -  {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />
            Hello, <br /> <br /> The Replicon CWF time extract for Fieldglass GSAP job is Completed successfully at {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}.
            Please find the file details below:<br /><br />
            File path: {{params.upload_path}}<br />
            File name: {{ result("log_message_filename_gsap") }}.pgp
            <br /></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
            ''',
            params={
                'upload_path': config.field_glass_output_filepath,
            },
        )

        gsap_task_finish = rail.EmptyOperator(
            task_id='gsap_task_finish'
        )

        gsap_task_start >> has_gsap_userdata
        has_gsap_userdata >> rail.Label('Yes') >> query_list_uniqueusers_for_gsap >> getkeyvalue_for_dxc_po_rate_gsap >> create_csv_purchase_order_gsap >> \
            load_csv_purchase_order_gsap >> create_finaldata_collection_gsap >> query_list_getuniquedatacombinations_gsap >> \
            compose_finaldata_csv_gsap >> log_message_filename_gsap >> encrypt_finaldata_csv_gsap >> upload_file_to_sftp_gsap
        upload_file_to_sftp_gsap >> rail.Label(
            'error') >> send_mail_timedata_file_export_failed >> gsap_task_finish
        upload_file_to_sftp_gsap >> rail.Label('success') >> send_export_complete_mail_gsap >> \
            gsap_task_finish
        has_gsap_userdata >> rail.Label(
            'No') >> gsap_task_finish

    return gsap_task
