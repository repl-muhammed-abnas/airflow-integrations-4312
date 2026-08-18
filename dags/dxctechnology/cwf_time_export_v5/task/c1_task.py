from datetime import datetime
import itertools
import json

import rail


def get_c1_task(config, header):
    with rail.TaskGroup(group_id='c1_task', prefix_group_id=False) as c1_task:

        c1_task_start = rail.EmptyOperator(
            task_id='c1_task_start'
        )

        has_c1userdata = rail.IfOperator(
            task_id='has_c1userdata',
            test=lambda: rail.result('query_list_c1userdata', 'length') > 0,
            yes_task="query_list_uniqueusersforc1",
            no_task="c1_task_finish",
        )

        query_list_uniqueusersforc1 = rail.QueryCollectionOperator(
            task_id='query_list_uniqueusersforc1',
            query='''SELECT DISTINCT loginname FROM query_list_c1userdata''',
        )

        getkeyvalue_for_dxc_po_rate = rail.RepliconServiceCallForEachItemOperator(
            task_id='getkeyvalue_for_dxc_po_rate',
            endpoint="/services/GenericKeyValueStoreService1.svc/GetKeyValue",
            items='{{ result("query_list_uniqueusersforc1") }}',
            flatten=True,
            data={
                "keyNamespace": "DXC_PurchaseOrderRateTypesBalanceDetails",
                "key": "{{item.loginname}}"
            },
            all_result_data_handler=lambda data: list(
                map(lambda item:  {'key': item['key'], 'jsonValue': json.loads(item['jsonValue'])},
                    filter(lambda item: item, data))),
        )

        def get_work_order_id_for_po(item):
            return next(reversed(list(filter(lambda x: x['loginName'] == item['loginname'] and
                                             datetime.strptime(x['itemStartDate'], config.output_date_format) <=
                                             datetime.strptime(item['entrydate'], config.input_date_format) and
                                             datetime.strptime(x['itemEndDate'], config.output_date_format) >= datetime.strptime(
                item['entrydate'], config.input_date_format),
                list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('getkeyvalue_for_dxc_po_rate')))
                     )))), {}).get('workOrderNumber', '')

        create_csv_purchase_order = rail.WriteCSVFileOperator(
            task_id='create_csv_purchase_order',
            source="{{ result('query_list_c1userdata') }}",
            header=header,
            row=lambda item:  {
                'Work_Order_Id': get_work_order_id_for_po(item),
                'Last_Name': item['userlastname'],
                'First_Name': item['userfirstname'],
                'Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Week_Start_Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Cost_Center_Code': item['costcenter'],
                'Task_Code': '799' if item['attendancetypecode'] == "799" else "Hours Worked - Billable",
                'Rate_Category_Code': "DT" if item['ratetype'] == "Double Time" else
                                      "OT" if item['ratetype'] == "Overtime" else
                                      "ST" if item['ratetype'] == "Straight Time" else None,
                'UOM': 'Hr',
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

        load_csv_purchase_order = rail.LoadCSVFileOperator(
            task_id="load_csv_purchase_order",
            document="{{ result('create_csv_purchase_order') }}",
        )

        create_finaldata_collection = rail.CreateCollectionOperator(
            task_id='create_finaldata_collection',
            source="{{ result('load_csv_purchase_order') }}",
            name="finaldata_c1",
        )

        query_list_getuniquedatacombinations = rail.QueryCollectionOperator(
            task_id='query_list_getuniquedatacombinations',
            query='''SELECT  work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,
                    SUM( CAST(sat_hrs as DECIMAL) ) as sat_hrs,
                    SUM( CAST(sun_hrs as DECIMAL) ) as sun_hrs,
                    SUM( CAST(mon_hrs as DECIMAL) ) as mon_hrs,
                    SUM( CAST(tue_hrs as DECIMAL) ) as tue_hrs,
                    SUM( CAST(wed_hrs as DECIMAL) ) as wed_hrs,
                    SUM( CAST(thu_hrs as DECIMAL) ) as thu_hrs,
                    SUM( CAST(fri_hrs as DECIMAL) ) as fri_hrs,
                    _c__CATW as catw
                    FROM finaldata_c1
                    GROUP BY work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,_c__CATW
                    ''',
        )

        compose_finaldata_csv = rail.WriteCSVFileOperator(
            task_id='compose_finaldata_csv',
            source="{{ result('query_list_getuniquedatacombinations') }}",
            header=header,
            row=lambda item: item.values(),
        )

        log_message_filename = rail.PythonOperator(
            task_id='log_message_filename',
            python_callable=lambda: f"RepTS_C1_AMS_{datetime.utcnow().strftime('%y%m%d_%H%M%S')}.csv"
        )

        encrypt_finaldata_csv = rail.PGPEncryptionOperator(
            task_id='encrypt_finaldata_csv',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('compose_finaldata_csv') }}",
        )

        upload_file_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_file_to_sftp',
            content="{{ result('encrypt_finaldata_csv') }}",
            remote_filepath=config.field_glass_output_filepath +
            '/{{ result("log_message_filename") }}.pgp',
        )

        send_mail_timedatafileexportfailed = rail.EmailOperator(
            task_id='send_mail_timedatafileexportfailed',
            trigger_rule='one_failed',
            to=config.alert_email,
            subject='''{{get_company_key()}} | Compass Time data export automation (C1) - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}''',
            # pylint: disable=line-too-long
            html_content='''<p>Hi Team,<br /> <br /> The Replicon CWF time extract for Fieldglass C1 has been completed at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}},
                        however the file upload to sftp has failed with error.</p>
                        <ul>
                        <li>Recipe ID: {{ dag_run.dag_id}}</li>
                            <li>Job ID: {{ ecid() }}</li>
                            <li>Instance: {{ get_company_key() }}</li>
                            <li>File Name:  {{ result("log_message_filename") }}.pgp</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + '''</li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp.
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.field_glass_output_filepath
            },
            files=[
                ('{{ result("log_message_filename") }}.pgp',
                 "{{ result('encrypt_finaldata_csv') }}"
                 ),
            ]
        )

        send_export_complete_mail = rail.EmailOperator(
            task_id='send_export_complete_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=line-too-long
            subject='''{{ get_company_key() }} | Replicon CWF time extract for Fieldglass (C1) - Completed Successfully -  {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />
            Hello, <br /> <br /> The Replicon CWF time extract for Fieldglass C1 job is Completed successfully at {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}.
            Please find the file details below:<br /><br />
            File path: {{params.upload_path}}<br />
            File name: {{ result("log_message_filename") }}.pgp
            <br /></p>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>
            ''',
            params={
                'upload_path': config.field_glass_output_filepath,
            },
        )

        c1_task_finish = rail.EmptyOperator(
            task_id='c1_task_finish'
        )

        c1_task_start >> has_c1userdata
        has_c1userdata >> rail.Label('Yes') >> query_list_uniqueusersforc1 >> getkeyvalue_for_dxc_po_rate >> create_csv_purchase_order >> \
            load_csv_purchase_order >> create_finaldata_collection >> query_list_getuniquedatacombinations >> \
            compose_finaldata_csv >> log_message_filename >> encrypt_finaldata_csv >> upload_file_to_sftp
        upload_file_to_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed >> c1_task_finish
        upload_file_to_sftp >> rail.Label('success') >> send_export_complete_mail >> \
            c1_task_finish
        has_c1userdata >> rail.Label(
            'No') >> c1_task_finish

    return c1_task
