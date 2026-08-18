from datetime import datetime
import itertools

import rail

open_bracket = '{{'
close_braket = '}}'


def get_compass_task(config, task_type, region, header, output_filename):
    with rail.TaskGroup(group_id=f'compass_task_{task_type}', prefix_group_id=False) as compass_task:

        compass_task_start = rail.EmptyOperator(
            task_id=f'compass_task_start_{task_type}'
        )

        query_list = rail.QueryCollectionOperator(
            task_id=f'query_list_{task_type}',
            query=f'''SELECT * FROM compassdatatodivide WHERE region='{region}' ''',
        )

        has_data = rail.IfOperator(
            task_id=f'has_{task_type}_data',
            test=lambda: rail.result(f'query_list_{task_type}', 'length') > 0,
            yes_task=f"create_csv_compass_{task_type}",
            no_task=f"compass_task_finish_{task_type}",
        )

        def map_row_compass(item):
            return{
                'Work_Order_Id': next(reversed(list(filter(lambda x: x['loginName'] == item['loginname'] and
                                                           datetime.strptime(x['workOrderStartDate'], config.output_date_format) <=
                                                           datetime.strptime(item['entrydate'], config.input_date_format) and
                                                           datetime.strptime(x['workOrderEndDate'], config.output_date_format) >=
                                                           datetime.strptime(
                                                               item['entrydate'], config.input_date_format),
                                                           list(itertools.chain(*map(lambda x: x['jsonValue'],  rail.result('getkeyvalue_compass_rates')))
                                                                )))), {}).get('workOrderId', ''),
                'Last_Name': item['lastname'],
                'First_Name': item['firstname'],
                'Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Week_Start_Date': datetime.strptime(item['timesheetperiod'].split('-')[0], config.report_date_format).strftime(config.output_date_format),
                'Cost_Center_Code':  next(reversed(list(filter(lambda x: x['loginName'] == item['loginname'] and
                                                               x['workOrderStartDate'] <= item['entrydate'] and
                                                               x['workOrderEndDate'] >= item['entrydate'],
                                                               list(itertools.chain(*map(lambda x: x['jsonValue'],  rail.result('getkeyvalue_compass_rates')))
                                                                    )))), {}).get('costCenterCode'),
                'Task_Code': '799' if item['attendencetypecode'] == "799" else "Hours Worked - Billable",
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

        create_csv_compass = rail.WriteCSVFileOperator(
            task_id=f'create_csv_compass_{task_type}',
            source=f"{open_bracket} result('query_list_{task_type}') {close_braket}",
            header=header,
            row=map_row_compass,
        )

        load_csv = rail.LoadCSVFileOperator(
            task_id=f"load_csv_{task_type}",
            document=f"{open_bracket} result('create_csv_compass_{task_type}') {close_braket}",
        )

        create_collection = rail.CreateCollectionOperator(
            task_id=f'create_collection_{task_type}_list',
            source=f"{open_bracket} result('load_csv_{task_type}') {close_braket}",
            name=f"finaldata2_{task_type}"
        )

        query_list_getuniquedata = rail.QueryCollectionOperator(
            task_id=f'query_list_getuniquedata_{task_type}',
            query=f'''SELECT  work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,
                    SUM( CAST(sat_hrs as DECIMAL) ) as sat_hrs,
                    SUM( CAST(sun_hrs as DECIMAL) ) as sun_hrs,
                    SUM( CAST(mon_hrs as DECIMAL) ) as mon_hrs,
                    SUM( CAST(tue_hrs as DECIMAL) ) as tue_hrs,
                    SUM( CAST(wed_hrs as DECIMAL) ) as wed_hrs,
                    SUM( CAST(thu_hrs as DECIMAL) ) as thu_hrs,
                    SUM( CAST(fri_hrs as DECIMAL) ) as fri_hrs,
                    _c__CATW as catw 
                    FROM finaldata2_{task_type}
                    GROUP BY work_order_id, last_name,first_name,date,week_start_date,cost_center_code,task_code,rate_category_code,uom,_c__CATW
                    ''',
        )

        create_csv_finalcompass_data = rail.WriteCSVFileOperator(
            task_id=f'create_csv_finalcompass_data_{task_type}',
            source=f"{open_bracket} result('query_list_getuniquedata_{task_type}') {close_braket}",
            header=header,
            row=lambda item: item.values(),
        )

        log_message_compass_filename = rail.PythonOperator(
            task_id=f'log_message_compass_filename_{task_type}',
            python_callable=lambda: f'{output_filename}_{datetime.utcnow().strftime("%Y%m%d_%H%M%S")}.csv',
        )

        encrypt_compass_finaldata_csv = rail.PGPEncryptionOperator(
            task_id=f'encrypt_compass_finaldata_csv_{task_type}',
            pgp_conn_id=config.pgp_conn_id,
            source=f"{open_bracket} result('create_csv_finalcompass_data_{task_type}') {close_braket}",
        )

        upload_compassto_sftp = rail.SFTPUploadFileOperator(
            task_id=f'upload_compassto_sftp_{task_type}',
            content=f"{open_bracket} result('encrypt_compass_finaldata_csv_{task_type}') {close_braket}",
            remote_filepath=f'{config.field_glass_output_filepath}/{open_bracket} result("log_message_compass_filename_{task_type}") {close_braket}.pgp',
        )

        # pylint: disable=line-too-long
        send_mail_timedatafileexportfailed = rail.EmailOperator(
            task_id=f'send_mail_timedatafileexportfailed_{task_type}',
            trigger_rule='one_failed',
            to=config.tenant_email,
            subject=f'''{open_bracket}get_company_key(){close_braket} | Compass Time data export automation {region} - SFTP upload failure - {open_bracket}current_time("%Y-%m-%dT%H:%M:%S.%f%z"){close_braket}''',
            html_content=f'''<p>Hi Team,<br /> <br /> The Replicon CWF time extract for Fieldglass {region} has been completed at {open_bracket}current_time("%Y-%m-%dT%H:%M:%S.%f%z"){close_braket},
                        however the file upload to sftp has failed with error.</p>
                        <ul>
                        <li>Recipe ID: {open_bracket} dag_run.dag_id{close_braket}</li>
                            <li>Job ID: {open_bracket} ecid() {close_braket}</li>
                            <li>Instance: {open_bracket} get_company_key() {close_braket}</li>
                            <li>File Name:  {open_bracket} result("log_message_compass_filename_{task_type}") {close_braket}.pgp</li>
                            <li>SFTP Path: {open_bracket} params.sftp_path {close_braket}</li>
                            <li>Error: {config.error_template}</li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp. 
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.field_glass_output_filepath
            },
            files=[
                (f'{open_bracket} result("log_message_compass_filename_{task_type}") {close_braket}.pgp',
                 f"{open_bracket} result('encrypt_compass_finaldata_csv_{task_type}') {close_braket}",
                 ),
            ]
        )

        # pylint: disable=line-too-long
        send_compass_mail = rail.EmailOperator(
            task_id=f'send_compass_mail_{task_type}',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject=f'''{open_bracket} get_company_key() {close_braket} | Replicon CWF time extract for Fieldglass Compass {region} - Completed successfully -  {open_bracket} current_time("%Y-%m-%dT%H:%M:%S.%f%z") {close_braket} ''',
            html_content=f'''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />
            Hello, <br /> <br /> The Replicon CWF time extract for Fieldglass Compass {region} job is Completed successfully at {open_bracket} current_time("%Y-%m-%dT%H:%M:%S.%f%z") {close_braket}. 
            Please find the file details below:<br /><br />
            File path: {open_bracket}params.upload_path{close_braket}<br />
            File name: {open_bracket} result("log_message_compass_filename_{task_type}") {close_braket}.pgp
            <br /></p>
            <p>
            Regards,<br />
            Deltek Inc.
            </p> ''',
            params={
                'upload_path': config.field_glass_output_filepath,
            }
        )

        compass_task_finish = rail.EmptyOperator(
            task_id=f'compass_task_finish_{task_type}'
        )

        compass_task_start >> query_list >> has_data
        has_data >> rail.Label('Yes') >> create_csv_compass >> load_csv >> create_collection >> query_list_getuniquedata >> \
            create_csv_finalcompass_data >> log_message_compass_filename >>\
            encrypt_compass_finaldata_csv >> upload_compassto_sftp
        upload_compassto_sftp >> rail.Label(
            'success') >> send_compass_mail >> compass_task_finish
        upload_compassto_sftp >> rail.Label(
            'error') >> send_mail_timedatafileexportfailed >> compass_task_finish
        has_data >> rail.Label('Yes') >> compass_task_finish

    return compass_task
