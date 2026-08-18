from datetime import datetime
import itertools

import rail


header = [
    'Entryid',
    'Shortid',
    'Externalsystemidentifier',
    'Perner',
    'Date',
    'Projectname',
    'Attendanceabsencetype',
    'Hours',
    'Comments',
    'Iwoexternalsystem',
    'Attribute1',
    'Attribute2',
    'Externalprojecttask',
    'Remainingwork',
    'Cfield1',
    'Cfield2',
    'Cfield3',
    'workorder',
    'Ratetype',
    'Tmrate',
    'Gsapbillingkey',
    'Gsaptask',
    'Gsapbillableflag',
    'region'
]

open_bracket = '{{'
close_braket = '}}'


def get_compass_task(config, task_type, region, output_filename):
    with rail.TaskGroup(group_id=f'compass_task_{task_type}', prefix_group_id=False) as compass_task:

        compass_task_start = rail.EmptyOperator(
            task_id=f'compass_task_start_{task_type}'
        )

        query_list_data = rail.QueryCollectionOperator(
            task_id=f'query_list_data_{task_type}',
            query=f'''SELECT * from datatodivide WHERE region='{region}' ORDER BY CAST(hours as DECIMAL) ASC''',
        )

        has_data = rail.IfOperator(
            task_id=f'has_data_{task_type}',
            test=f"{open_bracket} result('query_list_data_{task_type}','length') > 0 {close_braket}",
            yes_task=f"create_csv_lines_{task_type}",
            no_task=f"compass_task_finish_{task_type}",
        )

        def map_compass_row(item):
            item['length'] = int(item['length'])
            return {
                "column_0": item['timentryid2'],
                "column_1": item['timeentryid'] if item['timentryid2'] else item['timeoffbookingid'],
                "column_2": "REPLICON",
                "column_3": item['employeeid'],
                "column_4": datetime.strptime(item['entrydate'], config.entry_date_format).strftime("%Y%m%d"),
                "column_5": item['iwowbselement'] if item['companycodecode'] == "C1" else item['projectname'],
                "column_6": "400" if item['companycodecode'] == "C1" else
                item['attendancetypecode'] if item['attendancetypecode'] else
                item['timeofftypedescription'] if item['timeoffbookingid'] else '',
                "column_7": round(float(item['hours']), 2),
                "column_8": item['comments'],
                "column_9": item['wbstype'] if item['companycodecode'] == "C1" else '',
                "column_10": item['taskname'] if item['tasktype'] == "Attribute 1" else
                    item['taskfullpath'].split(" / ")[0] if item['tasktype'] == "GSAP Task" and item['length'] == 4 else
                    item['taskfullpath'].split(" / ")[0] if item['tasktype'] == "GSAP Billing Key" and item['length'] == 3 else
                    item['taskfullpath'].split(" / ")[0] if item['tasktype'] == "Attribute 2" and item['length'] == 2 else
                    item['taskfullpath'].split(" / ")[0] if item['tasktype'] == "PPMC Project & Task" and item['length'] == 3 else
                    item['taskfullpath'].split(
                        " / ")[0] if item['length'] == 2 else item['attributecode1'],
                "column_11": item['taskname'] if item['tasktype'] == "Attribute 2" else
                    item['attributecode2'] if item['attributecode2'] else
                    item['taskfullpath'].split(" / ")[1] if item['tasktype'] == "GSAP Task" and item['length'] == 4 else
                    item['taskfullpath'].split(" / ")[1] if item['tasktype'] == "GSAP Billing Key" and item['length'] == 3 else
                    item['taskfullpath'].split(
                        " / ")[1] if item['tasktype'] == "PPMC Project & Task" and item['length'] == 3 else '',
                "column_12": item['taskname'] if "PPMC" in item['tasktype'] else '',
                "column_13": item['newremainningwork'],
                "column_14": item['customer1'],
                "column_15": item['customer2'],
                "column_16": item['customer3'],
                "column_17":  '' if item['projecttype'] == "ES" and item['projectname'].startswith("E-") else
                next(reversed(list(filter(lambda x: x['loginName'] == item['loginname'] and
                                          datetime.strptime(x['workOrderStartDate'], config.output_date_format) <=
                                          datetime.strptime(item['entrydate'], config.entry_date_format) and
                                          datetime.strptime(x['workOrderEndDate'], config.output_date_format) >= datetime.strptime(
                    item['entrydate'], config.entry_date_format),
                    list(itertools.chain(*map(lambda x: x['jsonValue'], rail.result('get_key_value_workorder_rate')))))),
                ), {}).get('workOrderId', ''),

                "column_18": '' if item['projecttype'] == "ES" and item['projectname'].startswith("E-") else
                "DT" if item['ratetype'] == "Double Time" else
                "OT" if item['ratetype'] == "Overtime" else
                "ST" if item['ratetype'] == "Straight Time" else
                '',
                "column_19": "",
                "column_20": item['taskname'] if item['tasktype'] == "GSAP Billing Key" else
                item['taskfullpath'].split(" / ")[0] if item['tasktype'] == "GSAP Task" and item['length'] == 2 else
                item['taskfullpath'].split(" / ")[1] if item['length'] == 3 else
                item['taskfullpath'].split(
                    " / ")[2] if item['length'] == 4 else '',
                "column_21": item['taskname']if item['tasktype'] == "GSAP Task" else '',
                "column_22": "X" if item['gsapbillableflag'] else '',
                "column_23": region,
            }.values()

        create_csv_line = rail.WriteCSVFileOperator(
            task_id=f'create_csv_lines_{task_type}',
            source=f"{open_bracket} result('query_list_data_{task_type}') {close_braket}",
            header=header,
            row=map_compass_row
        )

        load_csv_create_list_from_csv = rail.LoadCSVFileOperator(
            task_id=f"load_csv_create_list_from_csv_{task_type}",
            document=f"{open_bracket} result('create_csv_lines_{task_type}') {close_braket}",
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id=f'create_collection_create_list_from_csv_{task_type}',
            source=f"{open_bracket} result('load_csv_create_list_from_csv_{task_type}') {close_braket}",
            name=f"finaldata{task_type}"
        )

        def do_get_dataset():
            records = rail.load_all_records(rail.result(
                f'create_collection_create_list_from_csv_{task_type}'))
            region = records[0]['region']
            return {
                'records': records,
                'region': [{'name': region}]
            }

        create_document = rail.RenderTemplateOperator(
            task_id=f'create_document_{task_type}',
            target='artifact',
            dataset=do_get_dataset,
            template_file='compass_cwf_time_outbound.xml'
        )

        upload_uploadfiletosftp = rail.SFTPUploadFileOperator(
            task_id=f'upload_uploadfiletosftp_{task_type}',
            content=f"{open_bracket} result('create_document_{task_type}') {close_braket}",
            remote_filepath=f'{config.compass_output_filepath}/{output_filename}{open_bracket} result("log_message_filename_compass") {close_braket}.xml',
        )

        send_mail_timedatafileexportfailed = rail.EmailOperator(
            task_id=f'send_mail_timedatafileexportfailed_{task_type}',
            trigger_rule='one_failed',
            to=config.tenant_email,
            subject='''{{get_company_key()}} | Compass Time data export automation - SFTP upload failure - {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}''',
            html_content='''<p>Hi Team,<br /> <br /> The Compass time date export has been completed at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}},
                        however the file upload to sftp has failed with error.</p>
                        <ul>
                        <li>Recipe ID: {{ dag_run.dag_id}}</li>
                            <li>Job ID: {{ ecid() }}</li>
                            <li>Instance: {{ get_company_key() }}</li>
                            <li>File Name:  ''' + output_filename + '''{{ result("log_message_filename_compass") }}.xml</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + '''</li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp. 
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.compass_output_filepath
            },
            files=[
                (f'{output_filename}{open_bracket} result("log_message_filename_compass") {close_braket}.xml',
                 f'{open_bracket} result("create_document_{task_type}") {close_braket}',
                 ),
            ]
        )

        send_mail = rail.EmailOperator(
            task_id=f'send_mail_{task_type}',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=f-string-without-interpolation
            subject='{{get_company_key()}} | Replicon CWF time extract for Compass ' +
            region + ' - Completed Successfully {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ''',
            html_content=f'''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for Compass ''' + region + ''' is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                Please find the file details below:<br /><br />
                File path: {{ params.sftp_path}}<br />
                File Name:  ''' + output_filename + '''{{ result("log_message_filename_compass") }}.xml
                <br /></p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={
                'sftp_path': config.compass_output_filepath
            }
        )

        compass_time_data_postdatatoendpoint = rail.HTTPUploadFileOperator(
            task_id=f'compass_time_data_postdatatoendpoint_{task_type}',
            http_conn_id=config.compass_http_conn_id,
            method='POST',
            content_type='application/xml',
            content=f"{open_bracket} result('create_document_{task_type}') {close_braket}",
            extra_options= {
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        compass_task_finish = rail.EmptyOperator(
            task_id=f'compass_task_finish_{task_type}'
        )

        compass_task_start >> query_list_data >> has_data
        has_data >> rail.Label('Yes') >> create_csv_line
        has_data >> rail.Label('No') >> compass_task_finish
        create_csv_line >> load_csv_create_list_from_csv >> create_collection_create_list_from_csv >> create_document >> upload_uploadfiletosftp
        upload_uploadfiletosftp >> rail.Label(
            'fail') >> send_mail_timedatafileexportfailed
        upload_uploadfiletosftp >> rail.Label(
            'success') >> send_mail >> compass_time_data_postdatatoendpoint >> compass_task_finish

    return compass_task
