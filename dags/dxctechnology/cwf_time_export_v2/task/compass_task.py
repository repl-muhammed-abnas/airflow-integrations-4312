from datetime import datetime, timedelta
import itertools

import rail
from dxctechnology.cwf_time_export_v2.utils import python_callable_method
from dxctechnology.cwf_time_export_v2.utils import request_payload

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

#pylint: disable=too-many-arguments


def get_compass_task(config, task_type, region, output_filename, compass_oef_name, internal_oef_name, unique_id):
    with rail.TaskGroup(group_id=f'compass_task_{task_type}', prefix_group_id=False) as compass_task:

        compass_task_start = rail.EmptyOperator(
            task_id=f'compass_task_start_{task_type}'
        )

        check_previous_export_ack = rail.IfOperator(
            task_id=f'check_previous_export_ack_{task_type}',
            test=lambda: python_callable_method.check_ack_received(
                rail.result("get_last_time_export_details"), compass_oef_name),
            yes_task=f'check_current_export_ack_{task_type}',
            no_task=f'process_acknowledgement_not_received_{task_type}'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunForEachItemOperator(
            task_id=f'process_acknowledgement_not_received_{task_type}',
            retries=0,
            items='{{ dag_run.conf.twb_list | to_json }}',
            trigger_dag_id=f'dxctechnology_acknowledgement_not_received_notification_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "name": item["name"],
                "uri": item["uri"],
                "createdatetime": item["createdatetime"],
                "erp": "compass",
                "twbname": python_callable_method.get_dag_run_conf()['twbname'],
                "oef_name": compass_oef_name,
                # pylint: disable=comparison-of-constants
                'sender': output_filename[:3] if '{{ get_company_key() }}' == "DXCTechnology" else compass_oef_name[12:15]
            }
        )

        wait_to_process_acknowledgement_not_received = rail.WaitForDagRunsSensor(
            task_id=f'wait_to_process_acknowledgement_not_received_{task_type}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs=f"{open_bracket} result('process_acknowledgement_not_received_{task_type}') {close_braket}",
        )

        gather_all_unckn_export_details = rail.GatherResultsFromDagRunsOperator(
            task_id=f'gather_all_unckn_export_details_{task_type}',
            dag_runs=f"{open_bracket} result('process_acknowledgement_not_received_{task_type}') {close_braket}",
            dagrun_task_id='time_export_details_output',
            flatten=True,
        )

        get_unackn_email_content = rail.RenderTemplateOperator(
            task_id=f'get_unackn_email_content_{task_type}',
            target='result',
            template_file='templates/compass_output_template.html',
            dataset=lambda: request_payload.output_payload(
                rail.result(f"gather_all_unckn_export_details_{task_type}"))
        )

        send_unackn_email = rail.EmailOperator(
            task_id=f'send_unackn_email_{task_type}',
            to=config.compass_acknowledgement_email,
            
            # pylint: disable=comparison-of-constants
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for " }}' + \
            (output_filename[:3] if '{{ get_company_key() }}' == "DXCTechnology" else compass_oef_name[12:15]),
            html_content=f"{open_bracket} result('get_unackn_email_content_{task_type}') {close_braket}",
        )

        check_current_export_ack = rail.IfOperator(
            task_id=f'check_current_export_ack_{task_type}',
            test=lambda: python_callable_method.check_ack_received(
                rail.result("get_current_time_export_details"), internal_oef_name),
            no_task=f'log_message_filename_compass_{task_type}',
            yes_task=f'compass_task_finish_{task_type}'
        )

        query_list_data = rail.QueryCollectionOperator(
            task_id=f'query_list_data_{task_type}',
            query=f'''SELECT * from datatodivide WHERE region='{region}' ORDER BY CAST(hours as DECIMAL) ASC''',
        )

        has_data = rail.IfOperator(
            task_id=f'has_data_{task_type}',
            test=f"{open_bracket} result('query_list_data_{task_type}','length') > 0 {close_braket}",
            yes_task=f"create_csv_lines_{task_type}",
            no_task=f"get_final_line_no_data_{task_type}",
        )

        get_final_line_no_data = rail.PythonOperator(
            task_id=f"get_final_line_no_data_{task_type}",
            python_callable=lambda: python_callable_method.get_compass_final_line_data(
                region, internal_oef_name)
        )

        get_final_line_no_data_collection = rail.CreateCollectionOperator(
            task_id=f"get_final_line_no_data_collection_{task_type}",
            source=lambda: rail.result(f'get_final_line_no_data_{task_type}'),
            name=f"getfinaldatanodatacollection{task_type}"
        )

        def do_get_empty_dataset():
            records = rail.load_all_records(rail.result(
                f'get_final_line_no_data_collection_{task_type}'))
            region = records[0]['region']
            return {
                'records': records,
                'region': [{'name': region}]
            }

        generate_xml_time_no_data = rail.RenderTemplateOperator(
            task_id=f'generate_xml_time_no_data_{task_type}',
            target='artifact',
            dataset=do_get_empty_dataset,
            template_file='xml_schema/compass_cwf_time_outbound.xml'
        )

        send_time_no_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id=f'send_time_no_data_to_sftp_{task_type}',
            remote_filepath=config.compass_output_filepath +
            f'/{open_bracket} result("log_message_filename_compass_{task_type}") {close_braket}',
            content='{{result(\'' + generate_xml_time_no_data.task_id + '\')}}',
        )

        is_allowed_send_export_no_data = rail.IfOperator(
            task_id=f'is_allowed_send_export_no_data_{task_type}',
            test=config.is_allowed_send_export_data,
            yes_task=f'upload_time_no_data_{task_type}',
            no_task=f'send_mail_no_data_{task_type}'
        )

        upload_time_no_data = rail.HTTPUploadFileOperator(
            task_id=f'upload_time_no_data_{task_type}',
            content='{{result(\'' + generate_xml_time_no_data.task_id + '\')}}',
            retries=0,
            content_type="application/xml",
            http_conn_id=config.compass_http_conn_id,
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        send_mail_no_data = rail.EmailOperator(
            task_id=f'send_mail_no_data_{task_type}',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=f-string-without-interpolation
            subject='{{get_company_key()}} | Replicon CWF time extract for Compass ' +
            region + \
            ' - No records to export {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ''',
            html_content=f'''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for Compass ''' + region + ''' is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                There are no records to export.The payload identifier is ''' f'{open_bracket} dag_run.conf.{unique_id} {close_braket}.' '''</p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> '''
        )

        update_eof_field = rail.RepliconServiceOperator(
            task_id=f'update_eof_field_{task_type}',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_param(internal_oef_name)
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

        log_message_filename_compass = rail.PythonOperator(
            task_id=f'log_message_filename_compass_{task_type}',
            python_callable=lambda dag_run: output_filename[:4] +
            dag_run.conf['twbname']+'.xml'
        )

        create_collection_create_list_from_csv = rail.CreateCollectionOperator(
            task_id=f'create_collection_create_list_from_csv_{task_type}',
            source=f"{open_bracket} result('load_csv_create_list_from_csv_{task_type}') {close_braket}",
            name=f"finaldata{task_type}"
        )

        get_final_line = rail.PythonOperator(
            task_id=f"get_final_line_{task_type}",
            python_callable=lambda: python_callable_method.get_compass_final_line_data(
                region, internal_oef_name)
        )

        def do_get_dataset():
            unique_record = rail.result(f'get_final_line_{task_type}')
            all_records = rail.load_all_records(rail.result(
                f'create_collection_create_list_from_csv_{task_type}'))
            records = [*unique_record, *all_records]
            region = records[0]['region']
            return {
                'records': records,
                'region': [{'name': region}]
            }

        create_document = rail.RenderTemplateOperator(
            task_id=f'create_document_{task_type}',
            target='artifact',
            dataset=do_get_dataset,
            template_file='xml_schema/compass_cwf_time_outbound.xml'
        )

        upload_uploadfiletosftp = rail.SFTPUploadFileOperator(
            task_id=f'upload_uploadfiletosftp_{task_type}',
            content=f"{open_bracket} result('create_document_{task_type}') {close_braket}",
            remote_filepath=f'{config.compass_output_filepath}/{open_bracket} result("log_message_filename_compass_{task_type}") {close_braket}',
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
                            <li>File Name: ''' f'{open_bracket} result("log_message_filename_compass_{task_type}") {close_braket}' '''</li>
                            <li>SFTP Path: {{ params.sftp_path}}</li>
                            <li>Error: ''' + config.error_template + '''</li>
                            </ul>
                            <p>Please find the attached file to be uploaded to sftp.
                            Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p> ''',
            params={
                'sftp_path': config.compass_output_filepath
            },
            files=[
                (f'{output_filename}{open_bracket} result("log_message_filename_compass_{task_type}") {close_braket}.xml',
                 f'{open_bracket} result("create_document_{task_type}") {close_braket}',
                 ),
            ]
        )

        send_success_mail = rail.EmailOperator(
            task_id=f'send_success_mail_{task_type}',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            # pylint: disable=f-string-without-interpolation
            subject='{{get_company_key()}} | Replicon CWF time extract for Compass ' +
            region + \
            ' - Completed Successfully {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}} ''',
            html_content=f'''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br />
                The Replicon time extract for CWFTime for Compass ''' + region + ''' is completed successfully at {{current_time("%Y-%m-%dT%H:%M:%S.%f%z")}}.
                Please find the file details below:<br /><br />
                File path: {{ params.sftp_path}}<br />
                File Name: ''' f'{open_bracket} result("log_message_filename_compass_{task_type}") {close_braket}' ''' <br />
                Payload identifier: ''' f'{open_bracket} dag_run.conf.{unique_id} {close_braket}.' '''
                <br /></p>
                <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={
                'sftp_path': config.compass_output_filepath
            }
        )

        is_allowed_send_time_data = rail.IfOperator(
            task_id=f'is_allowed_send_time_data_{task_type}',
            test=config.is_allowed_send_export_data,
            yes_task=f'compass_time_data_postdatatoendpoint_{task_type}',
            no_task=f'send_success_mail_{task_type}'
        )

        compass_time_data_postdatatoendpoint = rail.HTTPUploadFileOperator(
            task_id=f'compass_time_data_postdatatoendpoint_{task_type}',
            http_conn_id=config.compass_http_conn_id,
            method='POST',
            content_type='application/xml',
            content=f"{open_bracket} result('create_document_{task_type}') {close_braket}",
            extra_options={
                'verify': False
            } if config.instance == "DXCSandbox" else None
        )

        compass_task_finish = rail.EmptyOperator(
            task_id=f'compass_task_finish_{task_type}'
        )

        compass_task_start >> check_previous_export_ack >> rail.Label(
            "No") >> process_acknowledgement_not_received >> wait_to_process_acknowledgement_not_received >> \
            gather_all_unckn_export_details >> get_unackn_email_content >> send_unackn_email >> check_current_export_ack

        check_previous_export_ack >> rail.Label(
            "Yes") >> check_current_export_ack >> rail.Label("Yes") >> compass_task_finish
        check_current_export_ack >> rail.Label(
            "No") >> log_message_filename_compass >> query_list_data >> has_data
        has_data >> rail.Label('Yes') >> create_csv_line
        has_data >> rail.Label('No') >> get_final_line_no_data >> get_final_line_no_data_collection >> generate_xml_time_no_data >> \
            send_time_no_data_to_sftp >> is_allowed_send_export_no_data >> rail.Label(
                "No") >> send_mail_no_data >> update_eof_field
        is_allowed_send_export_no_data >> rail.Label(
            "Yes") >> upload_time_no_data >> send_mail_no_data
        create_csv_line >> load_csv_create_list_from_csv >> create_collection_create_list_from_csv >> get_final_line >> \
            create_document >> upload_uploadfiletosftp
        upload_uploadfiletosftp >> rail.Label(
            'fail') >> send_mail_timedatafileexportfailed
        upload_uploadfiletosftp >> rail.Label(
            'success') >> is_allowed_send_time_data >> rail.Label("No") >> send_success_mail >> update_eof_field >> compass_task_finish
        is_allowed_send_time_data >> rail.Label(
            "Yes") >> compass_time_data_postdatatoendpoint >> send_success_mail

    return compass_task
