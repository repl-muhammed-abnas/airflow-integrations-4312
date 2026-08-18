import json
from datetime import datetime
import rail
from dxctechnology.ftp_time_export import config


with rail.create_airflow_dag(
    dag_id='dxctechnology_time_export_ftp',
    description='Export time data for FTP ',
    company_key=config.company_key,
    replicon_conn_id=config.replicon_conn_id,
    schedule_interval='0 0,6,12,18 * * *',
    default_args={
        'sftp_conn_id': config.sftp_conn_id,
        'http_conn_id': config.http_conn_id,
    }
) as dag:

    def getExportName():
        return 'Time_Extract_FTP_{{data_interval_end.strftime(\'%Y%m%d%H%M%S\')}}'

    def findEmployeeType(response, name):
        for ele in response.json()['d']:
            if ele['displayText'] == name:
                return ele['uri']
        raise Exception(f'Unable to locate Employee Type {name}')

    def getScriptUri(response):
        for ele in response.json()['d']:
            if ele['displayText'] == 'Time Export - Master':
                return ele['uri']
        raise Exception('Unable to locate script Time Export - Master')

    is_sunday_run = rail.IfOperator(
        task_id="is_sunday_run",
        test="{{data_interval_end.strftime(\'%H%M%S\') == '060000' and data_interval_end.strftime(\'%A\') == 'Sunday'}}",
        no_task='start',
    )

    start = rail.EmptyOperator(
        task_id='start'
    )

    get_actual_start_time = rail.RenderTemplateOperator(
        task_id="get_actual_start_time",
        target="result",
        template="{{macros.datetime.now()}}"
    )

    resolve_file_name = rail.RenderTemplateOperator(
        task_id='resolve_file_name',
        target='result',
        template='ReplicontoFTP{{data_interval_end.strftime(\'%m%d%YT%H%M%S\')}}.xml'
    )

    get_ftp_divisions = rail.GetGroupsMatchingFilterOperator(
        task_id='get_ftp_divisions',
        group_type='Division',
        text_search='FTP',
    )

    time_export_download_script = rail.RepliconServiceOperator(
        task_id='time_export_download_script',
        endpoint='/services/TimeDataDownloadScriptAdministrationService1.svc/GetAllScripts',
        response_filter=getScriptUri
    )

    employee_type_contractor = rail.RepliconServiceOperator(
        task_id='employee_type_contractor',
        endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups',
        response_filter=lambda response: findEmployeeType(
            response, 'Contractor')
    )

    employee_type_agency_contractor = rail.RepliconServiceOperator(
        task_id='employee_type_agency_contractor',
        endpoint='/services/EmployeeTypeGroupService1.svc/GetAllEmployeeTypeGroups',
        response_filter=lambda response: findEmployeeType(
            response, 'Agency Contractor')
    )
    log_filename_and_twbname = rail.DumpToOutputOperator(
        task_id='log_filename_and_twbname',
        source=json.dumps({
            'exportfilename': '{{result(\'' + resolve_file_name.task_id + '\')}}',
            'twbname': getExportName()
        }),
        raw=True
    )

    def getExportRequest():
        return json.dumps({
            'columnUris': [],
            'filterExpression': {
                'leftExpression': {
                    'leftExpression': {
                        'leftExpression': {
                            'leftExpression': {
                                'filterDefinitionUri': 'urn:replicon:time-data-export-filter:entry-date-range'
                            },
                            'operatorUri': 'urn:replicon:filter-operator:in',
                            'rightExpression': {
                                'value': {
                                    'dateRange': {
                                        'startDate': {
                                            'year': '{{ (data_interval_end - macros.timedelta(days=90)).strftime(\'%Y\') }}',
                                            'month': '{{(data_interval_end - macros.timedelta(days=90)).strftime(\'%m\')}}',
                                            'day': '{{(data_interval_end - macros.timedelta(days=90)).strftime(\'%d\')}}'
                                        },
                                        'endDate': {
                                            'year': '{{ (data_interval_end + macros.timedelta(days=30)).strftime(\'%Y\') }}',
                                            'month': '{{(data_interval_end + macros.timedelta(days=30)).strftime(\'%m\')}}',
                                            'day': '{{(data_interval_end + macros.timedelta(days=30)).strftime(\'%d\')}}'
                                        }
                                    }
                                }
                            }
                        },
                        'operatorUri': 'urn:replicon:filter-operator:and',
                        'rightExpression': {
                            'leftExpression': {
                                'filterDefinitionUri': 'urn:replicon:time-data-export-filter:time-data-export-status'
                            },
                            'operatorUri': 'urn:replicon:filter-operator:in',
                            'rightExpression': {
                                'value': {
                                    'uris': [
                                        'urn:replicon:time-data-item-time-data-export-status:none'
                                    ]
                                }
                            }
                        }
                    },
                    'operatorUri': 'urn:replicon:filter-operator:and',
                    'rightExpression': {
                        'leftExpression': {
                            'filterDefinitionUri': 'urn:replicon:time-data-export-filter:employee-type-group'
                        },
                        'operatorUri': 'urn:replicon:filter-operator:not-in',
                        'rightExpression': {
                            'value': {
                                'uris': [
                                    '{{result(\'' + employee_type_contractor.task_id + '\')}}',
                                    '{{result(\'' + employee_type_agency_contractor.task_id + '\')}}'
                                ]
                            }
                        }
                    }
                },
                'operatorUri': 'urn:replicon:filter-operator:and',
                'rightExpression': {
                    'leftExpression': {
                        'leftExpression': {
                            'filterDefinitionUri': 'urn:replicon:time-data-export-filter:division'
                        },
                        'operatorUri': 'urn:replicon:filter-operator:in',
                        'rightExpression': {
                            'value': {
                                'uris': 'DIVISIONS'
                            }
                        }
                    },
                    'operatorUri': 'urn:replicon:filter-operator:and',
                    'rightExpression': {
                        'leftExpression': {
                            'filterDefinitionUri': 'urn:replicon:time-data-export-filter:time-entry-approval-status'
                        },
                        'operatorUri': 'urn:replicon:filter-operator:in',
                        'rightExpression': {
                            'value': {
                                'uris': [
                                    'urn:replicon:approval-status:approved'
                                ]
                            }
                        }
                    }
                }
            }
        }).replace('"DIVISIONS"', '{{ result(\'' + get_ftp_divisions.task_id + '\') | to_json }}')
    export = rail.time_data_export(
        group_id='time_data_export',
        generate_request=getExportRequest,
        get_export_name=getExportName,
        file_script_uri='result(\'' + time_export_download_script.task_id + '\')'
    )

    rename_export = rail.RepliconServiceOperator(
        task_id='rename_export',
        endpoint='/services/TimeDataExportService1.svc/UpdateTimeDataExportName',
        # TODO:this result is a bit weird cause it's getting the uri from deep
        # in the task_group
        data={
            'target': {
                'uri': '{{ result(\'time_data_export.get_export_uri\') }}'},
            'name': '' + getExportName() + '_Nodata'},
    )

    # pylint: disable=line-too-long
    empty_export_email_content = '''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon time extract for FTP is completed successfully at {{ data_interval_end }}. There are no records to export.</p><p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>'''
    empty_export_email_subject = '{{ get_company_key() }} | Daily Replicon time extract for FTP - No records to export - {{ data_interval_end }}'
    send_raw_empty_export_email = rail.EmailOperator(
        task_id='send_raw_empty_export_email',
        to=config.tenant_email,
        bcc=config.internal_email,
        subject=empty_export_email_subject,
        html_content=empty_export_email_content
    )
    send_filtered_empty_export_email = rail.EmailOperator(
        task_id='send_filtered_empty_export_email',
        to=config.tenant_email,
        bcc=config.internal_email,
        subject=empty_export_email_subject,
        html_content=empty_export_email_content,
    )

    def row_filter(row):
        if(row and row['Company Code Code'] == 'FTP'):
            return row

        return None

    filter_for_ftp_company_code = rail.DataAdaptorOperator(
        task_id='filter_for_ftp_company_code',
        # TODO:this result is a bit weird cause it's getting the uri from deep
        # in the task_group
        source='{{result(\'time_data_export.load_export\')}}',
        data=row_filter
    )
    send_result_to_downstream = rail.IfOperator(
        task_id="send_result_to_downstream",
        test="{{ var.value.dxctechnology_ftp_time_export_send_downstream == 'true' }}",
        yes_task=filter_for_ftp_company_code.task_id,
    )

    export_has_data = rail.HasDataOperator(
        task_id='export_has_data',
        source='{{ result("' + export[1].task_id + '") }}',
        yes_task=send_result_to_downstream.task_id,
        no_task=rename_export.task_id
    )

    def translate_row(row):
        if row:
            return {
                'Entryid': row['Short Time Entry ID'] if row['Time Entry ID'] != '' else row['Entry Date'] + row['Time Off Booking ID'],
                'Uniqueid': 'REPLICON',
                'Employeeid': row['Employee ID'],
                'Date': datetime.strptime(row['Entry Date'], '%Y%m%d').strftime('%m%d%Y'),
                'Wbs': row['WBS / SO Name'] if row['Master WBS (SO, WO)'] == 'WBS' else '',
                'Vblen': row['WBS / SO Name'][0:10] if row['Master WBS (SO, WO)'] == 'RO' else '',
                'Salesitem': row['WBS / SO Name'][-3:] if row['Master WBS (SO, WO)'] == 'RO' else '',
                # pylint: disable=line-too-long
                'Attendancetype': '1010' if row['IWO Indicator'] == 'X' else row['Time Type'].split('-')[0] if row['Time Type'] != '' else row['Attendance Type Code'] if row['Attendance Type Code'] != '' else row['Time Off Type Description'] if row['Time Off Booking ID'] != '' else '1010',
                'Hours': round(float(row['Hours (Current)']), 2),
                'Comments': '0 Hour' if float(row['Hours (Current)']) == 0 else 'Time Off' if row['Time Off Booking ID'] != '' else row['Comments'][0:39]
            }

        return None

    translate_csv = rail.DataAdaptorOperator(
        task_id='translate_csv',
        source='{{result(\'' + filter_for_ftp_company_code.task_id + '\')}}',
        data=translate_row,
        columns=[
            'Entryid',
            'Uniqueid',
            'Employeeid',
            'Date',
            'Wbs',
            'Vblen',
            'Salesitem',
            'Attendancetype',
            'Hours',
            'Comments']
    )

    filtered_export_has_data = rail.HasDataOperator(
        task_id='filtered_export_has_data',
        source='{{ result("' + filter_for_ftp_company_code.task_id + '") }}',
        yes_task=translate_csv.task_id,
        no_task=send_filtered_empty_export_email.task_id
    )

    create_export_data_collection = rail.CreateCollectionOperator(
        task_id='create_export_data_collection',
        source="{{ result('translate_csv') }}",
    )

    query_export_data = rail.QueryCollectionOperator(
        task_id='query_export_data',
        query='SELECT Entryid,Uniqueid,Employeeid,Date,Wbs,Vblen,Salesitem,Attendancetype,Hours,Comments from create_export_data_collection  ' +
        'ORDER BY cast(Hours as NUMERIC) ASC',
    )

    generate_xml_time_data = rail.RenderTemplateOperator(
        task_id='generate_xml_time_data',
        dataset='{{result(\'' + query_export_data.task_id + '\')}}',
        target='artifact',
        template_file='output_template.xml'
    )

    def file_upload_failed(context):
        subject = '{{ get_company_key() }} | Replicon time extract for FTP - SFTP upload failure - {{ data_interval_end }}'
        body = """<p>Hi Team,<br /> <br /> The C1 time date export has been completed at
{{data_interval_end}} however the file upload to sftp has failed with error.</p>
<ul>
<li>Dag Run: {{ run_id }}</li>
<li>File Name: {{result('""" + resolve_file_name.task_id + """')}}</li>
<li>SFTP Path: """ + config.sftp_upload_path + """</li>
<li>Error: """ + repr(context['exception']) + """</li>
</ul>
<p>Please find the attached file to be uploaded to sftp. Upload the file to the given sftp and debug the issue.<br /> <br /> Regards,<br /> Deltek Inc</p>
        """
        email = rail.EmailOperator(
            task_id='send_time_data_to_sftp_failure_email',
            to=config.exception_email,
            bcc=config.internal_email,
            subject=subject,
            html_content=body,
            files=[
                ("{{result('" + resolve_file_name.task_id + "')}}",
                 "{{result('" + generate_xml_time_data.task_id + "')}}")
            ]
        )
        email.render_template_fields(context)
        email.execute(context)

    send_time_data_to_sftp = rail.SFTPUploadFileOperator(
        task_id='send_time_data_to_sftp',
        remote_filepath=config.sftp_upload_path +
        '/{{result(\'' + resolve_file_name.task_id + '\')}}',
        content='{{result(\'' + generate_xml_time_data.task_id + '\')}}',
        on_failure_callback=file_upload_failed
    )

    send_success_email = rail.EmailOperator(
        task_id='send_success_email',
        to=config.tenant_email,
        bcc=config.internal_email,
        subject='{{ get_company_key() }} | Replicon time extract for FTP - Completed Successfully - {{ data_interval_end }}',
        html_content="""
        <p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon time extract for FTP is completed successfully at
Job created at {{data_interval_end}}. Please find the file details below:<br /><br />
File path: """ + config.sftp_upload_path + """ <br/>
File name: {{result('""" + resolve_file_name.task_id + """')}}<br /></p>
<p>
Regards,<br />
Deltek Inc.
</p>"""
    )

    upload_time_data = rail.HTTPUploadFileOperator(
        task_id='upload_time_data',
        content='{{result(\'' + generate_xml_time_data.task_id + '\')}}',
        retries=0,
        content_type="application/xml",
    )

    get_actual_end_time = rail.RenderTemplateOperator(
        task_id="get_actual_end_time",
        target="result",
        template="{{macros.datetime.now()}}"
    )

    log_to_sumo = rail.SendToSumoOperator(
        task_id='log_to_sumo',
        data={
            'exporttype': 'Employee',
            'downstreamapp': 'FTP',
            'twbrowcount': '{{result(\'' + filter_for_ftp_company_code.task_id + '\',key=\'length\')}}',
            'twbname': getExportName(),
            'exportrowcount': '{{result(\'' + translate_csv.task_id + '\',key=\'length\')}}',
            'exportfilepath': config.sftp_upload_path,
            'payloadthreshold': '{{\'Yes\' if result(\'' + translate_csv.task_id + '\',key=\'length\') > ' + str(config.row_threshold) + ' else  \'No\'  }}',
            'exportfilename': '{{result(\'' + resolve_file_name.task_id + '\')}}',
            'jobstarttime': '{{result(\'' + get_actual_start_time.task_id + '\')}}',
            'jobendtime': '{{result(\'' + get_actual_end_time.task_id + '\')}}',
            'identifier': 'DXC_Timexport_logger'
        },
        sumo_conn_id=config.sumo_conn_id

    )

    log_nodata_to_sumo = rail.SendToSumoOperator(
        task_id='log_nodata_to_sumo',
        data={
            'exporttype': 'Employee',
            'downstreamapp': 'FTP',
            'twbrowcount': '0',
            'twbname': getExportName()+'_Nodata',
            'exportrowcount': '0',
            'exportfilepath': 'null',
            'payloadthreshold': 'null',
            'exportfilename': 'null',
            'jobstarttime': '{{result(\'' + get_actual_start_time.task_id + '\')}}',
            'jobendtime': '{{result(\'' + get_actual_end_time.task_id + '\')}}',
            'identifier': 'DXC_Timexport_logger'
        },
        sumo_conn_id=config.sumo_conn_id
    )

    is_sunday_run >> rail.Label('No') >> start >> [get_ftp_divisions, employee_type_contractor,
                                                   employee_type_agency_contractor, time_export_download_script,
                                                   resolve_file_name, get_actual_start_time] >> log_filename_and_twbname >> export[0]
    export[1] >> export_has_data

    export_has_data >> rail.Label('Yes') >> send_result_to_downstream >> rail.Label(
        'Yes') >> filter_for_ftp_company_code
    export_has_data >> rail.Label('No') >> rename_export

    rename_export >> log_nodata_to_sumo >> send_raw_empty_export_email

    filter_for_ftp_company_code >> filtered_export_has_data

    filtered_export_has_data >> rail.Label(
        'Yes') >> translate_csv >> create_export_data_collection >> query_export_data >> generate_xml_time_data >>  \
        upload_time_data >> get_actual_end_time >> log_to_sumo >> send_success_email >> send_time_data_to_sftp

    filtered_export_has_data >> rail.Label(
        'No') >> send_filtered_empty_export_email
