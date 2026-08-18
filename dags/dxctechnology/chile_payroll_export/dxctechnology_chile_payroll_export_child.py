from datetime import datetime
import rail
from dxctechnology.chile_payroll_export import request_payload
from dxctechnology.chile_payroll_export import email_contents
from dxctechnology.chile_payroll_export import debug_test_data


def findItemByDisplayText(response, name):
    for item in response.json()['d']:
        if item['displayText'] == name:
            return item['uri']
    raise Exception('Unable to locate item {name}')

# pylint: disable=too-many-arguments too-many-statements


def get_task_group(startDate, endDate,
                   divisionUris, division_name, fileFormatScriptUri,config):
    with rail.TaskGroup(group_id='DXC_CHILE_PayrollData_Export_Child', prefix_group_id=False) as group:
        payload = request_payload.get_create_payroll_download_batch_payload(
            startDate, endDate, divisionUris, fileFormatScriptUri)

        def get_filenames():
            cur_date = datetime.now()
            file_name_format = f'{cur_date.year}{cur_date.month}{cur_date.day}_{cur_date.hour}{cur_date.minute}{cur_date.second}'
            return {
                'date_stamp': file_name_format,
                'itemfilename': "ITEMS_" + file_name_format + ".csv",
                'absesnsefilename': "LICENCIAS_" + file_name_format + ".csv",
                'vacationfilename': "VACACIONES_" + file_name_format + ".csv",
            }

        file_names = get_filenames()
        payrun_datestamp = datetime.now().strftime("%m%d%YT%H%M%S")
        payrun_name = f"{payrun_datestamp}.{division_name}"

        create_payroll_download_batch = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=payload
        )

        batchuri = "{{ result('create_payroll_download_batch') }}"
        execute_payroll_download_batch, wait_for_payroll_download_batch = rail.batch_execution(
            'execute_payroll_download_batch', create_payroll_download_batch.task_id)

        payload = {"payrollDownloadBatchUri": batchuri}
        get_payroll_run_batch_result = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data=payload
        )

        download_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_payload_file_from_url",
            url="{{ result('get_payroll_run_batch_result').downloadUrl }}"
        )

        load_payload_file = rail.LoadCSVFileOperator(
            task_id="load_payload_file",
            document=debug_test_data.create_final_payroll_data_collection if config.can_debug_test_data else "{{ result('download_payload_file_from_url') }}"
        )

        create_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_payroll_data_collection',
            name='payroll_data',
            source="{{ result('load_payload_file') }}"
        )

        has_payroll_data = rail.IfOperator(
            task_id='has_payroll_data',
            test="{{ result('create_payroll_data_collection','length') > 0 }}",
            yes_task='create_payroll_download_batch_prev_month',
            no_task='send_no_data_email'
        )

        payload = request_payload.get_create_payroll_download_batch_prev_month_payload(
            startDate, endDate, divisionUris, fileFormatScriptUri)
        create_payroll_download_batch_prev_month = rail.RepliconServiceOperator(
            task_id="create_payroll_download_batch_prev_month",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=payload
        )

        batchuri = "{{ result('create_payroll_download_batch_prev_month') }}"
        execute_month_download_batch, wait_for_month_download_batch = rail.batch_execution(
            'execute_month_download_batch', create_payroll_download_batch_prev_month.task_id)

        payload = {"payrollDownloadBatchUri": batchuri}
        get_payroll_run_batch_result_month = rail.RepliconServiceOperator(
            task_id="get_payroll_run_batch_result_month",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data=payload
        )

        download_month_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_month_payload_file_from_url",
            url="{{ result('get_payroll_run_batch_result_month').downloadUrl }}"
        )

        load_month_payload_file = rail.LoadCSVFileOperator(
            task_id="load_month_payload_file",
            document="{{ result('download_month_payload_file_from_url') }}"
        )

        create_month_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_month_payroll_data_collection',
            source="{{ result('load_month_payload_file') }}"
        )

        payload = request_payload.get_create_approved_payrun_batch_payload(
            startDate, endDate, divisionUris)

        create_approved_payrun_batch = rail.RepliconServiceOperator(
            task_id="create_approved_payrun_batch",
            endpoint="/services/PayRunService1.svc/CreatePayRunBatch",
            data=payload
        )

        batchuri = "{{ result('create_approved_payrun_batch') }}"
        execute_approved_payrun_batch, wait_for_approved_payrun_batch = rail.batch_execution(
            'execute_approve_payrun_batch', create_approved_payrun_batch.task_id)

        payload = {"payRunBatchUri": batchuri}
        get_approved_payrun_batch_result = rail.RepliconServiceOperator(
            task_id="get_approved_payrun_batch_result",
            endpoint="/services/PayRunService1.svc/GetCreatePayRunBatchResults",
            data=payload
        )

        payload = {
            "target": {
                "uri": "{{ result('get_approved_payrun_batch_result').payRunUri }}",
            },
            "name": payrun_name
        }
        update_payrun_name = rail.RepliconServiceOperator(
            task_id="update_payrun_name",
            endpoint="/services/PayRunService1.svc/UpdatePayRunName",
            data=payload
        )

        payRunUri = "{{ result('get_approved_payrun_batch_result').payRunUri }}"
        payload = request_payload.get_create_payrun_download_batch_payload(
            fileFormatScriptUri, payRunUri)

        create_payrun_download_batch = rail.RepliconServiceOperator(
            task_id="create_payrun_download_batch",
            endpoint="/services/PayRunService1.svc/CreatePayrollDownloadBatch",
            data=payload
        )

        execute_payrun_download_batch, wait_for_payrun_download_batch = rail.batch_execution(
            'execute_payrun_download_batch', create_payrun_download_batch.task_id)

        batchuri = "{{ result('create_payrun_download_batch') }}"
        payload = {"payrollDownloadBatchUri": batchuri}
        get_payrun_download_batch_result = rail.RepliconServiceOperator(
            task_id="get_payrun_download_batch_result",
            endpoint="/services/PayRunService1.svc/GetPayrollDownloadBatchResults",
            data=payload
        )

        payload = {
            "target": {
                "uri": "{{ result('get_approved_payrun_batch_result').payRunUri }}"
            }
        }
        mark_payrun_as_complete = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_complete",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsComplete",
            data=payload
        )

        download_final_payload_file_from_url = rail.HTTPDownloadFileOperator(
            task_id="download_final_payload_file_from_url",
            url="{{ result('get_payrun_download_batch_result').downloadUrl }}"
        )

        load_final_payload_file = rail.LoadCSVFileOperator(
            task_id="load_final_payload_file",
            document=debug_test_data.create_final_payroll_data_collection if config.can_debug_test_data else
            "{{ result('download_final_payload_file_from_url') }}"
        )

        create_final_payroll_data_collection = rail.CreateCollectionOperator(
            task_id='create_final_payroll_data_collection',
            name='finalpayrolldata',
            source="{{ result('load_final_payload_file') }}"
        )

        query_final_payroll_data_without_empid = rail.QueryCollectionOperator(
            task_id='query_final_payroll_data_without_empid',
            query='SELECT * From finalpayrolldata WHERE Codigo IS NULL OR Codigo="" '
        )

        has_empty_empid_data = rail.IfOperator(
            task_id='has_empty_empid_data',
            test="{{ result('query_final_payroll_data_without_empid','length') > 0 }}",
            yes_task='mark_payrun_as_draft',
            no_task='query_list_in_final_payroll_collection'
        )

        mark_payrun_as_draft = rail.RepliconServiceOperator(
            task_id="mark_payrun_as_draft",
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data=payload
        )

        cancel_payrun = rail.RepliconServiceOperator(
            task_id="cancel_payrun",
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data=payload
        )

        raise_error = rail.PythonOperator(
            task_id="raise_error",
            python_callable=lambda: (_ for _ in ()).throw(Exception(
                'RUT is not present for some users. Users available to validate in payrun'))
        )

        query_list_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_list_in_final_payroll_collection',
            name='final_payroll_item_data',
            query='''SELECT * From finalpayrolldata WHERE
                        Paycode_name !="Time Off" AND
                        Paycode_name !="[Chile] Vacation" AND
                        Paycode_name !="[Chile] examenes medicos" AND
                        Paycode_name !="[Chile] Matrimonio" AND
                        Paycode_name !="[Chile] defunción Conyuje e hijos" AND
                        Paycode_name !="[Chile] defunción Padres" AND
                        Paycode_name !="[Chile] defunción Hermanos o abuelos" AND
                        Paycode_name !="[Chile] Cambio de casa" AND
                        Paycode_name !="[Chile] Nacimiento" AND
                        Paycode_name !="[Chile] cumpleaños" AND
                        Paycode_name !="[Chile] Medical Leave" AND
                        Paycode_name !="Regular Time (Chile)"'''
        )

        has_item_data = rail.IfOperator(
            task_id='has_item_data',
            test="{{ result('query_list_in_final_payroll_collection','length') > 0 }}",
            yes_task='compose_item_payroll_csv_file',
            no_task='send_no_item_data_email',
        )

        email_content, email_subject = email_contents.get_send_no_item_data_email(
            division_name, config)

        send_no_item_data_email = rail.EmailOperator(
            task_id='send_no_item_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        email_content, email_subject = email_contents.get_send_no_data_email(
            division_name, config)

        send_no_data_email = rail.EmailOperator(
            task_id='send_no_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        def row_map_payroll_item(item):
            current_month = datetime(**endDate).replace(day=1)
            mapper = config.paycode_mapper
            # entrydate: 2021-11-10,
            item['Codigo'] = item['Codigo'] if len(
                item['Actual_Employee_ID']) == 0 else item['Actual_Employee_ID']
            entrydate = datetime.strptime(item['Entry_Date'], '%d/%m/%Y')
            if entrydate < current_month:
                paycode_map = next(
                    filter(lambda x: x['paycode'] == item['cohade'], mapper), None)
                if paycode_map is not None:
                    item['cohade'] = paycode_map['negative'] if float(
                        item['monto']) < 0 else paycode_map['positive']

            return [
                item['Codigo'],
                item['cohade'],
                item['nro'],
                item['periodo'],
                float(item['monto']),
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                "",
                ""
            ]

        compose_item_payroll_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_item_payroll_csv_file',
            row=row_map_payroll_item,
            # pylint: disable=line-too-long
            header=["Codigo", "cohade", "nro", "periodo", "monto", "cencos", "sperimp", "cuotot",
                    "obs", "codpres", "coform", "fecha_ini", "fecha_fin", "propor", "moti_mod", "simes"],
            source="{{ result('query_list_in_final_payroll_collection') }}"
        )

        pgp_encyrpt_item_file = rail.PGPEncryptionOperator(
            task_id="pgp_encyrpt_item_file",
            source="{{ result('compose_item_payroll_csv_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_payroll_item_file_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_payroll_item_file_sftp",
            content="{{ result('pgp_encyrpt_item_file') }}",
            remote_filepath=config.output_filepath +
            f'{file_names["itemfilename"]}.txt'
        )

        records = "{{ result('query_list_in_final_payroll_collection','length') }}"
        email_content, email_subject = email_contents.get_send_payroll_email(
            division_name, config, file_names, payrun_datestamp, records)

        send_payroll_email = rail.EmailOperator(
            task_id='send_payroll_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        query_timeoff_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_timeoff_in_final_payroll_collection',
            name='final_payroll_timeoff_data',
            query='''SELECT * From finalpayrolldata WHERE
                        Paycode_name ="[Chile] Vacation" OR
                        Paycode_name ="[Chile] examenes medicos" OR
                        Paycode_name ="[Chile] Matrimonio" OR
                        Paycode_name ="[Chile] defunción Conyuje e hijos" OR
                        Paycode_name ="[Chile] defunción Padres" OR
                        Paycode_name ="[Chile] defunción Hermanos o abuelos" OR
                        Paycode_name ="[Chile] Cambio de casa" OR
                        Paycode_name ="[Chile] Nacimiento" OR
                        Paycode_name ="[Chile] cumpleaños" '''
        )

        has_timeoff_data = rail.IfOperator(
            task_id='has_timeoff_data',
            test="{{ result('query_timeoff_in_final_payroll_collection','length') > 0 }}",
            yes_task='query_min_max_in_final_payroll_collection',
            no_task='send_no_timeoff_data_email'
        )

        email_content, email_subject = email_contents.get_send_no_timeoff_data_email(
            division_name, config)

        send_no_timeoff_data_email = rail.EmailOperator(
            task_id='send_no_timeoff_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        query_min_max_in_final_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_min_max_in_final_payroll_collection',
            query='''SELECT * From final_payroll_timeoff_data WHERE
                        Paycode_name ="[Chile] Vacation" OR
                        Paycode_name ="[Chile] examenes medicos" OR
                        Paycode_name ="[Chile] Matrimonio" OR
                        Paycode_name ="[Chile] defunción Conyuje e hijos" OR
                        Paycode_name ="[Chile] defunción Padres" OR
                        Paycode_name ="[Chile] defunción Hermanos o abuelos" OR
                        Paycode_name ="[Chile] Cambio de casa" OR
                        Paycode_name ="[Chile] Nacimiento" OR
                        Paycode_name ="[Chile] cumpleaños" '''
        )

        get_time_off_report_details = rail.RepliconReportDetailsOperator(
            task_id="get_time_off_report_details",
            report_name=config.time_off_report_name
        )

        def get_date_range_report_filter(**_):
            report_details = rail.result('get_time_off_report_details')
            enabled_filters = report_details['filterConfiguration']['enabledFilters']
            date_range_filter = next(
                filter(lambda x: x['displayText'] == 'DateRangeFilter', enabled_filters), None)
            if date_range_filter is None:
                raise Exception('Report filter not found.')

            return date_range_filter['uri']

        get_date_range_report_filter_uri = rail.PythonOperator(
            task_id="get_date_range_report_filter_uri",
            python_callable=get_date_range_report_filter
        )

        query_min_max_entry_date_from_final_payroll = rail.QueryCollectionOperator(
            task_id='query_min_max_entry_date_from_final_payroll',
            query='''SELECT MIN(Entry_Date),MAX(Entry_Date) FROM finalpayrolldata'''
        )

        load_min_max_date = rail.PythonOperator(
            task_id="load_min_max_date",
            python_callable=lambda: rail.load_all_records(
                rail.result('query_min_max_entry_date_from_final_payroll'))
        )

        def report_payload():
            date_range_filter_uri = rail.result(
                'get_date_range_report_filter_uri')
            return {
                "reportParameters": [
                   {
                       "reportUri": rail.result('get_time_off_report_details')['uri'],
                       "filterValues": [
                           {
                               "reportFilterUri": date_range_filter_uri,
                               "value": None
                           },
                           {
                               "reportFilterUri": date_range_filter_uri,
                               "value": datetime.strptime(rail.result('load_min_max_date')[0]['MIN_Entry_Date_'], "%d/%m/%Y").strftime('%m/%d/%Y')
                           },
                           {
                               "reportFilterUri": date_range_filter_uri,
                               "value": datetime.strptime(rail.result('load_min_max_date')[0]['MAX_Entry_Date_'], "%d/%m/%Y").strftime('%m/%d/%Y')
                           }
                       ],
                       "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                   }
                ]
            }

        create_report_generation_batch = rail.RepliconServiceOperator(
            task_id="create_report_generation_batch",
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=report_payload
        )

        batchuri = "{{ result('create_report_generation_batch') }}"

        execute_report_batch, wait_for_report_batch = rail.batch_execution(
            'execute_report_generation_batch', create_report_generation_batch.task_id)

        payload = {
            "reportGenerationBatchUri": batchuri
        }

        get_report_batch_result = rail.RepliconServiceOperator(
            task_id="get_report_batch_result",
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data=payload,
            response_filter=lambda x: debug_test_data.report_batch_result if config.can_debug_test_data else x.json()[
                'd']
        )

        has_valid_report_data = rail.IfOperator(
            task_id='has_valid_report_data',
            test="{{ result('get_report_batch_result').reportGenerationResults[0].payload  != 'No Data' }}",
            yes_task='load_timeoff_csv_file',
            no_task='send_no_timeoff_data_email',
        )

        load_timeoff_csv_file = rail.LoadCSVFileOperator(
            task_id="load_timeoff_csv_file",
            document="{{ result('get_report_batch_result').reportGenerationResults[0].payload }}"
        )

        create_timeoff_data_list_collection = rail.CreateCollectionOperator(
            task_id='create_timeoff_data_list_collection',
            name='timeoffdata',
            source="{{ result('load_timeoff_csv_file') }}"
        )

        query_chile_vacation_from_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_chile_vacation_from_payroll_collection',
            query='SELECT * FROM finalpayrolldata WHERE Paycode_name = "[Chile] Vacation"'
        )

        has_vacation_data = rail.IfOperator(
            task_id='has_vacation_data',
            test="{{ result('query_chile_vacation_from_payroll_collection', 'length') > 0 }}",
            yes_task='query_chile_vacation_timeoff_data',
            no_task='send_no_vacation_data_email'
        )

        query_chile_vacation_timeoff_data = rail.QueryCollectionOperator(
            task_id='query_chile_vacation_timeoff_data',
            name='chile_vacation',
            query='''SELECT * FROM
                            timeoffdata
                            WHERE EXISTS (SELECT Codigo
                                            FROM
                                                finalpayrolldata
                                            WHERE
                                                finalpayrolldata.Codigo=timeoffdata.Employee_ID
                                                AND timeoffdata.Time_Off_Type="[Chile] Vacation"
                                            )
                 '''
        )

        has_chile_vacation_timeoff_data = rail.IfOperator(
            task_id='has_chile_vacation_data',
            test="{{ result('query_chile_vacation_timeoff_data', 'length') > 0 }}",
            yes_task='compose_final_vacation_data_csv_file',
            no_task='query_absence_data_from_payroll_collection'
        )

        def row_map_vacation_data(item):
            item['end_time'] = '1' if str(item['end_time']).strip(
            ) == 'AM' else '2' if str(item['end_time']).strip() == 'PM' else '0'
            item['Booking_Start_Date_Time'] = datetime.strptime(
                (item['Booking_Start_Date_Time'].split('-')[0]).strip(), '%d %B %Y').strftime("%d/%m/%Y")
            item['Booking_End_Date_Time'] = datetime.strptime(
                (item['Booking_End_Date_Time'].split('-')[0]).strip(), '%d %B %Y').strftime("%d/%m/%Y")
            item['Employee_ID'] = item['Employee_ID'] if len(
                item['Actual_Employee_ID']) == 0 else item['Actual_Employee_ID']
            return [
                item['Employee_ID'],
                item['Booking_Start_Date_Time'],
                item['Time_Off_Days'],
                item['Booking_End_Date_Time'],
                "V",
                item['end_time'],
                ""
            ]

        compose_final_vacation_data_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_final_vacation_data_csv_file',
            header=["Codigo", "Feini", "Dias",
                    "FeFin", "Status", "AmPM", "Pertom"],
            source="{{ result('query_chile_vacation_timeoff_data') }}",
            row=row_map_vacation_data
        )

        encrypt_vacation_data = rail.PGPEncryptionOperator(
            task_id="encrypt_vacation_data",
            source="{{ result('compose_final_vacation_data_csv_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        upload_vacation_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="upload_vacation_data_to_sftp",
            content="{{ result('encrypt_vacation_data') }}",
            remote_filepath=config.output_filepath +
            f'{file_names["vacationfilename"]}.pgp'
        )

        records = "{{ result('query_chile_vacation_timeoff_data','length') }}"
        email_content, email_subject = email_contents.get_send_vacation_data_email(
            division_name, config, file_names, payrun_datestamp, records)

        send_vacation_data_email = rail.EmailOperator(
            task_id='send_vacation_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        email_content, email_subject = email_contents.get_send_no_vacation_data_email(
            division_name, config)

        send_no_vacation_data_email = rail.EmailOperator(
            task_id='send_no_vacation_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        query_absence_data_from_payroll_collection = rail.QueryCollectionOperator(
            task_id='query_absence_data_from_payroll_collection',
            query='''SELECT * FROM
                        finalpayrolldata
                    WHERE
                        Paycode_name="[Chile] examenes medicos" OR
                        Paycode_name="[Chile] Matrimonio" OR
                        Paycode_name="[Chile] defunción Conyuje e hijos" OR
                        Paycode_name="[Chile] defunción Padres" OR
                        Paycode_name="[Chile] defunción Hermanos o abuelos" OR
                        Paycode_name="[Chile] Cambio de casa" OR
                        Paycode_name="[Chile] Nacimiento" OR
                        Paycode_name="[Chile] cumpleaños"
                 '''
        )

        has_absence_data = rail.IfOperator(
            task_id='has_absence_data',
            test="{{ result('query_absence_data_from_payroll_collection', 'length') > 0 }}",
            yes_task='query_all_absense_data',
            no_task='send_no_absence_data_email'
        )

        query_all_absense_data = rail.QueryCollectionOperator(
            task_id='query_all_absense_data',
            name='absense_data',
            query='''SELECT * FROM
                        timeoffdata
                    WHERE EXISTS (SELECT  Codigo
                                    FROM
                                        finalpayrolldata
                                    WHERE
                                        finalpayrolldata.Codigo  = timeoffdata.Employee_ID
                                        AND timeoffdata.Time_Off_Type = "[Chile] examenes medicos" OR
                                        timeoffdata.Time_Off_Type = "[Chile] Matrimonio" OR
                                        timeoffdata.Time_Off_Type = "[Chile] defunción Conyuje e hijos" OR
                                        timeoffdata.Time_Off_Type = "[Chile] defunción Padres" OR
                                        timeoffdata.Time_Off_Type = "[Chile] defunción Hermanos o abuelos" OR
                                        timeoffdata.Time_Off_Type = "[Chile] Cambio de casa" OR
                                        timeoffdata.Time_Off_Type = "[Chile] Nacimiento" OR
                                        timeoffdata.Time_Off_Type = "[Chile] cumpleaños"
                                ) ;

            '''
        )

        has_all_absence_data = rail.IfOperator(
            task_id='has_all_absence_data',
            test="{{ result('query_all_absense_data', 'length') > 0 }}",
            yes_task='compose_absense_csv_file',
            no_task='send_no_absence_data_email'
        )

        def row_map_absense_data(item):
            item['Employee_ID'] = item['Employee_ID'] if len(
                item['Actual_Employee_ID']) == 0 else item['Actual_Employee_ID']
            tipo = '2' if item['Time_Off_Type'] == '[Chile] Nacimiento' else '5'
            timeofftype = item['Time_Off_Type']
            motivo = '5' if timeofftype == '[Chile] Nacimiento' else \
                '12' if timeofftype == '[Chile] examenes medicos' else \
                '6' if timeofftype == '[Chile] Matrimonio' else \
                '8' if timeofftype == '[Chile] defunción Conyuje e hijos' else \
                '7' if timeofftype == '[Chile] defunción Padres' else \
                '9' if timeofftype == '[Chile] defunción Hermanos o abuelos' else \
                '10' if timeofftype == '[Chile] Cambio de casa' else \
                '11' if timeofftype == '[Chile] cumpleaños' else 1
            bookingstartdate = datetime.strptime(
                (item['Booking_Start_Date_Time'].split('-')[0]).strip(), '%d %B %Y').strftime("%d/%m/%Y")
            bookingenddate = datetime.strptime(
                (item['Booking_End_Date_Time'].split('-')[0]).strip(), '%d %B %Y').strftime("%d/%m/%Y")
            return [
                item['Employee_ID'],
                bookingstartdate,
                item['Time_Off_Days'],
                bookingenddate,
                tipo,
                motivo,
                "N",
                "",
                "",
                "",
                "",
                ""
            ]

        compose_absense_csv_file = rail.WriteCSVFileOperator(
            task_id='compose_absense_csv_file',
            header=["Codigo", "Fecha_real", "Dias", "Fecha_ini", "Tipo", "Motivo",
                    "Rebsal", "Escon", "fecha_ico", "Medios", "Dulic", "Detalle"],
            row=row_map_absense_data,
            source="{{ result('query_all_absense_data') }}"
        )

        encrypt_absense_data_csv = rail.PGPEncryptionOperator(
            task_id="encrypt_absense_data_csv",
            source="{{ result('compose_absense_csv_file') }}",
            pgp_conn_id=config.pgp_conn_id
        )

        export_absense_data_to_sftp = rail.SFTPUploadFileOperator(
            task_id="export_absense_data_to_sftp",
            content="{{ result('encrypt_absense_data_csv') }}",
            remote_filepath=config.output_filepath +
            f'{file_names["absesnsefilename"]}.pgp'
        )

        records = "{{ result('query_all_absense_data', 'length') }}"
        email_content, email_subject = email_contents.get_send_absense_data_email(
            division_name, config, file_names, payrun_datestamp, records)

        send_absense_data_email = rail.EmailOperator(
            task_id='send_absense_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )

        email_content, email_subject = email_contents.get_send_no_absense_data_email(
            division_name, config)

        send_no_absence_data_email = rail.EmailOperator(
            task_id='send_no_absence_data_email',
            to=config.tenant_email,
            bcc=config.internal_email,
            subject=email_subject,
            html_content=email_content
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                "company_code": division_name,
                "pay_run_name": payrun_name,
                "no_of_records": "{{ result('create_payroll_data_collection', 'length') }}"
            }
        )
        # pylint: disable=line-too-long
        create_payroll_download_batch >> execute_payroll_download_batch >> wait_for_payroll_download_batch >> get_payroll_run_batch_result >> \
            download_payload_file_from_url >> load_payload_file >> create_payroll_data_collection >> has_payroll_data

        has_payroll_data >> rail.Label(
            'Yes') >> create_payroll_download_batch_prev_month
        has_payroll_data >> rail.Label(
            'No') >> send_no_data_email >> log_to_sumo

        create_payroll_download_batch_prev_month >> execute_month_download_batch >> wait_for_month_download_batch >> \
            get_payroll_run_batch_result_month >> download_month_payload_file_from_url >> \
            load_month_payload_file >> create_month_payroll_data_collection >> create_approved_payrun_batch >> \
            execute_approved_payrun_batch >> wait_for_approved_payrun_batch >> \
            get_approved_payrun_batch_result >> update_payrun_name >> create_payrun_download_batch >> execute_payrun_download_batch >> \
            wait_for_payrun_download_batch >> get_payrun_download_batch_result >> mark_payrun_as_complete >> download_final_payload_file_from_url >> \
            load_final_payload_file >> create_final_payroll_data_collection >> query_final_payroll_data_without_empid >> has_empty_empid_data

        has_empty_empid_data >> rail.Label(
            'Yes') >> mark_payrun_as_draft >> cancel_payrun >> raise_error >> log_to_sumo
        has_empty_empid_data >> rail.Label(
            'No') >> query_list_in_final_payroll_collection >> has_item_data

        has_item_data >> rail.Label('Yes') >> compose_item_payroll_csv_file
        has_item_data >> rail.Label(
            'No') >> send_no_item_data_email >> query_timeoff_in_final_payroll_collection

        compose_item_payroll_csv_file >> pgp_encyrpt_item_file >> upload_payroll_item_file_sftp >> \
            send_payroll_email >> query_timeoff_in_final_payroll_collection

        query_timeoff_in_final_payroll_collection >> has_timeoff_data

        has_timeoff_data >> rail.Label(
            'Yes') >> query_min_max_in_final_payroll_collection
        has_timeoff_data >> rail.Label('No') >> send_no_timeoff_data_email

        query_min_max_in_final_payroll_collection >> \
            get_time_off_report_details >> get_date_range_report_filter_uri >> query_min_max_entry_date_from_final_payroll >> load_min_max_date >> create_report_generation_batch >> execute_report_batch >> \
            wait_for_report_batch >> get_report_batch_result >> has_valid_report_data

        has_valid_report_data >> rail.Label(
            'Yes') >> load_timeoff_csv_file >> create_timeoff_data_list_collection >> query_chile_vacation_from_payroll_collection
        has_valid_report_data >> rail.Label(
            'No') >> send_no_timeoff_data_email >> log_to_sumo

        query_chile_vacation_from_payroll_collection >> has_vacation_data

        has_vacation_data >> rail.Label(
            'Yes') >> query_chile_vacation_timeoff_data
        has_vacation_data >> rail.Label(
            'No') >> send_no_vacation_data_email >> query_absence_data_from_payroll_collection

        query_chile_vacation_timeoff_data >> has_chile_vacation_timeoff_data

        has_chile_vacation_timeoff_data >> rail.Label('Yes') >> compose_final_vacation_data_csv_file >> \
            encrypt_vacation_data >> upload_vacation_data_to_sftp >> send_vacation_data_email >> query_absence_data_from_payroll_collection
        has_chile_vacation_timeoff_data >> rail.Label(
            'No') >> query_absence_data_from_payroll_collection

        query_absence_data_from_payroll_collection >> has_absence_data

        has_absence_data >> rail.Label('Yes') >> query_all_absense_data
        has_absence_data >> rail.Label(
            'No') >> send_no_absence_data_email >> log_to_sumo

        query_all_absense_data >> has_all_absence_data
        has_all_absence_data >> rail.Label(
            'Yes') >> compose_absense_csv_file >> encrypt_absense_data_csv >> export_absense_data_to_sftp >> send_absense_data_email >> log_to_sumo
        has_all_absence_data >> rail.Label(
            'No') >> send_no_absence_data_email >> log_to_sumo

    return group
