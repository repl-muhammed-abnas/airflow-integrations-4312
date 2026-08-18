
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_earned_leave_export_child_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_earned_leave_export_child_ {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_report_details3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_report_details3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_report_details3 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details3',
            report_name='IN ES Earned Leave Balance Report'
        )

        generate_reports_batch_3 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_3',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data={
                "reportParameters": [{
                    "reportUri": "{{ result('get_report_details3').uri }}",
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                }],
            },
        )

        execute_generate_reports_batch_3 = rail.batch_execution(
            group_id='execute_execute_generate_reports_batch_3',
            creation_task_id='generate_reports_batch_3',
        )

        get_report_batch_results_6 = rail.RepliconServiceOperator(
            task_id='get_report_batch_results_6',
            endpoint="/services/ReportService1.svc/GetReportGenerationBatchResults",
            data={
                'reportGenerationBatchUri':  "{{ result('generate_reports_batch_3') }}"
            },
        )

        load_csv_create_list_from_csv_7 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_7",
            document="{{ result('get_report_batch_results_6').reportGenerationResults[0].payload }}",
        )

        create_collection_create_list_from_csv_7 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_7',
            source="{{ result('load_csv_create_list_from_csv_7') }}",
            name="terminatedusertimeoffbalance",
            columns={
                'User Name': 'username',
                'Login Name': 'loginname',
                'Employee ID': 'employeeid',
                'useruri': 'useruri',
                'Time Off Type': 'timeofftype',
                ',Time Off Type Description': 'timeofftypedescription',
                'Time Off Balance': 'balance',
                'User End Date': 'userenddate'
            }
        )

        if_create_list_from_csv_7_row_count_greater_than_0_8 = rail.IfOperator(
            task_id='if_create_list_from_csv_7_row_count_greater_than_0_8',
            test='''{{ result('create_collection_create_list_from_csv_7','length') > 0 }}''',
            yes_task="declare_list_logforpayroll_9",
            no_task="finish",
        )

        declare_list_logforpayroll_9 = rail.SetVariableOperator(
            task_id='declare_list_logforpayroll_9',
            append=False,
            name='Payrolllog',
            value=[]
        )

        insert_to_list_10 = rail.SetVariableOperator(
            task_id='insert_to_list_10',
            append=True,
            name='{{ result("declare_list_logforpayroll_9").name }}',
            value={
                "log": "{{ dag_run.conf.timenow }}- Process started",
                "Company Code": "{{ dag_run.conf.division }}"
            }
        )

        log_requiredfilename_11 = rail.PythonOperator(
            task_id='log_requiredfilename_11',
            python_callable=lambda: rail.render_template(
                "{{ dag_run.conf.filename}}")
        )

        query_list_terminated_user_balance_12 = rail.QueryCollectionOperator(
            task_id='query_list_terminated_user_balance_12',
            query="""SELECT * FROM  terminatedusertimeoffbalance""",
        )

        def get_export_value_for_item(item):
            if item:
                return item
            return null

        create_csv_lines_13 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_13',
            source="{{ result('query_list_terminated_user_balance_12') }}",
            header=null,
            delimiter='|',
            row=lambda item: {
                "column_0": "P2010",
                "column_1": get_export_value_for_item(item['employeeid']),
                "column_2": "IN",
                "column_3": null,
                "column_4": "INS",
                "column_5": "2010",
                "column_6": "9720",
                "column_7": datetime.utcnow().strftime("%Y%m%d"),
                "column_8": datetime.utcnow().strftime("%Y%m%d"),
                "column_9": null,
                "column_10": null,
                "column_11": null,
                "column_12": null,
                "column_13": "9720",
                "column_14": null,
                "column_15": null,
                "column_16": null,
                "column_17": null,
                "column_18": null,
                "column_19": item['balance'],
                "column_20": "010",
                "column_21": null,
                "column_22": null,
                "column_23": null,
                "column_24": null,
                "column_25": null,
                "column_26": null,
                "column_27": null,
                "column_28": null,
                "column_29": null,
                "column_30": null,
                "column_31": null,
                "column_32": null,
                "column_33": null,
                "column_34": null,
                "column_35": null,
                "column_36": null,
                "column_37": null,
                "column_38": null,
                "column_39": null,
                "column_40": null
            }.values(),
        )

        log_total_recordsincludingheaderandfooter_14 = rail.PythonOperator(
            task_id='log_total_recordsincludingheaderandfooter_14',
            python_callable=lambda: rail.result(
                'query_list_terminated_user_balance_12', 'length') + 2
        )

        log_formatted_data_15 = rail.PythonOperator(
            task_id='log_formatted_data_15',
            python_callable=lambda: rail.render_template("HEADR|G2DX|DXC|REPLICON|||{{result('log_requiredfilename_11')}}.SAP|{{dag_run.conf.rundateinYYYYMMDDformat}}|{{ dag_run.conf.runtimeinHHMMSSformat}}|P|03") + "\r\n" + rail.read_artifact(
                rail.result('create_csv_lines_13')) + rail.render_template("TRAIL|{{result('log_total_recordsincludingheaderandfooter_14')}}")
        )

        replace_format_data_16 = rail.PythonOperator(
            task_id='replace_format_data_16',
            python_callable=lambda:  rail.result(
                'log_formatted_data_15').replace('|', '|"')
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list_logforpayroll_9").name }}',
            value=lambda: {
                "log": (datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")) + " INFO admin No of records exported = " + str(rail.result('query_list_terminated_user_balance_12', 'length'))
            }
        )

        upload_file_uploadfiletos3_19 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletos3_19',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/{{ result('log_requiredfilename_11') }}.SAP",
            source="{{ result('replace_format_data_16') }}"
        )

        encrypt2_a_d_p_public_key_20 = rail.PGPEncryptionOperator(
            task_id='encrypt2_a_d_p_public_key_20',
            pgp_conn_id=config.pgp_conn_id,
            source="{{ result('replace_format_data_16') }}",
        )

        upload_uploadfiletosftp_21 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadfiletosftp_21',
            content='''{{ result('encrypt2_a_d_p_public_key_20') }}''',
            # append = false,
            remote_filepath=config.datafilepath + \
            "/{{ result('log_requiredfilename_11') }}.SAP.pgp"
        )

        insert_to_list_24 = rail.SetVariableOperator(
            task_id='insert_to_list_24',
            append=True,
            name='{{ result("declare_list_logforpayroll_9").name }}',
            value=lambda: {
                "log": (datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")) + " INFO admin Export File_" + rail.result('log_requiredfilename_11') + ".txt created"
            }
        )

        log_processended_25 = rail.PythonOperator(
            task_id='log_processended_25',
            python_callable=lambda:  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        insert_to_list_26 = rail.SetVariableOperator(
            task_id='insert_to_list_26',
            append=True,
            name='{{ result("declare_list_logforpayroll_9").name }}',
            value=lambda: {
                "log": (datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")) + " - Process ended"
            }
        )

        create_csv_lines_compsepayrolllog_27 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_compsepayrolllog_27',
            source="{{ dag_run_var(result('declare_list_logforpayroll_9').name) | to_json }}",
            header=['Log file'],
            row=lambda item: {
                "column_0": item['log']
            }.values(),
        )

        send_mail_sendemailafterpayrollexport_28 = rail.EmailOperator(
            task_id='send_mail_sendemailafterpayrollexport_28',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon monthy balance export for IN ES enabled users completed - {{ dag_run.conf.timenow }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon monthy balance export for IN ES enabled users is completed successfully on {{ dag_run.conf.timenow }}. Please find the export details for reference. </p>
            <ul>
            <li>Process started: {{ dag_run.conf.timenow }} </li>
            <li>Payroll extract file name: {{ result('log_requiredfilename_11') }} </li>
            <li>File path: {{ params.datafilepath }} </li>
            <li>Company Code: {{ dag_run.conf.division }} </li>
            <li>Number of records in payroll extract: {{ result('query_list_terminated_user_balance_12','length') }} </li>
            <li>Payroll log file name: Log_{{ result('log_requiredfilename_11') }} </li>
            <li>Log file path: {{params.logfilepath}} </li>
            <li>Process ended: {{ result('log_processended_25')}} </li>
            </ul>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'datafilepath': config.datafilepath,
                    'logfilepath': config.logfilepath},
        )

        upload_file_uploadfiletos3log_30 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletos3log_30',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/Log_{{ result('log_requiredfilename_11') }}.txt",
            source="{{ result('create_csv_lines_compsepayrolllog_27') }}"
        )

        upload_upload_payrolllogstosftp_31 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_payrolllogstosftp_31',
            content='''{{ result('create_csv_lines_compsepayrolllog_27') }}''',
            # append = false,
            remote_filepath=config.logfilepath + \
            "/Log_{{ result('log_requiredfilename_11') }}.txt",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> get_report_details3 >> generate_reports_batch_3
        get_report_details3 >> generate_reports_batch_3 >> execute_generate_reports_batch_3[0] >> execute_generate_reports_batch_3[
            1] >> get_report_batch_results_6 >> load_csv_create_list_from_csv_7 >> create_collection_create_list_from_csv_7 >> if_create_list_from_csv_7_row_count_greater_than_0_8
        if_create_list_from_csv_7_row_count_greater_than_0_8 >> rail.Label(
            'Yes') >> declare_list_logforpayroll_9 >> insert_to_list_10 >> log_requiredfilename_11 >> query_list_terminated_user_balance_12 >> create_csv_lines_13 >> log_total_recordsincludingheaderandfooter_14 >> log_formatted_data_15 >> replace_format_data_16 >> insert_to_list_17 >> upload_file_uploadfiletos3_19 >> encrypt2_a_d_p_public_key_20 >> upload_uploadfiletosftp_21 >> insert_to_list_24 >> log_processended_25 >> insert_to_list_26 >> create_csv_lines_compsepayrolllog_27 >> send_mail_sendemailafterpayrollexport_28 >> upload_file_uploadfiletos3log_30 >> upload_upload_payrolllogstosftp_31 >> finish
        if_create_list_from_csv_7_row_count_greater_than_0_8 >> rail.Label('Yes') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
