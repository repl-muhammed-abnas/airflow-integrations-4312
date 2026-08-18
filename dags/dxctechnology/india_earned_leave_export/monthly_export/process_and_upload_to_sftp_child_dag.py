
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_process_and_upload_to_sftp_child_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_process_and_upload_to_sftp_child_ {config.instance}',
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
            no_task='get_file_uploadfiletosftp_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_file_uploadfiletosftp_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_file_uploadfiletosftp_3 = rail.S3DownloadFileOperator(
            task_id='get_file_uploadfiletosftp_3',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="{{ dag_run.conf.s3path }}"
        )

        load_csv_create_list_from_csv_4 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_4",
            document="{{result('get_file_uploadfiletosftp_3')}}",
        )

        create_collection_create_list_from_csv_4 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_4',
            source="{{result('load_csv_create_list_from_csv_4') }}",
            name="finalpayrolldata",
            columns={
                'RECTY': 'RECTY',
                'CLID': 'CLID',
                'INTCA': 'INTCA',
                'ORDNO': 'ORDNO',
                'IOPER': 'IOPER',
                'INFTY': 'INFTY',
                'BEGDA': 'BEGDA',
                'ENDDA': 'ENDDA',
                'OBJPS': 'OBJPS',
                'SPRPS': 'SPRPS',
                'SEQNR': 'SEQNR',
                'EXTRA': 'EXTRA',
                'Pay Code Code': 'paycodecode',
                'STDAZ': 'STDAZ',
                'BEGUZ': 'BEGUZ',
                'ENDUZ': 'ENDUZ',
                'BETRG': 'BETRG',
                'WAERS': 'WAERS',
                'Pay Code Hours': 'PayCodeHours',
                'ZEINH': 'ZEINH',
                'VTKEN': 'VTKEN',
                'BWGRL': 'BWGRL',
                'AUFKZ': 'AUFKZ',
                'ENDOF': 'ENDOF',
                'UFLD1': 'UFLD1',
                'UFLD2': 'UFLD2',
                'UFLD3': 'UFLD3',
                'KEYPR': 'KEYPR',
                'TRFGR': 'TRFGR',
                'TRFST': 'TRFST',
                'PRAKN': 'PRAKN',
                'PRAKZ': 'PRAKZ',
                'OTYPE': 'OTYPE',
                'PLANS': 'PLANS',
                'VERSL': 'VERSL',
                'EXBEL': 'EXBEL',
                'WTART': 'WTART',
                'TDLANGU': 'TDLANGU',
                'TDSUBLA': 'TDSUBLA',
                'TDTYPE': 'TDTYPE'
            }
        )

        if_create_list_from_csv_4_row_count_less_than_1_5 = rail.IfOperator(
            task_id='if_create_list_from_csv_4_row_count_less_than_1_5',
            test='''{{ result('create_collection_create_list_from_csv_4','length') < 1 }}''',
            yes_task="stop_6",
            no_task="query_list_final_datawithout_employee_i_d_7",
        )

        stop_6 = rail.EmptyOperator(
            task_id='stop_6',

        )

        query_list_final_datawithout_employee_i_d_7 = rail.QueryCollectionOperator(
            task_id='query_list_final_datawithout_employee_i_d_7',
            query="""SELECT * FROM  finalpayrolldata WHERE  NULLIF(finalpayrolldata.CLID,'') IS NULL""",
        )

        if_query_list_final_datawithout_employee_i_d_7_rows_greater_than_0_8 = rail.IfOperator(
            task_id='if_query_list_final_datawithout_employee_i_d_7_rows_greater_than_0_8',
            test='''{{ result('query_list_final_datawithout_employee_i_d_7','length') > 0 }}''',
            yes_task="mark_pay_run_as_draft_9",
            no_task="declare_list_12",
        )

        mark_pay_run_as_draft_9 = rail.RepliconServiceOperator(
            task_id='mark_pay_run_as_draft_9',
            endpoint="/services/PayRunService1.svc/MarkPayRunAsDraft",
            data={
                "target": {
                    "uri": "{{dag_run.conf.twburi }}",
                    "name": null
                }
            }
        )

        cancel_pay_run_10 = rail.RepliconServiceOperator(
            task_id='cancel_pay_run_10',
            endpoint="/services/PayRunService1.svc/CancelPayRun",
            data={
                "target": {
                    "uri": "{{dag_run.conf.twburi }}",
                    "name": null
                }
            }
        )

        stop_11 = rail.FailOperator(
            task_id='stop_11',
            message='''Employee ID not present for some users. Users available to validate in payrun "{{ dag_run.conf.filename }}" '''
        )

        declare_list_12 = rail.SetVariableOperator(
            task_id='declare_list_12',
            append=False,
            name='log',
            value=[]
        )

        query_list_final_data_except_paycode2301_13 = rail.QueryCollectionOperator(
            task_id='query_list_final_data_except_paycode2301_13',
            query="""SELECT * FROM  finalpayrolldata WHERE  finalpayrolldata.paycodecode='2504' OR  finalpayrolldata.paycodecode='2511' OR  finalpayrolldata.paycodecode='2512' OR  finalpayrolldata.paycodecode='2513' OR  finalpayrolldata.paycodecode='2514' OR  finalpayrolldata.paycodecode='2515' OR  finalpayrolldata.paycodecode='9720'""",
        )

        query_list_final_datafor_paycode2301_15 = rail.QueryCollectionOperator(
            task_id='query_list_final_datafor_paycode2301_15',
            query="""SELECT * FROM  finalpayrolldata WHERE  finalpayrolldata.paycodecode='2301'""",
        )

        query_list_final_data_for_paycode_combined = rail.QueryCollectionOperator(
            task_id='query_list_final_data_for_paycode_combined',
            query="""SELECT * FROM  finalpayrolldata WHERE  finalpayrolldata.paycodecode IN ('2301','2504','2511','2512','2513','2514','2515','9720')""",
        )

        if_query_list_final_data_for_paycode_combined_rows_greater_than_0_16 = rail.IfOperator(
            task_id='if_query_list_final_data_for_paycode_combined_rows_greater_than_0_16',
            test='''{{ result('query_list_final_data_for_paycode_combined','length') > 0 }}''',
            yes_task="map_item_data_17",
            no_task="finish",
        )

        map_item_data_17 = rail.PythonOperator(
            task_id='map_item_data_17',
            python_callable=lambda: list(map(lambda item: {
                "RECTY": item['RECTY'],
                "CLID": item['CLID'],
                "INTCA": item['INTCA'],
                "ORDNO": item['ORDNO'],
                "IOPER": "INS" if item['paycodecode'] == "2301" else ("INS" if (item['PayCodeHours'] and float(item['PayCodeHours'])) > 0 else "DEL"),
                "INFTY": item['INFTY'],
                "paycodecode": item['paycodecode'] if item['paycodecode'] != "2301" else ("2301" if float(item['PayCodeHours']) > 0 else "2302"),
                "BEGDA": item['BEGDA'],
                "ENDDA": item['ENDDA'],
                "OBJPS": item['OBJPS'],
                "SPRPS": item['SPRPS'],
                "SEQNR": item['SEQNR'],
                "EXTRA": item['EXTRA'],
                "paycodecode2": item['paycodecode'] if item['paycodecode'] != "2301" else ("2301" if float(item['PayCodeHours']) > 0 else "2302"),
                "STDAZ": item['STDAZ'],
                "BEGUZ": item['BEGUZ'],
                "ENDUZ": item['ENDUZ'],
                "BETRG": item['BETRG'],
                "WAERS": item['WAERS'],
                "PayCodeHours": abs(float(item['PayCodeHours'])) if item['paycodecode'] != "2301" else ("1" if abs(float(item['PayCodeHours'] or '0')) == 9 else "0.5"),
                "ZEINH": item['ZEINH'],
                "VTKEN": item['VTKEN'],
                "BWGRL": item['BWGRL'],
                "AUFKZ": item['AUFKZ'],
                "ENDOF": item['ENDOF'],
                "UFLD1": item['UFLD1'],
                "UFLD2": item['UFLD2'],
                "UFLD3": item['UFLD3'],
                "KEYPR": item['KEYPR'],
                "TRFGR": item['TRFGR'],
                "TRFST": item['TRFST'],
                "PRAKN": item['PRAKN'],
                "PRAKZ": item['PRAKZ'],
                "OTYPE": item['OTYPE'],
                "PLANS": item['PLANS'],
                "VERSL": item['VERSL'],
                "EXBEL": item['EXBEL'],
                "WTART": item['WTART'],
                "TDLANGU": item['TDLANGU'],
                "TDSUBLA": item['TDSUBLA'],
                "TDTYPE": item['TDTYPE']
            }, rail.load_all_records(rail.result('query_list_final_data_for_paycode_combined')),
            ))
        )

        def get_export_value_for_item(item):
            if item:
                return item
            return null

        create_csv_lines_18 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_18',
            source="{{result('map_item_data_17') | to_json }}",
            header=null,
            delimiter='|',
            row=lambda item: {
                "column_0": "P2010",
                "column_1": get_export_value_for_item(item['CLID']),
                "column_2": "IN",
                "column_3": null,
                "column_4": get_export_value_for_item(item['IOPER']),
                "column_5": "2010",
                "column_6": get_export_value_for_item(item['paycodecode']),
                "column_7": get_export_value_for_item(datetime.strptime(item['BEGDA'], '%d %B %Y').strftime("%Y%m%d")),
                "column_8": get_export_value_for_item(datetime.strptime(item['ENDDA'], '%d %B %Y').strftime("%Y%m%d")),
                "column_9": null,
                "column_10": null,
                "column_11": null,
                "column_12": null,
                "column_13": get_export_value_for_item(item['paycodecode2']),
                "column_14": null,
                "column_15": null,
                "column_16": null,
                "column_17": null,
                "column_18": null,
                "column_19": get_export_value_for_item(item['PayCodeHours']),
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

        log_total_recordsincludingheaderandfooter_19 = rail.PythonOperator(
            task_id='log_total_recordsincludingheaderandfooter_19',
            python_callable=lambda:  len(rail.result('map_item_data_17')) + 2
        )

        log_formatted_data_20 = rail.PythonOperator(
            task_id='log_formatted_data_20',
            python_callable=lambda:  rail.render_template("HEADR|G2DX|DXC|REPLICON|||{{dag_run.conf.filename}}.SAP|{{ dag_run.conf.rundateinYYYYMMDDformat }}|{{ dag_run.conf.runtimeinHHMMSSformat }}|P|01") + "\r\n" + rail.read_artifact(
                rail.result('create_csv_lines_18')) + rail.render_template("TRAIL|{{result('log_total_recordsincludingheaderandfooter_19')}}")
        )

        log_g_s_u_bwith_21 = rail.PythonOperator(
            task_id='log_g_s_u_bwith_21',
            python_callable=lambda:  rail.result(
                'log_formatted_data_20').replace('|', '|"')
        )

        insert_to_list_22 = rail.SetVariableOperator(
            task_id='insert_to_list_22',
            append=True,
            name='{{ result("declare_list_12").name }}',
            value=lambda: {
                "log": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " INFO admin No of records exported = " + str(len(rail.result('map_item_data_17')))
            }
        )

        upload_file_uploadfiletosftp_24 = rail.S3UploadFileOperator(
            task_id='upload_file_uploadfiletosftp_24',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/{{ dag_run.conf.filename }}.SAP",
            source="{{ result('log_g_s_u_bwith_21') }}"
        )

        encrypt2_a_d_p_public_key_25 = rail.PGPEncryptionOperator(
            task_id='encrypt2_a_d_p_public_key_25',
            source="{{ result('log_g_s_u_bwith_21') }}",
            pgp_conn_id=config.pgp_conn_id,
        )

        upload_uploadfiletosftp_26 = rail.SFTPUploadFileOperator(
            task_id='upload_uploadfiletosftp_26',
            content="{{ result('encrypt2_a_d_p_public_key_25') }}",
            # append = false,
            remote_filepath=config.datafilepath + \
            "/{{ dag_run.conf.filename }}.SAP.pgp"
        )

        insert_to_list_29 = rail.SetVariableOperator(
            task_id='insert_to_list_29',
            append=True,
            name='{{ result("declare_list_12").name }}',
            value=lambda: {
                "log": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S") + " INFO admin Export File_" + rail.render_template("{{ dag_run.conf.filename }}.SAP.pgp") + " created"
            }
        )

        log_processended_30 = rail.PythonOperator(
            task_id='log_processended_30',
            python_callable=lambda:  datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")
        )

        insert_to_list_31 = rail.SetVariableOperator(
            task_id='insert_to_list_31',
            append=True,
            name='{{ result("declare_list_12").name }}',
            value=lambda: {
                "log": (datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S")) + " - Process ended"
            }
        )

        create_csv_lines_compsepayrolllog_32 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_compsepayrolllog_32',
            source="{{ dag_run_var(result('declare_list_12').name) | to_json }}",
            header=['Log file'],
            row=lambda item: {
                "column_0": item['log']
            }.values(),
        )

        send_mail_sendemailafterpayrollexport_33 = rail.EmailOperator(
            task_id='send_mail_sendemailafterpayrollexport_33',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Replicon monthly payroll export for IN ES completed - {{ dag_run.conf.timenow }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Replicon monthly payroll export for IN ES is completed successfully on {{ dag_run.conf.timenow }}. Please find the export details for reference. </p>
            <ul>
            <li>Process started: {{ dag_run.conf.timenow }} </li>
            <li>Payroll extract file name: {{ dag_run.conf.filename }} </li>
            <li>File path: {{ params.datafilepath}}  </li>
            <li>Company Code: {{ dag_run.conf.division }}  </li>
            <li>Number of records in payroll extract: {{ result('query_list_final_data_except_paycode2301_13','length') }} </li>
            <li>Payroll log file name: Log_{{ dag_run.conf.filename }}  </li>
            <li>Log file path: {{params.logfilepath }} </li>
            <li>Process ended: {{ result('log_processended_30') }} </li>
            </ul>
            <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params={'datafilepath': config.datafilepath,
                    'logfilepath': config.logfilepath},
        )

        upload_file_35 = rail.S3UploadFileOperator(
            task_id='upload_file_35',
            aws_conn_id=config.aws_conn_id,
            bucket_name=config.s3_bucket_name,
            key_name="Dxctechnology/Payrollexport/INDIAES/Log_{{ dag_run.conf.filename }}.txt",
            source="{{ result('create_csv_lines_compsepayrolllog_32') }}"
        )

        upload_upload_payrolllogstosftp_36 = rail.SFTPUploadFileOperator(
            task_id='upload_upload_payrolllogstosftp_36',
            content='''{{ result('create_csv_lines_compsepayrolllog_32') }}''',
            # append = false,
            remote_filepath=config.logfilepath + \
            "/Log_{{ dag_run.conf.filename }}.txt",
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_file_uploadfiletosftp_3
        get_file_uploadfiletosftp_3 >> load_csv_create_list_from_csv_4 >> create_collection_create_list_from_csv_4 >> if_create_list_from_csv_4_row_count_less_than_1_5
        if_create_list_from_csv_4_row_count_less_than_1_5 >> rail.Label(
            'Yes') >> stop_6 >> finish
        if_create_list_from_csv_4_row_count_less_than_1_5 >> rail.Label(
            'No') >> query_list_final_datawithout_employee_i_d_7 >> if_query_list_final_datawithout_employee_i_d_7_rows_greater_than_0_8
        if_query_list_final_datawithout_employee_i_d_7_rows_greater_than_0_8 >> rail.Label(
            'Yes') >> mark_pay_run_as_draft_9 >> cancel_pay_run_10 >> stop_11 >> finish
        if_query_list_final_datawithout_employee_i_d_7_rows_greater_than_0_8 >> rail.Label(
            'No') >> declare_list_12 >> query_list_final_data_except_paycode2301_13 >> query_list_final_datafor_paycode2301_15 >> query_list_final_data_for_paycode_combined >> if_query_list_final_data_for_paycode_combined_rows_greater_than_0_16
        if_query_list_final_data_for_paycode_combined_rows_greater_than_0_16 >> rail.Label(
            'No') >> finish
        if_query_list_final_data_for_paycode_combined_rows_greater_than_0_16 >> rail.Label(
            'Yes') >> map_item_data_17 >> create_csv_lines_18 >> log_total_recordsincludingheaderandfooter_19 >> log_formatted_data_20 >> log_g_s_u_bwith_21 >> insert_to_list_22 >> upload_file_uploadfiletosftp_24 >> encrypt2_a_d_p_public_key_25 >> upload_uploadfiletosftp_26 >> insert_to_list_29 >> log_processended_30 >> insert_to_list_31 >> create_csv_lines_compsepayrolllog_32 >> send_mail_sendemailafterpayrollexport_33 >> upload_file_35 >> upload_upload_payrolllogstosftp_36 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
