
from datetime import timedelta
import itertools
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'siliconvalleycleanwater_workordersync_webhook_master_{config.instance}',
        description=f'SVC_workordersync_Master - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        webhook_conf=[
            rail.WebhookConf(bearer_token_var=config.webhook_shared_secrete)
        ],
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_document_fields_less_than_1_4',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        if_document_fields_less_than_1_4 = rail.IfOperator(
            task_id='if_document_fields_less_than_1_4',
            test='''{{ dag_run.conf.webhook.data.fields | is_falsy or dag_run.conf.webhook.data.fields[0] | is_falsy }}''',
            yes_task="send_mail_5",
            no_task="report_details_7",
        )

        send_mail_5 = rail.EmailOperator(
            task_id='send_mail_5',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Silicon valley clean water Workorder Import - Blank Payload - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The Silicon valley clean water Workorder Import job is skipped on {{ current_time() }} as the payload is blank. <p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p> ''',
            params=None,
        )

        stop_6 = rail.EmptyOperator(
            task_id='stop_6',

        )

        report_details_7 = rail.RepliconReportDetailsOperator(
            task_id='report_details_7',
            report_name="User details via UDF"
        )

        generate_report_7 = rail.run_report2(
            group_id='generate_report_7',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('report_details_7').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact',
        )

        if_generate_report_7_payload_not_contains_nodata_8 = rail.IfOperator(
            task_id='if_generate_report_7_payload_not_contains_nodata_8',
            test='''{{ not (result('generate_report_7.get_report_result') | load_json_artifact).reportGenerationResults[0].payload |  matches('No Data') }}''',
            yes_task="if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9",
            no_task="finish",
        )

        if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9 = rail.IfOperator(
            task_id='if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9',
            test='''{{ not (result('generate_report_7.get_report_result') | load_json_artifact).reportGenerationResults[0].payload | starts_with('UserUri,WorkOrder ID,Project Manager ID') }}''',
            yes_task="stop_10",
            no_task="parse_csv_readreportdata_11",
        )

        stop_10 = rail.FailOperator(
            task_id='stop_10',
            message='''Base report column does not match'''
        )

        parse_csv_readreportdata_11 = rail.LoadCSVFileOperator(
            task_id='parse_csv_readreportdata_11',
            document="{{ (result('generate_report_7.get_report_result') | load_json_artifact).reportGenerationResults[0].payload }}"
        )

        load_csv_readreportdata_11 = rail.PythonOperator(
            task_id='load_csv_readreportdata_11',
            python_callable=lambda: rail.load_all_records(
                rail.result('parse_csv_readreportdata_11'))
        )

        create_list_12 = rail.CreateCollectionOperator(
            task_id='create_list_12',
            source=lambda: list(map(lambda item: {**item, 'equipmentposition': item.get(
                'equipmentposition')}, rail.get_dag_run_conf()['webhook']['data']['fields'])),
            name="inputdata",
        )

        query_list_distinct_project_name_13 = rail.QueryCollectionOperator(
            task_id='query_list_distinct_project_name_13',
            query="""SELECT DISTINCT  inputdata.soServiceOrderNumber,  inputdata.soServiceOrderDescription,  inputdata.soRequestID, inputdata.soServiceOrderStatus,  inputdata.soRequestedDate,  inputdata.umPhysicalLocation,  inputdata.soDateCompleted, inputdata.equipmentposition FROM  inputdata""",
        )

        declare_dagrun_list_var = rail.SetVariableOperator(
            task_id='declare_dagrun_list_var',
            name='dagruns',
            value=[]
        )

        foreach_query_list_distinct_project_name_13_14 = rail.ForEachOperator(
            task_id='foreach_query_list_distinct_project_name_13_14',
            items="{{ result('query_list_distinct_project_name_13') }}",
            start_task='query_list_allproject_resource_15',
            end_task='foreach_query_list_distinct_project_name_13_14_end'
        )

        query_list_allproject_resource_15 = rail.QueryCollectionOperator(
            task_id='query_list_allproject_resource_15',
            query="""SELECT DISTINCT  inputdata.umRequestedByID30 FROM  inputdata WHERE  inputdata.soServiceOrderNumber='{{ result('foreach_query_list_distinct_project_name_13_14').soServiceOrderNumber }}'""",
        )

        load_allproject_resource_15 = rail.PythonOperator(
            task_id='load_allproject_resource_15',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_list_allproject_resource_15'))
        )

        trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16',
            retries=0,
            items=[1],
            trigger_dag_id=f'siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0_{config.instance}',
            execution_timeout=timedelta(days=14),
            accumulate_result=True,
            conf=lambda: {
                "projectnumber": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soServiceOrderNumber | sn }}"),
                "projectname": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soServiceOrderDescription | sn }}"),
                "Status": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soServiceOrderStatus | sn }}"),
                "startdate": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soRequestedDate| sn  }}"),
                "enddate": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soDateCompleted| sn  }}"),
                "projectdata": list(map(lambda item: {
                    "resourceuri": rail.find_first_by_attr_and_get_attr(rail.result('load_csv_readreportdata_11'), 'WorkOrder ID', item['umRequestedByID30'], ('UserUri')),
                    "resourcename": rail.find_first_by_attr_and_get_attr(rail.result('load_csv_readreportdata_11'), 'WorkOrder ID', item['umRequestedByID30'], ('WorkOrder ID'))
                }, rail.result('load_allproject_resource_15'))),
                "masterjobid": rail.render_template("{{ dag_run_ecid() }}"),
                "requestid": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').soRequestID | sn }}"),
                "physicallocation": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').umPhysicalLocation | sn }}"),
                "equipmentposition": rail.render_template("{{ result('foreach_query_list_distinct_project_name_13_14').equipmentposition | sn }}")
            }
        )

        accumulate_list_trigger_dags_runs = rail.SetVariableOperator(
            task_id='accumulate_list_trigger_dags_runs',
            name='dagruns',
            append=True,
            value=lambda: rail.result(
                "trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16")[0]
        )

        foreach_query_list_distinct_project_name_13_14_end = rail.EmptyOperator(
            task_id='foreach_query_list_distinct_project_name_13_14_end',
        )

        wait_for_completion_trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("accumulate_list_trigger_dags_runs").value or []}}'
        )

        if_document_fields_greater_than_0_17 = rail.IfOperator(
            task_id='if_document_fields_greater_than_0_17',
            test='''{{ dag_run.conf.webhook.data.fields | is_truthy and dag_run.conf.webhook.data.fields[0] | is_truthy }}''',
            yes_task="gather_child_logs",
            no_task="finish",
        )

        gather_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_child_logs',
            dag_runs='{{ result("accumulate_list_trigger_dags_runs").value }}',
            dagrun_task_id='create_log',
            flatten=True,
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=lambda: rail.write_json_artifact(list(list(itertools.chain(
                *list(map(rail.load_all_records, rail.result('gather_child_logs')))))))
        )

        get_logged_errors = rail.PythonOperator(
            task_id='get_logged_errors',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.load_all_records(rail.result('format_logs')), 'properties.status', 'Error')
        )

        get_logged_exceptions = rail.PythonOperator(
            task_id='get_logged_exceptions',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.load_all_records(rail.result('format_logs')), 'properties.status', 'Exception')
        )

        get_logged_success = rail.PythonOperator(
            task_id='get_logged_success',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.load_all_records(rail.result('format_logs')), 'properties.status', 'Success')
        )

        create_csv_lines_finallog_12 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_finallog_12',
            source="{{ result('format_logs') }}",
            header=['soServiceOrderNumber',
                    'soServiceOrderDescription',
                    'soServiceOrderStatus',
                    'umRequestedByID30',
                    'soRequestedDate  and  soDateCompleted',
                    'Project level oef',
                    'Status',
                    'Details',
                    'Jobid'],
            row=[
                "{{ item.properties.soserviceordernumber }}",
                "{{ item.properties.soserviceorderdescription }}",
                "{{ item.properties.soserviceorderstatus }}",
                "{{ item.properties.umrequestedbyid30 }}",
                "{{ item.properties.sorequestedd_and_sodatecompletedate }}",
                '''{{ item.properties["project_level_oef's"] }}''',
                "{{ item.properties.status }}",
                "{{ item.properties.message }}",
                "{{ item.ecid }}"
            ],
        )

        upload_logsupload_15 = rail.SFTPUploadFileOperator(
            task_id='upload_logsupload_15',
            content='''{{ result('create_csv_lines_finallog_12') }}''',
            # append = false,
            remote_filepath='''/ProjectSync/WorkOrder/Logs/Logs_Workorder_{{ dag_run_ecid()| replace(":", "-")  }}.csv''',
        )

        get_email_content = rail.PythonOperator(
            task_id='get_email_content',
            python_callable=lambda: {
                "email": config.alert_email if (rail.result('get_logged_errors')) else config.internal_logs_email,
                "errorcheck": bool(rail.result('get_logged_errors')),
                "exceptioncheck": bool(rail.result('get_logged_exceptions')),
                "subject": "completed with errors" if rail.result('get_logged_errors') else "completed with exceptions" if rail.result('get_logged_exceptions') else "completed succesfully",
                "body": "<br />For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>" if (rail.result('get_logged_errors')) else "<p>For any queries, please contact our support team at https://support.deltek.com <br /><br />Regards, <br />Deltek Inc.</p>",
            }
        )

        send_mail_20 = rail.EmailOperator(
            task_id='send_mail_20',
            to=config.tenant_email,
            bcc='''{{result('get_email_content').email}}''',
            subject='''{{ get_company_key() }}| Workorder Sync {{ result('get_email_content').subject }} - {{ current_time() }} ''',
            html_content='''<p><strong>This is an automated mail, please don't reply.</strong><br /> <br />Hello, <br /> <br /> The  SVC Workorder Sync job is {{ result('get_email_content').subject }} on {{ current_time() }}. Please find the path below.</p>
<p>File Path:/ProjectSync/WorkOrder/Logs</p>
<p>File name:  Logs_Workorder_{{ dag_run_ecid() }}.csv</p>
<p>{{ result('get_email_content').body }}</p> ''',
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        batch_task >> finish
        batch_task >> if_document_fields_less_than_1_4
        if_document_fields_less_than_1_4 >> rail.Label(
            'Yes') >> send_mail_5 >> stop_6 >> finish
        if_document_fields_less_than_1_4 >> rail.Label(
            'No') >> report_details_7 >> generate_report_7 >> if_generate_report_7_payload_not_contains_nodata_8
        if_generate_report_7_payload_not_contains_nodata_8 >> rail.Label(
            'Yes') >> if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9
        if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9 >> rail.Label(
            'Yes') >> stop_10 >> finish
        if_generate_report_7_payload_not_starts_with_useruriworkorderidprojectmanagerid_9 >> rail.Label(
            'No') >> parse_csv_readreportdata_11 >> load_csv_readreportdata_11 >> create_list_12 >> query_list_distinct_project_name_13 >> declare_dagrun_list_var >> foreach_query_list_distinct_project_name_13_14 >> query_list_allproject_resource_15 >> load_allproject_resource_15 >> trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16 >> accumulate_list_trigger_dags_runs >> foreach_query_list_distinct_project_name_13_14_end
        foreach_query_list_distinct_project_name_13_14 >> foreach_query_list_distinct_project_name_13_14_end >> wait_for_completion_trigger_dag_run_siliconvalleycleanwater_workorder_sync_svc_workordersync_child_v1_0async_16 >> if_document_fields_greater_than_0_17
        if_document_fields_greater_than_0_17 >> rail.Label(
            'Yes') >> gather_child_logs >> format_logs >> get_logged_errors >> get_logged_exceptions >> get_logged_success >> create_csv_lines_finallog_12 >> upload_logsupload_15 >> get_email_content >> send_mail_20 >> finish
        if_document_fields_greater_than_0_17 >> rail.Label('No') >> finish
        if_generate_report_7_payload_not_contains_nodata_8 >> rail.Label(
            'No') >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
