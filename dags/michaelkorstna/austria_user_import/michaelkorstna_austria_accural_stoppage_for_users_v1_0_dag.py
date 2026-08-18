
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'michaelkorstna_austria_user_import_accural_stoppage_for_users_child_{config.instance}',
        description=f'MichaelKorsTnA Austria_Accural stoppage for users v1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_today_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_today_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_today_3 = rail.PythonOperator(
            task_id='log_today_3',
            python_callable=lambda: {
                'format1': datetime.now().strftime("%d/%m/%Y"),
                'format2': datetime.now().strftime("%m/%d/%Y")
            }
        )

        def get_timeoff_types_uri(response):
            parentalleave = rail.find_first_by_attr_and_get_attr(response, 'name', '[AT] Parental Leave', 'uri', '')
            paternityleave = rail.find_first_by_attr_and_get_attr(response, 'name', '[AT] Paternity Leave', 'uri', '')
            return {
                'annualleave': rail.find_first_by_attr_and_get_attr(response, 'name', '[AT] Annual leave', 'uri', ''),
                'parentalleave': (parentalleave.split(':'))[-1] if parentalleave else '',
                'paternityleave': (paternityleave.split(':'))[-1] if paternityleave else ''
            }

        get_all_time_off_types_6 = rail.RepliconServiceOperator(
            task_id='get_all_time_off_types_6',
            endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes",
            data_handler=get_timeoff_types_uri
        )

        get_usertimeoffbooking_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_usertimeoffbooking_report_details',
            report_name=config.user_timeoffbooking_report
        )

        if_log_get_user_timeoff_booking_listreport_11_blank_12 = rail.IfOperator(
            task_id='if_log_get_user_timeoff_booking_listreport_11_blank_12',
            test='''{{ result('get_usertimeoffbooking_report_details').uri | is_falsy }}''',
            yes_task="stop_13",
            no_task="log_get_date_range_filteruri_15",
        )

        stop_13 = rail.FailOperator(
            task_id='stop_13',
            message='''***User List to disable*** report not available in Replicon'''
        )

        log_get_date_range_filteruri_15 = rail.PythonOperator(
            task_id='log_get_date_range_filteruri_15',
            python_callable=lambda:  {
                'daterangefilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_usertimeoffbooking_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', ''),
                'timeofftypefilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_usertimeoffbooking_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'TimeOffTypeFilter', 'uri', ''),
                'approvalstatusfilteruri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_usertimeoffbooking_report_details')['filterConfiguration']['enabledFilters'], 'displayText', 'ApprovalStatusFilter', 'uri', '')
            }
        )

        run_usertimeoffbooking_report = rail.run_report2(
            group_id='run_usertimeoffbooking_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_usertimeoffbooking_report_details').uri }}",
                        "filterValues": [
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').daterangefilteruri }}",
                                "value": null
                            },
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').daterangefilteruri }}",
                                "value": "{{ result('log_today_3').format2 }}"
                            },
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').daterangefilteruri }}",
                                "value": null
                            },
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').timeofftypefilteruri }}",
                                "value": "{{ result('get_all_time_off_types_6').parentalleave }}"
                            },
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').timeofftypefilteruri }}",
                                "value": "{{ result('get_all_time_off_types_6').paternityleave }}"
                            },
                            {
                                "reportFilterUri": "{{ result('log_get_date_range_filteruri_15').approvalstatusfilteruri }}",
                                "value": "2"
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            target='artifact'
        )

        if_first_error_present_21 = rail.IfOperator(
            task_id='if_first_error_present_21',
            test='''{{ (result('run_usertimeoffbooking_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error | is_truthy }}''',
            yes_task="stop_22",
            no_task="if_first_payload_contains_nodata_23",
        )

        stop_22 = rail.FailOperator(
            task_id='stop_22',
            message='''{{ (result('run_usertimeoffbooking_report.get_report_result')| load_json_artifact).reportGenerationResults[0].error }}'''
        )

        if_first_payload_contains_nodata_23 = rail.IfOperator(
            task_id='if_first_payload_contains_nodata_23',
            test="{{(result('run_usertimeoffbooking_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload | matches('No Data')}}",
            yes_task="finish",
            no_task="load_csv_create_list_from_csv_27",
        )

        load_csv_create_list_from_csv_27 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_27",
            document="{{(result('run_usertimeoffbooking_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload }}",
        )

        create_collection_create_list_from_csv_27 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_27',
            source="{{ result('load_csv_create_list_from_csv_27') }}",
            name="usertimeoffdata",
            columns={
                'User Name': 'username',
                'Time Off Type': 'timeofftype',
                'Booking Start Date': 'bookingstartdate',
                'Booking End Date': 'bookingenddate',
                'Approval Status': 'approvalstatus',
                'Time Off Days': 'timeoffdays',
                'Time Off Comments': 'timeoffcomments',
                'day diff': 'daydiff',
                'useruri': 'useruri',
                'today': 'today',
                'Country (Current)': 'countrycurrent',
                'CBA': 'cba'
            }
        )

        query_list_getalltheuserswhoareenabledandhavetimeoffstartdateastoday_28 = rail.QueryCollectionOperator(
            task_id='query_list_getalltheuserswhoareenabledandhavetimeoffstartdateastoday_28',
            query="""SELECT * FROM  usertimeoffdata WHERE  usertimeoffdata.daydiff = CAST(0 as FLOAT) AND  usertimeoffdata.countrycurrent = "Austria" """,
        )

        trigger_child_timeofftype_accrual_stoppage = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_child_timeofftype_accrual_stoppage',
            retries=0,
            items="{{ result('query_list_getalltheuserswhoareenabledandhavetimeoffstartdateastoday_28') }}",
            trigger_dag_id=f'michaelkorstna_austria_user_import_timeoff_type_accrual_stoppage_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "callerjobid": "{{ dag_run_ecid() }}",
                "userloginname": "{{ item.username }}",
                "useruri": "{{ item.useruri }}",
                "timeofftype": "[AT] Annual Leave",
                "timeofftypeuri": "{{ result('get_all_time_off_types_6').annualleave }}",
                "bookingstartdate": "{{ item.bookingstartdate }}",
                "bookingenddate": "{{ item.bookingenddate }}",
                "timeoffdays": "{{ item.timeoffdays }}",
                "cba": "{{ item.cba }}",
                "country": "{{ item.countrycurrent }}"
            }
        )

        wait_for_child_timeofftype_accrual_stoppage = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_timeofftype_accrual_stoppage',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_timeofftype_accrual_stoppage") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_today_3
        log_today_3 >> get_all_time_off_types_6 >> get_usertimeoffbooking_report_details >> if_log_get_user_timeoff_booking_listreport_11_blank_12
        if_log_get_user_timeoff_booking_listreport_11_blank_12 >> rail.Label(
            'Yes') >> stop_13 >> finish
        if_log_get_user_timeoff_booking_listreport_11_blank_12 >> rail.Label(
            'No') >> log_get_date_range_filteruri_15 >> run_usertimeoffbooking_report >> if_first_error_present_21
        if_first_error_present_21 >> rail.Label('Yes') >> stop_22 >> finish
        if_first_error_present_21 >> rail.Label(
            'No') >> if_first_payload_contains_nodata_23
        if_first_payload_contains_nodata_23 >> rail.Label('Yes') >> finish
        if_first_payload_contains_nodata_23 >> rail.Label(
            'No') >> load_csv_create_list_from_csv_27 >> create_collection_create_list_from_csv_27
        create_collection_create_list_from_csv_27 >> query_list_getalltheuserswhoareenabledandhavetimeoffstartdateastoday_28
        query_list_getalltheuserswhoareenabledandhavetimeoffstartdateastoday_28 >> trigger_child_timeofftype_accrual_stoppage
        trigger_child_timeofftype_accrual_stoppage >> wait_for_child_timeofftype_accrual_stoppage >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
