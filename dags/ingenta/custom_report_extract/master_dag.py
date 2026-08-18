
from datetime import timedelta
from airflow.models import Variable
import rail
from ingenta.custom_report_extract.utils.python_callable import (
    build_initial_list as _build_initial_list,
    build_monthly_splits as _build_monthly_splits,
    build_timeoff_filter_values as _build_timeoff_filter_values,
    build_allocation_filter_values as _build_allocation_filter_values,
    build_timedata_filter_values as _build_timedata_filter_values,
)

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'ingenta_custom_report_extract_master_{config.instance}',
        description=f'Ingenta_custom_report_extract_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
        webhook_conf=[rail.WebhookConf(
            bearer_token_var=f'ingenta_custom_report_extract_{config.instance}_secret')],
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='ingenta_lookuptable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='ingenta_lookuptable',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        ingenta_lookuptable = rail.CreateLogOperator(
            task_id='ingenta_lookuptable'
        )

        log_daterangestart_3 = rail.PythonOperator(
            task_id='log_daterangestart_3',
            python_callable=lambda dag_run:  str(dag_run.conf['webhook']['data']['dateRange'].split("-")[0].strip()[:2]) + "/" + str(
                dag_run.conf['webhook']['data']['dateRange'].split("-")[0].strip()[2:4]) + "/" + str(dag_run.conf['webhook']['data']['dateRange'].split("-")[0].strip()[4:8])

        )

        log_daterangeend_4 = rail.PythonOperator(
            task_id='log_daterangeend_4',
            python_callable=lambda dag_run:  str(dag_run.conf['webhook']['data']['dateRange'].split("-")[-1].strip()[:2]) + "/" + str(
                dag_run.conf['webhook']['data']['dateRange'].split("-")[-1].strip()[2:4]) + "/" + str(dag_run.conf['webhook']['data']['dateRange'].split("-")[-1].strip()[4:8])
        )

        get_user_details_8 = rail.RepliconServiceOperator(
            task_id='get_user_details_8',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data=lambda dag_run: {
                "userUri": "urn:replicon-tenant:"+str(rail.get_tenant_slug())+":user:"+str(dag_run.conf['webhook']['data']['requestorid'])
            }
        )

        impersonate_and_create_interactive_session_9 = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_9',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data=lambda dag_run: {
                "impersonatedUserUri": "urn:replicon-tenant:"+str(rail.get_tenant_slug())+":user:"+str(dag_run.conf['webhook']['data']['requestorid'])
            }
        )

        log_authtoken_10 = rail.PythonOperator(
            task_id='log_authtoken_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'impersonate_and_create_interactive_session_9')['sessionCookies'], 'name', 'AUTHTOKEN', 'value')
        )

        log_effective_email = rail.PythonOperator(
            task_id='log_effective_email',
            python_callable=lambda dag_run: (
                dag_run.conf['webhook']['data']['emailIds']
                if dag_run.conf['webhook']['data'].get('emailIds')
                else config.internal_logs_email
            )
        )

        if_payload_daterange_contains_null_13 = rail.IfOperator(
            task_id='if_payload_daterange_contains_null_13',
            test='''{{ dag_run.conf.webhook.data.dateRange | matches('null') }}''',
            yes_task="send_mail_14",
            no_task="get_all_reports_16",
        )

        send_mail_14 = rail.EmailOperator(
            task_id='send_mail_14',
            to="{{result('log_effective_email')}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Project Report Export - No Data -  {{ current_time("%m/%d/%YT%H:%M:%S") }}''',
            html_content="templates/emails/no_data_mail.html",
        )

        get_all_reports_16 = rail.RepliconServiceOperator(
            task_id='get_all_reports_16',
            endpoint="/services/reportservice1.svc/GetAllReports",
        )

        log_report_urifor_r_i_t_contract_daysreport_17 = rail.PythonOperator(
            task_id='log_report_urifor_r_i_t_contract_daysreport_17',
            python_callable=lambda:  rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT Contract Days report', 'uri', null)
        )

        get_report_details2_r_i_t_contract_daysreport_18 = rail.RepliconServiceOperator(
            task_id='get_report_details2_r_i_t_contract_daysreport_18',
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data={
                "reportUri": "{{ result('log_report_urifor_r_i_t_contract_daysreport_17') }}"
            }
        )

        log_entry_date_filter_uri_19 = rail.PythonOperator(
            task_id='log_entry_date_filter_uri_19',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_r_i_t_contract_daysreport_18')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', null)
        )

        generate_r_i_t_contract_daysreport_20 = rail.RepliconServiceOperator(
            task_id='generate_r_i_t_contract_daysreport_20',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('log_report_urifor_r_i_t_contract_daysreport_17') }}",
                "filterValues": [
                    {
                        "reportFilterUri": "{{ result('log_entry_date_filter_uri_19') }}",
                        "value": null
                    },
                    {
                        "reportFilterUri": "{{ result('log_entry_date_filter_uri_19') }}",
                        "value": "{{ result('log_daterangestart_3') }}"
                    },
                    {
                        "reportFilterUri": "{{ result('log_entry_date_filter_uri_19') }}",
                        "value": "{{ result('log_daterangeend_4') }}"
                    }
                ],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_payload_starts_with_nodata_21 = rail.IfOperator(
            task_id='if_d_payload_starts_with_nodata_21',
            test='''{{ result('generate_r_i_t_contract_daysreport_20').payload | starts_with('No Data') }}''',
            yes_task="send_mail_22",
            no_task="if_d_error_present_24",
        )

        send_mail_22 = rail.EmailOperator(
            task_id='send_mail_22',
            to="{{result('log_effective_email')}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Project Report Export - No Data -  {{ current_time("%m/%d/%YT%H:%M:%S") }}''',
            html_content="templates/emails/no_data_in_payload_mail.html"
        )

        if_d_error_present_24 = rail.IfOperator(
            task_id='if_d_error_present_24',
            test='''{{ result('generate_r_i_t_contract_daysreport_20').error | is_truthy }}''',
            yes_task="stop_25",
            no_task="parse_csv_r_i_t_contract_daysreport_26",
        )

        stop_25 = rail.FailOperator(
            task_id='stop_25',
            message='''{{ result('generate_r_i_t_contract_daysreport_20').error }}'''
        )

        parse_csv_r_i_t_contract_daysreport_26 = rail.LoadCSVFileOperator(
            task_id="parse_csv_r_i_t_contract_daysreport_26",
            document="{{result('generate_r_i_t_contract_daysreport_20').payload }}",
            headers=["User Name", "Scheduled Hrs",
                     "Month (Entry Date)", "contractdays", "useruri"],
            delimiter=","
        )

        log_report_urifor_r_i_t_userrefefencefile_27 = rail.PythonOperator(
            task_id='log_report_urifor_r_i_t_userrefefencefile_27',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT - User reference file', 'uri', null)
        )

        generate_r_i_t_userreferencefile_28 = rail.RepliconServiceOperator(
            task_id='generate_r_i_t_userreferencefile_28',
            endpoint="/services/reportService1.svc/GenerateReport",
            data={
                "reportUri": "{{ result('log_report_urifor_r_i_t_userrefefencefile_27') }}",
                "filterValues": [],
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_payload_not_starts_with_nodata_29 = rail.IfOperator(
            task_id='if_d_payload_not_starts_with_nodata_29',
            test='''{{ result('generate_r_i_t_userreferencefile_28').payload | starts_with('No Data') | is_falsy }}''',
            yes_task="parse_csv_r_i_t_userreferencefile_30",
            no_task="log_report_urifor_r_i_t_time_off_bookingsreference_31",
        )

        parse_csv_r_i_t_userreferencefile_30 = rail.LoadCSVFileOperator(
            task_id="parse_csv_r_i_t_userreferencefile_30",
            document="{{result('generate_r_i_t_userreferencefile_28').payload}}",
            headers=["User Name", "User Status", "useruri"],
            delimiter=","
        )

        log_report_urifor_r_i_t_time_off_bookingsreference_31 = rail.PythonOperator(
            task_id='log_report_urifor_r_i_t_time_off_bookingsreference_31',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT Time Off Bookings reference', 'uri', null)
        )

        get_report_details2_r_i_t_time_off_bookingsreference_32 = rail.RepliconServiceOperator(
            task_id='get_report_details2_r_i_t_time_off_bookingsreference_32',
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data={
                "reportUri": "{{ result('log_report_urifor_r_i_t_time_off_bookingsreference_31') }}"
            }
        )

        log_date_range_filter_uri_33 = rail.PythonOperator(
            task_id='log_date_range_filter_uri_33',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_r_i_t_time_off_bookingsreference_32')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', null)
        )

        log_department_filter_uri_34 = rail.PythonOperator(
            task_id='log_department_filter_uri_34',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_r_i_t_time_off_bookingsreference_32')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDepartmentGroupFilter', 'uri', null)
        )

        log_user_filter_uri_35 = rail.PythonOperator(
            task_id='log_user_filter_uri_35',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_r_i_t_time_off_bookingsreference_32')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', null)
        )

        build_timeoff_filter_values = rail.PythonOperator(
            task_id='build_timeoff_filter_values',
            python_callable=lambda dag_run: _build_timeoff_filter_values(dag_run),
        )

        generate_r_i_t_time_off_bookingsreference_60 = rail.RepliconServiceOperator(
            task_id='generate_r_i_t_time_off_bookingsreference_60',
            endpoint="/services/reportService1.svc/GenerateReport",
            data=lambda: {
                "reportUri": rail.result('log_report_urifor_r_i_t_time_off_bookingsreference_31'),
                "filterValues": rail.result('build_timeoff_filter_values'),
                "outputFormatUri": "urn:replicon:report-output-format-option:csv"
            }
        )

        if_d_error_present_61 = rail.IfOperator(
            task_id='if_d_error_present_61',
            test='''{{ result('generate_r_i_t_time_off_bookingsreference_60').error | is_truthy }}''',
            yes_task="stop_62",
            no_task="if_d_payload_not_starts_with_nodata_63",
        )

        stop_62 = rail.FailOperator(
            task_id='stop_62',
            message='''{{ result('generate_r_i_t_time_off_bookingsreference_60').error }}'''
        )

        if_d_payload_not_starts_with_nodata_63 = rail.IfOperator(
            task_id='if_d_payload_not_starts_with_nodata_63',
            test='''{{ result('generate_r_i_t_time_off_bookingsreference_60').payload | starts_with('No Data') | is_falsy }}''',
            yes_task="parse_csv_r_i_t_time_off_bookingsreference_64",
            no_task="log_report_urifor_r_i_t_custom_report_allocationdata_66",
        )

        parse_csv_r_i_t_time_off_bookingsreference_64 = rail.LoadCSVFileOperator(
            task_id="parse_csv_r_i_t_time_off_bookingsreference_64",
            document="{{result('generate_r_i_t_time_off_bookingsreference_60').payload}}",
            headers=['User Name', 'Time Off Type', 'Time Off Days',
                     'Month (Time Off Date)', 'useruri', 'Department (Current)', 'User Dept for TRANS'],
            delimiter=','
        )

        create_list_65 = rail.CreateCollectionOperator(
            task_id='create_list_65',
            source=lambda: rail.load_all_records(rail.result(
                'parse_csv_r_i_t_time_off_bookingsreference_64')),
            columns=['User Name', 'Time Off Type', 'Time Off Days',
                     'Month (Time Off Date)', 'useruri', 'Department (Current)', 'User Dept for TRANS'],
            name="Time_Off_reference",
        )

        log_report_urifor_r_i_t_custom_report_allocationdata_66 = rail.PythonOperator(
            task_id='log_report_urifor_r_i_t_custom_report_allocationdata_66',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT-Custom Report - Allocation data', 'uri', null)
        )

        get_report_details2_67 = rail.RepliconServiceOperator(
            task_id='get_report_details2_67',
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data={
                "reportUri": "{{ result('log_report_urifor_r_i_t_custom_report_allocationdata_66') }}"
            }
        )

        log_project_filter_uri_69 = rail.PythonOperator(
            task_id='log_project_filter_uri_69',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_67')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'ProjectFilter', 'uri', null)
        )

        log_department_filteruri_70 = rail.PythonOperator(
            task_id='log_department_filteruri_70',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_67')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDepartmentGroupFilter', 'uri', null)
        )

        log_date_range_filter_uri_71 = rail.PythonOperator(
            task_id='log_date_range_filter_uri_71',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_67')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', null)
        )

        log_user_filter_uri_72 = rail.PythonOperator(
            task_id='log_user_filter_uri_72',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_67')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', null)
        )

        log_date_range_filter_uri_73 = rail.PythonOperator(
            task_id='log_date_range_filter_uri_73',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_67')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'DateRangeFilter', 'uri', null)
        )

        build_allocation_filter_values = rail.PythonOperator(
            task_id='build_allocation_filter_values',
            python_callable=lambda dag_run: _build_allocation_filter_values(dag_run),
        )

        generate_report_group = rail.run_report2(
            group_id='generate_report_group',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('log_report_urifor_r_i_t_custom_report_allocationdata_66'),
                        "filterValues": rail.result('build_allocation_filter_values'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        log_report_urifor_r_i_t_client_projectreference_113 = rail.PythonOperator(
            task_id='log_report_urifor_r_i_t_client_projectreference_113',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT Client-Project reference', 'uri', null)
        )

        generate_report_group1 = rail.run_report2(
            group_id='generate_report_group1',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('log_report_urifor_r_i_t_client_projectreference_113'),
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        parse_csv_122 = rail.LoadCSVFileOperator(
            task_id="parse_csv_122",
            document="{{result('generate_report_group1.get_report_result').reportGenerationResults[0].payload}}",
            headers=['Client Name', 'Project Name', 'projecturi'],
            delimiter=","
        )

        log_report_uri_r_i_t_custom_report_timedata_124 = rail.PythonOperator(
            task_id='log_report_uri_r_i_t_custom_report_timedata_124',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_all_reports_16'), 'displayText', '***RIT-Custom Report - Time data', 'uri', null)
        )

        get_report_details2_126 = rail.RepliconServiceOperator(
            task_id='get_report_details2_126',
            endpoint="/services/reportservice1.svc/GetReportDetails2",
            data={
                "reportUri": "{{ result('log_report_uri_r_i_t_custom_report_timedata_124') }}"
            }
        )

        log_project_filter_uri_127 = rail.PythonOperator(
            task_id='log_project_filter_uri_127',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_126')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'ProjectFilter', 'uri', null)
        )

        log_date_range_filter_uri_128 = rail.PythonOperator(
            task_id='log_date_range_filter_uri_128',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_126')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', null)
        )

        log_user_filter_uri_129 = rail.PythonOperator(
            task_id='log_user_filter_uri_129',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_126')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'UserFilter', 'uri', null)
        )

        log_department_filteruri_130 = rail.PythonOperator(
            task_id='log_department_filteruri_130',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_126')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'CurrentDepartmentGroupFilter', 'uri', null)
        )

        log_client_filter_131 = rail.PythonOperator(
            task_id='log_client_filter_131',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('get_report_details2_126')[
                                                                         'filterConfiguration']['enabledFilters'], 'displayText', 'ClientFilter', 'uri', null)
        )

        build_timedata_filter_values = rail.PythonOperator(
            task_id='build_timedata_filter_values',
            python_callable=lambda dag_run: _build_timedata_filter_values(dag_run),
        )

        generate_report_group2 = rail.run_report2(
            group_id='generate_report_group2',
            report_params=lambda: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('log_report_uri_r_i_t_custom_report_timedata_124'),
                        "filterValues": rail.result('build_timedata_filter_values'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        load_csv_create_list_from_csv_175 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_175",
            document="{{result('generate_report_group2.get_report_result').reportGenerationResults[0].payload}}",
        )

        create_collection_create_list_from_csv_175 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_175',
            source="{{ result('load_csv_create_list_from_csv_175') }}",
            name="draft_time_list",
            columns={
                'User Name': 'username',
                'User Dept for TRANS': 'userdeptfortrans',
                'Client Name': 'clientname',
                'Project Name': 'projectname',
                'Project Dept for TRANS': 'projectdeptfortrans',
                'Actual Days': 'actualdays',
                'proejct uri': 'projecturi',
                'user uri': 'useruri',
                'Department (Current)': 'department',
                'Month (Entry Date)': 'monthactual'
            }
        )

        query_list_get_allentriesfromtimedata_176 = rail.QueryCollectionOperator(
            task_id='query_list_get_allentriesfromtimedata_176',
            name='time_list',
            query="""SELECT * FROM  draft_time_list WHERE draft_time_list.department != '' """,
        )

        query_list_getuniqueuserurisfrom_time_data_177 = rail.QueryCollectionOperator(
            task_id='query_list_getuniqueuserurisfrom_time_data_177',
            query="""SELECT DISTINCT  time_list.useruri FROM  time_list WHERE  NULLIF('useruri','') IS NOT NULL""",
        )

        load_csv_create_list_from_csv_178 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_178",
            document="{{result('generate_report_group.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_collection_create_list_from_csv_178 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_178',
            source="{{ result('load_csv_create_list_from_csv_178') }}",
            name="draft_allocation_data",
            columns={
                'Project Name': 'projectname',
                'User Name': 'username',
                'Project Dept for TRANS': 'projectdeptfortrans',
                'User Dept for TRANS': 'userdeptfortrans',
                'Project Allocated Days': 'projectallocateddays',
                'Month (Allocation Date)': 'monthallocationdate',
                'projecturi': 'projecturi',
                'useruri': 'useruri',
                'Department (Current)': 'department',
            }
        )

        query_list_get_allentriesfrom_allocationdata_179 = rail.QueryCollectionOperator(
            task_id='query_list_get_allentriesfrom_allocationdata_179',
            name="allocation_data",
            query="""SELECT * FROM  draft_allocation_data  WHERE  draft_allocation_data.projectname != '' and draft_allocation_data.department != '' """,
        )

        query_list_get_alltimeoffbookingsforeachuser_192 = rail.QueryCollectionOperator(
            task_id='query_list_get_alltimeoffbookingsforeachuser_192',
            query="""SELECT * FROM  Time_Off_reference WHERE  NULLIF('useruri','') IS NOT NULL""",
        )

        build_initial_list = rail.PythonOperator(
            task_id='build_initial_list',
            python_callable=lambda: _build_initial_list(),
        )

        build_monthly_splits = rail.PythonOperator(
            task_id='build_monthly_splits',
            python_callable=lambda: _build_monthly_splits(),
        )

        if_first_username_present_195 = rail.IfOperator(
            task_id='if_first_username_present_195',
            test=lambda: rail.result('build_initial_list')[0]['username'],
            yes_task="create_list_196",
            no_task="send_mail_223",
        )

        create_list_196 = rail.CreateCollectionOperator(
            task_id='create_list_196',
            source=lambda: rail.result('build_initial_list'),
            columns=['username', 'userdepartmentname', 'userdeptfortrans', 'projectdeptfortrans', 'clientname', 'projectname', 'timeofftype',
                     'month', 'timeoffdays', 'netcontractdays', 'actualdays', 'allocateddays', 'availabledays', 'actualvsplanned', 'useruri'],
            name="Data_to_be_exported",
        )

        query_list_getdistinctmonthfrom_datatoexport_197 = rail.QueryCollectionOperator(
            task_id='query_list_getdistinctmonthfrom_datatoexport_197',
            query="""SELECT DISTINCT  Data_to_be_exported.month FROM  Data_to_be_exported WHERE NULLIF('month','') IS NOT NULL""",
        )

        declare_child_triggered_list = rail.SetVariableOperator(
            task_id='declare_child_triggered_list',
            name='childtriggered',
            append=False,
            value=0
        )

        foreach_query_list_getdistinctmonthfrom_datatoexport_197_219 = rail.ForEachOperator(
            task_id='foreach_query_list_getdistinctmonthfrom_datatoexport_197_219',
            items=lambda: rail.load_all_records(rail.result(
                'query_list_getdistinctmonthfrom_datatoexport_197')),
            start_task='query_list_getindividualmonthsfinaldata_220',
            end_task='foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end'
        )

        query_list_getindividualmonthsfinaldata_220 = rail.QueryCollectionOperator(
            task_id='query_list_getindividualmonthsfinaldata_220',
            query="""SELECT * FROM  Data_to_be_exported WHERE  Data_to_be_exported.month='{{ result('foreach_query_list_getdistinctmonthfrom_datatoexport_197_219').month }}'""",
        )

        def _month_stats():
            month = rail.result('foreach_query_list_getdistinctmonthfrom_datatoexport_197_219')['month']
            return rail.result('build_monthly_splits').get(month, {})

        def get_nettimeoffdays():
            return _month_stats().get('timeoffdays')

        def get_net_contract_days():
            return _month_stats().get('netcontractdays')

        def get_actual_days():
            return _month_stats().get('actualdays')

        def get_allocated_days():
            return _month_stats().get('allocateddays')

        def get_available_days():
            return _month_stats().get('availabledays')

        def get_actual_vs_planned():
            return _month_stats().get('actualvsplanned')

        process_child = rail.TriggerDagRunOperator(
            task_id='process_child',
            retries=0,
            trigger_dag_id=f'ingenta_custom_report_extract_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "rows": rail.result('create_list_196'),  # Pass main collection, child will filter by month
                "jobid": rail.render_template("{{ dag_run_ecid() }}"),
                "islast": "true" if (rail.get_dag_run_var(rail.result('declare_child_triggered_list')['name']) + 1) == rail.result('query_list_getdistinctmonthfrom_datatoexport_197', 'length') else "false",
                "month": rail.result('foreach_query_list_getdistinctmonthfrom_datatoexport_197_219')['month'],
                "nettimeoffdays": get_nettimeoffdays(),
                "netcontractdays": get_net_contract_days(),
                "actualdays": get_actual_days(),
                "allocateddays": get_allocated_days(),
                "availabledays": get_available_days(),
                "actualvsplanned": get_actual_vs_planned(),
                "email": rail.result('log_effective_email'),
                "lookuptable": rail.result('ingenta_lookuptable')
            }
        )

        insert_child_to_triggered_list = rail.SetVariableOperator(
            task_id='insert_child_to_triggered_list',
            name="{{result('declare_child_triggered_list').name}}",
            append=False,
            value=lambda: rail.get_dag_run_var(rail.result('declare_child_triggered_list')[
                'name']) + 1
        )

        foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end = rail.EmptyOperator(
            task_id='foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end',
        )

        send_mail_223 = rail.EmailOperator(
            task_id='send_mail_223',
            to="{{result('log_effective_email')}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | Custom Project Report Export - No Data -  {{ current_time("%m/%d/%YT%H:%M:%S") }}''',
            html_content="templates/emails/no_data_to_upload_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> ingenta_lookuptable
        ingenta_lookuptable >> log_daterangestart_3 >> log_daterangeend_4 >> get_user_details_8
        get_user_details_8 >> impersonate_and_create_interactive_session_9
        impersonate_and_create_interactive_session_9 >> log_authtoken_10 >> log_effective_email >> if_payload_daterange_contains_null_13
        if_payload_daterange_contains_null_13 >> rail.Label(
            'Yes') >> send_mail_14 >> log_to_sumo
        if_payload_daterange_contains_null_13 >> rail.Label(
            'No') >> get_all_reports_16 >> log_report_urifor_r_i_t_contract_daysreport_17 >> get_report_details2_r_i_t_contract_daysreport_18
        get_report_details2_r_i_t_contract_daysreport_18 >> log_entry_date_filter_uri_19
        log_entry_date_filter_uri_19 >> generate_r_i_t_contract_daysreport_20
        generate_r_i_t_contract_daysreport_20 >> if_d_payload_starts_with_nodata_21
        if_d_payload_starts_with_nodata_21 >> rail.Label(
            'Yes') >> send_mail_22 >> log_to_sumo
        if_d_payload_starts_with_nodata_21 >> rail.Label(
            'No') >> if_d_error_present_24
        if_d_error_present_24 >> rail.Label('Yes') >> stop_25 >> log_to_sumo
        if_d_error_present_24 >> rail.Label(
            'No') >> parse_csv_r_i_t_contract_daysreport_26 >> log_report_urifor_r_i_t_userrefefencefile_27
        log_report_urifor_r_i_t_userrefefencefile_27 >> generate_r_i_t_userreferencefile_28
        generate_r_i_t_userreferencefile_28 >> if_d_payload_not_starts_with_nodata_29
        if_d_payload_not_starts_with_nodata_29 >> rail.Label(
            'Yes') >> parse_csv_r_i_t_userreferencefile_30 >> log_report_urifor_r_i_t_time_off_bookingsreference_31
        if_d_payload_not_starts_with_nodata_29 >> rail.Label(
            'No') >> log_report_urifor_r_i_t_time_off_bookingsreference_31 >> get_report_details2_r_i_t_time_off_bookingsreference_32
        get_report_details2_r_i_t_time_off_bookingsreference_32 >> log_date_range_filter_uri_33
        log_date_range_filter_uri_33 >> log_department_filter_uri_34 >> log_user_filter_uri_35 >> build_timeoff_filter_values
        build_timeoff_filter_values >> generate_r_i_t_time_off_bookingsreference_60 >> if_d_error_present_61
        if_d_error_present_61 >> rail.Label(
            'Yes') >> stop_62 >> log_to_sumo
        if_d_error_present_61 >> rail.Label(
            'No') >> if_d_payload_not_starts_with_nodata_63
        if_d_payload_not_starts_with_nodata_63 >> rail.Label(
            'Yes') >> parse_csv_r_i_t_time_off_bookingsreference_64
        parse_csv_r_i_t_time_off_bookingsreference_64 >> create_list_65 >> log_report_urifor_r_i_t_custom_report_allocationdata_66
        if_d_payload_not_starts_with_nodata_63 >> rail.Label(
            'No') >> log_report_urifor_r_i_t_custom_report_allocationdata_66
        log_report_urifor_r_i_t_custom_report_allocationdata_66 >> get_report_details2_67
        get_report_details2_67 >> log_project_filter_uri_69
        log_project_filter_uri_69 >> log_department_filteruri_70 >> log_date_range_filter_uri_71
        log_date_range_filter_uri_71 >> log_user_filter_uri_72 >> log_date_range_filter_uri_73 >> build_allocation_filter_values
        build_allocation_filter_values >> generate_report_group
        generate_report_group >> log_report_urifor_r_i_t_client_projectreference_113
        log_report_urifor_r_i_t_client_projectreference_113 >> generate_report_group1
        generate_report_group1 >> parse_csv_122 >> log_report_uri_r_i_t_custom_report_timedata_124 >> get_report_details2_126
        get_report_details2_126 >> log_project_filter_uri_127 >> log_date_range_filter_uri_128
        log_date_range_filter_uri_128 >> log_user_filter_uri_129
        log_user_filter_uri_129 >> log_department_filteruri_130 >> log_client_filter_131 >> build_timedata_filter_values
        build_timedata_filter_values >> generate_report_group2 >> load_csv_create_list_from_csv_175
        load_csv_create_list_from_csv_175 >> create_collection_create_list_from_csv_175
        create_collection_create_list_from_csv_175 >> query_list_get_allentriesfromtimedata_176
        query_list_get_allentriesfromtimedata_176 >> query_list_getuniqueuserurisfrom_time_data_177
        query_list_getuniqueuserurisfrom_time_data_177 >> load_csv_create_list_from_csv_178
        load_csv_create_list_from_csv_178 >> create_collection_create_list_from_csv_178
        create_collection_create_list_from_csv_178 >> query_list_get_allentriesfrom_allocationdata_179
        query_list_get_allentriesfrom_allocationdata_179 >> query_list_get_alltimeoffbookingsforeachuser_192
        query_list_get_alltimeoffbookingsforeachuser_192 >> build_initial_list
        build_initial_list >> build_monthly_splits >> if_first_username_present_195
        if_first_username_present_195 >> rail.Label(
            'Yes') >> create_list_196 >> query_list_getdistinctmonthfrom_datatoexport_197
        if_first_username_present_195 >> rail.Label(
            'No') >> send_mail_223 >> log_to_sumo
        query_list_getdistinctmonthfrom_datatoexport_197 >> declare_child_triggered_list
        declare_child_triggered_list >> foreach_query_list_getdistinctmonthfrom_datatoexport_197_219
        foreach_query_list_getdistinctmonthfrom_datatoexport_197_219 >> query_list_getindividualmonthsfinaldata_220
        query_list_getindividualmonthsfinaldata_220 >> process_child >> insert_child_to_triggered_list
        insert_child_to_triggered_list >> foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end
        foreach_query_list_getdistinctmonthfrom_datatoexport_197_219 >> foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end
        foreach_query_list_getdistinctmonthfrom_datatoexport_197_219_end >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
