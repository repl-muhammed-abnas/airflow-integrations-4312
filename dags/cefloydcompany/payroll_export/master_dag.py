from datetime import timedelta, datetime
from airflow.models import Variable
import rail

from cefloydcompany.payroll_export.utils.custom_methods import *

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'cefloydcompany_payroll_report_export_master_{config.instance}',
        description=f'Cefloydcompany Payroll Report Export {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_startdate_to_date_greater_than_enddate'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_startdate_to_date_greater_than_enddate',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_startdate_to_date_greater_than_enddate = rail.IfOperator(
            task_id='if_startdate_to_date_greater_than_enddate',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], "%m-%d-%Y") > datetime.strptime(dag_run.conf['enddate'], "%m-%d-%Y"),
            yes_task="log_startdateformatted_4",
            no_task="get_all_reports_10",
        )

        log_startdateformatted_4 = rail.PythonOperator(
            task_id='log_startdateformatted_4',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['startdate'], "%m-%d-%Y").strftime("%m/%d/%Y")
        )

        log_enddateformatted_5 = rail.PythonOperator(
            task_id='log_enddateformatted_5',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['enddate'], "%m-%d-%Y").strftime("%m/%d/%Y")
        )

        send_incorrect_dateformat_mail = rail.EmailOperator(
            task_id='send_incorrect_dateformat_mail',
            to="{{dag_run.conf.email}}",
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}} | ADP payroll data from Replicon - Incorrect Date - {{current_time()}}",
            html_content='templates/emails/incorrect_dateformat_mail.html',
        )

        get_all_reports_10 = rail.RepliconServiceOperator(
            task_id='get_all_reports_10',
            endpoint="/services/reportService1.svc/GetAllReports",
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, "displayText", config.adp_export_report, "uri")
        )

        get_report_details2_12 = rail.RepliconServiceOperator(
            task_id='get_report_details2_12',
            endpoint="/services/reportService1.svc/GetReportDetails2",
            data={
                "reportUri": "{{result('get_all_reports_10')}}"
            }
        )

        log_entry_date_filterfilteruri_15 = rail.PythonOperator(
            task_id='log_entry_date_filterfilteruri_15',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details2_12')['filterConfiguration']['enabledFilters'], 'displayText', 'EntryDateFilter', 'uri', '')
        )

        log_u_d_f_filter_user = rail.PythonOperator(
            task_id='log_u_d_f_filter_user',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_report_details2_12')['filterConfiguration']['enabledFilters'], 'displayText', 'UDFFilter_User4_ADPPayrollCompanyCode', 'uri', '')
        )

        log_startdateformatted = rail.PythonOperator(
            task_id='log_startdateformatted',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['startdate'], "%m-%d-%Y").strftime("%m/%d/%Y")
        )

        log_enddateformatted = rail.PythonOperator(
            task_id='log_enddateformatted',
            python_callable=lambda dag_run:  datetime.strptime(
                dag_run.conf['enddate'], "%m-%d-%Y").strftime("%m/%d/%Y")
        )

        generate_report_group = rail.run_report2(
            group_id='generate_report_group',
            report_params=lambda dag_run: {
                "reportParameters": [
                    {
                        "reportUri": rail.result('get_all_reports_10'),
                        "filterValues": [
                            {
                                "reportFilterUri": rail.result('log_entry_date_filterfilteruri_15'),
                                "value": null
                            },
                            {
                                "reportFilterUri": rail.result('log_entry_date_filterfilteruri_15'),
                                "value": rail.result('log_startdateformatted')
                            },
                            {
                                "reportFilterUri": rail.result('log_entry_date_filterfilteruri_15'),
                                "value": rail.result('log_enddateformatted')
                            },
                            {
                                "reportFilterUri": rail.result('log_u_d_f_filter_user'),
                                "value": dag_run.conf['adp_payroll_company_code']
                            }
                        ],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
        )

        if_payload_has_data = rail.IfOperator(
            task_id='if_payload_has_data',
            test='''{{ result('generate_report_group.get_report_result','has_data')}}''',
            yes_task="if_payload_has_no_columns",
            no_task="send_nodata_mail",
        )

        send_nodata_mail = rail.EmailOperator(
            task_id='send_nodata_mail',
            to="{{dag_run.conf.email}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | ADP payroll data from Replicon - No data to export - {{ current_time() }} ''',
            html_content="templates/emails/nodata_mail.html"
        )

        if_payload_has_no_columns = rail.IfOperator(
            task_id='if_payload_has_no_columns',
            # pylint: disable=consider-using-f-string,line-too-long
            test="{{result('generate_report_group.get_report_result').reportGenerationResults[0].payload | starts_with('%s')| is_falsy}}" % config.expected_report_columns,
            yes_task="stop_job_with_error",
            no_task="load_csv_create_list_from_csv_30",
        )

        stop_job_with_error = rail.FailOperator(
            task_id='stop_job_with_error',
            message='''Base report column order doesn't match'''
        )

        load_csv_create_list_from_csv_30 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv_30",
            document="{{result('generate_report_group.get_report_result').reportGenerationResults[0].payload}}",
        )

        create_collection_create_list_from_csv_30 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_30',
            source="{{ result('load_csv_create_list_from_csv_30') }}",
            name="timesheetdata",
            columns={
                'User Name': 'username',
                'ADP Payroll Company Code': 'adppayrollcompanycode',
                'File Number (ADP Pay Statements)': 'filenumberadppaystatements',
                'REG': 'reg',
                'OT': 'ot',
                'PTO': 'pto',
                'Total Hrs': 'totalhrs',
                'Home Department Code': 'homedepartmentcode',
                'Project Code': 'projectcode',
                'Task Code': 'taskcode',
                'OH Hours Code': 'activitycode',
                'UserUri': 'useruri'
            }
        )

        query_list_31 = rail.QueryCollectionOperator(
            task_id='query_list_31',
            query="""SELECT * FROM  timesheetdata""",
        )

        adp_export_log = rail.CreateLogOperator(
            task_id='adp_export_log'
        )

        foreach_query_list_31_33 = rail.ForEachOperator(
            task_id='foreach_query_list_31_33',
            items="{{ result('query_list_31') }}",
            start_task='log_batch_i_dvalue_34',
            end_task='foreach_query_list_31_33_end'
        )

        log_batch_i_dvalue_34 = rail.PythonOperator(
            task_id='log_batch_i_dvalue_34',
            python_callable=lambda: {"H44": "Hourly", "H46": "Salary"}[
                rail.result('foreach_query_list_31_33')['adppayrollcompanycode']] or null
        )

        log_temporarycostnumbervalue_35 = rail.PythonOperator(
            task_id='log_temporarycostnumbervalue_35',
            python_callable=lambda: rail.result('foreach_query_list_31_33')['activitycode'] if rail.result('foreach_query_list_31_33')['activitycode'] else (rail.result('foreach_query_list_31_33')['homedepartmentcode'] if rail.result('foreach_query_list_31_33')[
                'homedepartmentcode'] else "") + (rail.result('foreach_query_list_31_33')['projectcode'] if rail.result('foreach_query_list_31_33')['projectcode'] else "") + (rail.result('foreach_query_list_31_33')['taskcode'] if rail.result('foreach_query_list_31_33')['taskcode'] else "") + "L"
        )

        if_foreach_query_list_31_33_reg_present_36 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_reg_present_36',
            test='''{{ result('foreach_query_list_31_33').reg | is_truthy }}''',
            yes_task="add_entry_to_list1",
            no_task="if_foreach_query_list_31_33_ot_present_38",
        )

        add_entry_to_list1 = rail.WriteLogOperator(
            task_id='add_entry_to_list1',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "{{ result('log_temporarycostnumbervalue_35') }}",
                "reghours": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_ot_present_38 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_ot_present_38',
            test='''{{ result('foreach_query_list_31_33').ot | is_truthy }}''',
            yes_task="add_entry_to_list2",
            no_task="if_foreach_query_list_31_33_pto_present_40",
        )

        add_entry_to_list2 = rail.WriteLogOperator(
            task_id='add_entry_to_list2',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "{{ result('log_temporarycostnumbervalue_35') }}",
                "reghours": "",
                "othours": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_present_40 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_present_40',
            test='''{{ result('foreach_query_list_31_33').pto | is_truthy }}''',
            yes_task="if_foreach_query_list_31_33_pto_equals_to_vaca_41",
            no_task="foreach_query_list_31_33_end",
        )

        if_foreach_query_list_31_33_pto_equals_to_vaca_41 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_vaca_41',
            test='''{{ result('foreach_query_list_31_33').pto == 'VACA' }}''',
            yes_task="add_entry_to_list3",
            no_task="if_foreach_query_list_31_33_pto_equals_to_sick_43",
        )

        add_entry_to_list3 = rail.WriteLogOperator(
            task_id='add_entry_to_list3',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "V",
                "houramount_VACA": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_sick_43 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_sick_43',
            test='''{{ result('foreach_query_list_31_33').pto == 'SICK' }}''',
            yes_task="add_entry_to_list4",
            no_task="if_foreach_query_list_31_33_pto_equals_to_soh_45",
        )

        add_entry_to_list4 = rail.WriteLogOperator(
            task_id='add_entry_to_list4',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "S",
                "houramount_SICK": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_soh_45 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_soh_45',
            test='''{{ result('foreach_query_list_31_33').pto == 'SOH' }}''',
            yes_task="add_entry_to_list5",
            no_task="if_foreach_query_list_31_33_pto_equals_to_pers_47",
        )

        add_entry_to_list5 = rail.WriteLogOperator(
            task_id='add_entry_to_list5',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "SOH",
                "houramount_SOH": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_pers_47 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_pers_47',
            test='''{{ result('foreach_query_list_31_33').pto == 'PERS' }}''',
            yes_task="add_entry_to_list6",
            no_task="if_foreach_query_list_31_33_pto_equals_to_jury_49",
        )

        add_entry_to_list6 = rail.WriteLogOperator(
            task_id='add_entry_to_list6',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "M",
                "houramount_PERS": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_jury_49 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_jury_49',
            test='''{{ result('foreach_query_list_31_33').pto == 'JURY' }}''',
            yes_task="add_entry_to_list7",
            no_task="if_foreach_query_list_31_33_pto_equals_to_hol_51",
        )

        add_entry_to_list7 = rail.WriteLogOperator(
            task_id='add_entry_to_list7',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "J",
                "houramount_JURY": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_hol_51 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_hol_51',
            test='''{{ result('foreach_query_list_31_33').pto == 'HOL' }}''',
            yes_task="add_entry_to_list8",
            no_task="if_foreach_query_list_31_33_pto_equals_to_ffpsle_53",
        )

        add_entry_to_list8 = rail.WriteLogOperator(
            task_id='add_entry_to_list8',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "H",
                "houramount_HOL": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_ffpsle_53 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_ffpsle_53',
            test='''{{ result('foreach_query_list_31_33').pto == 'FFPSLE' }}''',
            yes_task="add_entry_to_list9",
            no_task="if_foreach_query_list_31_33_pto_equals_to_berev_55",
        )

        add_entry_to_list9 = rail.WriteLogOperator(
            task_id='add_entry_to_list9',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "1FE",
                "houramount_FFPSLE": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_berev_55 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_berev_55',
            test='''{{ result('foreach_query_list_31_33').pto == 'BEREV' }}''',
            yes_task="add_entry_to_list10",
            no_task="if_foreach_query_list_31_33_pto_equals_to_anv_57",
        )

        add_entry_to_list10 = rail.WriteLogOperator(
            task_id='add_entry_to_list10',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "Y",
                "houramount_BEREV": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_anv_57 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_anv_57',
            test='''{{ result('foreach_query_list_31_33').pto == 'ANV' }}''',
            yes_task="add_entry_to_list11",
            no_task="if_foreach_query_list_31_33_pto_equals_to_volntr_59",
        )

        add_entry_to_list11 = rail.WriteLogOperator(
            task_id='add_entry_to_list11',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "ANV",
                "houramount_ANV": "{{ result('foreach_query_list_31_33').totalhrs }}",
                "hourcode_VOL": "",
                "houramount_VOL": ""
            }
        )

        if_foreach_query_list_31_33_pto_equals_to_volntr_59 = rail.IfOperator(
            task_id='if_foreach_query_list_31_33_pto_equals_to_volntr_59',
            test='''{{ result('foreach_query_list_31_33').pto == 'VOLNTR' }}''',
            yes_task="add_entry_to_list12",
            no_task="foreach_query_list_31_33_end",
        )

        add_entry_to_list12 = rail.WriteLogOperator(
            task_id='add_entry_to_list12',
            log="{{result('adp_export_log')}}",
            message="na",
            severity="Success",
            properties={
                "cocode": "{{ result('foreach_query_list_31_33').adppayrollcompanycode }}",
                "batchid": "{{ result('log_batch_i_dvalue_34') }}",
                "fileno": "{{ result('foreach_query_list_31_33').filenumberadppaystatements }}",
                "tempcostnumber": "",
                "reghours": "",
                "othours": "",
                "hourcode_VACA": "",
                "houramount_VACA": "",
                "hourcode_SICK": "",
                "houramount_SICK": "",
                "hourcode_SOH": "",
                "houramount_SOH": "",
                "hourcode_PERS": "",
                "houramount_PERS": "",
                "hourcode_JURY": "",
                "houramount_JURY": "",
                "hourcode_HOL": "",
                "houramount_HOL": "",
                "hourcode_FFPSLE": "",
                "houramount_FFPSLE": "",
                "hourcode_BEREV": "",
                "houramount_BEREV": "",
                "hourcode_ANV": "",
                "houramount_ANV": "",
                "hourcode_VOL": "VOL",
                "houramount_VOL": "{{ result('foreach_query_list_31_33').totalhrs }}"
            }
        )

        foreach_query_list_31_33_end = rail.EmptyOperator(
            task_id='foreach_query_list_31_33_end',
        )

        search_entries_adp_log_table = rail.FilterLogEntriesOperator(
            task_id='search_entries_adp_log_table',
            log="{{result('adp_export_log')}}",
            severity='Success'
        )

        log_filenamebasedon_a_d_pcode_63 = rail.PythonOperator(
            task_id='log_filenamebasedon_a_d_pcode_63',
            python_callable=lambda dag_run: {"H44": "PRH44EPI", "H46": "PRH46EPI"}[
                dag_run.conf['adp_payroll_company_code']]
        )

        create_csv_lines_64 = rail.WriteCSVFileOperator(
            task_id='create_csv_lines_64',
            source="{{ result('search_entries_adp_log_table')}}",
            header=['Co Code',
                    'Batch ID',
                    'File #',
                    'Temp Cost Number',
                    'Reg Hours',
                    'O/T Hours',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount',
                    'Hours 3 Code',
                    'Hours 3 Amount'],
            row=lambda item: [
                '""' if not item['properties']['cocode'] else item['properties']['cocode'],
                '""' if not item['properties']['batchid'] else item['properties']['batchid'],
                '""' if not item['properties']['fileno'] else item['properties']['fileno'],
                '""' if not item['properties']['tempcostnumber'] else item['properties']['tempcostnumber'],

                '""' if not item['properties']['reghours'] else round(
                    (to_float(item['properties']['reghours'])), 1),

                '""' if not item['properties']['othours'] else round(
                    (to_float(item['properties']['othours'])), 1),

                '""' if not item['properties']['hourcode_VACA'] else item['properties']['hourcode_VACA'],

                '""' if not item['properties']['houramount_VACA'] else round(
                    (to_float(item['properties']['houramount_VACA'])), 1),

                '""' if not item['properties']['hourcode_SICK'] else item['properties']['hourcode_SICK'],

                '""' if not item['properties']['houramount_SICK'] else round(
                    (to_float(item['properties']['houramount_SICK'])), 1),

                '""' if not item['properties']['hourcode_SOH'] else item['properties']['hourcode_SOH'],

                '""' if not item['properties']['houramount_SOH'] else round(
                    (to_float(item['properties']['houramount_SOH'])), 1),

                '""' if not item['properties']['hourcode_PERS'] else item['properties']['hourcode_PERS'],

                '""' if not item['properties']['houramount_PERS'] else round(
                    (to_float(item['properties']['houramount_PERS'])), 1),

                '""' if not item['properties']['hourcode_JURY'] else item['properties']['hourcode_JURY'],

                '""' if not item['properties']['houramount_JURY'] else round(
                    (to_float(item['properties']['houramount_JURY'])), 1),

                '""' if not item['properties']['hourcode_HOL'] else item['properties']['hourcode_HOL'],

                '""' if not item['properties']['houramount_HOL'] else round(
                    (to_float(item['properties']['houramount_HOL'])), 1),

                '""' if not item['properties']['hourcode_FFPSLE'] else item['properties']['hourcode_FFPSLE'],

                '""' if not item['properties']['houramount_FFPSLE'] else round(
                    (to_float(item['properties']['houramount_FFPSLE'])), 1),

                '""' if not item['properties']['hourcode_BEREV'] else item['properties']['hourcode_BEREV'],

                '""' if not item['properties']['houramount_BEREV'] else round(
                    (to_float(item['properties']['houramount_BEREV'])), 1),

                '""' if not item['properties']['hourcode_ANV'] else item['properties']['hourcode_ANV'],

                '""' if not item['properties']['houramount_ANV'] else round(
                    (to_float(item['properties']['houramount_ANV'])), 1),

                '""' if not item['properties']['hourcode_VOL'] else item['properties']['hourcode_VOL'],

                '""' if not item['properties']['houramount_VOL'] else round(
                    (to_float(item['properties']['houramount_VOL'])), 1),
            ],
        )

        cefloyd_file_csv_data_update = rail.PythonOperator(
            task_id="cefloyd_file_csv_data_update",
            python_callable=lambda: rail.write_artifact(rail.read_artifact(
                rail.result("create_csv_lines_64")).replace('""""""', '""'))
        )

        get_log_filename = rail.PythonOperator(
            task_id='get_log_filename',
            python_callable=lambda: rail.result(
                "log_filenamebasedon_a_d_pcode_63") + "_" + datetime.today().strftime("%Y%m%dT%H%M%S") + ".csv"
        )

        generate_downloadlink = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_downloadlink',
            artifact_name="{{ result('cefloyd_file_csv_data_update')}}",
            output_file_name="{{ result('get_log_filename') }}",
            expires_in_seconds=7*24*60*60,
        )

        send_success_mail = rail.EmailOperator(
            task_id='send_success_mail',
            to="{{dag_run.conf.email}}",
            bcc=config.internal_logs_email,
            subject='''{{get_company_key()}} | ADP payroll data from Replicon - Completed - {{ current_time() }} ''',
            html_content="templates/emails/success_mail.html"
        )

        catch_error = rail.EmptyOperator(
            task_id='catch_error',
            trigger_rule='one_failed'
        )

        send_failure_mail = rail.EmailOperator(
            task_id='send_failure_mail',
            trigger_rule='all_success',
            to="{{dag_run.conf.email}}",
            bcc=config.alert_email,
            subject='''{{get_company_key()}} | ADP payroll data from Replicon - Failed - {{ current_time() }} ''',
            html_content="templates/emails/failure_mail.html"
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> if_startdate_to_date_greater_than_enddate
        if_startdate_to_date_greater_than_enddate >> rail.Label(
            'Yes') >> log_startdateformatted_4 >> log_enddateformatted_5 >> send_incorrect_dateformat_mail >> catch_error

        if_startdate_to_date_greater_than_enddate >> rail.Label(
            'No') >> get_all_reports_10
        get_all_reports_10 >> get_report_details2_12 >> log_entry_date_filterfilteruri_15
        log_entry_date_filterfilteruri_15 >> log_u_d_f_filter_user
        log_u_d_f_filter_user >> log_startdateformatted >> log_enddateformatted >> generate_report_group
        generate_report_group >> if_payload_has_data
        if_payload_has_data >> rail.Label('Yes') >> if_payload_has_no_columns
        if_payload_has_data >> rail.Label(
            'No') >> send_nodata_mail >> catch_error
        if_payload_has_no_columns >> rail.Label(
            'Yes') >> stop_job_with_error >> catch_error
        if_payload_has_no_columns >> rail.Label(
            'No') >> load_csv_create_list_from_csv_30
        load_csv_create_list_from_csv_30 >> create_collection_create_list_from_csv_30
        create_collection_create_list_from_csv_30 >> query_list_31 >> adp_export_log
        adp_export_log >> foreach_query_list_31_33 >> log_batch_i_dvalue_34 >> log_temporarycostnumbervalue_35
        log_temporarycostnumbervalue_35 >> if_foreach_query_list_31_33_reg_present_36
        if_foreach_query_list_31_33_reg_present_36 >> rail.Label(
            'Yes') >> add_entry_to_list1 >> if_foreach_query_list_31_33_ot_present_38
        if_foreach_query_list_31_33_reg_present_36 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_ot_present_38
        if_foreach_query_list_31_33_ot_present_38 >> rail.Label(
            'Yes') >> add_entry_to_list2 >> if_foreach_query_list_31_33_pto_present_40
        if_foreach_query_list_31_33_ot_present_38 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_present_40
        if_foreach_query_list_31_33_pto_present_40 >> rail.Label(
            'Yes') >> if_foreach_query_list_31_33_pto_equals_to_vaca_41
        if_foreach_query_list_31_33_pto_equals_to_vaca_41 >> rail.Label(
            'Yes') >> add_entry_to_list3 >> if_foreach_query_list_31_33_pto_equals_to_sick_43
        if_foreach_query_list_31_33_pto_equals_to_vaca_41 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_sick_43
        if_foreach_query_list_31_33_pto_equals_to_sick_43 >> rail.Label(
            'Yes') >> add_entry_to_list4 >> if_foreach_query_list_31_33_pto_equals_to_soh_45
        if_foreach_query_list_31_33_pto_equals_to_sick_43 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_soh_45
        if_foreach_query_list_31_33_pto_equals_to_soh_45 >> rail.Label(
            'Yes') >> add_entry_to_list5 >> if_foreach_query_list_31_33_pto_equals_to_pers_47
        if_foreach_query_list_31_33_pto_equals_to_soh_45 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_pers_47
        if_foreach_query_list_31_33_pto_equals_to_pers_47 >> rail.Label(
            'Yes') >> add_entry_to_list6 >> if_foreach_query_list_31_33_pto_equals_to_jury_49
        if_foreach_query_list_31_33_pto_equals_to_pers_47 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_jury_49
        if_foreach_query_list_31_33_pto_equals_to_jury_49 >> rail.Label(
            'Yes') >> add_entry_to_list7 >> if_foreach_query_list_31_33_pto_equals_to_hol_51
        if_foreach_query_list_31_33_pto_equals_to_jury_49 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_hol_51
        if_foreach_query_list_31_33_pto_equals_to_hol_51 >> rail.Label(
            'Yes') >> add_entry_to_list8 >> if_foreach_query_list_31_33_pto_equals_to_ffpsle_53
        if_foreach_query_list_31_33_pto_equals_to_hol_51 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_ffpsle_53
        if_foreach_query_list_31_33_pto_equals_to_ffpsle_53 >> rail.Label(
            'Yes') >> add_entry_to_list9 >> if_foreach_query_list_31_33_pto_equals_to_berev_55
        if_foreach_query_list_31_33_pto_equals_to_ffpsle_53 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_berev_55
        if_foreach_query_list_31_33_pto_equals_to_berev_55 >> rail.Label(
            'Yes') >> add_entry_to_list10 >> if_foreach_query_list_31_33_pto_equals_to_anv_57
        if_foreach_query_list_31_33_pto_equals_to_berev_55 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_anv_57
        if_foreach_query_list_31_33_pto_equals_to_anv_57 >> rail.Label(
            'Yes') >> add_entry_to_list11 >> if_foreach_query_list_31_33_pto_equals_to_volntr_59
        if_foreach_query_list_31_33_pto_equals_to_anv_57 >> rail.Label(
            'No') >> if_foreach_query_list_31_33_pto_equals_to_volntr_59
        if_foreach_query_list_31_33_pto_equals_to_volntr_59 >> rail.Label(
            'Yes') >> add_entry_to_list12 >> foreach_query_list_31_33_end
        if_foreach_query_list_31_33_pto_equals_to_volntr_59 >> rail.Label(
            'No') >> foreach_query_list_31_33_end
        if_foreach_query_list_31_33_pto_present_40 >> rail.Label(
            'No') >> foreach_query_list_31_33_end
        foreach_query_list_31_33 >> foreach_query_list_31_33_end >> search_entries_adp_log_table
        search_entries_adp_log_table >> log_filenamebasedon_a_d_pcode_63 >> create_csv_lines_64
        create_csv_lines_64 >> cefloyd_file_csv_data_update >> get_log_filename >> generate_downloadlink
        generate_downloadlink >> send_success_mail >> catch_error
        catch_error >> send_failure_mail

    return dag


rail.for_each_instance(create_dag)
