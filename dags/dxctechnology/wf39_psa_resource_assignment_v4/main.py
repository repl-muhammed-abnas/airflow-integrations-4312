from datetime import timedelta
import itertools
import rail
from airflow.exceptions import AirflowException
from airflow.models import Variable

from dxctechnology.wf39_psa_resource_assignment_v4.utils import request_payload
from dxctechnology.wf39_psa_resource_assignment_v4.utils import response_filter
from dxctechnology.wf39_psa_resource_assignment_v4.utils import python_callable_method

CONCAT_STRING_DELIMITER = "*^*^*"

# pylint: disable=too-many-statements
def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'DXC_WF39 PSA Resource Assignment Master v4 - SFTP {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=timedelta(seconds=config.master_dag_interval),
        max_active_runs=config.master_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        }
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=config.file_sensor_timeout)
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('new_file_sensor') }}"
        )

        can_decrypt_file = rail.IfOperator(
            task_id ="can_decrypt_file",
            test=lambda: Variable.get(config.can_decrypt_file_var_name, default_var='true').lower() == 'true',
            yes_task='decrypt_file',
            no_task='load_input_data'
        )

        decrypt_file = rail.PGPDecryptionOperator(
            task_id='decrypt_file',
            source='{{ result("download_file") }}',
            pgp_conn_id=config.pgp_conn_id
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            trigger_rule='all_done',
            existing_filename='{{ result("new_file_sensor") }}',
            new_filename=config.archive_filepath +
            "/{{ dag_run_ecid() | replace(':', '-')}}_{{ result('new_file_sensor') | file_name }}"
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')
        
        load_input_data = rail.PythonOperator(
            task_id= "load_input_data",
            python_callable= lambda: rail.result('decrypt_file') if Variable.get(
                config.can_decrypt_file_var_name, default_var='true').lower()== 'true' else  rail.result('download_file'),
            show_return_value_in_logs= False
        )

        load_labour_types_data = rail.LoadCSVFileOperator(
            task_id='load_labour_types_data',
            document="{{ result('load_input_data') }}",
            delimiter='|'
        )

        create_master_log = rail.CreateLogOperator(
            task_id='create_master_log'
        )

        create_labour_type_data_collection = rail.CreateCollectionOperator(
            task_id='create_labour_type_data_collection',
            source="{{ result('load_labour_types_data') }}",
            name="labourtypedata",
            columns={
                'PERN': 'employeeid',
                'WBS': 'wbs',
                'StartDate': 'startdate',
                'EndDate': 'enddate',
                'Role': 'role'
            }
        )

        has_labour_type_data = rail.IfOperator(
            task_id='has_labour_type_data',
            test="{{ result('create_labour_type_data_collection','length') > 0 }}",
            yes_task='get_all_billing_rates',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='{{ get_company_key() }} | Replicon WF39 Resource assignment sync - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/blank_payload.html"
        )

        get_all_billing_rates = rail.RepliconServiceOperator(
            task_id="get_all_billing_rates",
            endpoint="/services/BillingRateService1.svc/GetAllBillingRates",
            response_filter=response_filter.map_billing_rates
        )

        existing_billing_rates_in_replicon = rail.CreateCollectionOperator(
            task_id="existing_billing_rates_in_replicon",
            source="{{ result('get_all_billing_rates') | to_json }}",
            name="existingbillingrates"
        )

        query_labour_type_data = rail.QueryCollectionOperator(
            task_id='query_labour_type_data',
            name="labourtypesdata",
            query="""SELECT DISTINCT wbs, role, startdate, enddate, employeeid
                    FROM labourtypedata
                    WHERE NULLIF(wbs, '') IS NOT NULL AND NULLIF(employeeid, '') IS NOT NULL"""
        )

        has_labour_types = rail.IfOperator(
            task_id="has_labour_types",
            test="{{ result('query_labour_type_data','length') > 0 }}",
            yes_task="input_combined_list"
        )

        input_combined_list = rail.PythonOperator(
            task_id="input_combined_list",
            python_callable=python_callable_method.get_input_combined_list,
            op_args=['query_labour_type_data']
        )

        input_combined_data_collection = rail.CreateCollectionOperator(
            task_id="input_combined_data_collection",
            source="{{ result('input_combined_list') | to_json }}",
            name="inputcombineddata"
        )

        query_invalid_labour_type_data = rail.QueryCollectionOperator(
            task_id="query_invalid_labour_type_data",
            query="""SELECT * FROM labourtypedata WHERE NULLIF(employeeid, '') IS NULL or  NULLIF(wbs, '') IS NULL""",
            name="invalid_labordata"
        )

        has_any_invalid_labour_type_data = rail.IfOperator(
            task_id = "has_any_invalid_labour_type_data",
            test= "{{ result('query_invalid_labour_type_data','length') > 0 }}",
            yes_task= "log_invalid_records_for_wbs",
            no_task= "format_log_records"
        )

        def logMessage(item):
            message = []
            if not bool(item['employeeid']):
                message.append("Personnel number not present for the record")
            if not bool(item['wbs']):
                message.append("WBS element not present for the record")
            return ', '.join(message)

        log_invalid_records_for_wbs = rail.WriteLogOperator(
            task_id='log_invalid_records_for_wbs',
            items="{{result('query_invalid_labour_type_data')}}",
            log='{{ result("create_master_log") }}',
            message=lambda item: logMessage(item),
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': item['wbs'],
                'role': item['role'],
                'billingtype': "",
                'status': 'Exception',
                'action': 'Validation'
            }
        )

        distinct_labour_types = rail.QueryCollectionOperator(
            task_id="distinct_labour_types",
            name="feedlabourtypes",
            query="""SELECT DISTINCT role FROM inputcombineddata WHERE NULLIF(role, '') IS NOT NULL"""
        )

        labour_types_to_be_skipped_in_replicon = rail.QueryCollectionOperator(
            task_id="labour_types_to_be_skipped_in_replicon",
            name="skippedlabourtypes",
            query="""SELECT * FROM feedlabourtypes WHERE LOWER(role) NOT IN (SELECT DISTINCT LOWER(name) FROM existingbillingrates)"""
        )

        has_labour_types_to_be_skipped = rail.IfOperator(
            task_id='has_labour_types_to_be_skipped',
            test='{{ result("labour_types_to_be_skipped_in_replicon", "length") > 0 }}',
            yes_task="get_all_records_for_role",
            no_task="query_distinct_projects"
        )

        get_all_records_for_role = rail.QueryCollectionOperator(
            task_id="get_all_records_for_role",
            query="""SELECT * FROM labourtypesdata WHERE LOWER(role) IN (SELECT DISTINCT LOWER(role) FROM skippedlabourtypes)"""
        )

        log_labour_types_skipped = rail.WriteLogOperator(
            task_id="log_labour_types_skipped",
            log='{{ result("create_master_log") }}',
            message='Labour Type Not Available in Replicon',
            items='{{ result("get_all_records_for_role") }}',
            severity='Skipped',
            properties={
                'wbs': '{{ item.wbs }}',
                'role': '{{ item.role }}',
                'status': 'Skipped',
                'action': 'Validation',
                'employeeid': '{{ item.employeeid }}'
            }
        )

        query_distinct_projects = rail.QueryCollectionOperator(
            task_id="query_distinct_projects",
            # pylint: disable=line-too-long
            query="""SELECT DISTINCT wbs FROM labourtypesdata WHERE COALESCE(role,'') NOT IN (SELECT DISTINCT role FROM skippedlabourtypes)"""
        )

        has_final_query_data = rail.IfOperator(
            task_id='has_final_query_data',
            test='{{ result("query_distinct_projects", "length") > 0 }}',
            yes_task='get_all_filter_definitions',
            no_task='format_log_records'
        )

        get_all_filter_definitions = rail.RepliconServiceOperator(
            task_id="get_all_filter_definitions",
            endpoint="/services/ProjectListService1.svc/GetAllFilterDefinitions",
        )

        get_all_columns = rail.RepliconServiceOperator(
            task_id="get_all_columns",
            endpoint="/services/ProjectListService1.svc/GetAllColumns",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(
                response.json()['d'][0]['columns'], 'displayText', 'Parent WBS', 'uri')
        )

        get_c1_leanstaffing_import_base_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_c1_leanstaffing_import_base_report_details',
            report_name=config.extract_report_name,
        )

        report_group_entry, report_group_exit = rail.run_report(
            group_id='c1_leanstaffing_import_base_report',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{ result('get_c1_leanstaffing_import_base_report_details').uri }}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        report_has_data = rail.IfOperator(
            task_id="report_has_data",
            test="{{ result('c1_leanstaffing_import_base_report.get_report_result', 'has_data') }}",
            yes_task='load_report_data',
            no_task='fail_no_report_data',
        )

        fail_no_report_data = rail.FailOperator(
            task_id="fail_no_report_data",
            message="Report \"**C1 Lean staffing Import base report\" execution failed",
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id='load_report_data',
            document="{{ result('c1_leanstaffing_import_base_report.get_report_result').reportGenerationResults[0].payload }}",
        )

        report_data_collection= rail.CreateCollectionOperator(
            task_id="report_data_collection",
            source= "{{result('load_report_data')}}",
            name = 'report_data'
        )

        query_min_start_max_end_assignment_dates = rail.QueryCollectionOperator(
            task_id = "query_min_start_max_end_assignment_dates",
            query=f"""SELECT
                        unique_wbs_user_data.*, --WBS, Employee ID
                        (
                            SELECT MAX(l1.enddate) FROM labourtypesdata l1
                            WHERE l1.wbs = unique_wbs_user_data.wbs
                                AND l1.employeeid = unique_wbs_user_data.employeeid
                        ) as max_end_date, -- Gives the maximum date from the end_date field for the wbs-user records (includes skipped roles, matching v3 behaviour)
                        (
                            SELECT MIN(l1.startdate) FROM labourtypesdata l1
                            WHERE l1.wbs = unique_wbs_user_data.wbs
                                AND l1.employeeid = unique_wbs_user_data.employeeid
                        ) as min_start_date, -- Gives the minimum date from the start_date field for the wbs-user records (includes skipped roles, matching v3 behaviour)
                        (
                            SELECT GROUP_CONCAT(unique_roles."role", '{CONCAT_STRING_DELIMITER}') FROM (
                                SELECT DISTINCT COALESCE(l1."role", '') as "role" FROM labourtypesdata l1
                                WHERE l1.wbs = unique_wbs_user_data.wbs AND l1.employeeid = unique_wbs_user_data.employeeid
                                AND COALESCE(l1.role,'') NOT IN (SELECT DISTINCT role FROM skippedlabourtypes)
                                ) unique_roles -- Gives the unique non-skipped roles for the wbs-user records
                        ) as roles -- returns ['a', '', 'b'] as a{CONCAT_STRING_DELIMITER}{CONCAT_STRING_DELIMITER}b
                    FROM (
                        SELECT DISTINCT l.wbs, l.employeeid FROM labourtypesdata l
                        WHERE COALESCE(l.role,'') NOT IN (SELECT DISTINCT role FROM skippedlabourtypes)
                    ) unique_wbs_user_data
                """,
            name="filtered_feed_data"
        )

        feed_report_data_combined = rail.QueryCollectionOperator(
            task_id = "feed_report_data_combined",
            query="""SELECT l.*,
                        CASE
                            WHEN (SELECT rd.UserUri FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1) IS NOT NULL
                                THEN (SELECT rd.UserUri FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1)
                            ELSE
                                CASE 
                                    WHEN (SELECT rd.UserUri FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                        THEN (SELECT rd.UserUri FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1)
                                    ELSE
                                        CASE
                                            WHEN (SELECT rd.UserUri FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                                THEN (SELECT rd.UserUri FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1)
                                            ELSE
                                                NULL
                                        END
                                END
                        END as user_uri,
                        CASE
                            WHEN (SELECT  rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1) IS NOT NULL
                                THEN (SELECT rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1)
                            ELSE
                                CASE 
                                    WHEN (SELECT  rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                        THEN (SELECT  rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1)
                                    ELSE
                                        CASE
                                            WHEN (SELECT  rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                                THEN (SELECT  rd."Company_Code__Current___Full_Path_" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1)
                                            ELSE
                                                NULL
                                        END
                                END
                        END as user_company_code,
                        CASE
                            WHEN (SELECT  rd."User_Status" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1) IS NOT NULL
                                THEN (SELECT rd."User_Status" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1)
                            ELSE
                                CASE 
                                    WHEN (SELECT  rd."User_Status" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                        THEN (SELECT  rd."User_Status" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1)
                                    ELSE
                                        CASE
                                            WHEN (SELECT  rd."User_Status" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                                THEN (SELECT  rd."User_Status" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1)
                                            ELSE
                                                NULL
                                        END
                                END
                        END as user_status,
                        CASE
                            WHEN (SELECT  rd."User_Start_Date" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1) IS NOT NULL
                                THEN (SELECT rd."User_Start_Date" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1)
                            ELSE
                                CASE
                                    WHEN (SELECT  rd."User_Start_Date" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                        THEN (SELECT  rd."User_Start_Date" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1)
                                    ELSE
                                        CASE
                                            WHEN (SELECT  rd."User_Start_Date" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                                THEN (SELECT  rd."User_Start_Date" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1)
                                            ELSE
                                                NULL
                                        END
                                END
                        END as user_start_date,
                        CASE
                            WHEN (SELECT  rd."User_End_Date" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1) IS NOT NULL
                                THEN (SELECT rd."User_End_Date" FROM report_data rd WHERE rd.Employeeid == l.employeeid LIMIT 1)
                            ELSE
                                CASE
                                    WHEN (SELECT  rd."User_End_Date" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                        THEN (SELECT  rd."User_End_Date" FROM report_data rd WHERE rd."IA_Perner_ID" == l.employeeid LIMIT 1)
                                    ELSE
                                        CASE
                                            WHEN (SELECT  rd."User_End_Date" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1) IS NOT NULL
                                                THEN (SELECT  rd."User_End_Date" FROM report_data rd WHERE rd."CWF_C1_alternate_ID" == l.employeeid LIMIT 1)
                                            ELSE
                                                NULL
                                        END
                                END
                        END as user_end_date
                        FROM filtered_feed_data l""",
                name= "raw_feed_report_data"
            )
        
        query_valid_user_data = rail.QueryCollectionOperator(
            task_id= "query_valid_user_data",
            query="""SELECT * FROM raw_feed_report_data WHERE NULLIF(user_uri,'') IS NOT NULL and 
                    user_status == 'Enabled' and user_company_code LIKE 'C1%' """,
            name = "valid_feed_file_records"
        )

        has_any_valid_records = rail.IfOperator(
            task_id = "has_any_valid_records",
            test= "{{ result('query_valid_user_data','length') > 0 }}",
            yes_task= "process_each_wbs_item",
            no_task= "format_log_records"
        )

        query_invalid_user_data = rail.QueryCollectionOperator(
            task_id= "query_invalid_user_data",
            query="""SELECT * FROM raw_feed_report_data WHERE NULLIF(user_uri,'') IS NULL or 
                    user_status != 'Enabled' or user_company_code NOT LIKE 'C1%' """,
            name = "invalid_feed_file_user_records"
        )

        has_invalid_user_data = rail.IfOperator(
            task_id = "has_invalid_user_data",
            test= "{{ result('query_invalid_user_data','length') > 0 }}",
            yes_task= "log_invalid_user_records",
            no_task= "format_log_records"
        )

        def logMessage_user(item):
            if not item['user_uri']:
                return f"Required user {item['employeeid']} is not available in Replicon"
            if item['user_status'] != 'Enabled':
                return f"Required user {item['employeeid']} is disabled in Replicon"
            if not str(item['user_company_code']).startswith("C1"):
                return f"Required user {item['employeeid']} is not C1 User"
            raise AirflowException('Record went for invalid even though all the mandatory field are present')

        log_invalid_user_records = rail.WriteLogOperator(
            task_id='log_invalid_user_records',
            items=lambda: python_callable_method.expand_per_role(
                list(rail.load_all_records(rail.result('query_invalid_user_data')))
            ),
            log='{{ result("create_master_log") }}',
            message=lambda item: logMessage_user(item),
            properties=lambda item: {
                'employeeid': item['employeeid'],
                'wbs': item['wbs'],
                'role': item['roles'],
                'status': 'Exception',
                'action': 'Validation'
            }
        )

        process_each_wbs_item =rail.EmptyOperator(
            task_id = "process_each_wbs_item"
        )

        process_billing_rate_for_each_wbs_item = rail.trigger_parallel_dagrun(
            task_id='process_billing_rate_for_each_wbs_item',
            items="{{ result('query_distinct_projects') }}",
            parallel_count= config.parallel_count,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.child_dagid,
            conf=request_payload.get_process_billing_rate_wbs_conf
        )

        get_process_each_wbs_dag_ids =rail.PythonOperator(
            task_id= 'get_process_each_wbs_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_billing_rate_for_each_wbs_item_{x+1}'), range(config.parallel_count))))),
            show_return_value_in_logs= False
        )

        gather_process_billing_rates_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_process_billing_rates_logs',
            dag_runs='{{ result("get_process_each_wbs_dag_ids") }}',
            dagrun_task_id='create_log',
            execution_timeout=timedelta(
                hours=config.gather_logs_timeout_hours),
            flatten=True
        )

        format_log_records = rail.CreateCollectionOperator(
            task_id='format_log_records',
            source=python_callable_method.do_format_logs,
            columns=[
                'employeeid',
                'wbs',
                'role',
                'action',
                'status',
                'message',
                'ecid'],
            name='final_log_records'
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_log_records'),
            header=[
                'PERN__C',
                'WBS__C',
                'Labor_Type__C',
                'Transaction_Type__C',
                'Status__C',
                'Status_Description__C',
                'Replicon_Job__ID'],
            row=[
                '{{ item.employeeid }}',
                '{{ item.wbs }}',
                '{{ item | attr_or_default("role", "") }}',
                '{{item.action}}',
                '{{ item.status }}',
                '{{ item.message }}',
                '{{ item.ecid }}'],
        )

        upload_log_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_log_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.log_filepath +
            '/log_{{ dag_run_ecid() | replace(":", "-") }}_{{ result("new_file_sensor") | file_base }}.csv')

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('format_log_records', key='get_errored_billing_rates')== 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Replicon C1-PSA Resource Assignment for WF39 - " }} \
                {%- if result("format_log_records", key="get_errored_billing_rates") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_log_records", key="get_exception_billing_rates") > 0 -%} \
                        completed with exceptions  \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email/import_complete.html",
            params={
                'log_filepath': config.log_filepath,
            }
        )

        new_file_sensor >> download_file >> can_decrypt_file

        can_decrypt_file >> rail.Label('No') >>  load_input_data
        can_decrypt_file >> rail.Label('Yes') >> decrypt_file >> load_input_data

        load_input_data >> load_labour_types_data >> \
            create_master_log >> create_labour_type_data_collection >> has_labour_type_data

        has_labour_type_data >> rail.Label(
            "No") >> send_blank_payload_email
        has_labour_type_data >> rail.Label(
            "Yes") >> get_all_billing_rates

        get_all_billing_rates >> existing_billing_rates_in_replicon >> query_labour_type_data >> has_labour_types

        has_labour_types >> rail.Label(
            "Yes") >> input_combined_list >> input_combined_data_collection >> distinct_labour_types >> [query_invalid_labour_type_data, ]
        
        query_invalid_labour_type_data >> has_any_invalid_labour_type_data
        has_any_invalid_labour_type_data >> rail.Label("Yes") >> log_invalid_records_for_wbs >> format_log_records
        has_any_invalid_labour_type_data >> rail.Label("No") >> format_log_records
        
        distinct_labour_types >> \
            labour_types_to_be_skipped_in_replicon >> has_labour_types_to_be_skipped

        has_labour_types_to_be_skipped >> rail.Label(
            "Yes") >> get_all_records_for_role >> log_labour_types_skipped >> query_distinct_projects

        has_labour_types_to_be_skipped >> rail.Label(
            "No") >> query_distinct_projects >> has_final_query_data

        has_final_query_data >> rail.Label(
            "Yes") >> get_all_filter_definitions

        has_final_query_data >> rail.Label(
            "No") >> format_log_records

        get_all_filter_definitions >> get_all_columns >> get_c1_leanstaffing_import_base_report_details >> report_group_entry
        report_group_exit >> report_has_data >> rail.Label(
            "YES") >> load_report_data
        report_has_data >> rail.Label('NO') >> fail_no_report_data
        load_report_data >> report_data_collection >> query_min_start_max_end_assignment_dates >> feed_report_data_combined >> [query_valid_user_data,query_invalid_user_data]
        query_valid_user_data >> has_any_valid_records
        has_any_valid_records >> rail.Label("Yes") >> process_each_wbs_item >> process_billing_rate_for_each_wbs_item >> get_process_each_wbs_dag_ids
        has_any_valid_records >> rail.Label("No") >> format_log_records
        
        query_invalid_user_data >> has_invalid_user_data >> rail.Label('Yes') >> log_invalid_user_records >> format_log_records
        has_invalid_user_data >> rail.Label('No') >> format_log_records

        
        get_process_each_wbs_dag_ids >> gather_process_billing_rates_logs >> format_log_records

        format_log_records >> render_logs_csv >> \
            upload_log_to_sftp >> send_import_complete_email

        download_file >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label(
                "Yes") >> archive_file
        was_new_file_found >> rail.Label(
            "No") >> delete_this_dagrun

    return dag


rail.for_each_instance(create_main_dag)
