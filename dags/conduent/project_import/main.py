from datetime import timedelta, datetime
from pendulum import datetime as dt
import rail
from conduent.project_import.utils import python_callable_methods

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dagid,
        description=f'Conduent Master Project Import {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2024, 1, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(minutes=10)
        )

        is_file_csv = rail.IfOperator(
            task_id='is_file_csv',
            test='{{ result("new_file_sensor") | file_ext | lower == "csv" }}',
            yes_task="download_input_csv",
            no_task="send_incorrect_file_format_mail",
        )

        send_incorrect_file_format_mail = rail.EmailOperator(
            task_id='send_incorrect_file_format_mail',
            to=config.tenant_email,
            cc=config.cc_email,
            subject='''{{ get_company_key() }} | Project Import - Incorrect file format received - {{ current_time() }} ''',
            html_content="templates/incorrect_file_format.html"
        )

        download_input_csv = rail.SFTPDownloadFileOperator(
            task_id='download_input_csv',
            remote_filepath="{{ result('new_file_sensor') }}",
        )

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test='{{ get_task_state("new_file_sensor") == "success" }}',
            yes_task='archive_file',
            no_task='delete_this_dagrun',
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        archive_file = rail.SFTPMoveFileOperator(
            task_id='archive_file',
            new_filename=config.archive_filepath +
            '''/{{ dag_run_ecid() }}_{{ result("new_file_sensor") | file_name }}''',
            existing_filename=config.input_filepath +
            '''/{{ result("new_file_sensor") | file_name }}''',
        )

        parse_csv = rail.LoadCSVFileOperator(
            task_id='parse_csv',
            document="{{ result('download_input_csv') }}"
        )

        create_collection_create_list_from_csv_raw_data = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_from_csv_raw_data',
            source="{{ result('parse_csv') }}",
            name="inputfile",
            columns={
                'Project ID': 'project_code',
                'Project Status': 'project_status',
                'Project Name': 'project_name',
                'Description': 'project_description',
                'Date Opened': 'start_date',
                'Date Closed': 'end_date',
                'Project Manager':	'project_manager_id',
                'Project Category': 'project_category',
                'Project Type': 'project_type',
                'Billable / Non-Billable': 'billable_non_billable',
                'CapSW Sub category': 'capsw_sub_category',
                'Attestation Y/N': 'attestation',
                'R&D': 'rnd',
                'Cost Center Name': 'cost_center_name',
                'Capex Number': 'capex_number',
                'Opportunity ID': 'opportunity_id',
                'Client Name': 'client_name',
                'Requested By': 'requested_by'
            }
        )

        if_csv_has_data = rail.IfOperator(
            task_id='if_csv_has_data',
            test="{{ result('create_collection_create_list_from_csv_raw_data','length') > 0 }}",
            yes_task='query_list_projects_missing_required_fields',
            no_task='send_blank_payload_email'
        )

        send_blank_payload_email = rail.EmailOperator(
            task_id='send_blank_payload_email',
            to=config.tenant_email,
            cc=config.cc_email,
            subject='{{ get_company_key() }} | Replicon project import - Blank File - {{ current_time("%Y-%m-%dT%H:%M:%S.%f%z") }}',
            html_content="templates/email_blank_payload.html"
        )

        query_list_projects_missing_required_fields = rail.QueryCollectionOperator(
            task_id='query_list_projects_missing_required_fields',
            query="""SELECT * FROM  inputfile WHERE NULLIF(project_code,'') IS NULL OR NULLIF(project_status,'') IS NULL OR
              NULLIF(project_name,'') IS NULL OR NULLIF(project_description,'') IS NULL OR NULLIF(start_date,'') IS NULL OR NULLIF(project_manager_id,'') IS NULL
                OR NULLIF(project_category,'') IS NULL OR NULLIF(project_type,'') IS NULL OR NULLIF(billable_non_billable,'') IS NULL
                  OR NULLIF(cost_center_name,'') IS NULL OR NULLIF(requested_by,'') IS NULL"""
        )

        create_project_import_logs_lookuptable = rail.CreateLogOperator(
            task_id='create_project_import_logs_lookuptable'
        )

        if_query_list_projects_missing_required_fields_has_data = rail.IfOperator(
            task_id='if_query_list_projects_missing_required_fields_has_data',
            test='''{{ result('query_list_projects_missing_required_fields','length') > 0 }}''',
            yes_task="logs_add_missing_required_fields",
            no_task="query_list_projects_valid_records",
        )

        logs_add_missing_required_fields = rail.WriteLogOperator(
            task_id='logs_add_missing_required_fields',
            log="{{ result('create_project_import_logs_lookuptable')}}",
            items="{{result('query_list_projects_missing_required_fields')}}",
            message='One or more mandatory field is missing.',
            severity='Info',
            properties=lambda item: {
                "project_name": item['project_name'],
                "project_code": item['project_code'],
                "project_type": item['project_type'],
                "action": "Validation",
                "status": "Skipped",
                "details": python_callable_methods.get_missing_field_message(item),
                "jobid": rail.render_template("{{dag_run_ecid()}}"),
                "childjobid": "",
            }
        )

        query_list_projects_valid_records = rail.QueryCollectionOperator(
            task_id='query_list_projects_valid_records',
            name="validatedinputlist",
            query="""SELECT * FROM  inputfile WHERE NULLIF(project_code,'') IS NOT NULL AND NULLIF(project_status,'') IS NOT NULL AND
              NULLIF(project_name,'') IS NOT NULL AND NULLIF(project_description,'') IS NOT NULL AND NULLIF(start_date,'') IS NOT NULL AND NULLIF(project_manager_id,'') IS NOT NULL
                AND NULLIF(project_category,'') IS NOT NULL AND NULLIF(project_type,'') IS NOT NULL AND NULLIF(billable_non_billable,'') IS NOT NULL
                  AND NULLIF(cost_center_name,'') IS NOT NULL AND NULLIF(requested_by,'') IS NOT NULL"""
        )

        get_projects_list_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_projects_list_report_details',
            report_name=config.project_list_report
        )

        generate_project_list_report = rail.run_report2(
            group_id='generate_project_list_report',
            target='artifact',
            report_params={
                "reportParameters": [
                    {
                        "reportUri": "{{result('get_projects_list_report_details').uri}}",
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }
        )

        parse_csv_replicon_report = rail.LoadCSVFileOperator(
            task_id='parse_csv_replicon_report',
            document="{{(result('generate_project_list_report.get_report_result')| load_json_artifact).reportGenerationResults[0].payload}}",
            delimiter=','
        )

        create_collection_project_list_replicon = rail.CreateCollectionOperator(
            task_id='create_collection_project_list_replicon',
            source="{{ result('parse_csv_replicon_report') }}",
            name="projectlistfromreplicon",
            columns={
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Project Status': 'projectstatus',
                'Project Start Date': 'projectstartdate',
                'Project Type': 'projecttype'
            }
        )

        query_list_get_unique_project_types = rail.QueryCollectionOperator(
            task_id='query_list_get_unique_project_types',
            query="""SELECT DISTINCT project_type FROM  validatedinputlist""",
        )

        get_template_project_details = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_template_project_details',
            items="{{result('query_list_get_unique_project_types')}}",
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda item:{
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code":item['project_type'],
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda response, item: {
                'project_type': item['project_type'],
                'uri': response[0]['projectDetails']['uri'] if response[0]['projectDetails'] else null
            }
        )

        get_all_cost_centers = rail.RepliconServiceOperator(
            task_id='get_all_cost_centers',
            endpoint="/services/CostCenterService1.svc/GetAllCostCenters",
        )

        get_all_custom_fields = rail.RepliconServiceOperator(
            task_id="get_all_custom_fields",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                    "objectUri": "urn:replicon:object-type:project"
            },
            data_handler=python_callable_methods.get_all_custom_fields_data
        )

        get_all_project_type_custom_field_options = rail.RepliconServiceOperator(
            task_id="get_all_project_type_custom_field_options",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_custom_fields'), 'displayText', 'Project Type', 'uri')
            },
        )

        query_get_new_projects_to_process = rail.QueryCollectionOperator(
            task_id='query_get_new_projects_to_process',
            query="""SELECT * FROM  validatedinputlist WHERE validatedinputlist.project_code NOT IN
              (SELECT DISTINCT projectlistfromreplicon.projectcode FROM  projectlistfromreplicon)""",
        )

        query_get_update_projects_to_process = rail.QueryCollectionOperator(
            task_id='query_get_update_projects_to_process',
            query="""SELECT * FROM  validatedinputlist WHERE validatedinputlist.project_code IN
              (SELECT DISTINCT projectlistfromreplicon.projectcode FROM  projectlistfromreplicon)""",
        )

        trigger_dag_run_project_add_async = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_project_add_async',
            items="{{ result('query_get_new_projects_to_process')}}",
            parallel_count=config.child_parallel_count,
            trigger_dag_id=config.project_add_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'cost_center_uri':  python_callable_methods.get_unique_cost_center_uri(rail.result('get_all_cost_centers'), item['cost_center_name']),
                'get_all_custom_fields': rail.result('get_all_custom_fields'),
                'project_type_custom_field_options': rail.result('get_all_project_type_custom_field_options'),
                'template_project_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_template_project_details'), 'project_type', item['project_type'], 'uri', ''),
                'parent_jobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        trigger_dag_run_project_update_async = rail.trigger_parallel_dagrun(
            task_id='trigger_dag_run_project_update_async',
            items="{{ result('query_get_update_projects_to_process')}}",
            parallel_count=config.child_parallel_count,
            trigger_dag_id=config.project_update_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                **item,
                'cost_center_uri': python_callable_methods.get_unique_cost_center_uri(rail.result('get_all_cost_centers'), item['cost_center_name']),
                'get_all_custom_fields': rail.result('get_all_custom_fields'),
                'parent_jobid': rail.render_template("{{dag_run_ecid()}}")
            }
        )

        get_all_process_project_dag_runs = rail.PythonOperator(
            task_id="get_all_process_project_dag_runs",
            python_callable=lambda: python_callable_methods.get_all_triggered_child_dags_callable(config),
            show_return_value_in_logs=False
        )

        get_all_project_child_logs = rail.GatherResultsFromDagRunsOperator(
            task_id="get_all_project_child_logs",
            dag_runs="{{result('get_all_process_project_dag_runs')}}",
            dagrun_task_id="create_project_import_child_logs",
            flatten=True
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=config.process_log_generation_dagid,
            conf=lambda dag_run: {
                'input_csv_collection_length': rail.result('create_collection_create_list_from_csv_raw_data', 'length'),
                'main_logs': rail.result('create_project_import_logs_lookuptable'),
                'project_child_logs': rail.result('get_all_project_child_logs'),
                'input_filename': rail.render_template("{{result('new_file_sensor') | file_name }}"),
                'log_filename': f'Logs_Project_{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        new_file_sensor >> is_file_csv
        is_file_csv >> rail.Label(
            'No') >> send_incorrect_file_format_mail >> finish
        is_file_csv >> rail.Label('Yes') >> download_input_csv

        download_input_csv >> parse_csv >> create_collection_create_list_from_csv_raw_data\
            >> if_csv_has_data
        if_csv_has_data >> rail.Label('Yes') >> query_list_projects_missing_required_fields\
            >> create_project_import_logs_lookuptable \
                >> if_query_list_projects_missing_required_fields_has_data
        if_query_list_projects_missing_required_fields_has_data >> rail.Label('Yes') >> logs_add_missing_required_fields\
            >> query_list_projects_valid_records
        if_query_list_projects_missing_required_fields_has_data >> rail.Label('No') >> query_list_projects_valid_records\
            >> get_projects_list_report_details >> generate_project_list_report >> parse_csv_replicon_report \
            >> create_collection_project_list_replicon >> query_list_get_unique_project_types >> get_template_project_details \
            >> get_all_cost_centers >> get_all_custom_fields >> get_all_project_type_custom_field_options \
                >> query_get_new_projects_to_process >> query_get_update_projects_to_process \
            >> trigger_dag_run_project_add_async >> trigger_dag_run_project_update_async >> get_all_process_project_dag_runs >> get_all_project_child_logs  \
            >> process_log_generation >> finish
        if_csv_has_data >> rail.Label(
            'No') >> send_blank_payload_email >> finish
        download_input_csv >> rail.Label(
            "Always") >> was_new_file_found >> rail.Label(
                "Yes") >> archive_file
        was_new_file_found >> rail.Label('No') >> delete_this_dagrun
    return dag


rail.for_each_instance(create_dag)
