from datetime import timedelta
import itertools
import rail
from crl.project_import_v2.utils import request_payload,custom_method

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.projects_child_dag_id,
        description='CRL Process Each Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_exception_log = rail.CreateLogOperator(
             task_id='create_exception_log'
        )

        create_collection_input_data = rail.CreateCollectionOperator(
            task_id = 'create_collection_input_data',
            source=lambda dag_run: dag_run.conf['project_data']['ProjectRecord'],
            name= 'inputdata',
            columns= {
                "WC_COMPANY_CODE": "businessarea",
                "COMPANY_CODE": "companycode",
                "CRL_REFERENCE_NUMBER": "projectname",
                "NETWORK_ACTIVITY_DESCRIPTION": "taskname",
                "NETWORK_ACTIVITY_ID": "taskcode",
                "NETWORK_ACTIVITY_STATUS": "taskstatus",
                "NETWORK_DESCRIPTION": "projectdescription",
                "NETWORK_ID": "projectcode",
                "PROJECT_DESCRIPTION": "clientname",
                "PROJECT_ID": "clientcode",
                "NETWORK_STATUS": "projectstatus",
                "Business_Area_Network": "project_business_area",
                "Business_Area_Network_Activity": "task_business_area",
                "Profit_Center": "profit_center"
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NULL OR NULLIF(projectname,'') IS NULL"""
        )

        has_any_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_blank_mandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_wbs_blankmandatory_field_log',
            no_task='query_valid_data_from_rawdata'
        )

        write_wbs_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_wbs_blankmandatory_field_log",
            items="{{result('query_any_blankmandatory_check')}}",
            log= "{{ result('create_exception_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_logs_property_conf
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            name='validwbsdata',
            query="""SELECT ROW_NUMBER() OVER(ORDER BY ROWID) AS record_id,* FROM inputdata WHERE
                NULLIF(projectcode,'') IS NOT NULL AND NULLIF(projectname,'') IS NOT NULL"""
        )

        has_valid_projects = rail.IfOperator(
            task_id='has_valid_projects',
            test="{{ result('query_valid_data_from_rawdata', 'length') > 0 }}",
            yes_task='query_distict_clients',
            no_task= 'format_logs'
        )

        query_distict_clients = rail.QueryCollectionOperator(
            task_id='query_distict_clients',
            name='distinctclients',
            query="""SELECT DISTINCT clientcode,record_id from validwbsdata WHERE (NULLIF(clientcode, '')
                IS NOT NULL and NULLIF(clientname, '')IS NOT NULL) GROUP BY clientcode"""
        )

        def get_process_clients_trigger_id(item):
            modulo = int(item['record_id']) % config.CLIENT_BATCH_COUNT
            if modulo == 0:
                return config.client_child_dag_id
            return f"{config.client_child_dag_id}_batch_{str(modulo)}"

        process_clients = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_clients',
            items= '{{ result("query_distict_clients") }}',
            retries = 0,
            trigger_dag_id= get_process_clients_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'clientcode': item['clientcode'],
                'exception_log': rail.result("create_exception_log")
            }
        )

        wait_for_process_clients = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_clients',
            dag_runs= '{{ result("process_clients") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_distict_projects = rail.QueryCollectionOperator(
            task_id='query_distict_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectname,record_id from validwbsdata GROUP BY projectname"""
        )

        get_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:project'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Business Area', 'uri', '')
        )

        get_task_custom_fields = rail.RepliconServiceOperator(
            task_id='get_task_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:task'
            },
            data_handler=lambda response: {
                'wc_company_code': rail.find_first_by_attr_and_get_attr(
                                        response, 'displayText', 'WC Company Code', 'uri', ''),
                'task_business_area': rail.find_first_by_attr_and_get_attr(
                                        response, 'displayText', 'Business Area Network Activity', 'uri', ''),
                'profit_center': rail.find_first_by_attr_and_get_attr(
                                        response, 'displayText', 'Profit Center', 'uri', ''),
            }
        )

        def get_process_projects_trigger_id(item):
            modulo = int(item['record_id']) % config.PROJECT_BATCH_COUNT
            if modulo == 0:
                return config.process_project_dag_id
            return f"{config.process_project_dag_id}_batch_{str(modulo)}"

        process_projects = rail.trigger_parallel_dagrun(
            task_id = 'process_projects',
            items= '{{ result("query_distict_projects") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= get_process_projects_trigger_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'projectname': item['projectname'],
                'project_custom_fields': rail.result("get_project_custom_fields"),
                'task_custom_fields': rail.result("get_task_custom_fields"),
                'exception_log': rail.result("create_exception_log")
            }
        )

        get_process_project_dag_ids =rail.PythonOperator(
            task_id= 'get_process_project_dag_ids',
            python_callable= lambda: list(itertools.chain(
                *list(map(lambda x: rail.result(
                    f'process_projects_{x+1}'), range(config.parallel_count))))),
            show_return_value_in_logs= False
        )

        gather_project_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_project_logs',
            dag_runs='{{ result("get_process_project_dag_ids") }}',
            dagrun_task_id='log_project_and_exception_log',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            flatten=True
        )

        format_logs = rail.PythonOperator(
            task_id="format_logs",
            python_callable=custom_method.format_logs_callable
        )

        create_csv_log = rail.WriteCSVFileOperator(
            task_id='create_csv_log',
            source="{{result('format_logs')}}",
            header=[
                'projectcode',
                'projectname',
                'clientcode',
                'taskcode',
                'taskname',
                'action',
                'details',
                'status',
                'ecid',
                'master_ecid'
            ],
            row=[
                "{{item.properties.projectcode}}",
                "{{item.properties.projectname}}",
                "{{item.properties.clientcode}}",
                "{{item.properties.taskcode}}",
                "{{item.properties.taskname}}",
                "{{item.properties.action}}",
                "{{item.properties.details}}",
                "{{item.properties.Status}}",
                "{{item.ecid}}",
                "{{dag_run.conf.master_ecid}}"
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda dag_run: f'Logs_Project_Import_{dag_run.conf["log_filename"]}.csv'
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('create_csv_log')}}",
            output_file_name="{{ result('get_log_file_name') }}",
            expires_in_seconds=7*24*60*60
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('create_csv_log') }}",
            remote_filepath=config.log_filepath + "{{ result('get_log_file_name') }}"
        )

        send_import_complete_email = rail.EmailOperator(
            task_id="send_import_complete_email",
            to=config.tenant_email,
            bcc="{%- if result('format_logs', key='error_record_count') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project import - " }} \
                {%- if result("format_logs", key="error_record_count") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    {%- if result("format_logs", key="exception_record_count") > 0 -%} \
                        completed with exceptions \
                    {%- else -%} \
                        completed successfully  \
                    {%- endif -%} \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content="templates/emails/email_import_complete.html",
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info= lambda dag_run: {
                "distinct_projects": rail.result("query_distict_projects", "length"),
                "distinct_clients": rail.result("query_distict_clients", "length"),
                "master_ecid": dag_run.conf['master_ecid']
            }
        )

        create_exception_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_any_blank_mandatory_field >> rail.Label(
            "Yes") >> write_wbs_blankmandatory_field_log >> query_valid_data_from_rawdata

        has_any_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_data_from_rawdata >> has_valid_projects

        has_valid_projects >> rail.Label(
            "No") >> format_logs

        has_valid_projects >> rail.Label(
            "Yes") >> query_distict_clients >> process_clients >> wait_for_process_clients >>\
                query_distict_projects >> get_project_custom_fields >> get_task_custom_fields >> process_projects
        process_projects >> get_process_project_dag_ids >> gather_project_logs >> format_logs >> create_csv_log >> \
            get_log_file_name >> generate_downloadable_link >> upload_logs_to_sftp >> send_import_complete_email >> log_to_sumo
    return dag

rail.for_each_instance(create_child_dag_wbs)
