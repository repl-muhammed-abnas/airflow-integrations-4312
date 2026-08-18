from datetime import timedelta,datetime
import rail
from mammoet.project_import_v1.utils import request_payload,custom_method,response_filter

#pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.projects_child_dag_id,
        description='Mammoet Process Each Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_project_log = rail.CreateLogOperator(
             task_id='create_project_log'
        )

        create_collection_input_data = rail.CreateCollectionOperator(
            task_id = 'create_collection_input_data',
            source=lambda dag_run: dag_run.conf['project_data']['project'],
            name= 'inputdata',
            columns= {
                "programcode": "programcode",
                "programexternalcode": "programexternalcode",
                "programname": "programname",
                "programstartdate": "programstartdate",
                "programenddate": "programenddate",
                "programstatus": "programstatus",
                "projectcode": "projectcode",
                "projectexternalcode": "projectexternalcode",
                "projectname": "projectname",
                "projectstartdate": "projectstartdate",
                "projectenddate": "projectenddate",
                "projectstatus": "projectstatus",
                "projectmanager": "projectmanager",
                "clientcode": "clientcode",
                "clientname": "clientname",
                "projecttype": "projecttype"
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_project_type_blank_check'
        )

        query_any_project_type_blank_check = rail.QueryCollectionOperator(
            task_id='query_any_project_type_blank_check',
            query="""SELECT * FROM inputdata WHERE (NULLIF(projecttype,'') IS NULL OR
                    (projecttype != 'PM Order' AND projecttype != 'WBS'))"""
        )

        has_any_project_type_blank = rail.IfOperator(
            task_id='has_any_project_type_blank',
            test="{{ result('query_any_project_type_blank_check', 'length') > 0 }}",
            yes_task='write_project_type_blank_log',
            no_task='query_any_wbs_blankmandatory_check'
        )

        write_project_type_blank_log = rail.WriteLogOperator(
            task_id="write_project_type_blank_log",
            items="{{result('query_any_project_type_blank_check')}}",
            log= "{{ result('create_project_log') }}",
            severity="Skipped",
            message="project type is not allowed/blank",
            properties=request_payload.get_invalid_project_type
        )

        query_any_wbs_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_wbs_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE (NULLIF(programcode,'') IS NULL OR NULLIF(programexternalcode,'') IS NULL OR
                NULLIF(programname,'') IS NULL OR NULLIF(programstatus,'') IS NULL OR NULLIF(projectcode,'') IS NULL OR NULLIF(projectexternalcode,'') IS NULL OR
                NULLIF(projectname,'') IS NULL OR NULLIF(projectstatus,'') IS NULL OR NULLIF(projectmanager,'') IS NULL OR NULLIF(clientcode,'') IS NULL OR
                NULLIF(clientname,'') IS NULL OR NULLIF(projecttype,'') IS NULL) AND projecttype == 'WBS' """
        )

        has_any_wbs_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_wbs_blank_mandatory_field',
            test="{{ result('query_any_wbs_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_wbs_blankmandatory_field_log',
            no_task='query_any_pmo_blankmandatory_check'
        )

        write_wbs_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_wbs_blankmandatory_field_log",
            items="{{result('query_any_wbs_blankmandatory_check')}}",
            log= "{{ result('create_project_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_logs_property_conf
        )

        query_any_pmo_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_pmo_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE (NULLIF(projectcode,'') IS NULL OR NULLIF(projectexternalcode,'') IS NULL OR
                NULLIF(projectname,'') IS NULL OR NULLIF(projectstatus,'') IS NULL OR NULLIF(projectmanager,'') IS NULL OR
                NULLIF(projecttype,'') IS NULL) AND projecttype == 'PM Order' """
        )

        has_any_pmo_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_pmo_blank_mandatory_field',
            test="{{ result('query_any_pmo_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_pmo_blankmandatory_field_log',
            no_task='get_project_type_oef_details'
        )

        write_pmo_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_pmo_blankmandatory_field_log",
            items="{{result('query_any_pmo_blankmandatory_check')}}",
            log= "{{ result('create_project_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=request_payload.get_invalid_logs_property_conf
        )

        get_project_type_oef_details = rail.RepliconServiceOperator(
            task_id = 'get_project_type_oef_details',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:project"
                },
            data_handler= response_filter.get_project_type_oef_uri
        )

        get_oef_drop_down_project_type_values = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_project_type_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_project_type_oef_details")[0]['uri'],
            },
            data_handler= lambda resp: {
                'wbs_oef': rail.find_first_by_attr_and_get_attr(resp['tags'],'name', 'WBS', 'uri'),
                'pmorder_oef': rail.find_first_by_attr_and_get_attr(resp['tags'],'name', 'PM Order', 'uri')
            }
        )

        get_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "get_project_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda resp: rail.find_first_by_attr_and_get_attr(
                resp,'displayText','Project Manager','uri')
        )

        query_valid_pmo_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_pmo_data_from_rawdata',
            name='validpmodata',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NOT NULL AND NULLIF(projectexternalcode,'') IS NOT NULL AND
                NULLIF(projectname,'') IS NOT NULL AND NULLIF(projectstatus,'') IS NOT NULL AND NULLIF(projectmanager,'') IS NOT NULL AND
                NULLIF(projecttype,'') IS NOT NULL AND projecttype == 'PM Order' """
        )

        has_pmo_projects_to_create = rail.IfOperator(
            task_id='has_pmo_projects_to_create',
            test="{{ result('query_valid_pmo_data_from_rawdata', 'length') > 0 }}",
            yes_task='start_processing_pmo_projects',
            no_task='query_valid_wbs_data_from_rawdata'
        )

        start_processing_pmo_projects = rail.EmptyOperator(
            task_id = 'start_processing_pmo_projects'
        )

        process_create_pmo_projects = rail.trigger_parallel_dagrun(
            task_id = 'process_create_pmo_projects',
            items= '{{ result("query_valid_pmo_data_from_rawdata") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'projectmanager': '{{ item.projectmanager }}',
                'projectstatus': '{{ item.projectstatus }}',
                'projectname(name)': '{{ item.projectname }}',
                'projectname(code)': '{{ item.projectexternalcode }}',
                'projectcode': '{{ item.projectcode }}',
                'projecttype': '{{ item.projecttype}}',
                'projectstartdate': '{{ item.projectstartdate}}',
                'projectenddate': '{{ item.projectenddate}}',
                'project_log': '{{ result("create_project_log") }}',
                'project_type_uri': '{{ result("get_project_type_oef_details")[0].uri }}',
                'project_type_definition_uri': '{{ result("get_oef_drop_down_project_type_values").pmorder_oef }}',
                'project_manager_permission_uri': '{{ result("get_project_manager_permission_set") }}'
            }
        )

        query_valid_wbs_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_wbs_data_from_rawdata',
            name='validwbsdata',
            query="""SELECT * FROM inputdata WHERE NULLIF(programcode,'') IS NOT NULL AND NULLIF(programexternalcode,'') IS NOT NULL AND
                NULLIF(programname,'') IS NOT NULL AND NULLIF(programstatus,'') IS NOT NULL AND NULLIF(projectcode,'') IS NOT NULL AND NULLIF(projectexternalcode,'') IS NOT NULL AND
                NULLIF(projectname,'') IS NOT NULL AND NULLIF(projectstatus,'') IS NOT NULL AND NULLIF(projectmanager,'') IS NOT NULL AND NULLIF(clientcode,'') IS NOT NULL AND
                NULLIF(clientname,'') IS NOT NULL AND NULLIF(projecttype,'') IS NOT NULL AND projecttype == 'WBS' """
        )

        has_wbs_projects_to_create = rail.IfOperator(
            task_id='has_wbs_projects_to_create',
            test="{{ result('query_valid_wbs_data_from_rawdata', 'length') > 0 }}",
            yes_task='query_distict_programs',
            no_task='format_logs'
        )

        query_distict_programs = rail.QueryCollectionOperator(
            task_id='query_distict_programs',
            name='distinctprograms',
            query="""SELECT DISTINCT programcode from validwbsdata"""
        )

        process_programs = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_programs',
            items= '{{ result("query_distict_programs") }}',
            retries = 0,
            trigger_dag_id= config.program_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'programcode': '{{ item.programcode }}',
                'project_log': '{{ result("create_project_log") }}',
                'project_manager_permission_uri': '{{ result("get_project_manager_permission_set") }}'
            }
        )

        wait_for_process_programs = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_programs',
            dag_runs= '{{ result("process_programs") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        query_distict_clients = rail.QueryCollectionOperator(
            task_id='query_distict_clients',
            name='distinctclients',
            query="""SELECT DISTINCT clientcode from validwbsdata"""
        )

        process_clients = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_clients',
            items= '{{ result("query_distict_clients") }}',
            retries = 0,
            trigger_dag_id= config.client_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'clientcode': '{{ item.clientcode }}',
                'project_log': '{{ result("create_project_log") }}'
            }
        )

        wait_for_process_clients = rail.WaitForDagRunsSensor(
            task_id = 'wait_for_process_clients',
            dag_runs= '{{ result("process_clients") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        process_create_wbs_projects = rail.trigger_parallel_dagrun(
            task_id = 'process_create_wbs_projects',
            items= '{{ result("query_valid_wbs_data_from_rawdata") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf= {
                'projectcode': '{{ item.projectcode}}',
                'projectname(code)': '{{ item.projectexternalcode}}',
                'projectname(name)': '{{ item.projectname}}',
                'projectmanager': '{{ item.projectmanager }}',
                'projectstatus': '{{ item.projectstatus }}',
                'projectstartdate': '{{ item.projectstartdate }}',
                'projectenddate': '{{ item.projectenddate }}',
                'programcode': '{{ item.programcode}}',
                'programname(code)': '{{ item.programexternalcode}}',
                'programname(name)': '{{ item.programname}}',
                'clientname': '{{ item.clientname}}',
                'clientcode': '{{ item.clientcode}}',
                'projecttype': '{{ item.projecttype}}',
                'project_log': '{{ result("create_project_log") }}',
                'project_type_uri': '{{ result("get_project_type_oef_details")[0].uri }}',
                'project_type_definition_uri': '{{ result("get_oef_drop_down_project_type_values").wbs_oef }}',
                'project_manager_permission_uri': '{{ result("get_project_manager_permission_set") }}'
            }
        )

        format_logs = rail.PythonOperator(
            task_id='format_logs',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            python_callable=custom_method.do_format_logs
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source=lambda: rail.result('format_logs'),
            header=[
                'projectcode',
                'projectname(code)',
                'projectname(name)',
                'programcode',
                'programname(code)',
                'programname(name)',
                'clientname',
                'clientcode',
                'projecttype',
                'details',
                'status',
                'ecid'
            ],
            row=lambda item:[
                item['projectcode'],
                item['projectname(code)'],
                item['projectname(name)'],
                item['programcode'],
                item['programname(code)'],
                item['programname(name)'],
                item['clientname'],
                item['clientcode'],
                item['projecttype'],
                item['details'],
                item['status'],
                item['ecid']
            ],
        )

        get_log_file_name = rail.PythonOperator(
            task_id = 'get_log_file_name',
            python_callable= lambda: 'logs_' + datetime.now().strftime('%m%d%YT%H%M%S')
        )

        upload_logs_to_sftp = rail.SFTPUploadFileOperator(
            task_id='upload_logs_to_sftp',
            content="{{ result('render_logs_csv') }}",
            remote_filepath=config.project_log_filepath +
            '/{{ result("get_log_file_name") }}.csv',
        )

        get_errored_logs = rail.PythonOperator(
            task_id='get_errored_logs',
            python_callable=lambda: rail.set_result(
                len(list(filter(lambda x: x['status'] == "error", rail.result('format_logs')))), 'length')
        )

        generate_downloadable_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id="generate_downloadable_link",
            artifact_name="{{result('render_logs_csv')}}",
            output_file_name="Log_file_"+'{{result("get_log_file_name")}}'+".csv",
            expires_in_seconds=7*24*60*60
        )

        send_import_complete_email = rail.EmailOperator(
            task_id='send_import_complete_email',
            to=config.tenant_email,
            bcc="{%- if result('get_errored_logs', key='length') == 0 -%}\
                    "+config.internal_logs_email+"\
                {%- else -%}\
                    "+config.alert_email+"\
                {%- endif -%}",
            subject='{{ get_company_key() + " | Project import - " }} \
                {%- if result("get_errored_logs", key="length") > 0 -%} \
                    completed with errors  \
                {%- else -%} \
                    completed successfully  \
                {%- endif -%} \
                {{ " - " + current_time("%Y/%m/%d/%H:%M:%S") }}',
            html_content='templates/import_complete.html',
            params= {
                'type': 'Project'
            }
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger'
        )

        create_project_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_project_type_blank_check >> has_any_project_type_blank

        has_any_project_type_blank >> rail.Label(
            "Yes") >> write_project_type_blank_log >> query_any_wbs_blankmandatory_check

        has_any_project_type_blank >> rail.Label(
            "No") >> query_any_wbs_blankmandatory_check >> has_any_wbs_blank_mandatory_field

        has_any_wbs_blank_mandatory_field >> rail.Label(
            "Yes") >> write_wbs_blankmandatory_field_log >> query_any_pmo_blankmandatory_check

        has_any_wbs_blank_mandatory_field >> rail.Label(
            "No") >> query_any_pmo_blankmandatory_check >> has_any_pmo_blank_mandatory_field

        has_any_pmo_blank_mandatory_field >> rail.Label(
            "Yes") >> write_pmo_blankmandatory_field_log >> get_project_type_oef_details

        has_any_pmo_blank_mandatory_field >> rail.Label(
            "No") >> get_project_type_oef_details >> get_oef_drop_down_project_type_values >> \
                get_project_manager_permission_set >> query_valid_pmo_data_from_rawdata >> has_pmo_projects_to_create

        has_pmo_projects_to_create >> rail.Label(
            "Yes") >> start_processing_pmo_projects >> process_create_pmo_projects >> query_valid_wbs_data_from_rawdata

        has_pmo_projects_to_create >> rail.Label(
            "No") >> query_valid_wbs_data_from_rawdata >> has_wbs_projects_to_create

        has_wbs_projects_to_create >> rail.Label(
            "Yes") >> query_distict_programs >> process_programs >> wait_for_process_programs >>\
                query_distict_clients >> process_clients >> wait_for_process_clients >> process_create_wbs_projects

        has_wbs_projects_to_create >> rail.Label(
            "No") >> format_logs

        process_create_wbs_projects >> format_logs >> render_logs_csv >> get_log_file_name >> upload_logs_to_sftp >> get_errored_logs >>\
            generate_downloadable_link >> send_import_complete_email >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
