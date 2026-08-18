from datetime import timedelta,datetime as dt
import rail
from wipro.project_import_v2.utils import custom_methods,response_filter

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.projects_child_dag_id,
        description='Wipro Process Each Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_second_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_project_log = rail.CreateLogOperator(
             task_id='create_project_log'
        )

        create_collection_input_data = rail.CreateCollectionOperator(
            task_id = 'create_collection_input_data',
            source=lambda dag_run: dag_run.conf['project_data'],
            name= 'inputdata',
            columns= {
                "PROJECT_ID": "projectcode",
                "PROJTEXT": "projectname",
                "PM_ID": "pm_empid",
                "PM_ADID": "pm_loginname",
                "PM_NAME": "pm_name",
                "PM_MAIL": "pm_email",
                "Project_START_DATE": "projectstartdate",
                "Project_END_DATE": "projectenddate",
            }
        )

        has_collection_data = rail.IfOperator(
            task_id='has_collection_data',
            test="{{ result('create_collection_input_data', 'length') > 0 }}",
            yes_task='query_any_blankmandatory_check'
        )

        query_any_blankmandatory_check = rail.QueryCollectionOperator(
            task_id='query_any_blankmandatory_check',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NULL"""
        )

        has_any_blank_mandatory_field = rail.IfOperator(
            task_id='has_any_blank_mandatory_field',
            test="{{ result('query_any_blankmandatory_check', 'length') > 0 }}",
            yes_task='write_blankmandatory_field_log',
            no_task='query_valid_data_from_rawdata'
        )

        write_blankmandatory_field_log = rail.WriteLogOperator(
            task_id="write_blankmandatory_field_log",
            items="{{result('query_any_blankmandatory_check')}}",
            log= "{{ result('create_project_log') }}",
            severity="Skipped",
            message="mandatory field is not present",
            properties=custom_methods.get_invalid_logs_property_conf
        )

        query_valid_data_from_rawdata = rail.QueryCollectionOperator(
            task_id='query_valid_data_from_rawdata',
            query="""SELECT * FROM inputdata WHERE NULLIF(projectcode,'') IS NOT NULL""",
            name= 'final_data'
        )

        has_valid_projects = rail.IfOperator(
            task_id='has_valid_projects',
            test="{{ result('query_valid_data_from_rawdata', 'length') > 0 }}",
            yes_task='query_distict_projects',
            no_task= 'process_child_dag_for_exception_log'
        )    

        query_distict_projects = rail.QueryCollectionOperator(
            task_id='query_distict_projects',
            name='distinctprojects',
            query="""SELECT DISTINCT projectcode from final_data"""
        )

        get_project_last_modified_date_udf = rail.RepliconServiceOperator(
            task_id="get_project_last_modified_date_udf",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Integration_last_modified_date', 'uri')
        )

        get_project_export_type_oef_details = rail.RepliconServiceOperator(
            task_id = 'get_project_export_type_oef_details',
            endpoint= '/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data= {
                    "bindingContextUri": "urn:replicon:object-type:project"
                },
            data_handler= response_filter.get_project_type_oef_uri
        )

        get_oef_drop_down_project_export_type_values = rail.RepliconServiceOperator(
            task_id="get_oef_drop_down_project_export_type_values",
            endpoint="/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails",
            data=lambda: {
                "objectExtensionTagDefinitionUri": rail.result("get_project_export_type_oef_details")[0]['uri'],
            },
            data_handler= lambda resp: {
                'it_proj_details_dropdown_uri': rail.find_first_by_attr_and_get_attr(resp['tags'],'name', 'IT_PROJ_DETAILS', 'uri')
            }
        )

        get_all_employeetypes = rail.RepliconServiceOperator(
            task_id="get_all_employeetypes",
            endpoint="/services/EmployeeTypeService1.svc/GetAllEmployeeTypeDetails",
            data_handler=lambda resp: rail.find_first_by_attr_and_get_attr(resp, 'displayText', 'Foreign Managers', 'uri')
        )

        process_each_projects = rail.trigger_parallel_dagrun(
            task_id = 'process_each_projects',
            items= '{{ result("query_distict_projects") }}',
            parallel_count= config.parallel_count,
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'projectcode': item['projectcode'],
                'exception_log': rail.result("create_project_log"),
                'employeetypeuri': rail.result("get_all_employeetypes"),
                'can_process_project': 'Yes',
                'last_modified_date_udf_uri': rail.result("get_project_last_modified_date_udf"),
                'project_export_type_oef': rail.result("get_project_export_type_oef_details")[0]['uri'],
                'it_proj_details_dropdown_uri': rail.result("get_oef_drop_down_project_export_type_values")['it_proj_details_dropdown_uri']
            }
        )

        process_child_dag_for_exception_log = rail.TriggerDagRunOperator(
            task_id = 'process_child_dag_for_exception_log',
            trigger_dag_id= config.process_project_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: {
                'can_process_project': 'No',
                'exception_log': rail.result("create_project_log")
            }
        )

        create_project_log >> create_collection_input_data >> has_collection_data

        has_collection_data >> rail.Label(
            "Yes") >> query_any_blankmandatory_check >> has_any_blank_mandatory_field

        has_any_blank_mandatory_field >> rail.Label(
            "Yes") >> write_blankmandatory_field_log >> query_valid_data_from_rawdata

        has_any_blank_mandatory_field >> rail.Label(
            "No") >> query_valid_data_from_rawdata >> has_valid_projects

        has_valid_projects >> rail.Label(
            "Yes") >> query_distict_projects >> get_project_last_modified_date_udf >> get_project_export_type_oef_details >>\
                get_oef_drop_down_project_export_type_values >>\
                    get_all_employeetypes >> process_each_projects

        has_valid_projects >> rail.Label(
            "No") >> process_child_dag_for_exception_log

    return dag

rail.for_each_instance(create_child_dag_wbs)
