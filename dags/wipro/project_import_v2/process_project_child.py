import rail
from wipro.project_import_v2.utils import request_payload,response_filter,custom_methods

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dag_id,
        description='Wipro Process Each Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_project_log = rail.CreateLogOperator(
            task_id="create_project_log"
        )

        log_project_and_exception_log = rail.PythonOperator(
            task_id="log_project_and_exception_log",
            python_callable=lambda dag_run: {
                "exception_log": dag_run.conf['exception_log'],
                "project_log": rail.result("create_project_log")
            }
        )

        can_process_project = rail.IfOperator(
            task_id = 'can_process_project',
            test= '{{ dag_run.conf.can_process_project == "No" }}',
            yes_task= 'finish',
            no_task= 'get_project_data_from_query'
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        get_project_data_from_query = rail.QueryCollectionOperator(
            task_id='get_project_data_from_query',
            query="""SELECT * from final_data WHERE projectcode == :program_code""",
            query_params = {
                'program_code': '{{ dag_run.conf.projectcode }}'
            }
        )

        load_project_data_from_query = rail.PythonOperator(
            task_id="load_project_data_from_query",
            python_callable=lambda: rail.load_all_records(rail.result("get_project_data_from_query"))[0]
        )

        get_valid_dates = rail.PythonOperator(
            task_id="get_valid_dates",
            python_callable=custom_methods.check_dates_are_valid
        )

        check_dates_are_valid = rail.IfOperator(
            task_id="check_dates_are_valid",
            test=lambda: bool(rail.result("get_valid_dates")['action']),
            yes_task='get_project_details',
            no_task='log_dates_exception'
        )

        log_dates_exception = rail.WriteLogOperator(
            task_id="log_dates_exception",
            log="{{result('create_project_log')}}",
            message="Project start date or end date is missing in payload",
            severity="Error",
            properties=lambda: {
                "projectcode": rail.result("load_project_data_from_query")['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "action": "Validation",
                "Status": "Exception",
                'details': rail.result("get_valid_dates")["message"]
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id="get_project_details",
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails3",
            data={
                "projects": [
                    {
                        "code": "{{ dag_run.conf.projectcode }}"
                    }
                ]
            },
            data_handler=lambda response: response[0].get('projectDetails')
        )

        is_project_available = rail.IfOperator(
            task_id='is_project_available',
            test=lambda: bool(rail.result('get_project_details')),
            yes_task="create_project",
            no_task="log_project_exception"
        )

        log_project_exception = rail.WriteLogOperator(
            task_id='log_project_exception',
            log='{{ result("create_project_log") }}',
            message='project is not exists in replicon',
            severity='Exception',
            properties=lambda dag_run: {
                "projectcode": dag_run.conf['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "taskcode": '',
                "taskname": '',
                "action": "Validation",
                "Status": "Exception",
                "details": "The project update is skipped since the project is not present in replicon"
            }
        )

        create_project = rail.RepliconServiceOperator(
            task_id="create_project",
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=request_payload.create_projectorapply_modifications
        )

        is_project_manager_is_present = rail.IfOperator(
            task_id = 'is_project_manager_is_present',
            test=lambda: bool(rail.result("load_project_data_from_query")['pm_empid']),
            yes_task= 'get_project_manager_in_replicon',
            no_task= 'log_project_success'
        )

        get_project_manager_in_replicon = rail.RepliconServiceOperator(
            task_id= 'get_project_manager_in_replicon',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": '{{ result("load_project_data_from_query").pm_loginname }}' + '@wipro.com',
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_project_manager_available = rail.IfOperator(
            task_id = 'is_project_manager_available',
            test=lambda: bool(rail.result("get_project_manager_in_replicon")),
            yes_task= 'get_project_manager_permission_set',
            no_task= 'is_user_details_not_present'
        )

        def check_user_fields():
            user_data = rail.result("load_project_data_from_query")
            if not user_data['pm_name'] or not user_data['pm_loginname'] or not user_data['pm_email'] or not user_data['pm_empid']:
                return True
            return False

        is_user_details_not_present = rail.IfOperator(
            task_id = 'is_user_details_not_present',
            test= check_user_fields,
            yes_task= 'log_user_skipped',
            no_task= 'create_user'
        )

        log_user_skipped = rail.EmptyOperator(
            task_id = 'log_user_skipped'
        )

        create_user = rail.RepliconServiceOperator(
            task_id= "create_user",
            endpoint="/services/importService1.svc/PutUser2",
            data=request_payload.get_create_user_payload
        )

        put_product_assignments_for_user = rail.RepliconServiceOperator(
            task_id='put_product_assignments_for_user',
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data=lambda: {
                "userUri": rail.result('create_user')['uri'],
                "productUris": config.license_uris
            }
        )

        get_project_manager_permission_set = rail.RepliconServiceOperator(
            task_id= "get_project_manager_permission_set",
            endpoint="/services/PermissionSetService1.svc/GetAllPermissionSets",
            data_handler= lambda resp: rail.find_first_by_attr_and_get_attr(
                resp,'displayText','Project Manager','uri')
        )

        assign_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda: {
                "userUri": rail.result("get_project_manager_in_replicon")[0]['userDetails']['uri'],
                "permissionSetUri": rail.result("get_project_manager_permission_set")
            }
        )

        assign_project_manager_to_project = rail.RepliconServiceOperator(
            task_id='assign_project_manager_to_project',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: {
                "projectUri": rail.result('create_project')['uri'],
                "userUri": rail.result('create_user')['uri'] if rail.result("create_user") else rail.result(
                    "get_project_manager_in_replicon")[0]['userDetails']['uri']
            }
        )

        log_project_success = rail.WriteLogOperator(
            task_id="log_project_success",
            log="{{result('create_project_log')}}",
            message="Project synced successfully",
            properties=lambda dag_run: {
                "projectcode": dag_run.conf['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "action": "Update" if request_payload.does_wbs_exist() else "Add",
                "Status": "Success",
                "details": request_payload.get_log_message()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{result('create_project_log')}}",
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                "projectcode": dag_run.conf['projectcode'],
                "projectname": rail.result("load_project_data_from_query")['projectname'],
                "action": "Add",
                "Status": "Error",
                'details': '{{ get_error_message() }}'
            }
        )

        create_project_log >> log_project_and_exception_log >> can_process_project

        can_process_project >> rail.Label(
            "Yes") >> finish

        can_process_project >> rail.Label(
            "No") >> get_project_data_from_query

        get_project_data_from_query >> load_project_data_from_query >> get_valid_dates >> check_dates_are_valid

        check_dates_are_valid >> rail.Label(
            "Yes") >> get_project_details >> is_project_available

        check_dates_are_valid >> rail.Label(
            "No") >> log_dates_exception >> catch_and_log_errors

        is_project_available >> rail.Label(
            "Yes") >> create_project >> is_project_manager_is_present

        is_project_available >> rail.Label(
            "No") >> log_project_exception >> catch_and_log_errors

        is_project_manager_is_present >> rail.Label(
            "Yes") >> get_project_manager_in_replicon >> is_project_manager_available

        is_project_manager_is_present >> rail.Label(
            "No") >> log_project_success

        is_project_manager_available >> rail.Label(
            "Yes") >> get_project_manager_permission_set >> assign_permission_set >> assign_project_manager_to_project

        is_project_manager_available >> rail.Label(
            "No") >> is_user_details_not_present

        is_user_details_not_present >> rail.Label(
            "Yes") >> log_user_skipped >> log_project_success

        is_user_details_not_present >> rail.Label(
            "No") >> create_user >> put_product_assignments_for_user >> assign_project_manager_to_project >> log_project_success >>\
                catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag_wbs)
