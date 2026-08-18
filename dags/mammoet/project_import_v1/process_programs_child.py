from datetime import datetime as dt, timedelta
import rail
from mammoet.project_import_v1.utils import response_filter,request_payload
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.program_child_dag_id,
        description='Mammoet Process Programs Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_program_data_from_query'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_program_data_from_query',
            end_task='catch_and_log_errors',
        )


        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_program_data_from_query = rail.QueryCollectionOperator(
            task_id='get_program_data_from_query',
            query="""SELECT * from validwbsdata WHERE programcode == :program_code LIMIT 1""",
            query_params = {
                'program_code': '{{ dag_run.conf.programcode }}'
            }
        )

        get_query_data = rail.PythonOperator(
            task_id = 'get_query_data',
            python_callable= lambda: rail.load_all_records(rail.result("get_program_data_from_query"))[0]
        )

        search_program_in_replicon = rail.RepliconServiceOperator(
            task_id='search_program_in_replicon',
            endpoint='/services/ProgramService1.svc/GetAllPrograms',
            data_handler=response_filter.get_program_data
        )

        does_program_exist = rail.IfOperator(
            task_id="does_program_exist",
            test='{{ result("search_program_in_replicon") | is_truthy }}',
            yes_task="update_name",
            no_task="create_program"
        )

        create_program = rail.RepliconServiceOperator(
            task_id='create_program',
            endpoint='/services/ProgramService1.svc/PutProgram',
            data=request_payload.get_put_program_param
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": "{{ result('get_query_data').projectmanager  }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0] if res else []
        )

        is_user_available = rail.IfOperator(
            task_id = 'is_user_available',
            test= lambda: bool(rail.result("get_user_details")),
            yes_task= 'is_user_disabled',
            no_task= 'catch_and_log_errors'
        )

        is_user_disabled = rail.IfOperator(
            task_id = 'is_user_disabled',
            test= "{{ not result('get_user_details').userDetails.isEnabled }}",
            yes_task= 'is_enddate_less_than_today',
            no_task= 'empty_program_success'
        )

        empty_program_success = rail.EmptyOperator(
            task_id = 'empty_program_success'
        )

        is_enddate_less_than_today = rail.IfOperator(
            task_id='is_enddate_less_than_today',
            test=lambda: not request_payload.is_enddate_less_than_today(rail.result(
                'get_user_details')['userDetails']['employmentDateRange']['endDate'], dt.now().strftime("%Y-%m-%d"), '%Y-%m-%d'),
            yes_task='catch_and_log_errors',
            no_task='enable_login'
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ result('get_user_details').userDetails.uri }}"
            }
        )

        assign_permission_set = rail.RepliconServiceOperator(
            task_id= "assign_permission_set",
            endpoint="/services/PermissionSetService1.svc/AssignPermissionSetToUser",
            data=lambda dag_run: {
                "userUri": rail.result("get_user_details")['userDetails']['uri'],
                "permissionSetUri": dag_run.conf['project_manager_permission_uri']
            }
        )

        update_program_manager = rail.RepliconServiceOperator(
            task_id='update_program_manager',
            endpoint='/services/ProgramService1.svc/UpdateProgramManager',
            data=lambda:{
                "programUri": rail.result("create_program")['uri'] if rail.result(
                    "create_program") else rail.result("search_program_in_replicon")[0],
                "programManagerUri": rail.result('get_user_details')['userDetails']['uri']
            }
        )

        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint='/services/ProgramService1.svc/UpdateName',
            data=lambda:{
                "programUri": rail.result("search_program_in_replicon")[0],
                "name": rail.result("get_query_data")['programexternalcode'] +' '+ rail.result(
                    "get_query_data")['programname'] + ' (' + rail.result("get_query_data")['programcode'] + ')'
            }
        )

        update_status = rail.RepliconServiceOperator(
            task_id='update_status',
            endpoint='''{{ "/services/ProgramService1.svc/Inactivate" if result(
                "get_query_data").programstatus == "Inactive" else "/services/ProgramService1.svc/Activate" }}''',
            data={
                "programUri": '{{ result("search_program_in_replicon")[0] }}'
            }
        )

        get_program_details = rail.RepliconServiceOperator(
            task_id='get_program_details',
            endpoint='/services/ProgramService1.svc/GetProgramDetails',
            data= {
                "programUri": '{{ result("search_program_in_replicon")[0] }}'
            }
        )

        can_update_daterange = rail.IfOperator(
            task_id="can_update_daterange",
            test='{{ result("get_query_data").programstartdate | is_truthy or result("get_query_data").programenddate | is_truthy }}',
            yes_task="update_daterange",
            no_task="empty_update"
        )

        update_daterange = rail.RepliconServiceOperator(
            task_id='update_daterange',
            endpoint='/services/ProgramService1.svc/UpdateDateRange',
            data=request_payload.get_program_daterange_param
        )

        empty_update = rail.EmptyOperator(
            task_id = 'empty_update'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= '{{ dag_run.conf.project_log }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties={
                'projectcode': '{{ result("get_query_data").projectcode}}',
                'projectname(code)': '{{ result("get_query_data").projectexternalcode}}',
                'projectname(name)': '{{ result("get_query_data").projectname}}',
                'programcode': '{{ result("get_query_data").programcode}}',
                'programname(code)': '{{ result("get_query_data").programexternalcode}}',
                'programname(name)': '{{ result("get_query_data").programname}}',
                'clientname': '{{ result("get_query_data").clientname}}',
                'clientcode': '{{ result("get_query_data").clientcode}}',
                'projecttype': '{{ result("get_query_data").projecttype}}',
                'details': '{{ get_error_message() }}',
                'status': "error"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_program_data_from_query

        get_program_data_from_query >> get_query_data >> get_user_details >>\
            search_program_in_replicon

        search_program_in_replicon >> does_program_exist >> rail.Label(
            "Yes") >> update_name

        update_name >> update_status >> get_program_details >> can_update_daterange

        can_update_daterange >> rail.Label(
            "Yes") >> update_daterange >> empty_update

        can_update_daterange >> rail.Label(
            "No") >> empty_update >> is_user_available

        does_program_exist >> rail.Label(
            "No") >> create_program >> is_user_available

        is_user_available >> rail.Label(
            "Yes") >> is_user_disabled >> rail.Label(
                "Yes")>> is_enddate_less_than_today

        is_user_disabled >> rail.Label(
            "No") >> empty_program_success >> assign_permission_set

        is_user_available >> rail.Label(
            "No") >> catch_and_log_errors

        is_enddate_less_than_today >> rail.Label(
            "Yes") >> catch_and_log_errors

        is_enddate_less_than_today >> rail.Label(
            "No") >> enable_login >> assign_permission_set >> update_program_manager

        update_program_manager >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
