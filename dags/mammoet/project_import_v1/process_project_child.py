from datetime import datetime as dt, timedelta
import rail
from mammoet.project_import_v1.utils import request_payload,custom_method
from airflow.models import Variable

def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dag_id,
        description='Mammoet Process Each Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_details',
            end_task='catch_and_log_errors',
        )

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "code": '{{ dag_run.conf.projectcode }}',
                    }
                ]
            },
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails']
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.projectmanager }}",
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
            no_task= 'empty_project_skipped'
        )

        is_user_disabled = rail.IfOperator(
            task_id = 'is_user_disabled',
            test= "{{ not result('get_user_details').userDetails.isEnabled }}",
            yes_task= 'is_enddate_less_than_today',
            no_task= 'empty_project_success'
        )

        empty_project_success = rail.EmptyOperator(
            task_id = 'empty_project_success'
        )

        is_enddate_less_than_today = rail.IfOperator(
            task_id='is_enddate_less_than_today',
            test=lambda: not request_payload.is_enddate_less_than_today(rail.result(
                'get_user_details')['userDetails']['employmentDateRange']['endDate'], dt.now().strftime("%Y-%m-%d"), '%Y-%m-%d'),
            yes_task='log_project_skipped',
            no_task='enable_login'
        )

        empty_project_skipped = rail.EmptyOperator(
            task_id = 'empty_project_skipped'
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ result('get_user_details').userDetails.uri }}"
            }
        )

        log_project_skipped = rail.WriteLogOperator(
            task_id='log_project_skipped',
            log= '{{ dag_run.conf.project_log }}',
            message=custom_method.get_log_skipped_message,
            properties=lambda dag_run:{
                'projectcode': dag_run.conf['projectcode'],
                'projectname(code)': dag_run.conf['projectname(code)'],
                'projectname(name)': dag_run.conf['projectname(name)'],
                'programcode': dag_run.conf['programcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(code)': dag_run.conf['programname(code)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(name)': dag_run.conf['programname(name)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientname': dag_run.conf['clientname'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientcode': dag_run.conf['clientcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'projecttype': dag_run.conf['projecttype'],
                'details': custom_method.get_log_skipped_message(),
                'status': "Skipped"
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

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_projectorapply_modifications
        )

        update_project_type_oef= rail.RepliconServiceOperator(
            task_id = 'update_project_type_oef',
            endpoint= '/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data= request_payload.get_oef_update_payload
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log= '{{ dag_run.conf.project_log }}',
            message=lambda: "Project Updated Successfully" if request_payload.does_wbs_exist() else "Project Added Successfully",
            properties=lambda dag_run:{
                'projectcode': dag_run.conf['projectcode'],
                'projectname(code)': dag_run.conf['projectname(code)'],
                'projectname(name)': dag_run.conf['projectname(name)'],
                'programcode': dag_run.conf['programcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(code)': dag_run.conf['programname(code)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(name)': dag_run.conf['programname(name)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientname': dag_run.conf['clientname'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientcode': dag_run.conf['clientcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'projecttype': dag_run.conf['projecttype'],
                'details': "Project Updated Successfully" if request_payload.does_wbs_exist() else "Project Added Successfully",
                'status': "Success"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= '{{ dag_run.conf.project_log }}',
            message='{{ get_error_message() }}',
            severity= 'Error',
            properties=lambda dag_run:{
                'projectcode': dag_run.conf['projectcode'],
                'projectname(code)': dag_run.conf['projectname(code)'],
                'projectname(name)': dag_run.conf['projectname(name)'],
                'programcode': dag_run.conf['programcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(code)': dag_run.conf['programname(code)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'programname(name)': dag_run.conf['programname(name)'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientname': dag_run.conf['clientname'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'clientcode': dag_run.conf['clientcode'] if dag_run.conf['projecttype'] == 'WBS' else None,
                'projecttype': dag_run.conf['projecttype'],
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
        can_run_batch_task >> rail.Label("No") >> get_project_details

        get_project_details >> get_user_details >> is_user_available

        is_user_available >> rail.Label(
            "Yes") >> is_user_disabled >> rail.Label(
                "Yes")>> is_enddate_less_than_today

        is_enddate_less_than_today >> rail.Label(
                "Yes") >> log_project_skipped

        is_enddate_less_than_today >> rail.Label(
                "No") >> enable_login >> assign_permission_set

        is_user_disabled >> rail.Label(
                "No") >> empty_project_success >> assign_permission_set >> create_projectorapply_modifications >>\
                    update_project_type_oef >> log_success

        is_user_available >> rail.Label(
            "No") >> empty_project_skipped >> log_project_skipped >> catch_and_log_errors

        log_success >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag_wbs)
