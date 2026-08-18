from datetime import timedelta
from airflow.models import Variable
import rail
import json
from lead3rllc.project_import.utils.request_payload import is_projectmanager_permission, add_project_payload

null = None


def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.add_project_child_dag_id,
        description='LEAD3R LLC Project Import - Add Project Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_project_manager_in_input'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_project_manager_in_input',
            end_task='catch_and_log_error',
        )

        is_project_manager_in_input = rail.IfOperator(
            task_id='is_project_manager_in_input',
            test=lambda dag_run: bool(dag_run.conf['engagement_lead']),
            yes_task='is_project_manager_in_input_and_available_in_replicon',
            no_task='add_project'
        )

        is_project_manager_in_input_and_available_in_replicon = rail.IfOperator(
            task_id='is_project_manager_in_input_and_available_in_replicon',
            test=lambda dag_run: dag_run.conf['user_uri'] != null,
            yes_task='is_user_enabled',
            no_task='log_project_manager_not_available'
        )

        log_project_manager_not_available = rail.WriteLogOperator(
            task_id='log_project_manager_not_available',
            log="{{dag_run.conf.project_import_log}}",
            message='na',
            severity='Exception',
            properties=lambda dag_run: {
                "deal_id": dag_run.conf['deal_id'],
                "deal_name": dag_run.conf['deal_name'],
                "company_name": dag_run.conf['company_name'],
                "action": "add_project",
                "status": "Exception",
                "details": "Project Manager not available in Replicon"
            }
        )

        is_user_enabled = rail.IfOperator(
            task_id='is_user_enabled',
            test=lambda dag_run: dag_run.conf['user_isenabled'] == "True",
            yes_task='check_projectmanager_permission_assigned',
            no_task='log_project_manager_disabled',
        )

        log_project_manager_disabled = rail.WriteLogOperator(
            task_id='log_project_manager_disabled',
            log="{{dag_run.conf.project_import_log}}",
            message='na',
            severity='Exception',
            properties=lambda dag_run: {
                "deal_id": dag_run.conf['deal_id'],
                "deal_name": dag_run.conf['deal_name'],
                "company_name": dag_run.conf['company_name'],
                "action": "add_project",
                "status": "Exception",
                "details": "Project Manager is disabled in Replicon"
            }
        )

        check_projectmanager_permission_assigned = rail.RepliconServiceOperator(
            task_id='check_projectmanager_permission_assigned',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ dag_run.conf.user_uri  }}"
            },
            data_handler=is_projectmanager_permission
        )

        if_user_has_project_manager_permission = rail.IfOperator(
            task_id='if_user_has_project_manager_permission',
            test=lambda: rail.result(
                'check_projectmanager_permission_assigned'),
            yes_task='add_project',
            no_task='log_project_manager_permission_missing'
        )

        log_project_manager_permission_missing = rail.WriteLogOperator(
            task_id='log_project_manager_permission_missing',
            log="{{dag_run.conf.project_import_log}}",
            message='na',
            severity='Exception',
            properties=lambda dag_run: {
                "deal_id": dag_run.conf['deal_id'],
                "deal_name": dag_run.conf['deal_name'],
                "company_name": dag_run.conf['company_name'],
                "action": "add_project",
                "status": "Exception",
                "details": "Project Manager permission is missing"
            }
        )

        def get_addproject2_payload(dag_run):
            return json.dumps([
                {
                    'operationName': 'addProject2',
                    'variables': {
                        "project_name": dag_run.conf['deal_name'],
                        "project_code": dag_run.conf['deal_id']
                    },
                    'query': '''mutation addProject2($project_name: String, $project_code: String) {
                        addProject2(projectInput: {
                            name: $project_name
                            code: $project_code
                        }) {
                            project {
                            name
                            code
                            uri
                            }
                        }
                    }'''
                }
            ])

        add_project = rail.RepliconServiceOperator(
            task_id='add_project',
            endpoint='/graphql',
            data=get_addproject2_payload,
            app='polaris',
        )

        if_add_project_successful = rail.IfOperator(
            task_id='if_add_project_successful',
            test=lambda: bool(rail.result(
                'add_project')[0]['data']['addProject2']),
            yes_task='apply_project_modifications',
            no_task='failed_to_add_project'
        )

        failed_to_add_project = rail.FailOperator(
            task_id='failed_to_add_project',
            message='{{ result("add_project").0.errors.0.message }}'
        )

        apply_project_modifications = rail.RepliconServiceOperator(
            task_id='apply_project_modifications',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: add_project_payload(rail.result(
                'add_project')[0]['data']['addProject2']['project']['uri'], dag_run)
        )

        log_add_project_success_entry = rail.WriteLogOperator(
            task_id='log_add_project_success_entry',
            log="{{dag_run.conf.project_import_log}}",
            message='na',
            severity='Success',
            properties=lambda dag_run: {
                "deal_id": dag_run.conf['deal_id'],
                "deal_name": dag_run.conf['deal_name'],
                "company_name": dag_run.conf['company_name'],
                "action": "add_project",
                "status": "Success",
                "details": "Project imported successfully"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{dag_run.conf.project_import_log}}",
            message='na',
            severity='Error',
            properties=lambda dag_run: {
                "deal_id": dag_run.conf['deal_id'],
                "deal_name": dag_run.conf['deal_name'],
                "company_name": dag_run.conf['company_name'],
                "action": "add_project",
                "status": "Error",
                "details": rail.render_template("{{get_error_message()}}")
            }
        )

        check_if_project_created_but_details_not_added = rail.IfOperator(
            task_id='check_if_project_created_but_details_not_added',
            test='{{ get_task_state("apply_project_modifications") == "failed" }}',
            yes_task='delete_partially_created_project'
        )

        delete_partially_created_project = rail.RepliconServiceOperator(
            task_id='delete_partially_created_project',
            endpoint="/services/ProjectService1.svc/Delete",
            data=lambda: {
                "projectUri": rail.result(
                    'add_project')[0]['data']['addProject2']['project']['uri']
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> is_project_manager_in_input

        is_project_manager_in_input >> rail.Label(
            'Yes') >> is_project_manager_in_input_and_available_in_replicon
        is_project_manager_in_input >> rail.Label('No') >> add_project

        is_project_manager_in_input_and_available_in_replicon >> rail.Label(
            'Yes') >> is_user_enabled
        is_project_manager_in_input_and_available_in_replicon >> rail.Label(
            'No') >> log_project_manager_not_available >> catch_and_log_error

        is_user_enabled >> rail.Label(
            'Yes') >> check_projectmanager_permission_assigned >> if_user_has_project_manager_permission
        is_user_enabled >> rail.Label(
            'No') >> log_project_manager_disabled >> catch_and_log_error

        if_user_has_project_manager_permission >> rail.Label(
            'Yes') >> add_project
        if_user_has_project_manager_permission >> rail.Label(
            'No') >> log_project_manager_permission_missing >> catch_and_log_error

        add_project >> if_add_project_successful

        if_add_project_successful >> rail.Label(
            'No') >> failed_to_add_project >> catch_and_log_error
        if_add_project_successful >> rail.Label(
            'Yes') >> apply_project_modifications >> log_add_project_success_entry >> catch_and_log_error

        catch_and_log_error >> check_if_project_created_but_details_not_added

        check_if_project_created_but_details_not_added >> rail.Label(
            'Yes') >> delete_partially_created_project

    return dag


rail.for_each_instance(create_child_dag)
