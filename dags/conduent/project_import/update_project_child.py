from datetime import timedelta
import rail
from conduent.project_import.utils import python_callable_methods
from conduent.project_import.utils import request_payload

from airflow.models import Variable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_update_child_dagid,
        description=f'Conduent Project Import update Project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_project_import_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_project_import_child_logs',
            end_task='catch_and_log_errors',
        )

        create_project_import_child_logs = rail.CreateLogOperator(
            task_id='create_project_import_child_logs'
        )

        get_project_manager_details = rail.RepliconServiceOperator(
            task_id="get_project_manager_details",
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_supervisor_details,
            data_handler=python_callable_methods.get_supervisor_uri
        )

        is_user_available = rail.IfOperator(
            task_id='is_user_available',
            test=lambda: bool(rail.result("get_project_manager_details")),
            yes_task='if_multiple_user_found',
            no_task='add_logs_project_manager_not_available'
        )

        add_logs_project_manager_not_available = rail.WriteLogOperator(
            task_id='add_logs_project_manager_not_available',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project Manager not available in replicon',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Exception",
                "details": "Project not updated - Project Manager not available in Replicon or in Disabled status",
                "jobid":  "{{dag_run.conf.parent_jobid}}"
            }
        )

        if_multiple_user_found = rail.IfOperator(
            task_id='if_multiple_user_found',
            test="{{ result('get_project_manager_details') == 'Multiple Project Managers Found' }}",
            yes_task='add_logs_multiple_project_managers_found',
            no_task='get_assigned_projectmanager_permission'
        )

        add_logs_multiple_project_managers_found = rail.WriteLogOperator(
            task_id='add_logs_multiple_project_managers_found',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project Manager is disabled',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Exception",
                "details": "Project not updated - Multiple Project Managers found with same Id - {{dag_run.conf.project_manager_id}}",
                "jobid": "{{dag_run.conf.parent_jobid}}",
            }
        )

        get_assigned_projectmanager_permission = rail.RepliconServiceOperator(
            task_id='get_assigned_projectmanager_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{result('get_project_manager_details')}}"
            },
            data_handler=python_callable_methods.is_projectmanager_permission
        )

        if_user_has_project_manager_permission = rail.IfOperator(
            task_id='if_user_has_project_manager_permission',
            test=lambda: rail.result('get_assigned_projectmanager_permission'),
            yes_task='if_has_cost_center_uri',
            no_task='add_logs_project_manager_permission_missing'
        )

        add_logs_project_manager_permission_missing = rail.WriteLogOperator(
            task_id='add_logs_project_manager_permission_missing',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project not updated - Project Manager permission is missing',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Exception",
                "details": "Project not updated - Project Manager permission is missing",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        if_has_cost_center_uri = rail.IfOperator(
            task_id='if_has_cost_center_uri',
            test=lambda dag_run: bool(dag_run.conf['cost_center_uri']),
            yes_task='get_project_details',
            no_task='add_logs_cost_center_not_available'
        )

        add_logs_cost_center_not_available = rail.WriteLogOperator(
            task_id='add_logs_cost_center_not_available',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project not updated - Cost Center not available',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Exception",
                "details": "Project not updated - {{' More than 1 Cost center Found' if dag_run.conf.cost_center_uri == false else 'Cost Center not available'}}",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={
                "projects": [
                    {
                        "uri": null,
                        "name": null,
                        "code": '{{ dag_run.conf.project_code }}',
                        "parameterCorrelationId": null
                    }
                ]
            },
            data_handler=lambda resp: resp[0]['projectDetails'] if resp[0]['projectDetails'] else null,
        )

        if_project_type_changed = rail.IfOperator(
            task_id='if_project_type_changed',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')[
                                                                      'customFields'], 'customField.displayText', 'Project Type', 'text') != dag_run.conf["project_type"],
            yes_task='add_log_project_type_changed',
            no_task='create_projectorapply_modifications'
        )

        add_log_project_type_changed = rail.WriteLogOperator(
            task_id='add_log_project_type_changed',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project type changed',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Exception",
                "details": "Project not updated - Project type changed",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.update_projectorapply_modification_param
        )

        if_project_type_overhead = rail.IfOperator(
            task_id='if_project_type_overhead',
            test=lambda dag_run: dag_run.conf["project_type"].lower(
            ) == 'oh' and rail.result('get_project_details')['costCenter'] and dag_run.conf['cost_center_uri'] != rail.result('get_project_details')['costCenter']['uri'],
            yes_task='put_eligibleprojectteammember_dataaccessscopes',
            no_task='add_log_project_update_successful'
        )

        put_eligibleprojectteammember_dataaccessscopes = rail.RepliconServiceOperator(
            task_id='put_eligibleprojectteammember_dataaccessscopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_eligibleprojectteammember_dataaccessscopes
        )

        add_log_project_update_successful = rail.WriteLogOperator(
            task_id='add_log_project_update_successful',
            log="{{ result('create_project_import_child_logs') }}",
            severity='Success',
            message='Project Successfully updated',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Success",
                "details": "Project updated successfully",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_project_import_child_logs') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Update",
                "status": "Error",
                "details": "{{ get_error_message() }}",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_project_import_child_logs >> get_project_manager_details >> is_user_available
        is_user_available >> rail.Label('Yes') >> if_multiple_user_found
        if_multiple_user_found >> rail.Label(
            'Yes') >> add_logs_multiple_project_managers_found >> finish
        if_multiple_user_found >> rail.Label(
            'No') >> get_assigned_projectmanager_permission >> if_user_has_project_manager_permission
        if_user_has_project_manager_permission >> rail.Label(
            'Yes') >> if_has_cost_center_uri
        if_has_cost_center_uri >> rail.Label(
            'Yes') >> get_project_details >> if_project_type_changed
        if_project_type_changed >> rail.Label(
            'Yes') >> add_log_project_type_changed >> finish
        if_project_type_changed >> rail.Label('No') >>\
            create_projectorapply_modifications >> if_project_type_overhead
        if_project_type_overhead >> rail.Label('Yes') >> put_eligibleprojectteammember_dataaccessscopes \
        >> add_log_project_update_successful
        if_project_type_overhead >> rail.Label('No') >> add_log_project_update_successful >> finish
        if_has_cost_center_uri >> rail.Label(
            'No') >> add_logs_cost_center_not_available >> finish
        if_user_has_project_manager_permission >> rail.Label(
            'No') >> add_logs_project_manager_permission_missing >> finish
        is_user_available >> rail.Label(
            'No') >> add_logs_project_manager_not_available >> finish
        finish >> catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
