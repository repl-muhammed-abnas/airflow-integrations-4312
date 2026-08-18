from datetime import timedelta
import rail
from conduent.project_import_v1.utils import python_callable_methods
from conduent.project_import_v1.utils import request_payload

from airflow.models import Variable

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_add_child_dagid,
        description=f'Conduent Project Import Add Project {config.instance}',
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
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - Project Manager not available in Replicon or in Disabled status",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
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
            log="{{  result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Multiple Project Managers Found',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - Multiple Project Managers found with same id {{dag_run.conf.project_manager_id}}",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        get_assigned_projectmanager_permission = rail.RepliconServiceOperator(
            task_id='get_assigned_projectmanager_permission',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('get_project_manager_details') }}"
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
            log="{{  result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project Manager permission is missing',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - Project Manager permission is missing",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        if_has_cost_center_uri = rail.IfOperator(
            task_id='if_has_cost_center_uri',
            test=lambda dag_run: bool(dag_run.conf['cost_center_uri']),
            yes_task='if_template_project_found',
            no_task='add_logs_cost_center_not_available'
        )

        add_logs_cost_center_not_available = rail.WriteLogOperator(
            task_id='add_logs_cost_center_not_available',
            log="{{  result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Cost Center not available',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - {{' More than 1 Cost center Found' if dag_run.conf.cost_center_uri == false else 'Cost Center not available'}}",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        if_template_project_found = rail.IfOperator(
            task_id='if_template_project_found',
            test="{{dag_run.conf.template_project_uri | is_truthy}}",
            yes_task='if_project_type_in_custom_field_dropdown',
            no_task='add_log_template_project_not_found'
        )

        if_project_type_in_custom_field_dropdown = rail.IfOperator(
            task_id='if_project_type_in_custom_field_dropdown',
            test=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                dag_run.conf['project_type_custom_field_options'], 'displayText', dag_run.conf['project_type'], 'uri', ''),
            yes_task='create_project_copy_batch',
            no_task='add_log_project_type_not_in_customfield'
        )

        add_log_template_project_not_found = rail.WriteLogOperator(
            task_id='add_log_template_project_not_found',
            log="{{  result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Template Project not available',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - Project type not available {{dag_run.conf.project_type}}",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        add_log_project_type_not_in_customfield = rail.WriteLogOperator(
            task_id='add_log_project_type_not_in_customfield',
            log="{{  result('create_project_import_child_logs') }}",
            severity='Exception',
            message='Project type not available in dropdown',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Exception",
                "details": "Project not created - Project type - {{dag_run.conf.project_type}} not available in custom field values",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        create_project_copy_batch = rail.RepliconServiceOperator(
            task_id='create_project_copy_batch',
            endpoint='/services/ProjectService1.svc/CreateProjectCopyBatch2',
            data=request_payload.get_project_copy_batch_param
        )

        execute_projects_batch, wait_for_batch_completion = rail.batch_execution(
            'execute_projects_batch', create_project_copy_batch.task_id,
        )

        get_projectcopy_batch_results = rail.RepliconServiceOperator(
            task_id='get_projectcopy_batch_results',
            endpoint='/services/ProjectService1.svc/GetProjectCopyBatchResults',
            data=lambda: {"projectCopyBatchUri": rail.result(
                'create_project_copy_batch')}
        )

        create_projectorapply_modifications = rail.RepliconServiceOperator(
            task_id='create_projectorapply_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.create_projectorapply_modification_param
        )

        if_project_type_overhead = rail.IfOperator(
            task_id='if_project_type_overhead',
            test=lambda dag_run: dag_run.conf["project_type"].lower(
            ) == 'oh',
            yes_task='put_eligibleprojectteammember_dataaccessscopes',
            no_task='add_project_data_to_blob'
        )

        put_eligibleprojectteammember_dataaccessscopes = rail.RepliconServiceOperator(
            task_id='put_eligibleprojectteammember_dataaccessscopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_eligibleprojectteammember_dataaccessscopes
        )

        add_project_data_to_blob = rail.RepliconServiceOperator(
            task_id='add_project_data_to_blob',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data=lambda: request_payload.add_project_data_to_blob_param(config.project_manager_blob_key_name)
        )

        add_log_project_add_successful = rail.WriteLogOperator(
            task_id='add_log_project_add_successful',
            log="{{  result('create_project_import_child_logs') }}",
            severity='Success',
            message='Project Successfully Created',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
                "status": "Success",
                "details": "Project created successfully",
                "jobid":  "{{dag_run.conf.parent_jobid}}",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{  result('create_project_import_child_logs') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "childjobid": "{{ dag_run_ecid() }}",
                "project_name": "{{dag_run.conf.project_name}}",
                "project_code": "{{dag_run.conf.project_code}}",
                "project_type": "{{dag_run.conf.project_type}}",
                "action": "Add",
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
            'Yes') >> if_template_project_found
        if_template_project_found >> rail.Label(
            'No') >> add_log_template_project_not_found >> finish
        if_template_project_found >> rail.Label(
            'Yes') >> if_project_type_in_custom_field_dropdown
        if_project_type_in_custom_field_dropdown >> rail.Label(
            'No') >> add_log_project_type_not_in_customfield >> finish
        if_project_type_in_custom_field_dropdown >> rail.Label('Yes') >> create_project_copy_batch \
            >> execute_projects_batch
        wait_for_batch_completion >> get_projectcopy_batch_results >> create_projectorapply_modifications >> if_project_type_overhead
        if_project_type_overhead >> rail.Label(
            'Yes') >> put_eligibleprojectteammember_dataaccessscopes >> add_project_data_to_blob >> add_log_project_add_successful >> finish
        if_project_type_overhead >> rail.Label(
            'No') >> add_project_data_to_blob
        if_has_cost_center_uri >> rail.Label(
            'No') >> add_logs_cost_center_not_available >> finish
        if_user_has_project_manager_permission >> rail.Label(
            'No') >> add_logs_project_manager_permission_missing >> finish
        is_user_available >> rail.Label(
            'No') >> add_logs_project_manager_not_available >> finish
        finish >> catch_and_log_errors
    return dag


rail.for_each_instance(create_dag)
