from datetime import timedelta
from airflow.models import Variable
import rail
from pwcglobal.custom_import_for_teammanager_permission.utils import python_callable, request_payload


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_supervisory_org_permission_assignment_child,
        description=f'PwC Custom Import for Team Manager Permission for Supervisor Org Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
            no_task='create_process_supervisory_org_child_logs'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_process_supervisory_org_child_logs',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_process_supervisory_org_child_logs = rail.CreateLogOperator(
            task_id = 'create_process_supervisory_org_child_logs'
        )

        search_user = rail.RepliconServiceOperator(
            task_id="search_user",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.guid}}"
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('search_user')),
            yes_task="get_current_country_of_user",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.PythonOperator(
            task_id = "log_user_not_present",
            python_callable=lambda : "Supervisory Org for Team Manager is not updated since User is not found in the replicon instance "
        )

        get_current_country_of_user = rail.RepliconServiceOperator(
            task_id="get_current_country_of_user",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{ result('search_user')[0].userDetails.uri}}"
            },
            data_handler=request_payload.get_current_country
        )

        if_valid_current_country = rail.IfOperator(
            task_id="if_valid_current_country",
            test=lambda: bool(rail.result('get_current_country_of_user') and str(rail.result('get_current_country_of_user')).lower() == 'japan'),
            yes_task="has_valid_supervisory_org_levels",
            no_task="log_user_belong_to_other_location"
        )

        log_user_belong_to_other_location = rail.PythonOperator(
            task_id = "log_user_belong_to_other_location",
            python_callable=lambda : f"Supervisory Org for Team Manager is not updated since User belongs to {rail.result('get_current_country_of_user')} "
        )

        has_valid_supervisory_org_levels = rail.IfOperator(
            task_id='has_valid_supervisory_org_levels',
            test=lambda dag_run: bool(len(dag_run.conf['supervisory_org'].split('|')) <= 7),
            yes_task="if_permission_to_be_assigned_uri_present",
            no_task="log_supervisory_org_has_more_than_7_levels",
        )

        log_supervisory_org_has_more_than_7_levels = rail.PythonOperator(
            task_id = "log_supervisory_org_has_more_than_7_levels",
            python_callable=lambda : "Supervisory Org for Team Manager is not updated since Supervisory Org has more than 7 levels "
        )

        if_permission_to_be_assigned_uri_present = rail.IfOperator(
            task_id='if_permission_to_be_assigned_uri_present',
            test="{{ dag_run.conf.permission_to_be_assigned_uri | is_truthy }}",
            yes_task="get_supervisory_org_hierarchy_data",
            no_task="log_team_manager_permission_not_present",
        )

        log_team_manager_permission_not_present = rail.PythonOperator(
            task_id = "log_team_manager_permission_not_present",
            python_callable=lambda : "Supervisory Org for Team Manager is not updated since Team Manager Permission to be assigned is not present in replicon "
        )

        get_supervisory_org_hierarchy_data = rail.RepliconServiceOperator(
            task_id="get_supervisory_org_hierarchy_data",
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:name",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": None
            },
            data_handler=request_payload.filter_full_path_data
        )

        if_supervisory_org_level_path_present = rail.IfOperator(
            task_id='if_supervisory_org_level_path_present',
            test = "{{ result('get_supervisory_org_hierarchy_data') | is_truthy}}",
            yes_task="get_preassigned_restrictions",
            no_task="log_supervisory_org_level_path_missing",
        )

        log_supervisory_org_level_path_missing = rail.PythonOperator(
            task_id = "log_supervisory_org_level_path_missing",
            python_callable=lambda dag_run: f"Supervisory Org for Team Manager is not updated since Supervisory Org Path - {dag_run.conf['supervisory_org']} is not available in the Replicon/disabled. "
        )

        get_preassigned_restrictions = rail.RepliconServiceOperator(
            task_id="get_preassigned_restrictions",
            endpoint="/services/PermissionSetService1.svc/GetPolicyDataAccessScopeDetailsForUser2",
            data={
                "userUri": "{{ result('search_user')[0].userDetails.uri}}"
            },
            data_handler = request_payload.get_restrictions_for_TeamManager_for_user
        )

        remove_pre_assigned_restrictions = rail.RepliconServiceOperator(
            task_id="remove_pre_assigned_restrictions",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.remove_pre_assign_restrictions_payload
        )

        get_assigned_supervisory_org_child_levels_uri = rail.RepliconServiceOperator(
            task_id="get_assigned_supervisory_org_child_levels_uri",
            endpoint="/services/CostCenterListService1.svc/GetHierarchyData",
            data=request_payload.get_assigned_supervisory_org_child_levels,
            data_handler=python_callable.get_required_supervisory_org_level_uris
        )

        assign_supervisory_orgs = rail.RepliconServiceOperator(
            task_id="assign_supervisory_orgs",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.assign_supervisory_org_payload
        )

        log_project_process = rail.WriteLogOperator(
            task_id="log_project_process",
            log = "{{ result('create_process_supervisory_org_child_logs') }}",
            message='Success',
            properties=python_callable.get_status_and_details
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = "{{ result('create_process_supervisory_org_child_logs') }}",
            severity="Error",
            trigger_rule="one_failed",
            message='Error',
            properties={
                "guid": "{{ dag_run.conf.guid }}",
                "supervisory_org_level": "{{ dag_run.conf.supervisory_org }}",
                "status": "Error",
                'details': "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                "guid":"{{ dag_run.conf.guid }}",
                "permission_to_be_assigned":"{{ dag_run.conf.permission_name }}"
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> create_process_supervisory_org_child_logs

        create_process_supervisory_org_child_logs >> search_user >> is_user_present
        
        is_user_present >> rail.Label('Yes') >> get_current_country_of_user >> if_valid_current_country
        is_user_present >> rail.Label('No') >> log_user_not_present >> log_project_process

        if_valid_current_country >>rail.Label('Yes') >> has_valid_supervisory_org_levels
        if_valid_current_country >>rail.Label('No') >> log_user_belong_to_other_location >> log_project_process

        has_valid_supervisory_org_levels >> rail.Label('Yes') >> if_permission_to_be_assigned_uri_present
        has_valid_supervisory_org_levels >> rail.Label('No') >> log_supervisory_org_has_more_than_7_levels >> log_project_process

        if_permission_to_be_assigned_uri_present >> rail.Label('Yes') >> get_supervisory_org_hierarchy_data >> if_supervisory_org_level_path_present
        if_permission_to_be_assigned_uri_present >> rail.Label('No') >> log_team_manager_permission_not_present >> log_project_process

        if_supervisory_org_level_path_present >> rail.Label('Yes') >> get_preassigned_restrictions >> remove_pre_assigned_restrictions >> \
            get_assigned_supervisory_org_child_levels_uri >> assign_supervisory_orgs >> log_project_process
        if_supervisory_org_level_path_present >> rail.Label('No') >> log_supervisory_org_level_path_missing >> log_project_process

        log_project_process >> catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
