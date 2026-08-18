"""
Child DAG - process a single Oracle project into Replicon Polaris.

Triggered once per project by the master DAG. Applies the status + CUSP-POC gates,
upserts the managed project and its WBS task hierarchy (all levels, add-or-update by
name), writes project/task OEFs, and creates role placeholders for the planned resource
groups. All processed and skipped projects are written to the shared run log for the
audit trail.
"""
from datetime import timedelta

import rail
from airflow.models import Variable

from azenta.oracle_project_sync.utils import custom_methods, request_payload, response_filter

# pylint: disable=expression-not-assigned,pointless-statement

ACTIVE_STATUSES = {'ACTIVE', 'PENDING_CLOSE'}


def create_process_project_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_project_dag_id,
        description=f'Azenta Oracle->Polaris process project ({config.instance})',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'execution_timeout': timedelta(days=config.execution_timeout_days),
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_conf')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_project_detail',
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_project_detail',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # -- Oracle project detail -------------------------------------------
        get_project_detail = rail.SimpleHttpOperator(
            task_id='get_project_detail',
            method='GET',
            http_conn_id=config.oracle_conn_id,
            endpoint=request_payload.oracle_project_detail_endpoint(config.ORACLE_API_BASE),
            headers={'Accept': 'application/json'},
            retries=config.oracle_api_retries,
            response_filter=lambda response: response.json(),
        )

        # -- Status gate ------------------------------------------------------
        is_skip_status = rail.IfOperator(
            task_id='is_skip_status',
            test=lambda: custom_methods.should_skip_status(
                rail.get_current_context()['dag_run'].conf.get('ProjectStatusCode')),
            yes_task='log_status_excluded',
            no_task='get_classifications',
        )

        # -- CUSP-POC classification gate ------------------------------------
        get_classifications = rail.PythonOperator(
            task_id='get_classifications',
            retries=config.oracle_api_retries,
            python_callable=lambda: custom_methods.fetch_oracle_paginated(
                config.oracle_conn_id,
                request_payload.oracle_classifications_endpoint(
                    config.ORACLE_API_BASE,
                    rail.get_current_context()['dag_run'].conf.get('ProjectId'))),
        )

        is_cusp_poc = rail.IfOperator(
            task_id='is_cusp_poc',
            test=lambda: custom_methods.has_required_classification(
                rail.result('get_classifications')),
            yes_task='get_project_team_members',
            no_task='log_classification_excluded',
        )

        # -- PM resolution: Oracle team members -> Polaris user + permission --
        get_project_team_members = rail.PythonOperator(
            task_id='get_project_team_members',
            retries=config.oracle_api_retries,
            python_callable=lambda: custom_methods.fetch_oracle_paginated(
                config.oracle_conn_id,
                request_payload.oracle_project_team_members_endpoint(
                    config.ORACLE_API_BASE,
                    rail.get_current_context()['dag_run'].conf.get('ProjectId'))),
        )

        pick_pm_email = rail.PythonOperator(
            task_id='pick_pm_email',
            python_callable=lambda: custom_methods.pick_active_project_manager_email(
                rail.result('get_project_team_members')),
        )

        has_pm_email = rail.IfOperator(
            task_id='has_pm_email',
            test=lambda: bool(rail.result('pick_pm_email')),
            yes_task='search_pm_user',
            no_task='log_pm_missing',
        )

        search_pm_user = rail.RepliconServiceOperator(
            task_id='search_pm_user',
            endpoint='/services/UserListService1.svc/GetData',
            data=request_payload.search_user_by_login_payload,
            data_handler=custom_methods.pick_user_uri_from_user_list,
        )

        has_pm_user = rail.IfOperator(
            task_id='has_pm_user',
            test=lambda: bool(rail.result('search_pm_user')),
            yes_task='get_pm_permissions',
            no_task='log_pm_missing',
        )

        get_pm_permissions = rail.RepliconServiceOperator(
            task_id='get_pm_permissions',
            endpoint='/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2',
            data=request_payload.get_assigned_permissions_payload,
        )

        pm_has_permission = rail.IfOperator(
            task_id='pm_has_permission',
            test=lambda: custom_methods.pm_has_required_permission(
                rail.result('get_pm_permissions')),
            yes_task='mark_pm_ok',
            no_task='log_pm_missing',
        )

        mark_pm_ok = rail.PythonOperator(
            task_id='mark_pm_ok',
            python_callable=lambda: rail.result('search_pm_user'),
        )

        log_pm_missing = rail.EmptyOperator(
            task_id='log_pm_missing',
            trigger_rule='none_failed_min_one_success',
        )

        # Converge: PM uri from the success path, else None (project upserts without a leader).
        resolve_pm = rail.PythonOperator(
            task_id='resolve_pm',
            trigger_rule='none_failed_min_one_success',
            python_callable=lambda: rail.result('mark_pm_ok'),
        )

        # -- Project OEF definitions (uris needed to write objectExtensionFieldsToApply) --
        get_project_oef_details = rail.RepliconServiceOperator(
            task_id='get_project_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={'bindingContextUri': 'urn:replicon:object-type:project'},
        )

        # -- Project lookup by Oracle Project Id OEF (drives create-vs-update / ADD-only) -----
        get_project_by_oracle_project_id = rail.RepliconServiceOperator(
            task_id='get_project_by_oracle_project_id',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.get_project_by_oracle_project_id_payload,
            data_handler=custom_methods.pick_project_from_list_response,
        )

        # -- Upsert managed project (REST) -----------------------------------
        upsert_project = rail.RepliconServiceOperator(
            task_id='upsert_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.build_project_modifications,
        )

        is_active_status = rail.IfOperator(
            task_id='is_active_status',
            test=lambda: (rail.get_current_context()['dag_run'].conf.get(
                'ProjectStatusCode') or '').upper() in ACTIVE_STATUSES,
            yes_task='get_project_tasks',
            no_task='check_pm_resolved',
        )

        # -- Tasks (fetch all levels; create the whole WBS via the task child DAG) -------
        get_project_tasks = rail.PythonOperator(
            task_id='get_project_tasks',
            retries=config.oracle_api_retries,
            python_callable=lambda: custom_methods.fetch_oracle_paginated(
                config.oracle_conn_id,
                request_payload.oracle_tasks_endpoint(
                    config.ORACLE_API_BASE,
                    rail.get_current_context()['dag_run'].conf.get('ProjectId'))),
        )

        parse_tasks = rail.PythonOperator(
            task_id='parse_tasks',
            python_callable=lambda: (rail.result('get_project_tasks') or {}).get('items', []),
        )

        has_tasks = rail.IfOperator(
            task_id='has_tasks',
            test=lambda: len(rail.result('parse_tasks')) > 0,
            yes_task='trigger_process_project_tasks',
            no_task='check_pm_resolved',
        )

        trigger_process_project_tasks = rail.TriggerDagRunOperator(
            task_id='trigger_process_project_tasks',
            trigger_dag_id=config.process_project_tasks_dag_id,
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'ProjectId': dag_run.conf.get('ProjectId'),
                'ProjectNumber': dag_run.conf.get('ProjectNumber'),
                'project_uri': rail.result('upsert_project').get('uri'),
                'tasks': rail.result('parse_tasks'),
                'log': dag_run.conf.get('log'),
            },
        )

        wait_for_process_task_completion = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task_completion',
            dag_runs='{{ result("trigger_process_project_tasks") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        get_replicon_tasks = rail.RepliconServiceOperator(
            task_id='get_replicon_tasks',
            endpoint='/services/TaskService1.svc/GetDescendantTaskDetails',
            data=lambda: {'parentUri': (rail.result('upsert_project') or {}).get('uri')},
            data_handler=custom_methods.flatten_replicon_tasks,
        )

        # -- Planned hours ----------------------------------------------------
        get_financial_plan_version = rail.PythonOperator(
            task_id='get_financial_plan_version',
            retries=config.oracle_api_retries,
            python_callable=lambda: response_filter.pick_financial_plan_version_id(
                custom_methods.fetch_oracle_paginated(
                    config.oracle_conn_id,
                    request_payload.oracle_financial_plans_endpoint(
                        config.ORACLE_API_BASE,
                        rail.get_current_context()['dag_run'].conf.get('ProjectId')))),
        )

        has_plan_version = rail.IfOperator(
            task_id='has_plan_version',
            test=lambda: bool(rail.result('get_financial_plan_version')),
            yes_task='get_plan_assignments',
            no_task='check_pm_resolved',
        )

        get_plan_assignments = rail.PythonOperator(
            task_id='get_plan_assignments',
            retries=config.oracle_api_retries,
            python_callable=lambda: response_filter.flatten_planning_rows(
                custom_methods.fetch_oracle_paginated(
                    config.oracle_conn_id,
                    request_payload.oracle_plan_assignments_endpoint(
                        config.ORACLE_API_BASE,
                        rail.result('get_financial_plan_version')))),
        )

        prepare_resourcing = rail.PythonOperator(
            task_id='prepare_resourcing',
            python_callable=lambda: custom_methods.prepare_resourcing(
                rail.result('get_plan_assignments'),
                rail.result('get_replicon_tasks'),
            ),
        )

        # -- Task OEF definitions (uri needed to write the Resource Groups OEF) --
        get_task_oef_details = rail.RepliconServiceOperator(
            task_id='get_task_oef_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={'bindingContextUri': 'urn:replicon:object-type:task'},
        )

        # -- Task Resource Groups OEF (pipe-separated, incl. FSE) ------------
        set_resource_group_oefs = rail.RepliconServiceCallForEachItemOperator(
            task_id='set_resource_group_oefs',
            endpoint='graphql',
            app='polaris',
            items=lambda: rail.result('prepare_resourcing')['placeholder_oef_items'],
            data=lambda item: request_payload.put_task_resource_groups_oef_mutation(item),
        )

        # -- Role placeholders (get-or-create role, then estimate) -----------
        for_each_placeholder = rail.ForEachOperator(
            task_id='for_each_placeholder',
            items=lambda: rail.result('prepare_resourcing')['placeholder_items'],
            start_task='ph_get_roles',
            end_task='ph_end',
        )

        ph_get_roles = rail.RepliconServiceOperator(
            task_id='ph_get_roles',
            endpoint='/services/ProjectRoleService1.svc/GetActiveRoles',
            data=None,
            response_filter=custom_methods.extract_roles_from_response,
        )

        ph_resolve_role_uri = rail.PythonOperator(
            task_id='ph_resolve_role_uri',
            python_callable=lambda: custom_methods.find_role_uri_by_name(
                rail.result('ph_get_roles'),
                rail.result('for_each_placeholder')['resource_name'],
            ),
        )

        ph_has_role = rail.IfOperator(
            task_id='ph_has_role',
            test=lambda: bool(rail.result('ph_resolve_role_uri')),
            yes_task='ph_get_task_estimates',
            no_task='ph_create_role',
        )

        ph_create_role = rail.RepliconServiceOperator(
            task_id='ph_create_role',
            endpoint='/services/ProjectRoleService1.svc/PutProjectRole',
            data=request_payload.put_project_role_payload,
        )

        ph_get_task_estimates = rail.RepliconServiceOperator(
            task_id='ph_get_task_estimates',
            trigger_rule='none_failed_min_one_success',
            endpoint='graphql',
            app='polaris',
            data=request_payload.get_task_resource_estimates_query,
        )

        ph_should_update_estimate = rail.IfOperator(
            task_id='ph_should_update_estimate',
            trigger_rule='none_failed_min_one_success',
            test=lambda: custom_methods.estimate_is_updatable(
                rail.result('ph_get_task_estimates'),
                rail.result('ph_resolve_role_uri') or (rail.result('ph_create_role') or {}).get('uri'),
            ),
            yes_task='ph_put_estimate',
            no_task='ph_end',
        )

        ph_put_estimate = rail.RepliconServiceOperator(
            task_id='ph_put_estimate',
            trigger_rule='none_failed_min_one_success',
            endpoint='graphql',
            app='polaris',
            data=request_payload.put_task_resource_estimate_mutation,
        )

        ph_end = rail.EmptyOperator(
            task_id='ph_end',
            trigger_rule='none_failed_min_one_success',
        )

        # -- Cleanup: remove orphaned task resource estimates ----------
        get_active_roles_for_cleanup = rail.RepliconServiceOperator(
            task_id='get_active_roles_for_cleanup',
            endpoint='/services/ProjectRoleService1.svc/GetActiveRoles',
            data=None,
            response_filter=custom_methods.extract_roles_from_response,
        )

        get_task_estimates_for_cleanup = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_task_estimates_for_cleanup',
            endpoint='graphql',
            app='polaris',
            items=lambda: custom_methods.all_task_uris(rail.result('get_replicon_tasks')),
            data=lambda item: request_payload.get_task_resource_estimates_query_by_task(item),
        )

        find_orphaned_estimates = rail.PythonOperator(
            task_id='find_orphaned_estimates',
            python_callable=lambda: custom_methods.find_orphaned_resource_estimates(
                rail.result('prepare_resourcing')['placeholder_items'],
                rail.result('get_active_roles_for_cleanup'),
                custom_methods.all_task_uris(rail.result('get_replicon_tasks')),
               rail.result('get_task_estimates_for_cleanup'),
            ),
        )

        has_orphaned_estimates = rail.IfOperator(
            task_id='has_orphaned_estimates',
            test=lambda: bool(rail.result('find_orphaned_estimates')),
            yes_task='ph_remove_orphaned_estimates',
            no_task='check_pm_resolved',
        )

        ph_remove_orphaned_estimates = rail.RepliconServiceCallForEachItemOperator(
            task_id='ph_remove_orphaned_estimates',
            endpoint='graphql',
            app='polaris',
            items=lambda: rail.result('find_orphaned_estimates'),
            data=lambda item: request_payload.remove_task_resource_estimate_mutation(item),
        )

        # -- Logging ----------------------------------------------------------
        log_status_excluded = rail.WriteLogOperator(
            task_id='log_status_excluded',
            trigger_rule='none_failed_min_one_success',
            log="{{ dag_run.conf.log }}",
            severity='Exception',
            message='Project excluded from sync: ineligible Oracle status',
            properties=request_payload.status_excluded_log_properties,
        )

        log_classification_excluded = rail.WriteLogOperator(
            task_id='log_classification_excluded',
            trigger_rule='none_failed_min_one_success',
            log="{{ dag_run.conf.log }}",
            severity='Exception',
            message='Project excluded from sync: missing CUSP-POC classification',
            properties=request_payload.classification_excluded_log_properties,
        )

        check_pm_resolved = rail.IfOperator(
            task_id='check_pm_resolved',
            trigger_rule='none_failed_min_one_success',
            test=lambda: bool(rail.result('resolve_pm')),
            yes_task='log_success',
            no_task='log_pm_missing_final',
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log="{{ dag_run.conf.log }}",
            severity='Success',
            message='Project synced successfully',
            properties=request_payload.success_log_properties,
        )

        log_pm_missing_final = rail.WriteLogOperator(
            task_id='log_pm_missing_final',
            log="{{ dag_run.conf.log }}",
            severity='Exception',
            message='Project synced but project manager was not assigned',
            properties=request_payload.pm_missing_log_properties,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            severity='Error',
            message='Project sync failed: {{ get_error_message() }}',
            properties=request_payload.error_log_properties,
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        # -- Wiring -----------------------------------------------------------
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_project_detail

        get_project_detail >> is_skip_status
        is_skip_status >> rail.Label('Yes') >> log_status_excluded
        is_skip_status >> rail.Label('No') >> get_classifications

        get_classifications >> is_cusp_poc
        is_cusp_poc >> rail.Label('No') >> log_classification_excluded
        is_cusp_poc >> rail.Label('Yes') >> get_project_team_members >> pick_pm_email >> has_pm_email

        has_pm_email >> rail.Label('Yes') >> search_pm_user >> has_pm_user
        has_pm_email >> rail.Label('No') >> log_pm_missing
        has_pm_user >> rail.Label('Yes') >> get_pm_permissions >> pm_has_permission
        has_pm_user >> rail.Label('No') >> log_pm_missing
        pm_has_permission >> rail.Label('Yes') >> mark_pm_ok
        pm_has_permission >> rail.Label('No') >> log_pm_missing

        [mark_pm_ok, log_pm_missing] >> resolve_pm >> get_project_oef_details \
            >> get_project_by_oracle_project_id >> upsert_project >> is_active_status

        is_active_status >> rail.Label('No') >> check_pm_resolved
        is_active_status >> rail.Label('Yes') >> get_project_tasks >> parse_tasks >> has_tasks

        has_tasks >> rail.Label('No') >> check_pm_resolved
        has_tasks >> rail.Label('Yes') >> trigger_process_project_tasks >> wait_for_process_task_completion \
            >> get_replicon_tasks >> get_financial_plan_version >> has_plan_version

        has_plan_version >> rail.Label('No') >> check_pm_resolved
        has_plan_version >> rail.Label('Yes') >> get_plan_assignments >> prepare_resourcing \
            >> get_task_oef_details >> set_resource_group_oefs >> for_each_placeholder

        for_each_placeholder >> ph_get_roles >> ph_resolve_role_uri >> ph_has_role
        ph_has_role >> rail.Label('Yes') >> ph_get_task_estimates >> ph_should_update_estimate
        ph_has_role >> rail.Label('No') >> ph_create_role >> ph_get_task_estimates
        ph_should_update_estimate >> rail.Label('Yes') >> ph_put_estimate >> ph_end
        ph_should_update_estimate >> rail.Label('No') >> ph_end
        for_each_placeholder >> ph_end >> get_active_roles_for_cleanup >> get_task_estimates_for_cleanup \
            >> find_orphaned_estimates >> has_orphaned_estimates
        has_orphaned_estimates >> rail.Label('Yes') >> ph_remove_orphaned_estimates >> check_pm_resolved
        has_orphaned_estimates >> rail.Label('No') >> check_pm_resolved

        check_pm_resolved >> rail.Label('Yes') >> log_success
        check_pm_resolved >> rail.Label('No') >> log_pm_missing_final

        [log_status_excluded, log_classification_excluded, log_success, log_pm_missing_final] >> catch_and_log_errors >> log_to_sumo

        return dag


rail.for_each_instance(create_process_project_dag)
