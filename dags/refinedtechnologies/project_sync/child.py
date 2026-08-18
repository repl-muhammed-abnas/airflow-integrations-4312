from datetime import timedelta
import rail
from airflow.models import Variable
from refinedtechnologies.project_sync.utils import custom_function, request_payload, request_query


def create_child_dag(config):
    """Child DAG that processes one Salesforce opportunity into a Replicon project."""
    with rail.create_airflow_dag(
        dag_id=config.process_project_child_dag_id,
        description='Refined Technologies Project Sync - Process Opportunity Child DAG',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        view_config = rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        # Batch the whole flow into one task when the toggle Variable is enabled.
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='prepare_salesforce_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='prepare_salesforce_data',
            end_task='log_to_sumo',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        # Wrap the opportunity record in the {'records': [...]} structure downstream expects.
        prepare_salesforce_data = rail.PythonOperator(
            task_id='prepare_salesforce_data',
            python_callable=lambda dag_run: {
                'records': [dag_run.conf.get('opportunity_record', {})],
                'totalSize': 1
            }
        )

        check_opportunity_exists = rail.IfOperator(
            task_id='check_opportunity_exists',
            test=lambda: custom_function.safe_get_salesforce_record(
                rail.result('prepare_salesforce_data')
            ) is not None,
            yes_task='search_user_in_salesforce',
            no_task='no_opportunity_found'
        )

        no_opportunity_found = rail.WriteLogOperator(
            task_id='no_opportunity_found',
            message="No valid opportunity record found in dag_run.conf",
            severity='Error'
        )

        search_user_in_salesforce = rail.SalesforceQueryOperator2(
            task_id='search_user_in_salesforce',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.search_user_in_salesforce_query(
                rail.result("prepare_salesforce_data")
            ),
        )

        # Fetch the Account (by AccountId) for the name-change check.
        search_account = rail.SalesforceQueryOperator2(
            task_id='search_account',
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda: request_query.account_by_id_query(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {}
            ),
        )

        search_project = rail.RepliconServiceOperator(
            task_id='search_project',
            endpoint="/services/ProjectService1.svc/BulkGetProjectDetails2",
            data=lambda: request_payload.search_project_payload(
                rail.result("prepare_salesforce_data")
            ),
        )

        check_project_exists = rail.IfOperator(
            task_id='check_project_exists',
            test=lambda: custom_function.project_exists(rail.result("search_project")),
            no_task='project_not_found',
            yes_task='check_project_name_or_desc_changed'
        )

        project_not_found = rail.EmptyOperator(
            task_id='project_not_found',
        )

        # Project name/description differs -> update it; unchanged -> skip the update.
        check_project_name_or_desc_changed = rail.IfOperator(
            task_id='check_project_name_or_desc_changed',
            test=lambda: custom_function.project_name_or_desc_changed(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {},
                rail.result("search_project")[0]
            ),
            no_task='skip_project_update',
            yes_task='update_project_name_and_desc'
        )

        update_project_name_and_desc = rail.RepliconServiceOperator(
            task_id='update_project_name_and_desc',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: request_payload.update_project_branch_payload(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {},
                rail.result("search_project")[0]['projectDetails']
            ),
        )

        skip_project_update = rail.EmptyOperator(
            task_id='skip_project_update'
        )

        check_account_name_changed = rail.IfOperator(
            task_id='check_account_name_changed',
            test=lambda: custom_function.account_name_changed(
                rail.result("search_account"),
                rail.result("search_project")[0]['projectDetails']
            ),
            no_task='skip_client_reassignment',
            yes_task='check_account_has_legacy_id'
        )

        check_account_has_legacy_id = rail.IfOperator(
            task_id='check_account_has_legacy_id',
            test=lambda: custom_function.check_account_name_legacy_id(
                rail.result("search_account")
            ),
            no_task='account_legacy_id_missing',
            yes_task='trigger_searchclients_replicon'
        )

        trigger_searchclients_replicon = rail.TriggerDagRunOperator(
            task_id='trigger_searchclients_replicon',
            trigger_dag_id=config.search_client_replicon_child_dag_id,
            conf=lambda: {
                "salesforce_data": rail.result('prepare_salesforce_data')
            },
            wait_for_completion=True
        )

        gather_searchclients_result = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_searchclients_result',
            dag_runs='{{ result("trigger_searchclients_replicon") }}',
            dagrun_task_id='return_result'
        )

        account_legacy_id_missing = rail.EmptyOperator(
            task_id='account_legacy_id_missing'
        )

        check_client_resolved = rail.IfOperator(
            task_id='check_client_resolved',
            test=lambda: custom_function.client_uri_check(
                rail.result("gather_searchclients_result")[0] if rail.result("gather_searchclients_result") else {}
            ),
            no_task='skip_client_reassignment',
            yes_task='reassign_project_client'
        )

        reassign_project_client = rail.RepliconServiceOperator(
            task_id='reassign_project_client',
            endpoint="/services/ProjectService1.svc/ApplyNewClient2",
            data=lambda: request_payload.update_client_payload(
                rail.result("search_project")[0]['projectDetails'],
                rail.result("gather_searchclients_result")[0] if rail.result("gather_searchclients_result") else {}
            ),
        )

        search_replicon_user_update = rail.RepliconServiceOperator(
            task_id='search_replicon_user_update',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.search_user_payload(
                rail.result("search_user_in_salesforce")
            ),
        )

        get_owner_uri_for_update = rail.PythonOperator(
            task_id='get_owner_uri_for_update',
            python_callable=lambda: custom_function.extract_uri_from_rows(
                rail.result("search_replicon_user_update"),
                rail.result("search_user_in_salesforce")
            )
        )

        check_owner_uri_for_update = rail.IfOperator(
            task_id='check_owner_uri_for_update',
            test=lambda: len(rail.result("get_owner_uri_for_update")) > 0,
            no_task='skip_co_manager_update',
            yes_task='assign_co_manager_update'
        )

        assign_co_manager_update = rail.RepliconServiceOperator(
            task_id='assign_co_manager_update',
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: request_payload.update_co_manager_payload(
                rail.result("search_project")[0].get('projectDetails'),
                rail.result("get_owner_uri_for_update")
            ),
        )

        skip_co_manager_update = rail.EmptyOperator(
            task_id='skip_co_manager_update'
        )

        get_clients_by_code = rail.RepliconServiceOperator(
            task_id='get_clients_by_code',
            endpoint="/services/ClientListService1.svc/GetData",
            data=lambda: request_payload.search_client_by_code_payload(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {}
            ),
            data_handler=custom_function.convert_ruby_data_to_list
        )

        check_existing_client_matches = rail.IfOperator(
            task_id='check_existing_client_matches',
            test=lambda: custom_function.has_matching_client(
                rail.result("get_clients_by_code"),
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {}
            ),
            no_task='existing_client_not_matched',
            yes_task='create_project_with_matched_client'
        )

        create_project_with_matched_client = rail.RepliconServiceOperator(
            task_id='create_project_with_matched_client',
            endpoint="/services/ProjectService1.svc/PutProject5",
            data=lambda: request_payload.create_project_payload(
                rail.result("prepare_salesforce_data"),
                rail.result("get_clients_by_code")
            ),
        )

        existing_client_not_matched = rail.EmptyOperator(
            task_id="existing_client_not_matched"
        )

        # AccountId & legacy present -> resolve client via sub-child; else create without client.
        check_account_for_client_lookup = rail.IfOperator(
            task_id='check_account_for_client_lookup',
            test=lambda: custom_function.check_facility_legacy_id(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {}
            ),
            yes_task='resolve_client_via_subchild',
            no_task='create_project_no_client'
        )

        resolve_client_via_subchild = rail.TriggerDagRunOperator(
            task_id='resolve_client_via_subchild',
            trigger_dag_id=config.search_client_replicon_child_dag_id,
            conf=lambda: {
                "salesforce_data": rail.result('prepare_salesforce_data')
            },
            wait_for_completion=True
        )

        gather_facility_result = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_facility_result',
            dag_runs='{{ result("resolve_client_via_subchild") }}',
            dagrun_task_id='return_result'
        )

        prepare_create_project_payload = rail.PythonOperator(
            task_id='prepare_create_project_payload',
            python_callable=lambda ti: {
                'salesforce_data': rail.result("prepare_salesforce_data"),
                'facility_result': rail.result("gather_facility_result")[0] if ti.xcom_pull(task_ids='gather_facility_result') is not None else None
            }
        )

        create_project_with_resolved_client = rail.RepliconServiceOperator(
            task_id='create_project_with_resolved_client',
            endpoint="/services/ProjectService1.svc/PutProject5",
            data=lambda: request_payload.create_project_payload_condition(
                custom_function.safe_get_salesforce_record(
                    rail.result("prepare_create_project_payload")['salesforce_data']
                ) or {},
                rail.result("prepare_create_project_payload")['facility_result']
            ),
        )

        # Create the project without a client.
        create_project_no_client = rail.RepliconServiceOperator(
            task_id='create_project_no_client',
            endpoint="/services/ProjectService1.svc/PutProject5",
            data=lambda: request_payload.create_project_payload_condition(
                custom_function.safe_get_salesforce_record(rail.result("prepare_salesforce_data")) or {},
                None
            ),
        )

        search_replicon_user_create = rail.RepliconServiceOperator(
            task_id='search_replicon_user_create',
            endpoint="/services/UserListService1.svc/GetData",
            data=lambda: request_payload.search_user_payload(
                rail.result("search_user_in_salesforce")
            ),
        )

        get_owner_uri_for_create = rail.PythonOperator(
            task_id='get_owner_uri_for_create',
            python_callable=lambda: custom_function.extract_uri_from_rows(
                rail.result("search_replicon_user_create"),
                rail.result("search_user_in_salesforce")
            )
        )

        check_owner_uri_for_create = rail.IfOperator(
            task_id='check_owner_uri_for_create',
            test=lambda: len(
                rail.result("get_owner_uri_for_create")
            ) > 0,
            no_task='skip_co_manager_create',
            yes_task='prepare_co_manager_payload'
        )

        prepare_co_manager_payload = rail.PythonOperator(
            task_id='prepare_co_manager_payload',
            python_callable=lambda ti: {
                'project_data': (
                    rail.result("create_project_with_matched_client")
                    if ti.xcom_pull(task_ids='create_project_with_matched_client') is not None
                    else rail.result("create_project_with_resolved_client")
                    if ti.xcom_pull(task_ids='create_project_with_resolved_client') is not None
                    else rail.result("create_project_no_client")
                ),
                'user_uri': rail.result("get_owner_uri_for_create")
            }
        )

        assign_co_manager_create = rail.RepliconServiceOperator(
            task_id='assign_co_manager_create',
            endpoint="/services/ProjectService1.svc/PutExplicitSharingAssignments",
            data=lambda: request_payload.update_co_manager_payload(
                rail.result("prepare_co_manager_payload")['project_data'],
                rail.result("prepare_co_manager_payload")['user_uri']
            ),
        )

        skip_co_manager_create = rail.EmptyOperator(
            task_id='skip_co_manager_create'
        )

        child_success = rail.WriteLogOperator(
            task_id='child_success',
            message="Opportunity processed successfully",
            severity='Success'
        )

        skip_client_reassignment = rail.EmptyOperator(
            task_id='skip_client_reassignment'
        )

        # Terminal task / batch end boundary.
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label("No") >> prepare_salesforce_data

        prepare_salesforce_data >> check_opportunity_exists
        check_opportunity_exists >> rail.Label("Yes") >> search_user_in_salesforce >> search_account >> search_project >> check_project_exists
        check_opportunity_exists >> rail.Label("No") >> no_opportunity_found >> log_to_sumo

        check_project_exists >> rail.Label("No") >> project_not_found
        check_project_exists >> rail.Label("Yes") >> check_project_name_or_desc_changed

        check_project_name_or_desc_changed >> rail.Label("Yes") >> update_project_name_and_desc >> check_account_name_changed
        check_project_name_or_desc_changed >> rail.Label("No") >> skip_project_update >> check_account_name_changed
        check_account_name_changed >> rail.Label("Yes") >> check_account_has_legacy_id
        check_account_has_legacy_id >> rail.Label("Yes") >> trigger_searchclients_replicon >> gather_searchclients_result >> check_client_resolved
        check_account_has_legacy_id >> rail.Label("No") >> account_legacy_id_missing >> check_client_resolved

        check_client_resolved >> rail.Label("Yes") >> reassign_project_client >> search_replicon_user_update
        check_client_resolved >> rail.Label("No") >> skip_client_reassignment

        check_account_name_changed >> rail.Label("No") >> skip_client_reassignment >> search_replicon_user_update

        search_replicon_user_update >> get_owner_uri_for_update >> check_owner_uri_for_update
        check_owner_uri_for_update >> rail.Label("Yes") >> assign_co_manager_update >> child_success
        check_owner_uri_for_update >> rail.Label("No") >> skip_co_manager_update >> child_success

        project_not_found >> get_clients_by_code >> check_existing_client_matches
        check_existing_client_matches >> rail.Label("Yes") >> create_project_with_matched_client >> search_replicon_user_create
        check_existing_client_matches >> rail.Label("No") >> existing_client_not_matched

        existing_client_not_matched >> check_account_for_client_lookup
        check_account_for_client_lookup >> rail.Label("Yes") >> resolve_client_via_subchild >> gather_facility_result >> prepare_create_project_payload >> create_project_with_resolved_client >> search_replicon_user_create
        check_account_for_client_lookup >> rail.Label("No") >> create_project_no_client >> search_replicon_user_create

        search_replicon_user_create >> get_owner_uri_for_create >> check_owner_uri_for_create
        check_owner_uri_for_create >> rail.Label("Yes") >> prepare_co_manager_payload >> assign_co_manager_create >> child_success
        check_owner_uri_for_create >> rail.Label("No") >> skip_co_manager_create >> child_success

        child_success >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag)
