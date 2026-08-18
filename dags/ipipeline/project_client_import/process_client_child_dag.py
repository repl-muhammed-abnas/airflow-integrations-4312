# Child DAG for processing individual client records
# Handles creation and updates of Replicon Clients from Salesforce Accounts

from datetime import timedelta
from airflow.models import Variable
import rail
from ipipeline.project_client_import.utils import request_payload, custom_methods


def create_process_client_child_dag(config):
    """
    Create child DAG for processing individual client records
    """
    with rail.create_airflow_dag(
        dag_id=config.process_client_child_dag_id,
        description=f'iPipeline Project Client Import Process Client child {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        # View incoming configuration
        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_conf'
        )

        # Check if batch task can run based on Airflow Variable
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_client_log'
        )

        # Batch task to optimize processing in batches
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            start_task='create_client_log',
            end_task='catch_and_log_error',
        )

        # Create log for client processing
        create_client_log = rail.CreateLogOperator(
            task_id='create_client_log'
        )

        check_client_manager_in_feed = rail.IfOperator(
            task_id='check_client_manager_in_feed',
            test=lambda dag_run: bool(dag_run.conf['OwnerId']),
            yes_task='get_client_manager_email_from_salesforce',
            no_task='check_action_to_be_done'
        )

        get_client_manager_email_from_salesforce = rail.SalesforceQueryOperator2(
            task_id="get_client_manager_email_from_salesforce",
            salesforce_conn_id=config.salesforce_conn_id,
            query=lambda dag_run: request_payload.soql_query_for_user_lookup(
                dag_run.conf['OwnerId']),
            data_handler=lambda res: request_payload.get_user_email_from_payload(
                res)
        )

        # Failsafe check
        check_client_manager_present_in_sf = rail.IfOperator(
            task_id='check_client_manager_present_in_sf',
            test=lambda: rail.result(
                'get_client_manager_email_from_salesforce'),
            yes_task='get_client_manager_details',
            no_task='check_action_to_be_done'
        )

        get_client_manager_details = rail.RepliconServiceOperator(
            task_id="get_client_manager_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=lambda: {
                "users": [
                    {
                        "loginName": rail.result('get_client_manager_email_from_salesforce')
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: {
                'cm_uri': response[0]['userDetails']['uri'],
                # client manager permission is under Project Manager Permission set
                'project_manager_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'name', 'Project Manager', 'name', '') if response[0]['permissionSets'] else '',
                'project_manager_admin_permission': rail.find_first_by_attr_and_get_attr(
                    response[0]['permissionSets'], 'name', 'Project Manager Admin', 'name', '') if response[0]['permissionSets'] else '',
                'status': response[0]['userDetails']['isEnabled'],
            }if response else {}
        )

        if_cm_not_found_or_cm_permission_not_available_or_cm_disabled = rail.IfOperator(
            task_id='if_cm_not_found_or_cm_permission_not_available_or_cm_disabled',
            test=lambda: not (rail.result('get_client_manager_details')) or not (rail.result(
                'get_client_manager_details')['project_manager_permission'] or rail.result(
                    'get_client_manager_details')['project_manager_admin_permission']) or str(rail.result(
                        'get_client_manager_details')['status']).lower() == 'false',
            yes_task='log_cm_not_found_or_cm_permission_not_found_or_cm_disabled',
            no_task='check_action_to_be_done'
        )

        log_cm_not_found_or_cm_permission_not_found_or_cm_disabled = rail.PythonOperator(
            task_id='log_cm_not_found_or_cm_permission_not_found_or_cm_disabled',
            python_callable=lambda: ("Client manager permission not assigned to client manager in replicon" if (not (rail.result(
                'get_client_manager_details')['project_manager_permission']) or not (rail.result(
                    'get_client_manager_details')['project_manager_admin_permission'])) else "Client manager is disabled in replicon") if rail.result(
                        'get_client_manager_details') else 'Client manager not found in replicon'
        )

        # Determine if this is an update or create operation based on client_uri presence
        check_action_to_be_done = rail.IfOperator(
            task_id='check_action_to_be_done',
            test=lambda dag_run: bool(dag_run.conf.get('client_uri')),
            yes_task='get_client_details',
            no_task='validate_fields_for_client_creation'
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id='get_client_details',
            endpoint='/services/ClientService1.svc/GetClientDetails',
            data={
                "clientUri": "{{dag_run.conf.client_uri}}"
            }
        )

        update_client = rail.RepliconServiceOperator(
            task_id='update_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_update_client_payload
        )

        validate_fields_for_client_creation = rail.PythonOperator(
            task_id='validate_fields_for_client_creation',
            python_callable=lambda dag_run: custom_methods.validate_fields(
                dag_run, config.MANDATORY_FIELDS_NEW_CLIENT)
        )

        if_invalid_fields = rail.IfOperator(
            task_id='if_invalid_fields',
            test=lambda: rail.result(
                'validate_fields_for_client_creation'),
            yes_task='log_exception_for_invalid_fields',
            no_task='create_client'
        )

        log_exception_for_invalid_fields = rail.WriteLogOperator(
            task_id='log_exception_for_invalid_fields',
            log='{{ result("create_client_log") }}',
            severity='Exception',
            message='na',
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'account',
                'action': 'Add',
                'status': 'Exception',
                'details': 'Client not created ; ' + ' ; '.join(rail.result('validate_fields_for_client_creation'))
            }
        )

        # Create new client in Replicon
        create_client = rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_payload
        )

        # Log successful client processing
        log_client_process_completion = rail.WriteLogOperator(
            task_id='log_client_process_completion',
            log='{{ result("create_client_log") }}',
            severity=lambda dag_run: custom_methods.get_processing_details(
                'Client', 'status', dag_run.conf.get('client_uri')),
            message="Client Processing Completed",
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'account',
                'action': 'Update' if dag_run.conf.get('client_uri') else 'Add',
                'status': custom_methods.get_processing_details('Client', 'status', dag_run.conf.get('client_uri')),
                'details': custom_methods.get_processing_details('Client', 'details', dag_run.conf.get('client_uri')),
            },
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log='{{ result("create_client_log") }}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'name': dag_run.conf.get('Name'),
                'id': dag_run.conf.get('Id'),
                'type': 'account',
                'action': 'Update' if dag_run.conf.get('client_uri') else 'Add',
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        # Define workflow dependencies
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_client_log

        create_client_log >> check_client_manager_in_feed

        check_client_manager_in_feed >> rail.Label(
            "No") >> check_action_to_be_done

        check_client_manager_in_feed >> rail.Label(
            "Yes") >> get_client_manager_email_from_salesforce >> check_client_manager_present_in_sf

        check_client_manager_present_in_sf >> rail.Label(
            "Yes") >> get_client_manager_details
        check_client_manager_present_in_sf >> rail.Label(
            "No") >> check_action_to_be_done

        get_client_manager_details >> if_cm_not_found_or_cm_permission_not_available_or_cm_disabled

        if_cm_not_found_or_cm_permission_not_available_or_cm_disabled >> rail.Label(
            "Yes") >> log_cm_not_found_or_cm_permission_not_found_or_cm_disabled >> check_action_to_be_done
        if_cm_not_found_or_cm_permission_not_available_or_cm_disabled >> rail.Label(
            "No") >> check_action_to_be_done

        check_action_to_be_done >> rail.Label(
            "Update") >> get_client_details >> update_client >> log_client_process_completion >> catch_and_log_error
        check_action_to_be_done >> rail.Label(
            "Add") >> validate_fields_for_client_creation >> if_invalid_fields

        if_invalid_fields >> rail.Label(
            "No") >> create_client >> log_client_process_completion >> catch_and_log_error
        if_invalid_fields >> rail.Label(
            "Yes") >> log_exception_for_invalid_fields >> catch_and_log_error

    return dag


rail.for_each_instance(create_process_client_child_dag)
