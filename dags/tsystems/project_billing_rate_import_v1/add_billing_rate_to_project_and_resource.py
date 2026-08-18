"""
T-Systems Project Billing Rate Import - Add Billing Rate to Project and Resource DAG

This DAG handles the assignment of billing rates to projects and/or users (resources)
after the billing rates have been created or updated.
"""

from airflow.models import Variable
from datetime import timedelta
import rail
from tsystems.project_billing_rate_import_v1.utils import custom_methods
from tsystems.project_billing_rate_import_v1.utils import request_payload

null = None


def create_add_billing_rate_to_project_and_resource_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.add_billing_rate_to_project_and_resource_dag_id,
        description=f'T-Systems Project Billing Rate Import Add to Project and Resource Child {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dag_run_config'
        )

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

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ImportService1.svc/BulkGetProjects2',
            data=lambda dag_run: {
                "projects": [{
                    "code": dag_run.conf['Project_ID']
                }]
            },
            data_handler=custom_methods.get_required_project_details
        )

        if_project_found_in_replicon = rail.IfOperator(
            task_id='if_project_found_in_replicon',
            test=lambda: rail.result('get_project_details'),
            yes_task='if_project_billing_type_is_not_time_and_materials',
            no_task='log_project_not_found_in_replicon'
        )

        log_project_not_found_in_replicon = rail.PythonOperator(
            task_id='log_project_not_found_in_replicon',
            python_callable=lambda: "Project not found in Replicon",
        )

        if_project_billing_type_is_not_time_and_materials = rail.IfOperator(
            task_id='if_project_billing_type_is_not_time_and_materials',
            test=lambda dag_run: rail.result(
                'get_project_details')['project_billing_type'] != 'Time & Materials',
            yes_task='log_project_is_not_time_and_materials',
            no_task='if_billing_rate_is_already_assigned_to_project'
        )

        log_project_is_not_time_and_materials = rail.PythonOperator(
            task_id='log_project_is_not_time_and_materials',
            python_callable=lambda: "Billing Type of Project is not Time & Materials",
        )

        if_billing_rate_is_already_assigned_to_project = rail.IfOperator(
            task_id='if_billing_rate_is_already_assigned_to_project',
            test=lambda dag_run: dag_run.conf['billing_rate_uri'] in [x['uri'] for x in rail.result(
                'get_project_details')['existing_billing_rates']],
            yes_task='if_billing_rate_amount_updated',
            no_task='assign_billing_rate_to_project'
        )

        assign_billing_rate_to_project = rail.RepliconServiceOperator(
            task_id='assign_billing_rate_to_project',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/UpdateBillingRateIsAvailableForAssignmentToTeamMembers',
            data=request_payload.get_assign_billing_rate_payload
        )

        if_billing_rate_amount_updated = rail.IfOperator(
            task_id='if_billing_rate_amount_updated',
            test=lambda dag_run: dag_run.conf.get(
                'billing_rate_amount_updated').lower() == 'true',
            yes_task='get_project_billing_rate_schedule_entries',
            no_task='check_if_ciam_id_in_config'
        )

        get_project_billing_rate_schedule_entries = rail.RepliconServiceOperator(
            task_id='get_project_billing_rate_schedule_entries',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/GetProjectBillingRateSchedule',
            data=lambda dag_run: {
                "projectUri": rail.result('get_project_details')["project_uri"],
                "companyBillingRateUri": dag_run.conf["billing_rate_uri"],
            },
            data_handler=lambda res: custom_methods.get_project_billing_rate_schedule(
                res)
        )

        update_billing_rate_amount_in_project = rail.RepliconServiceOperator(
            task_id='update_billing_rate_amount_in_project',
            endpoint='/services/TimeAndMaterialsProjectService1.svc/PutBillingRateSchedule',
            data=lambda dag_run: custom_methods.get_update_billing_rate_amount_in_project_payload(
                dag_run, rail.result('get_project_billing_rate_schedule_entries'))
        )

        log_billing_rate_updated_in_project_successfully = rail.PythonOperator(
            task_id='log_billing_rate_updated_in_project_successfully',
            python_callable=lambda: "Billing rate is updated in project successfully"
        )

        log_billing_rate_assigned_to_project_successfully = rail.PythonOperator(
            task_id='log_billing_rate_assigned_to_project_successfully',
            python_callable=lambda: "Billing rate is assigned to project successfully"
        )

        check_if_ciam_id_in_config = rail.IfOperator(
            task_id='check_if_ciam_id_in_config',
            test=lambda dag_run: dag_run.conf['CIAM_ID'],
            yes_task='get_user_uri',
            no_task='log_add_entry_final'
        )

        get_user_uri = rail.RepliconServiceOperator(
            task_id='get_user_uri',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [{
                    "uri": null,
                    "loginName": null,
                    "employeeId": "{{dag_run.conf.CIAM_ID}}",
                    "parameterCorrelationId": null
                }],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda res: res[0]['userDetails']['uri'] if res else [
            ]
        )

        if_user_found_in_replicon = rail.IfOperator(
            task_id='if_user_found_in_replicon',
            test=lambda: rail.result('get_user_uri'),
            yes_task="if_resource_present_in_project",
            no_task="log_user_not_found_in_replicon"
        )

        log_user_not_found_in_replicon = rail.PythonOperator(
            task_id="log_user_not_found_in_replicon",
            python_callable=lambda: "User not found in Replicon",
        )

        if_resource_present_in_project = rail.IfOperator(
            task_id='if_resource_present_in_project',
            test=lambda: rail.result('get_user_uri') in [x['uri'] for x in rail.result(
                'get_project_details')['resources_assigned_to_project']],
            yes_task="get_existing_billing_rates_uris_for_resource_in_project",
            no_task="log_user_not_assigned_to_project"
        )

        log_user_not_assigned_to_project = rail.PythonOperator(
            task_id="log_user_not_assigned_to_project",
            python_callable=lambda: "User is not a resource assigned to the project",
        )

        get_existing_billing_rates_uris_for_resource_in_project = rail.RepliconServiceOperator(
            task_id='get_existing_billing_rates_uris_for_resource_in_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectTeamMemberDetailsForProjects',
            data=lambda dag_run: {
                "resourceUri": rail.result('get_user_uri'),
                "projectUris": [rail.result('get_project_details')['project_uri']],
                "asOfDate": rail.parse_date(dag_run.conf['run_date_time'], "%Y-%m-%dT%H:%M:%S%z")
            },
            data_handler=lambda response: [x['billingRate']['uri'] for x in response[0]['teamMemberDetails']['billingRatesAllowedForBillingTime']] if (
                response and response[0]['teamMemberDetails'] and response[0]['teamMemberDetails']['billingRatesAllowedForBillingTime']) else []
        )

        check_if_billing_rate_is_not_assigned_to_resource = rail.IfOperator(
            task_id='check_if_billing_rate_is_not_assigned_to_resource',
            test=lambda dag_run: dag_run.conf['billing_rate_uri'] not in rail.result(
                'get_existing_billing_rates_uris_for_resource_in_project'),
            yes_task='assign_billing_rate_to_resource',
            no_task='log_add_entry_final'
        )

        assign_billing_rate_to_resource = rail.RepliconServiceOperator(
            task_id="assign_billing_rate_to_resource",
            endpoint='/services/ImportService1.svc/BulkUpdateProjectTeamMembersBillingRatesAllowedForBillingTime',
            data=request_payload.get_assign_billing_rate_to_resource_payload
        )

        log_billing_rate_assigned_to_resource_successfully = rail.PythonOperator(
            task_id='log_billing_rate_assigned_to_resource_successfully',
            python_callable=lambda: "Billing Rate is assigned to the resource successfully"
        )

        log_add_entry_final = rail.WriteLogOperator(
            task_id='log_add_entry_final',
            log="{{dag_run.conf.log}}",
            message="na",
            severity=lambda dag_run: custom_methods.get_billing_rate_add_update_project_and_resource_log_details(
                dag_run)['status'],
            properties=custom_methods.get_billing_rate_add_update_project_and_resource_log_details
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log="{{dag_run.conf.log}}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda dag_run: {
                "billing_rate_id": dag_run.conf['Billing_Rate_ID'],
                "billing_rate_name": dag_run.conf['Billing_Rate_Name'],
                "project_id": dag_run.conf['Project_ID'],
                "ciam_id": dag_run.conf['CIAM_ID'],
                "action": dag_run.conf['operation_type'],
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> get_project_details

        get_project_details >> if_project_found_in_replicon

        if_project_found_in_replicon >> rail.Label(
            'No') >> log_project_not_found_in_replicon >> log_add_entry_final
        if_project_found_in_replicon >> rail.Label(
            'Yes') >> if_project_billing_type_is_not_time_and_materials

        if_project_billing_type_is_not_time_and_materials >> rail.Label(
            'Yes') >> log_project_is_not_time_and_materials >> log_add_entry_final
        if_project_billing_type_is_not_time_and_materials >> rail.Label(
            'No') >> if_billing_rate_is_already_assigned_to_project

        if_billing_rate_is_already_assigned_to_project >> rail.Label(
            'No') >> assign_billing_rate_to_project >> log_billing_rate_assigned_to_project_successfully >> check_if_ciam_id_in_config

        if_billing_rate_is_already_assigned_to_project >> rail.Label(
            'Yes') >> if_billing_rate_amount_updated

        if_billing_rate_amount_updated >> rail.Label(
            'Yes') >> get_project_billing_rate_schedule_entries >> update_billing_rate_amount_in_project \
            >> log_billing_rate_updated_in_project_successfully >> check_if_ciam_id_in_config
        if_billing_rate_amount_updated >> rail.Label(
            'No') >> check_if_ciam_id_in_config
        check_if_ciam_id_in_config >> rail.Label('No') >> log_add_entry_final
        check_if_ciam_id_in_config >> rail.Label(
            'Yes') >> get_user_uri >> if_user_found_in_replicon

        if_user_found_in_replicon >> rail.Label(
            'No') >> log_user_not_found_in_replicon >> log_add_entry_final
        if_user_found_in_replicon >> rail.Label(
            'Yes') >> if_resource_present_in_project

        if_resource_present_in_project >> rail.Label(
            'No') >> log_user_not_assigned_to_project >> log_add_entry_final
        if_resource_present_in_project >> rail.Label(
            'Yes') >> get_existing_billing_rates_uris_for_resource_in_project >> check_if_billing_rate_is_not_assigned_to_resource

        check_if_billing_rate_is_not_assigned_to_resource >> rail.Label(
            'Yes') >> assign_billing_rate_to_resource >> log_billing_rate_assigned_to_resource_successfully >> log_add_entry_final
        check_if_billing_rate_is_not_assigned_to_resource >> rail.Label(
            'No') >> log_add_entry_final

        log_add_entry_final >> catch_and_log_errors

    return dag


# Create DAG instances for each environment
rail.for_each_instance(create_add_billing_rate_to_project_and_resource_dag)
