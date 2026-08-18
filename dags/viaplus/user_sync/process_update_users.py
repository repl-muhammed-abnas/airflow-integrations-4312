"""
ViaPlus User Sync - Process Update Users Child DAG

This child DAG handles updates to existing users in Replicon.
It updates the following (based on changes from Keka):
1. Profile fields (Name, Display Name, Middle Name)
2. Supervisor assignment (with effective date = today)
3. Project Role / Job Title (with effective date = today)
4. Group memberships (Location, Department, Legal Entity)

Note: Email, Login Name, Employee ID, and Authentication ID are NOT updated.

Matches CRL user_import_ireland_v1 pattern.
"""
from datetime import timedelta
from uuid import uuid4
from airflow.models import Variable
import rail

from viaplus.user_sync.utils import request_payload,response_filter
from viaplus.user_sync.tasks.process_supervisor import process_supervisor_assignment_task_group

null = None


# pylint: disable=too-many-statements
def create_child_dag(config):
    """Create the process_update_users child DAGs (one per batch)."""
    update_dags = []

    for idx in range(0, config.BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_update_users_dagid}_batch_{idx + 1}",
            description='ViaPlus User Sync - Process Update Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_process_update_users,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            # ================================================================
            # Batch Task Control
            # ================================================================
            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name,
                    default_var='true'
                ).lower() == 'true',
                yes_task='batch_task',
                no_task='get_current_user_info'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(days=config.execution_timeout_days),
                start_task='get_current_user_info',
                end_task='catch_and_log_errors',
            )


            # ================================================================
            # Get Current User Info from Replicon
            # ================================================================
            get_current_user_info = rail.RepliconServiceOperator(
                task_id='get_current_user_info',
                endpoint='/services/ImportService1.svc/BulkGetUsers3',
                data=lambda dag_run: {
                    "users": [{
                        "uri": dag_run.conf.get('user_uri'),
                        "loginName": null,
                        "parameterCorrelationId": null
                    }],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
                },
                data_handler=lambda res: res[0] if res else None
            )

            # ================================================================
            # Check if User Exists
            # ================================================================
            is_end_date_present = rail.IfOperator(
                task_id='is_end_date_present',
                test=lambda dag_run: bool(dag_run.conf['end_date']),
                yes_task='is_end_date_valid',
                no_task='get_effective_user_groupmembership'
            )

           
            # ================================================================
            # Validate End Date
            # ================================================================
            def check_end_date_valid(dag_run):
                """Check if end date is valid and after start date."""
                exit_date = dag_run.conf.get('end_date')
                joining_date = dag_run.conf.get('start_date')

                if not exit_date:
                    return False

                exit_replicon = request_payload.get_replicon_date(exit_date)
                joining_replicon = request_payload.get_replicon_date(joining_date)

                if not exit_replicon:
                    return False

                if joining_replicon:
                    exit_dt = request_payload.get_date_from_replicon_date(exit_replicon)
                    joining_dt = request_payload.get_date_from_replicon_date(joining_replicon)
                    if exit_dt < joining_dt:
                        return False

                return True

            is_end_date_valid = rail.IfOperator(
                task_id='is_end_date_valid',
                test=check_end_date_valid,
                yes_task='update_end_date',
                no_task='log_end_date_exception'
            )

            log_end_date_exception = rail.WriteLogOperator(
                task_id='log_end_date_exception',
                log='{{ dag_run.conf.user_log }}',
                message=lambda dag_run: f"Invalid end date for user {dag_run.conf.get('emp_id')}",
                severity='Exception',
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf.get('emp_id', ''),
                    "first_name": dag_run.conf.get('first_name', ''),
                    "last_name": dag_run.conf.get('last_name', ''),
                    "action": "Disable",
                    "status": "Exception",
                    "details": "End date is invalid or prior to start date"
                }
            )

            # ================================================================
            # Update End Date
            # ================================================================
            update_end_date = rail.RepliconServiceOperator(
                task_id='update_end_date',
                endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
                data=request_payload.get_update_end_date_payload
            )

            # ================================================================
            # Check if End Date is in Future
            # ================================================================

            is_end_date_in_future = rail.IfOperator(
                task_id='is_end_date_in_future',
                test=request_payload.is_end_date_in_future,
                yes_task='log_future_end_date',
                no_task='disable_login'
            )

            log_future_end_date = rail.WriteLogOperator(
                task_id='log_future_end_date',
                log='{{ dag_run.conf.user_log }}',
                message=lambda dag_run: f"User end date set to future - will be disabled post end date",
                severity='Exception',
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf.get('emp_id', ''),
                    "first_name": dag_run.conf.get('first_name', ''),
                    "last_name": dag_run.conf.get('last_name', ''),
                    "action": "Disable",
                    "status": "Exception",
                    "details": f"User end date set to future - will be disabled post end date"
                }
            )

             # ================================================================
            # Disable Login
            # ================================================================
            disable_login = rail.RepliconServiceOperator(
                task_id='disable_login',
                endpoint='/services/SecurityService1.svc/DisableLogin',
                data=lambda dag_run: {
                    "userUri": dag_run.conf.get('user_uri')
                }
            )

            # ================================================================
            # Remove Licenses
            # ================================================================
            remove_licenses = rail.RepliconServiceOperator(
                task_id='remove_licenses',
                endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
                data=lambda dag_run: {
                    "target": {
                        "uri": dag_run.conf.get('user_uri')
                    },
                    "modifications": {
                        "products": [
                        {
                            "modificationOptionUri": "urn:replicon:collection-modification-option:remove",
                            "items": dag_run.conf['license_uris']
                        }
                        ]
                    },
                    "userModificationOptionUri": "urn:replicon:user-modification-option:save",
                    "unitOfWorkId": str(uuid4())
                    }
            )

            # ================================================================
            # Log DISABLE Completion
            # ================================================================
            log_disable_user_completion = rail.WriteLogOperator(
                task_id='log_disable_user_completion',
                log='{{ dag_run.conf.user_log }}',
                message=request_payload.get_disable_user_message(),
                severity='Success',
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf.get('emp_id', ''),
                    "first_name": dag_run.conf.get('first_name', ''),
                    "last_name": dag_run.conf.get('last_name', ''),
                    "action": "Disable",
                    "status": "Success",
                    "details": request_payload.get_disable_user_message()
                }
            )
            # ================================================================
            # Get Current Group Memberships
            # ================================================================
            get_effective_user_groupmembership = rail.RepliconServiceOperator(
                task_id='get_effective_user_groupmembership',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data=lambda dag_run: {
                    "userUri": dag_run.conf.get('user_uri'),
                    "dateRange": null
                },
                data_handler=response_filter.get_effective_user_groupmembership_filter
            )

            def assigned_timeoffs_types_to_user(response):
                if not response:
                    return None
                return list(map(lambda item: item['timeOffType']['displayText'], response['policiesByTimeOffType']))
            
            get_user_time_off_policy_summary = rail.RepliconServiceOperator(
                task_id="get_user_time_off_policy_summary",
                endpoint="/services/TimeOffPolicyService2.svc/GetUserTimeOffTypePolicySummary",
                data={
                    "userUri": "{{ dag_run.conf.user_uri }}"
                },
                data_handler= assigned_timeoffs_types_to_user
            )

            def assigned_project_role(response):
                if not response:
                    return null
                return response[-1]['projectRoles'][0]['projectRole']['displayText']

            get_user_project_role = rail.RepliconServiceOperator(
                task_id="get_user_project_role",
                endpoint="/services/ResourceService1.svc/GetProjectRoleAssignmentScheduleForUser",
                data={
                    "userUri": "{{ dag_run.conf.user_uri }}"
                },
                data_handler= assigned_project_role
            )

            apply_user_modifications = rail.RepliconServiceOperator(
                task_id='apply_user_modifications',
                endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
                data=lambda dag_run:request_payload.apply_user_modifications_payload(dag_run,config)
            )

            # ================================================================
            # Check and Update Supervisor
            # ================================================================
            is_supervisor_in_payload = rail.IfOperator(
                task_id='is_supervisor_in_payload',
                test=lambda dag_run: bool(dag_run.conf.get('sup_emp_id')),
                yes_task='search_supervisor_in_replicon',
                no_task='log_user_completion'
            )

            # ================================================================
            # Process Supervisor Assignment TaskGroup
            # ================================================================
            process_supervisor_entry, process_supervisor_exit = process_supervisor_assignment_task_group(
                'user_uri', 'update_user')

            # ================================================================
            # Log Completion
            # ================================================================
            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log='{{ dag_run.conf.user_log }}',
                message=request_payload.get_update_user_message,
                severity=request_payload.get_update_user_severity,
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf.get('emp_id', ''),
                    "first_name": dag_run.conf.get('first_name', ''),
                    "last_name": dag_run.conf.get('last_name', ''),
                    "action": "Update",
                    "status": request_payload.get_update_user_severity(dag_run),
                    "details": request_payload.get_update_user_message(dag_run)
                }
            )

            # ================================================================
            # Error Handling
            # ================================================================
            catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                log='{{ dag_run.conf.user_log }}',
                trigger_rule='one_failed',
                severity='Error',
                message='{{ get_error_message() }}',
                properties={
                    "employee_id": "{{ dag_run.conf.emp_id }}",
                    "first_name": "{{ dag_run.conf.first_name }}",
                    "last_name": "{{ dag_run.conf.last_name }}",
                    "action": "Update",
                    "status": "Error",
                    "details": '{{ get_error_message() }}'
                }
            )

            # ================================================================
            # Task Dependencies 
            # ================================================================
            can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> get_current_user_info

            get_current_user_info >> is_end_date_present >> rail.Label('Yes') >> is_end_date_valid >> rail.Label('Yes') >> update_end_date >> is_end_date_in_future
            is_end_date_in_future >> rail.Label('Yes') >> log_future_end_date >> catch_and_log_errors
            is_end_date_in_future >> rail.Label('No') >> disable_login >> remove_licenses >> log_disable_user_completion >> catch_and_log_errors
            is_end_date_valid >> rail.Label('No') >> log_end_date_exception >> catch_and_log_errors

            is_end_date_present >> rail.Label('No') >> get_effective_user_groupmembership >> get_user_time_off_policy_summary >> get_user_project_role >> apply_user_modifications
            apply_user_modifications >> is_supervisor_in_payload >> rail.Label('No') >> log_user_completion
            is_supervisor_in_payload >> rail.Label('Yes') >> process_supervisor_entry
            process_supervisor_exit >> log_user_completion

            log_user_completion >> catch_and_log_errors

        update_dags.append(dag)

    return update_dags


rail.for_each_instance(create_child_dag)
