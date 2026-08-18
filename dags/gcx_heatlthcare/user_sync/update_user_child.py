import rail
from gcx_heatlthcare.user_sync.utils import request_payload, response_payload
from datetime import datetime, timedelta
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

# pylint: disable=too-many-statements

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dag_id,
        description=f"GCX UPDATE USER {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='has_mandatory_fields'
            )

        batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='has_mandatory_fields',
                end_task='catch_and_log_errors',
            )

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.mandatory_fields_check,
            yes_task="is_is_manager_value_present",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message='\
                {%- if dag_run.conf.employee_id | is_falsy -%} \
                    Employee id is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.employee_first_name | is_falsy -%} \
                    first name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.employee_last_name | is_falsy -%} \
                    last name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.email | is_falsy -%} \
                    email is not present in payload, \
                {%- endif -%}',
            severity='Exception',
            properties={
                'employeeid': "{{dag_run.conf.employee_id}}",
                'first_name': "{{dag_run.conf.employee_first_name}}",
                'last_name': "{{dag_run.conf.employee_last_name}}",
                'action': "Update",
                'status': "Exception",
                'details': '\
                    {%- if dag_run.conf.employee_id | is_falsy -%} \
                        Employee id is not present in payload, \
                    {%- endif -%}\
                    {%- if dag_run.conf.employee_first_name | is_falsy -%} \
                        first name is not present in payload, \
                    {%- endif -%}\
                    {%- if dag_run.conf.employee_last_name | is_falsy -%} \
                        last name is not present in payload, \
                    {%- endif -%}\
                    {%- if dag_run.conf.email | is_falsy -%} \
                        email is not present in payload, \
                    {%- endif -%}',
                'jobid': "{{ dag_run_ecid() }}"
            }
        )

        is_is_manager_value_present = rail.IfOperator(
            task_id="is_is_manager_value_present",
            test='{{dag_run.conf.manager | is_falsy}}',
            yes_task="search_user_details",
            no_task="search_manager_user"
        )

        search_user_details = rail.RepliconServiceOperator(
            task_id="search_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_user_payload,
            data_handler=response_payload.get_user_details
        )

        get_effective_user_group_membership = rail.RepliconServiceOperator(
                task_id='get_effective_user_group_membership',
                endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
                data=request_payload.get_effective_user_group_membership_payload,
                data_handler=response_payload.get_effectivegroup_membership_filter
        )

        search_manager_user = rail.RepliconServiceOperator(
            task_id="search_manager_user",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data=request_payload.get_search_user_payload,
            data_handler=response_payload.get_user_details_permission
        )

        if_manager_present = rail.IfOperator(
            task_id='if_manager_present',
            test=lambda: bool(rail.result('search_manager_user')),
            yes_task="check_manager_status",
            no_task="search_user_details"  # CHANGED: Continue to search_user_details instead of logging exception
        )
        
        check_manager_status = rail.IfOperator(
            task_id='check_manager_status',
            test=lambda: rail.result('search_manager_user')[0]['status'] == True,
            yes_task="if_manager_as_supervisor",
            no_task="log_supervisor_disbaled"
        )

        log_supervisor_disbaled = rail.WriteLogOperator(
                task_id='log_supervisor_disbaled',
                message="User update is skipped since supervisor disabled in Replicon",
                log='{{dag_run.conf.log}}',
                severity='Exception',
                properties=lambda dag_run:{
                    'employeeid': dag_run.conf['employee_id'],
                    'first_name': dag_run.conf['employee_first_name'],
                    'last_name': dag_run.conf['employee_last_name'],
                    'action': "Update",
                    'status': "Exception",
                    'details': "User update is skipped since supervisor disabled in Replicon",
                    'jobid': get_dagrun_ecid(dag_run)
                }
            )

        if_manager_as_supervisor = rail.IfOperator(
            task_id='if_manager_as_supervisor',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('search_manager_user')[0]['permissionSets'], 'displayText', 'Supervisor')),
            yes_task="search_user_details",
            no_task="assign_supervisor_perminssion"
        )

        assign_supervisor_perminssion = rail.RepliconServiceOperator(
            task_id="assign_supervisor_perminssion",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.assign_supervisor_permission_payload
        )

        update_user = rail.RepliconServiceOperator(
            task_id="update_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run, **kwargs: request_payload.update_user_payload(dag_run, config)
        )

        update_to_log_success = rail.WriteLogOperator(
                task_id='update_to_log_success',
                message="User Updated successfully",
                log='{{dag_run.conf.log}}',
                severity='Success',
                properties=lambda dag_run:{
                    'employeeid': dag_run.conf['employee_id'],
                    'first_name': dag_run.conf['employee_first_name'],
                    'last_name': dag_run.conf['employee_last_name'],
                    'action': "Update",
                    'status': "Success",
                    'details': "User Updated successfully",
                    'jobid': get_dagrun_ecid(dag_run)
                }
            )

        catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log='{{dag_run.conf.log}}',
                severity='Error',
                message='{{ get_error_message() }}',
                properties=lambda dag_run:{
                    'employeeid': dag_run.conf['employee_id'],
                    'first_name': dag_run.conf['employee_first_name'],
                    'last_name': dag_run.conf['employee_last_name'],
                    'action': "Update",
                    'status': "Error",
                    'details': '{{ get_error_message() }}',
                    'jobid': get_dagrun_ecid(dag_run)
                },
            )

        # MODIFIED WORKFLOW - Removed supervisor not found validation, kept other validations
        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label('No') >> has_mandatory_fields
        has_mandatory_fields >> rail.Label("Yes") >> is_is_manager_value_present >> rail.Label("No") >> search_manager_user >> if_manager_present >> rail.Label("Yes") >> check_manager_status >> rail.Label("Yes") >> if_manager_as_supervisor >> rail.Label('No') >> assign_supervisor_perminssion >> search_user_details >> get_effective_user_group_membership >> update_user

        if_manager_present >> rail.Label("No") >> search_user_details  # CHANGED: Continue to search_user_details instead of error
        check_manager_status >> rail.Label("No") >> log_supervisor_disbaled >> catch_and_log_errors

        is_is_manager_value_present >> rail.Label("Yes") >> search_user_details >> get_effective_user_group_membership >> update_user

        if_manager_as_supervisor >> rail.Label('Yes') >> search_user_details >> get_effective_user_group_membership >> update_user >> update_to_log_success >> catch_and_log_errors
        has_mandatory_fields >> rail.Label("No") >> log_madatory_fields_not_present >> catch_and_log_errors

    return dag

rail.for_each_instance(create_child_dag)