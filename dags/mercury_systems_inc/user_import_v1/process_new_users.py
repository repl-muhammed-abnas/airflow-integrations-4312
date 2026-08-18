from datetime import timedelta
from airflow.models import Variable
import rail

from mercury_systems_inc.user_import_v1.utils import request_payload, custom_methods
from mercury_systems_inc.user_import_v1.task_groups.process_supervisor import process_supervisor_assignment_task_group

null = None

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.process_new_user_dagid,
        description='MercurySystemsInc User Import Process New Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_new_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_status_inactive'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_status_inactive',
            end_task='finish',
        )

        # this is just a secondary check to ensure that employee status is not in the disable list
        is_status_inactive = rail.IfOperator(
            task_id='is_status_inactive',
            test=lambda dag_run: dag_run.conf['Emp_Status'] in config.DISABLE_STATUS,
            yes_task="log_inactive_status",
            no_task="dummy_process_supervisor_validation"
        )

        log_inactive_status = rail.WriteLogOperator(
            task_id='log_inactive_status',
            log='{{ dag_run.conf.user_log }}',
            message="User not Created, as Employee Status is :'{{ dag_run.conf.Emp_Status}}'",
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                "action": "Validation",
                "status": "Exception",
                'details': f"User not Created, as Employee Status is :'{dag_run.conf['Emp_Status']}'",
            }
        )

        dummy_process_supervisor_validation = rail.EmptyOperator(
            task_id='dummy_process_supervisor_validation',
        )

        process_supervisor_entry,  process_supervisor_exit = process_supervisor_assignment_task_group(
            'new_user')

        validate_replicon_fields = rail.PythonOperator(
            task_id='validate_replicon_fields',
            python_callable=custom_methods.validate_replicon_field_names_uris
        )

        is_validation_successful = rail.IfOperator(
            task_id='is_validation_successful',
            test=lambda: rail.result("validate_replicon_fields")["is_valid"],
            yes_task='add_new_user',
            no_task='log_validation_errors'
        )

        log_validation_errors = rail.WriteLogOperator(
            task_id='log_validation_errors',
            log='{{ dag_run.conf.user_log }}',
            message=lambda: '; '.join(rail.result(
                'validate_replicon_fields')['missing_fields']),
            severity='Exception',
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                'action': 'Validation',
                'status': 'Exception',
                'details': "User not created ; " + ' ; '.join(rail.result('validate_replicon_fields')['missing_fields'])
            }
        )

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_create_user_payload(
                dag_run, config)
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            message="User Added Successfully",
            severity="Success",
            properties=lambda dag_run: {
                'employee_id': dag_run.conf['Employee_ID'],
                'first_name': dag_run.conf['First_Name'],
                'last_name': dag_run.conf['Last_Name'],
                "action": "Add",
                "status": "Success",
                'details': "User Added Successfully"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}",
            properties={
                'employee_id': '{{dag_run.conf.Employee_ID}}',
                "first_name": "{{dag_run.conf.First_Name}}",
                "last_name": "{{dag_run.conf.Last_Name}}",
                "action": "Add",
                'status': 'Error',
                'details': "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> is_status_inactive

        is_status_inactive >> rail.Label(
            'Yes') >> log_inactive_status >> finish
        is_status_inactive >> rail.Label(
            'No') >> dummy_process_supervisor_validation

        dummy_process_supervisor_validation >> process_supervisor_entry

        process_supervisor_exit >> validate_replicon_fields >> is_validation_successful

        is_validation_successful >> rail.Label('Yes') >> add_new_user
        is_validation_successful >> rail.Label(
            'No') >> log_validation_errors >> finish

        add_new_user >> log_user_completion

        # process_supervisor_entry >> process_supervisor_exit >>

        log_user_completion >> finish >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
