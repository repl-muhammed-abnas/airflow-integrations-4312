from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_non_live.utils import request_payload

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_users_dagid,
        description='CRL - User Import - Process New Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_new_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_enddate_available'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='is_enddate_available',
            end_task='catch_and_log_errors',
        )

        is_enddate_available = rail.IfOperator(
            task_id ='is_enddate_available',
            test = lambda dag_run: bool(dag_run.conf['end_date']),
            yes_task="log_endate_exception",
            no_task="add_new_user"
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = "User not Created, as End Date present while User Creation",
            severity='Exception',
            properties = {
                'employee_id': '{{dag_run.conf.emp_id}}',
                'first_name': '{{dag_run.conf.first_name}}',
                'last_name': '{{dag_run.conf.last_name}}',
                "action": "Validation",
                "status": "Exception",
                'details': "User not Created, as End Date present while User Creation"
            }
        )

        add_new_user = rail.RepliconServiceOperator(
            task_id="add_new_user",
            endpoint="/services/importService1.svc/PutUser3",
            data=request_payload.get_put_user_payload
        )

        remove_timeoff_assignments = rail.RepliconServiceOperator(
            task_id="remove_timeoff_assignments",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda:{
                "userUri": rail.result('add_new_user')['uri'],
                "timeOffTypeUris": []
            }
        )

        is_hrbp_yes = rail.IfOperator(
            task_id = "is_hrbp_yes",
            test=lambda dag_run: dag_run.conf['is_hrbp'] == 'Y',
            yes_task="assign_scope_for_hrbp_permissions",
            no_task="log_user_completion"
        )

        assign_scope_for_hrbp_permissions = rail.RepliconServiceOperator(
            task_id='assign_scope_for_hrbp_permissions',
            endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
            data=request_payload.assign_policyDataAccessScopes_to_projectmanager
        )

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            message="User Added Succesfully",
            severity="Success",
            properties=lambda dag_run: {
                "employee_id": dag_run.conf['emp_id'],
                "last_name": dag_run.conf['last_name'],
                "first_name": dag_run.conf['first_name'],
                "action": "Add",
                "status": "Success",
                'details': "User Added Succesfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}",
            properties={
                'employee_id': '{{dag_run.conf.emp_id}}',
                "last_name": "{{dag_run.conf.last_name}}",
                "first_name": "{{dag_run.conf.first_name}}",
                "action": "Add",
                'status': 'Error',
                'details': "\
                {%- if get_task_state('add_new_user') == 'success' -%} \
                    User Added Partially; {{ get_error_message() }}\
                {%- else -%}\
                    User not created; {{ get_error_message() }}\
                {%- endif -%}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> is_enddate_available

        is_enddate_available >> rail.Label('Yes') >> log_endate_exception >> catch_and_log_errors
        is_enddate_available >> rail.Label('No') >> add_new_user

        add_new_user >> remove_timeoff_assignments >> is_hrbp_yes
        is_hrbp_yes >> rail.Label("No") >> log_user_completion
        is_hrbp_yes >> rail.Label("Yes") >> assign_scope_for_hrbp_permissions >> log_user_completion

        log_user_completion >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
