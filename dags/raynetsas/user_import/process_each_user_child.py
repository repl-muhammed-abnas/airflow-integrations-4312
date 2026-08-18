from datetime import timedelta
from airflow.models import Variable
import rail
from raynetsas.user_import.utils import request_payload

def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.USER_BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_each_user_dagid}_batch_{idx}",
            description='Raynet SAS - User Import - Process New Users',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_runs_child,
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='is_valid_user'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='is_valid_user',
                end_task='catch_and_log_errors',
            )

            is_valid_user = rail.IfOperator(
                task_id = 'is_valid_user',
                test= '{{ dag_run.conf.process_user == "yes" }}',
                yes_task= 'get_user_details',
                no_task= 'log_user_exception'
            )

            log_user_exception = rail.WriteLogOperator(
                task_id='log_user_exception',
                log='{{ dag_run.conf.user_log }}',
                message='User is skipped since the received country is not available in the mapper',
                severity='Success',
                properties=lambda dag_run: {
                    "email": dag_run.conf['email'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "country": dag_run.conf['country'],
                    "action": "Validation",
                    "status": 'Skipped',
                    'details': 'User is skipped since the received country is not available in the mapper'
                }
            )

            get_user_details = rail.RepliconServiceOperator(
                task_id="get_user_details",
                endpoint="/services/ImportService1.svc/BulkGetUsers3",
                data={
                    "users": [
                        {
                            "loginName": "{{ dag_run.conf.email }}",
                        }
                    ],
                    "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
                }
            )

            is_user_found = rail.IfOperator(
                task_id="is_user_found",
                test="{{ result('get_user_details') | is_truthy }}",
                yes_task="log_exception",
                no_task="add_new_user"
            )

            add_new_user = rail.RepliconServiceOperator(
                task_id="add_new_user",
                endpoint="/services/importService1.svc/PutUser3",
                data=request_payload.get_put_user_payload
            )

            assign_product_licences = rail.RepliconServiceOperator(
                task_id = 'assign_product_licences',
                endpoint= '/services/AccountManagementService1.svc/help/test/PutProductAssignmentsForUser',
                data=lambda dag_run: {
                    "userUri": rail.result("add_new_user")['uri'],
                    "productUris": dag_run.conf['license_uris']
                }
            )

            remove_timeoff_assignment = rail.RepliconServiceOperator(
                task_id="remove_timeoff_assignment",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda: {
                    "userUri": rail.result("add_new_user")['uri'],
                    "timeOffTypeUris": []
                }
            )

            put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
                task_id="put_timeoff_assignment_for_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data= request_payload.put_timeoff_assignment_for_user
            )

            log_success = rail.WriteLogOperator(
                task_id='log_success',
                log='{{ dag_run.conf.user_log }}',
                message='User Created Successfully',
                severity='Success',
                properties=lambda dag_run: {
                    "email": dag_run.conf['email'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "country": dag_run.conf['country'],
                    "action": "Add",
                    "status": 'Success',
                    'details': 'User Created Successfully'
                }
            )

            log_exception = rail.WriteLogOperator(
                task_id='log_exception',
                log='{{ dag_run.conf.user_log }}',
                message='User is skipped since the user is already available in replicon',
                severity='Success',
                properties=lambda dag_run: {
                    "email": dag_run.conf['email'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "country": dag_run.conf['country'],
                    "action": "Validation",
                    "status": 'Skipped',
                    'details': 'User is skipped since the user is already available in replicon'
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
                    'email': '{{dag_run.conf.email}}',
                    "last_name": "{{dag_run.conf.last_name}}",
                    "first_name": "{{dag_run.conf.first_name}}",
                    "country": "{{dag_run.conf.country}}",
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
            can_run_batch_task >> rail.Label(
                'No') >> is_valid_user

            is_valid_user >> rail.Label(
                "Yes") >> get_user_details >> is_user_found

            is_valid_user >> rail.Label(
                "No") >> log_user_exception >> catch_and_log_errors

            is_user_found >> rail.Label(
                "Yes") >> log_exception >> catch_and_log_errors

            is_user_found >> rail.Label(
                "No") >> add_new_user >> assign_product_licences >> remove_timeoff_assignment >>\
                    put_timeoff_assignment_for_user >> log_success >> catch_and_log_errors >> log_to_sumo

        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_child_dag)
