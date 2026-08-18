from datetime import timedelta
from airflow.models import Variable
import rail

from crl.user_import_mauritius_v2.utils import request_payload, python_callable_methods
from crl.user_import_mauritius_v2.tasks.process_supervisor import process_supervisor_assignment_task_group

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    add_dags = []

    for idx in range(0, config.BATCH_COUNT):
        with rail.create_airflow_dag(
            dag_id=f"{config.process_new_users_dagid}_batch_{idx+1}",
            description='CRL - User Import Mauritius- Process New Users',
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
                no_task='is_status_inactive'
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='is_status_inactive',
                end_task='catch_and_log_errors',
            )

            is_status_inactive = rail.IfOperator(
                task_id ='is_status_inactive',
                test = lambda dag_run: dag_run.conf['emp_status'] in config.DISABLE_STATUS,
                yes_task="log_inactive_status",
                no_task="is_enddate_available"
            )

            log_inactive_status = rail.WriteLogOperator(
                task_id = 'log_inactive_status',
                log = '{{ dag_run.conf.user_log }}',
                message = "User not Created, as {{ dag_run.conf.emp_status}} Employee Status present while user creation",
                severity='Exception',
                properties = {
                    'employee_id': '{{dag_run.conf.emp_id}}',
                    'first_name': '{{dag_run.conf.first_name}}',
                    'last_name': '{{dag_run.conf.last_name}}',
                    "action": "Validation",
                    "status": "Exception",
                    'details': "User not Created, as {{ dag_run.conf.emp_status}} Employee Status present while user creation"
                }
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
                data=lambda dag_run:request_payload.get_put_user_payload(dag_run)
            )

            remove_timeoff_assignments = rail.RepliconServiceOperator(
                task_id="remove_timeoff_assignments",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda: request_payload.get_remove_timeoff_payload('add_new_user')
            )

            is_hrbp_yes = rail.IfOperator(
                task_id = "is_hrbp_yes",
                test=lambda dag_run: dag_run.conf['is_hrbp'] == 'Y',
                yes_task="assign_scope_for_hrbp_permissions",
                no_task="assign_place"
            )

            assign_scope_for_hrbp_permissions = rail.RepliconServiceOperator(
                task_id='assign_scope_for_hrbp_permissions',
                endpoint='/services/PermissionSetService1.svc/PutPolicyDataAccessScopesForUser',
                data=request_payload.assign_policyDataAccessScopes_to_projectmanager
            )

            assign_place = rail.RepliconServiceOperator(
                task_id="assign_place",
                endpoint="/services/PlaceService1.svc/PutPlaceAssignmentScheduleForUser",
                data=lambda dag_run: {
                    "userTarget": {
                        "uri": rail.result('add_new_user')['uri'],
                        "loginName": null,
                        "employeeId": null,
                        "parameterCorrelationId": null
                    },
                    "scheduleEntries": [
                        {
                        "effectiveDate": null,
                        "places": [
                            {
                            "uri": dag_run.conf['place_uri'],
                            "name": null
                            }
                        ]
                        }
                    ]
                    }
            )

            def map_impersonate_and_create_interactive_session(response):
                auth_token = list(
                    filter(lambda x: x['name'] == 'AUTHTOKEN', response['sessionCookies']))[0]['value']
                tenant = list(
                    filter(lambda x: x['name'] == 'TENANT', response['sessionCookies']))[0]['value']
                return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

            impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
                task_id='impersonate_and_create_interactive_session',
                endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
                data=lambda:{
                    "impersonatedUserUri": rail.result('add_new_user')['uri']
                },
                data_handler=map_impersonate_and_create_interactive_session
            )

            update_default_time_off_type_for_bookings = rail.RepliconServiceOperator(
                task_id='update_default_time_off_type_for_bookings',
                endpoint="/services/LegacyUIService1.svc/UpdateMyDefaultTimeOffTypeForBookings",
                data={
                    "timeOffTypeUri": "{{ dag_run.conf.default_time_off_type_uri }}"
                },
                headers=lambda: rail.result(
                    'impersonate_and_create_interactive_session')
            )

            is_supervisor_in_feed_file = rail.IfOperator(
                task_id='is_supervisor_in_feed_file',
                test=lambda dag_run: bool(dag_run.conf['sup_emp_id']),
                yes_task='search_supervisor_in_replicon',
                no_task='get_time_off_types_to_assign'
            )

            process_supervisor_entry,  process_supervisor_exit = process_supervisor_assignment_task_group(
                'add_new_user', 'new_user')

            get_time_off_types_to_assign = rail.PythonOperator(
                task_id = "get_time_off_types_to_assign",
                python_callable= lambda dag_run: python_callable_methods.get_time_off_to_be_assigned(dag_run, config)
            )

            has_time_off_type_to_assign = rail.IfOperator(
                task_id='has_time_off_type_to_assign',
                test=lambda : bool(rail.result('get_time_off_types_to_assign')),
                yes_task='process_time_off_type_assignment_new_user',
                no_task='log_user_completion'
            )

            def time_off_type_assignment_new_user_trigger_id(dag_run):
                return f"{config.process_timeoff_type_assignment_new_user_dagid}_batch_{dag_run.conf['modulo']+1}"

            process_time_off_type_assignment_new_user = rail.TriggerDagRunForEachItemOperator(
                task_id='process_time_off_type_assignment_new_user',
                items = [0],
                trigger_dag_id=time_off_type_assignment_new_user_trigger_id,
                conf=lambda dag_run:{
                    "employee_id": dag_run.conf['emp_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "useruri": rail.result('add_new_user')['uri'],
                    "user_log": dag_run.conf['user_log'],
                    "action": 'add',
                    "time_off_types_to_assign": rail.result('get_time_off_types_to_assign'),
                },
                execution_timeout=timedelta(hours=config.execution_timeout_days),
                retries=0,
            )

            wait_for_process_time_off_type_assignment_new_user = rail.WaitForDagRunsSensor(
                task_id='wait_for_process_time_off_type_assignment_new_user',
                dag_runs='{{ result("process_time_off_type_assignment_new_user") }}',
                execution_timeout=timedelta(days=config.execution_timeout_days),
            )

            gather_time_off_type_error_logs = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_error_logs',
                dag_runs='{{ result("process_time_off_type_assignment_new_user") }}',
                dagrun_task_id='catch_and_log_errors',
                flatten=True,
            )

            gather_time_off_type_exception_logs = rail.GatherResultsFromDagRunsOperator(
                task_id='gather_time_off_type_exception_logs',
                dag_runs='{{ result("process_time_off_type_assignment_new_user") }}',
                dagrun_task_id='log_time_off_type_not_available',
                flatten=True,
            )

            has_any_error_or_exceptions_present = rail.IfOperator(
                task_id="has_any_error_or_exceptions_present",
                test="{{ result('gather_time_off_type_error_logs') | is_truthy or result('gather_time_off_type_exception_logs') | is_truthy }}",
                yes_task= 'log_error_or_exceptions_present',
                no_task='log_user_completion'
            )

            log_error_or_exceptions_present = rail.EmptyOperator(
                task_id='log_error_or_exceptions_present'
            )


            log_user_completion = rail.WriteLogOperator(
                task_id='log_user_completion',
                log='{{ dag_run.conf.user_log }}',
                message=lambda dag_run:request_payload.get_add_user_message(dag_run),
                severity=lambda dag_run: request_payload.get_add_user_severity(dag_run),
                properties=lambda dag_run: {
                    "employee_id": dag_run.conf['emp_id'],
                    "last_name": dag_run.conf['last_name'],
                    "first_name": dag_run.conf['first_name'],
                    "action": "Add",
                    "status": request_payload.get_add_user_severity(dag_run),
                    'details': request_payload.get_add_user_message(dag_run)
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

            can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors
            can_run_batch_task >> rail.Label('No') >> is_status_inactive >> rail.Label('Yes') >> log_inactive_status >> catch_and_log_errors
            is_status_inactive >> rail.Label('No') >> is_enddate_available

            is_enddate_available >> rail.Label('Yes') >> log_endate_exception >> catch_and_log_errors
            is_enddate_available >> rail.Label('No') >> add_new_user

            add_new_user >> remove_timeoff_assignments >> is_hrbp_yes
            is_hrbp_yes >> rail.Label("No") >> assign_place
            is_hrbp_yes >> rail.Label("Yes") >> assign_scope_for_hrbp_permissions >> assign_place

            assign_place >> impersonate_and_create_interactive_session >> update_default_time_off_type_for_bookings

            update_default_time_off_type_for_bookings >> is_supervisor_in_feed_file

            is_supervisor_in_feed_file >> rail.Label('No') >> get_time_off_types_to_assign
            is_supervisor_in_feed_file >> rail.Label('Yes') >> process_supervisor_entry
            process_supervisor_exit >> get_time_off_types_to_assign >> has_time_off_type_to_assign
            has_time_off_type_to_assign >> rail.Label('Yes') >> process_time_off_type_assignment_new_user
            has_time_off_type_to_assign >> rail.Label('No') >> log_user_completion

            process_time_off_type_assignment_new_user >> wait_for_process_time_off_type_assignment_new_user
            wait_for_process_time_off_type_assignment_new_user >> gather_time_off_type_error_logs
            gather_time_off_type_error_logs >> gather_time_off_type_exception_logs >> has_any_error_or_exceptions_present
            has_any_error_or_exceptions_present >> rail.Label('No') >> log_user_completion >> catch_and_log_errors
            has_any_error_or_exceptions_present >> rail.Label('No') >> log_error_or_exceptions_present >> catch_and_log_errors


        add_dags.append(dag)

    return add_dags

rail.for_each_instance(create_child_dag)
