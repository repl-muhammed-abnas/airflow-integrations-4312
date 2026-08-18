from datetime import timedelta
import pendulum
import rail
from airflow.models import Variable

from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.utils import request_payload, custom_methods
from dxctechnology.workday_user_import_v1.user_import_uki_es_v2.tasks.supervisor_assignment import assign_supervisor

null = None

def create_add_user_dag(config):
    _dags = []
    for batch_index in range(1, config.DAG_BATCH_COUNT + 1):
        prefix = f"_{batch_index}"
        if batch_index == 1:
            prefix = ""
        with rail.create_airflow_dag(
            dag_id=f"{config.workday_user_import_uki_es_add_user_dag}{prefix}",
            description="DXC Technology Workday User Sync UK&I CSC Add User",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            start_date=pendulum.datetime(2025, 4, 1),
            max_active_runs=config.max_active_run_add_user_uki_es
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

            can_run_batch_task = rail.IfOperator(
                task_id="can_run_batch_task",
                test=lambda: Variable.get(
                    config.can_run_batch_task_var_name_uki_es, default_var='false').lower() == 'true',
                yes_task="batch_task",
                no_task="create_user"
            )

            batch_task = rail.BatchTaskRunOperator(
                task_id="batch_task",
                start_task="create_user",
                end_task="catch_and_log_error",
                execution_timeout=timedelta(days=14)
            )

            # Create the user in Replicon
            create_user = rail.RepliconServiceOperator(
                task_id="create_user",
                endpoint="/services/ImportService1.svc/PutUser3",
                data=lambda dag_run: request_payload.create_user_payload_uki_es(dag_run, config)
            )

            # Remove default timeoffs
            remove_timeoffs = rail.RepliconServiceOperator(
                task_id="remove_timeoffs",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run: request_payload.get_timeoff_to_assign_remove_payload_uki_es(dag_run, mode="hard-remove")
            )

            can_update_notification_settings = rail.IfOperator(
                task_id = "can_update_notification_settings",
                test=lambda dag_run: dag_run.conf['file_data']['management_lvl'] in ['L1', 'L2'],
                yes_task = "update_notification_preference",
                no_task = "update_product_assignment"
            )

            # Update notification preferences
            update_notification_preference = rail.RepliconServiceOperator(
                task_id="update_notification_preference",
                endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
                data=lambda dag_run: request_payload.get_notification_preference_to_assign_payload(dag_run, "add")
            )

            # Update product assignment
            update_product_assignment = rail.RepliconServiceOperator(
                task_id="update_product_assignment",
                endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
                data=request_payload.get_product_assignment_payload_uki_es
            )

            # Check if time entry approval path needs to be updated
            can_update_timeentry_path = rail.IfOperator(
                task_id="can_update_timeentry_path",
                test="{{dag_run.conf.mapper_data.time_entry_approval_path | is_truthy}}",
                yes_task="update_time_entry_path",
                no_task="dummy_supervisor_start"
            )

            update_time_entry_path = rail.RepliconServiceOperator(
                task_id="update_time_entry_path",
                endpoint="/services/ImportService1.svc/ApplyUserModifications2",
                data=request_payload.update_time_entry_path_payload_uki_es
            )

            dummy_supervisor_start = rail.EmptyOperator(
                task_id="dummy_supervisor_start"
            )

            # Supervisor assignment tasks
            supervisor_start, supervisor_end = assign_supervisor("add_user_supervisor_assignment", "add")

            # Get all timeoffs
            get_all_timeoffs = rail.RepliconServiceOperator(
                task_id="get_all_timeoffs",
                endpoint="/services/TimeOffService1.svc/GetAllTimeOffTypes"
            )

            # Get mapper timeoff data
            get_mapper_timeoff_data = rail.PythonOperator(
                task_id="get_mapper_timeoff_data",
                python_callable=lambda dag_run: custom_methods.get_mapper_timeoff_data(dag_run, [])
            )

            # Determine timeoffs to assign
            timeoff_to_assign = rail.PythonOperator(
                task_id="timeoff_to_assign",
                python_callable=custom_methods.timeoff_to_assign_uki_es
            )

            # Check if there are timeoffs to assign
            has_any_timeoff_to_assign = rail.IfOperator(
                task_id="has_any_timeoff_to_assign",
                test=lambda: rail.result("timeoff_to_assign") and len(rail.result("timeoff_to_assign").get('timeoff_data_to_assign', [])) > 0,
                yes_task="assign_timeoff_to_user",
                no_task="log_user_completion"
            )

            # Assign timeoffs to user
            assign_timeoff_to_user = rail.RepliconServiceOperator(
                task_id="assign_timeoff_to_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run: request_payload.get_timeoff_to_assign_remove_payload_uki_es(dag_run, mode="assign")
            )

            def get_process_timeoff_conf(dag_run, item):
                create_user_result = rail.result('create_user')
                user_uri = create_user_result.get('uri')
                login_name = create_user_result.get('loginName')

                file_data = dag_run.conf.get('file_data', {})
                mapper_data = dag_run.conf.get('mapper_data', {})

                return {
                    "file_name": dag_run.conf.get('master_file_name', ''),
                    "user_log": dag_run.conf.get('user_log', ''),
                    "emp_id": file_data.get('emp_id', ''),
                    "email_id": file_data.get('email_id', ''),
                    "user_uri": user_uri,
                    "loginName": login_name,
                    "company_code": file_data.get('company_code', ''),
                    "source": mapper_data.get('parent_company', ''),
                    "country": file_data.get('country', ''),
                    "time_type": file_data.get('time_type', ''),
                    "workshift": file_data.get('work_shift', ''),
                    "exempt": file_data.get('exempt', ''),
                    "timeoff_uri": item['uri'],
                    "timeoff_name": item['name'],
                    "mapper_details": item['mapper_data'],
                    "file_data": file_data,
                    # UK&I specific fields
                    "business_title": file_data.get('business_title', ''),
                    "cost_center_id": file_data.get('cost_center_id', ''),
                    "cost_center_name": file_data.get('cost_center_name', ''),
                    "worker_category": file_data.get('worker_category', ''),
                    "employee_representative_status": file_data.get("employee_representative_status", ""),
                    "effective_date":  file_data.get("employee_representative_effective_date", ""),
                    "special_timeoff": "yes" if item['name'] in ['[UK] Employee representative duties', '[IRL] Employee Representative Duties'] else "no"
                }

            # Process timeoff assignments
            process_new_user_timeoff_assignment = rail.TriggerDagRunForEachItemOperator(
                task_id="process_new_user_timeoff_assignment",
                trigger_dag_id=lambda dag_run: custom_methods.get_trigger_dag_id(
                    config.workday_user_import_uki_es_add_user_timeoff_assignment_dag,
                    config.DAG_BATCH_COUNT,
                    custom_methods.get_item_index(dag_run, config.DAG_BATCH_COUNT)
                ),
                items=lambda: rail.result('timeoff_to_assign')['timeoff_data_to_assign'],
                conf=get_process_timeoff_conf,
                execution_timeout=timedelta(days=14),
                retries=0
            )

            wait_for_timeoff_assignment = rail.WaitForDagRunsSensor(
                task_id="wait_for_timeoff_assignment",
                dag_runs="{{ result('process_new_user_timeoff_assignment') }}",
                execution_timeout=timedelta(days=14)
            )

            # Disable specific timeoffs after assignment
            reassign_timeoff_to_user = rail.RepliconServiceOperator(
                task_id="reassign_timeoff_to_user",
                endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
                data=lambda dag_run: request_payload.get_timeoff_to_assign_remove_payload_uki_es(dag_run, mode="soft-remove")
            )

            def get_log_message(dag_run):
                exception_msg = rail.result('create_user', 'exception_log') or []
                timesheet_exception = []
                
                # Check if timesheet template is missing
                timesheet_template = dag_run.conf.get('user_policies', {}).get('timesheet_template', {})
                if timesheet_template.get('timesheet_template') and not timesheet_template.get('uri'):
                    timesheet_exception = [f'Timesheet Template {timesheet_template["timesheet_template"]} not present in Replicon']
                
                if exception_msg or timesheet_exception:
                    return f"User created partially - {rail.smartjoin_by_delim(exception_msg + timesheet_exception, ',')}"
                return "User created successfully"

            def get_completion_log_properties(dag_run):
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Add",
                        "Status": "Unknown",
                        "Details": "Missing dag_run.conf data"
                    }

                file_data = dag_run.conf.get('file_data', {})
                emp_id = file_data.get('emp_id', 'Unknown')
                email_id = file_data.get('email_id', 'Unknown')
                status = "Exception" if get_log_message(dag_run) != "User created successfully" else "Success"

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Add",
                    "Status": status,
                    "Details": get_log_message(dag_run)
                }

            # Log successful completion
            log_user_completion = rail.WriteLogOperator(
                task_id="log_user_completion",
                message="User Add",
                log="{{dag_run.conf.user_log}}",
                severity="Success",
                properties=get_completion_log_properties
            )

            def get_add_user_error_log_properties(dag_run):
                if not dag_run or not hasattr(dag_run, 'conf') or not dag_run.conf:
                    return {
                        "Jobid": "",
                        "Userid": "Unknown",
                        "Email": "Unknown",
                        "Action": "Add",
                        "Status": "Error",
                        "Details": "Missing dag_run.conf data"
                    }

                file_data = dag_run.conf.get('file_data', {})
                emp_id = file_data.get('emp_id', 'Unknown')
                email_id = file_data.get('email_id', 'Unknown')
                error_message = rail.render_template("{{get_error_message()}}")

                return {
                    "Jobid": "",
                    "Userid": emp_id,
                    "Email": email_id,
                    "Action": "Add",
                    "Status": "Error",
                    "Details": error_message
                }

            # Catch and log any errors
            catch_and_log_error = rail.WriteLogOperator(
                task_id="catch_and_log_error",
                trigger_rule="one_failed",
                log="{{dag_run.conf.user_log}}",
                message="User Add Error",
                severity="Error",
                properties=get_add_user_error_log_properties
            )

            # Set up task dependencies
            can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
            can_run_batch_task >> rail.Label("No") >> create_user

            create_user >> remove_timeoffs >> can_update_notification_settings >> rail.Label("Yes") >> update_notification_preference
            update_notification_preference >> update_product_assignment
            can_update_notification_settings >> rail.Label("No") >> update_product_assignment
            update_product_assignment >> can_update_timeentry_path >> rail.Label("No") >> dummy_supervisor_start
            can_update_timeentry_path >> rail.Label("Yes") >> update_time_entry_path >> dummy_supervisor_start

            dummy_supervisor_start >> supervisor_start
            supervisor_end >> get_all_timeoffs

            get_all_timeoffs >> get_mapper_timeoff_data >> timeoff_to_assign >> has_any_timeoff_to_assign >> rail.Label(
                "Yes") >> assign_timeoff_to_user >> process_new_user_timeoff_assignment >> wait_for_timeoff_assignment >> reassign_timeoff_to_user >> log_user_completion
            has_any_timeoff_to_assign >> log_user_completion >> catch_and_log_error

        _dags.append(dag)
    return _dags

rail.for_each_instance(create_add_user_dag)