from datetime import timedelta
from pendulum import datetime
import rail
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from dxctechnology.workday_user_import.user_import_australia.utils import request_payload
from dxctechnology.workday_user_import.user_import_australia.utils.custom_methods import is_profile_enabled

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.workday_user_import_australia_users_add_user_child_dag,
        description="dxctechnology workday user sync process users child",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        start_date=datetime(2023, 9, 26),
        max_active_runs=config.user_process_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_conf")
        
        can_run_batch_task = rail.IfOperator(
            task_id = "can_run_batch_task",
            test=lambda: Variable.get(
            config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="create_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="create_user",
            end_task="catch_and_log_error",
            execution_timeout=timedelta(days=14)
        )

        create_user = rail.RepliconServiceOperator(
            task_id = "create_user",
            endpoint="/services/ImportService1.svc/PutUser3",
            data= lambda dag_run : request_payload.get_create_user_payload(dag_run, config)
        )

        remove_timeoffs = rail.RepliconServiceOperator(
            task_id = "remove_timeoffs",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data = {
                "userUri": "{{result('create_user').uri}}",
                "timeOffTypeUris": []
            }
        )

        put_product_assignment_for_user = rail.RepliconServiceOperator(
            task_id= "put_product_assignment_for_user",
            endpoint="/services/AccountManagementService1.svc/PutProductAssignmentsForUser",
            data= lambda dag_run: {
                "userUri" : rail.result("create_user")["uri"],
                "productUris": dag_run.conf['mapper_data']['product_uri'].split('|')
            }
        )

        is_timeentry_approval_path_present = rail.IfOperator(
            task_id= "is_timeentry_approval_path_present",
            test="{{dag_run.conf.mapper_data.time_entry_approval_path_name | is_truthy}}",
            yes_task="update_time_entry_path",
            no_task="can_add_punch_entry_policy"
        )

        update_time_entry_path = rail.RepliconServiceOperator(
            task_id = "update_time_entry_path",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=request_payload.get_update_time_entry_path_payload
        )

        can_add_punch_entry_policy = rail.IfOperator(
            task_id = "can_add_punch_entry_policy",
            # as per workato this will be always null -\(--)/-
            test="{{dag_run.conf.policy_sets.punch_entry_policy | is_truthy}}",
            yes_task="add_punch_entry_policy",
            no_task="is_profile_status_enabled"
        )

        add_punch_entry_policy = rail.RepliconServiceOperator(
            task_id = "add_punch_entry_policy",
            endpoint = "/services/PolicySetService1.svc/AssignPolicySetToUser",
            data = {
                'userUri': "{{ result('create_user').uri }}",
                'policySetUri': "{{ dag_run.conf.policy_sets.punch_entry_policy.uri}}"
            }
        )

        is_profile_status_enabled = rail.IfOperator(
            task_id = 'is_profile_status_enabled',
            test= is_profile_enabled,
            yes_task="validated_supervisor_id_same_as_user_id",
            no_task="process_new_user_timeoff_assignment"
        )

        validated_supervisor_id_same_as_user_id = rail.IfOperator(
            task_id = "validated_supervisor_id_same_as_user_id",
            test = lambda dag_run: dag_run.conf['file_data']['supervisor_id'] == dag_run.conf['file_data']['emp_id'],
            yes_task="log_supervisor_id_user_id_same",
            no_task="log_for_supervisor_assignment"
        )
    
        log_supervisor_id_user_id_same = rail.WriteLogOperator(
            task_id = "log_supervisor_id_user_id_same",
            message = "User Add Australia | Supervisor",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Exception",
                "Details": "Supervisor not updated - Supervisor is same as User"
            }
        )        

        log_for_supervisor_assignment = rail.WriteLogOperator(
            task_id = "log_for_supervisor_assignment",
            message = "User Add | Supervisor re-assignment",
            log="{{dag_run.conf.supervisor_user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'], #Emplid
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": 'Add',
                "Status": "pending",
                "Details": "Supervisor Reassignment",
                "login_name": dag_run.conf['file_data']['email_id'],
                "user_uri|country": f"{dag_run.conf.get('user_uri', rail.result('create_user')['uri'])}|{dag_run.conf['file_data']['country']}",
                "user_name": f"{dag_run.conf['file_data']['first_name']} {dag_run.conf['file_data']['last_name']}",
                "supervisor_login_name": f"{dag_run.conf['file_data']['supervisor_email_id']}|{dag_run.conf['file_data']['supervisor_id']}|{dag_run.conf['file_data']['supervisor_f_name']}|{dag_run.conf['file_data']['supervisor_l_name']}",
                "effective_date": dag_run.conf['json_formatted_dates']['supervisor_date'],
                "user_log": dag_run.conf['user_log'],
                "supervisor_end_user_permission": dag_run.conf['user_permission_sets']['supervisor_end_user_permission'],
                "supervisor_user_permission": dag_run.conf['user_permission_sets']['supervisor_user_permission'],
                'aus_supervisor_end_user_permission': dag_run.conf['user_permission_sets']['aus_supervisor_end_user_permission'],
                'parent_company': dag_run.conf['file_data']['parent_company']
            }
        )

        process_new_user_timeoff_assignment = rail.TriggerDagRunOperator(
            task_id = "process_new_user_timeoff_assignment",
            trigger_dag_id=config.workday_user_import_australia_users_add_user_timeoff_process_child_dag,
            conf=lambda dag_run:{
                **dag_run.conf,
                **{
                    "file_name": dag_run.conf['master_file_name'],
                    "user_uri": rail.result('create_user')['uri'],
                    "loginName": rail.result('create_user')['loginName'],
                    "company_code": dag_run.conf['file_data']['company_code'],
                    "parent_company_code": dag_run.conf['file_data']['parent_company'],
                    "country": dag_run.conf['file_data']['country'],
                    "fte": dag_run.conf['file_data']['fte'] if dag_run.conf['file_data']['fte'] else 0,
                    "state": dag_run.conf['file_data']['state'],
                    "ausjc": dag_run.conf['file_data']['ausjc'],
                    "industrial_instrument_classification": dag_run.conf['file_data']['industrial_instrument_classification'],
                    "is_ia": dag_run.conf['file_data']['is_ia']
                }
            },
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )
        
        wait_for_timeoff_assignment = rail.WaitForDagRunsSensor(
            task_id = "wait_for_timeoff_assignment",
            dag_runs="""{{ result('process_new_user_timeoff_assignment') }}""",
            execution_timeout=timedelta(
                    days=config.execution_timeout_days),
            retries=0
        )

        def get_log_message():
            exception_msg = rail.result('create_user', 'exception_log')
            if exception_msg:
                return f"User created partially - {rail.smartjoin_by_delim(exception_msg, ',')}"
            return "User created successfully"

        log_user_completion = rail.WriteLogOperator(
            task_id = "log_user_completion",
            message = "User Add",
            log="{{dag_run.conf.user_log}}",
            severity = "Success",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Success" if not bool(rail.result('create_user', 'exception_log')) else "Exception",
                "Details": get_log_message()
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id = "catch_and_log_error",
            trigger_rule = "one_failed",
            log="{{dag_run.conf.user_log}}",
            message = "User Add Error",
            severity = "Error",
            properties = lambda dag_run: {
                # WriteLogOperator ecid has ecid | run_id
                "Jobid": "",
                "Userid": dag_run.conf['file_data']['emp_id'],
                "Email": dag_run.conf['file_data']['email_id'],
                "Action": "Add",
                "Status": "Error",
                "Details": rail.render_template("{{get_error_message()}}")
            }
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> create_user
        create_user >> remove_timeoffs >> put_product_assignment_for_user >>is_timeentry_approval_path_present >> rail.Label(
            "Yes") >> update_time_entry_path >> can_add_punch_entry_policy >> rail.Label("No") >> is_profile_status_enabled
        is_timeentry_approval_path_present >> rail.Label("No") >> can_add_punch_entry_policy >> rail.Label(
            "Yes") >> add_punch_entry_policy >> is_profile_status_enabled >> rail.Label(
                "Yes") >> validated_supervisor_id_same_as_user_id
        is_profile_status_enabled >> rail.Label("No") >> process_new_user_timeoff_assignment
        validated_supervisor_id_same_as_user_id >> rail.Label("Yes") >> log_supervisor_id_user_id_same >> process_new_user_timeoff_assignment
        validated_supervisor_id_same_as_user_id >> rail.Label("No") >> log_for_supervisor_assignment >> process_new_user_timeoff_assignment

        process_new_user_timeoff_assignment >> wait_for_timeoff_assignment >> log_user_completion >> rail.Label("On Error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)