from datetime import timedelta
from pendulum import datetime
import rail
from mammoet.user_import_v4.utils import request_payload, response_filter
from mammoet.user_import_v4.utils.custom_methods import get_timeoff_assignment_log_message, get_replicon_date_from_str
from airflow.models import Variable

null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.user_import_update_users_child_dag_id,
        description= "Mammoet User Import Process Add User",
        start_date= datetime(2023,9,1),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.process_update_user_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")

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
            end_task='catch_and_log_error',
        )

        is_status_inactive = rail.IfOperator(
            task_id = "is_status_inactive",
            test="{{dag_run.conf.employee_status | lower() == 'inactive'}}",
            yes_task="update_user_end_date",
            no_task="is_user_rehire"
        )

        update_user_end_date = rail.RepliconServiceOperator(
            task_id="update_user_end_date",
            endpoint='/services/ImportService1.svc/ApplyUserModifications2',
            data=request_payload.update_user_end_date_payload
        )

        log_user_end_date_updated = rail.WriteLogOperator(
            task_id ="log_user_end_date_updated",
            severity="Success",
            message="User End date updated",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Success",
                "action":"Update",
                "details": "User End date updated"
            }
        )

        is_user_rehire = rail.IfOperator(
            task_id = "is_user_rehire",
            test="{{dag_run.conf.rehire}}",
            yes_task="enable_login",
            no_task="get_user_details"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": "{{ dag_run.conf.user_uri }}"
            }
        )

        remove_end_date_update_start_date = rail.RepliconServiceOperator(
            task_id = "remove_end_date_update_start_date",
            endpoint = "/services/UserService1.svc/UpdateEmploymentDateRange",
            data= lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "dateRange": {
                    "startDate": get_replicon_date_from_str(dag_run.conf['start_date']),
                    "endDate": null,
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id= "get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": "{{ dag_run.conf.user_uri }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        get_effectivegroup_membership = rail.RepliconServiceOperator(
            task_id="get_effectivegroup_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{dag_run.conf.user_uri}}",
                "dateRange": None
            },
            data_handler=response_filter.get_effectivegroup_membership_filter
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data=lambda dag_run: request_payload.get_update_user_payload(dag_run, config)
        )

        is_user_update_failed = rail.IfOperator(
            task_id = "is_user_update_failed",
            test="{{ result('update_user').errors | is_truthy }}",
            yes_task="log_update_user_failed",
            no_task="log_update_user_success"
        )

        log_update_user_failed = rail.WriteLogOperator(
            task_id='log_update_user_failed',
            log = '{{ dag_run.conf.log }}',
            message="{{ result('update_user').errors }}",
            severity='Error',
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Error",
                "action":"Update",
                "details":"{{ result('update_user').errors }}"
            }
        )

        def get_log_update_user_success_message(dag_run):
            if dag_run.conf['activities']['exception']:
                return f"User partially updated;{dag_run.conf['activities']['exception']}"
            return "User updated successfully"

        log_update_user_success = rail.WriteLogOperator(
            task_id='log_update_user_success',
            log = '{{ dag_run.conf.log }}',
            message="User updated successfully",
            severity='Success',
            properties=lambda dag_run: {
                "payload_id": dag_run.conf['payload_id'],
                "login_name": dag_run.conf['login_name'],
                "employee_id": dag_run.conf['employee_id'],
                "emp_record_index": dag_run.conf['emp_records_index'],
                "status": "Success",
                "action":"Update",
                "details":get_log_update_user_success_message(dag_run)
            }
        )

        def test_timeoff_should_be_updated(dag_run):            
            user_details = rail.result("get_user_details")[0]

            if 'timeOffTypePolicySummary' not in user_details:
                return True

            user_assigned_timeoffs = user_details['timeOffTypePolicySummary']

            if 'policiesByTimeOffType' not in user_assigned_timeoffs:
                return True

            if not user_assigned_timeoffs['policiesByTimeOffType']:
                return True

            mapper_timeoffs = dag_run.conf['mapper_derived']['timeoff_to_assign']
            user_assigned_timeoffs_uris = [ _timeoff['timeOffType']['uri'] for _timeoff in user_assigned_timeoffs['policiesByTimeOffType'] if _timeoff['isTimeOffAllowedAgainstThisTimeOffType']]
            for _mapper_timeoff in mapper_timeoffs:
                if _mapper_timeoff['uri']:
                    if _mapper_timeoff['uri'] not in user_assigned_timeoffs_uris:
                        rail.set_result(key="missing_timeoff", val=_mapper_timeoff)
                        return True
            return False

        has_any_timeoffs_to_assign = rail.IfOperator(
            task_id = "has_any_timeoffs_to_assign",
            test=test_timeoff_should_be_updated,
            yes_task="assign_timeoffs_user",
            no_task="log_no_timeoffs_to_assign"
        )

        assign_timeoffs_user = rail.RepliconServiceOperator(
            task_id='assign_timeoffs_user',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=lambda dag_run: {
                "userUri": dag_run.conf['user_uri'],
                "timeOffTypeUris": list(filter(None,map(lambda to: to['uri'], dag_run.conf['mapper_derived']['timeoff_to_assign'])))
            }
        )

        log_no_timeoffs_to_assign = rail.WriteLogOperator(
            task_id ="log_no_timeoffs_to_assign",
            severity="Exception",
            message="No Timeoff found to assign",
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Exception",
                "action":"Update",
                "details": "No Timeoff found to assign"
            }
        )

        log_timeoffs_assignment = rail.WriteLogOperator(
            task_id ="log_timeoffs_assignment",
            severity="Success",
            message="Timeoff assigned",
            log="{{dag_run.conf.log}}",
            properties=lambda dag_run :{
                "payload_id": dag_run.conf['payload_id'],
                "login_name": dag_run.conf['login_name'],
                "employee_id": dag_run.conf['employee_id'],
                "emp_record_index": dag_run.conf['emp_records_index'],
                "status": "Success",
                "action":"Update",
                "details":get_timeoff_assignment_log_message(dag_run)
            }
        )

        log_for_supervisor_assignment = rail.WriteLogOperator(
            task_id='log_for_supervisor_assignment',
            severity="To-reprocess",
            message="To re-process",
            log="{{dag_run.conf.supervisor_log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "action":"Update",
                "details": "For Reprocessing Supervisor",
                'user_uri': "{{dag_run.conf.user_uri}}",
                "supervisor_id":"{{dag_run.conf.manager_id}}",
                "user_log": "{{dag_run.conf.log}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "effective_date": "{{dag_run.conf.group_effective_start_date}}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            severity="Error",
            message="{{get_error_message()}}",
            trigger_rule='one_failed',
            log="{{dag_run.conf.log}}",
            properties={
                "payload_id": "{{dag_run.conf.payload_id}}",
                "login_name": "{{dag_run.conf.login_name}}",
                "employee_id": "{{dag_run.conf.employee_id}}",
                "emp_record_index": '{{dag_run.conf.emp_records_index}}',
                "status": "Error",
                "action":"Update",
                "details": "{{get_error_message()}}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule = "all_done"
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label("No") >> is_status_inactive
        is_status_inactive >> rail.Label("Yes") >> update_user_end_date >> log_user_end_date_updated >> rail.Label("On Error") >> catch_and_log_error
        is_status_inactive >> rail.Label("No") >>is_user_rehire >> rail.Label("Yes") >> enable_login >> remove_end_date_update_start_date >> get_user_details
        is_user_rehire >> rail.Label("No") >> get_user_details >> get_effectivegroup_membership >> update_user >> is_user_update_failed >> rail.Label("Yes")\
            >> log_update_user_failed >> catch_and_log_error >> log_to_sumo
        is_user_update_failed >> rail.Label("No") >> log_update_user_success >> has_any_timeoffs_to_assign\
            >> rail.Label("Yes") >> assign_timeoffs_user >> log_timeoffs_assignment >> log_for_supervisor_assignment
        has_any_timeoffs_to_assign >> rail.Label("No") >> log_no_timeoffs_to_assign >> log_for_supervisor_assignment >> rail.Label("On Error") >> catch_and_log_error

    return dag

rail.for_each_instance(create_main_dag)
