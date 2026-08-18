from datetime import timedelta
from airflow.models import Variable
import rail
from darkmattertechnologiesllc.user_sync_v1.utils import request_payload, python_callable
from darkmattertechnologiesllc.user_sync_v1.task.supervisor_assignment import supervisor_assignment

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.update_user_child_dagid,
        description=config.update_user_child_dagid,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.update_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_details_for_update'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_user_details_for_update',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_user_details_for_update = rail.RepliconServiceOperator(
            task_id='get_user_details_for_update',
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data = {
                "users": [
                    {
                        "uri": "{{ dag_run.conf.useruri }}"
                    }
                ]
            }
        )

        if_user_enabled_is_false = rail.IfOperator(
            task_id='if_user_enabled_is_false',
            test='''{{ result('get_user_details_for_update')[0].userDetails.isEnabled | is_falsy and \
                dag_run.conf.employeestatus | lower == 'active' }}''',
            yes_task="enable_user",
            no_task="get_effectivegroup_membership",
        )

        enable_user = rail.RepliconServiceOperator(
            task_id='enable_user',
            endpoint="/services/securityService1.svc/EnableLogin",
            data = {
                "userUri": "{{ dag_run.conf.useruri }}"
                }
        )

        update_employmentdaterange_rehire = rail.RepliconServiceOperator(
            task_id='update_employmentdaterange_rehire',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data = request_payload.update_emp_daterange_startdate
        )

        get_effectivegroup_membership = rail.RepliconServiceOperator(
            task_id="get_effectivegroup_membership",
            endpoint="/services/UserGroupService1.svc/GetEffectiveUserGroupMembership",
            data={
                "userUri": "{{dag_run.conf.useruri}}"
            },
            data_handler=python_callable.get_effectivegroup_membership_filter
        )

        is_user_active_for_update = rail.IfOperator(
            task_id='is_user_active_for_update',
            test='''{{ dag_run.conf.employeestatus | lower == 'active' }}''',
            yes_task="update_user",
            no_task="is_user_on_leave",
        )

        is_user_on_leave = rail.IfOperator(
            task_id='is_user_on_leave',
            test=lambda dag_run: bool(dag_run.conf['employeestatus'] in config.leave_status),
            yes_task="is_firstdayofleave_present",
            no_task="is_user_terminated_and_enddate_present",
        )

        is_firstdayofleave_present = rail.IfOperator(
            task_id='is_firstdayofleave_present',
            test='''{{ dag_run.conf.firstdayofleave | is_truthy }}''',
            yes_task="update_user",
            no_task="log_missing_loa_startdate_for_employeeonleave",
        )

        is_user_terminated_and_enddate_present = rail.IfOperator(
            task_id='is_user_terminated_and_enddate_present',
            test='''{{ dag_run.conf.employeestatus | lower == 'terminated' and \
                dag_run.conf.enddate | is_truthy }}''',
            yes_task="update_user",
            no_task="invalid_employeestatus_or_enddate_absent_for_termination",
        )

        invalid_employeestatus_or_enddate_absent_for_termination = rail.WriteLogOperator(
            task_id="invalid_employeestatus_or_enddate_absent_for_termination",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Validation",
                "status": "Exception",
                "details": "Invalid User status for disable or End date missing for Termination"
            }
        )

        log_missing_loa_startdate_for_employeeonleave = rail.WriteLogOperator(
            task_id="log_missing_loa_startdate_for_employeeonleave",
            log = '{{ dag_run.conf.logger}}',
            message="Success",
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Validation",
                "status": "Exception",
                "details": "LOA start date missing for Employeestatus - On Leave"
            }
        )

        update_user = rail.RepliconServiceOperator(
            task_id = "update_user",
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=lambda dag_run: request_payload.get_update_user_payload(dag_run, config)
        )

        search_for_supervisor_data = rail.RepliconServiceOperator(
            task_id='search_for_supervisor_data',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data = {
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.workermanager }}"
                    }
                ]
            }
        )

        if_supervisor_mismatch = rail.IfOperator(
            task_id='if_supervisor_mismatch',
            test='''{{ result('get_user_details_for_update')[0].userDetails.supervisor | is_falsy or \
                (result('search_for_supervisor') | is_falsy ) or \
                    ( result('search_for_supervisor') | is_truthy and \
                        result('get_user_details_for_update')[0].userDetails.supervisor.user.uri != result('search_for_supervisor')[0].userDetails.uri) }}''',
            yes_task="supervisorassign_dummytask",
            no_task="log_update_user_complete",
        )

        supervisorassign_dummytask = rail.EmptyOperator(
            task_id = "supervisorassign_dummytask"
        )

        supervisor_assignement_task = supervisor_assignment( "update")

        log_update_user_complete = rail.WriteLogOperator(
            task_id='log_update_user_complete',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties=python_callable.get_status_and_details_for_update
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "employeeid": "{{ dag_run.conf.employeeid }}",
                "action": "Update",
                "status": "Error",
                "details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_user_details_for_update

        get_user_details_for_update >> if_user_enabled_is_false

        if_user_enabled_is_false >> rail.Label('Yes') >> enable_user >> update_employmentdaterange_rehire >> get_effectivegroup_membership
        if_user_enabled_is_false >> rail.Label('No') >> get_effectivegroup_membership

        get_effectivegroup_membership >> is_user_active_for_update

        is_user_active_for_update >> rail.Label('Yes') >> update_user
        is_user_active_for_update >> rail.Label('No') >> is_user_on_leave

        is_user_on_leave >> rail.Label('Yes') >> is_firstdayofleave_present
        is_user_on_leave >> rail.Label('No') >> is_user_terminated_and_enddate_present

        is_firstdayofleave_present >> rail.Label('Yes') >> update_user
        is_firstdayofleave_present >> rail.Label('No') >> log_missing_loa_startdate_for_employeeonleave >> catch_and_log_error

        is_user_terminated_and_enddate_present >> rail.Label('Yes') >> update_user
        is_user_terminated_and_enddate_present >> rail.Label('No') >> invalid_employeestatus_or_enddate_absent_for_termination >> catch_and_log_error
        
        
        update_user >> search_for_supervisor_data >> if_supervisor_mismatch
        
        if_supervisor_mismatch >> rail.Label('Yes') >> supervisorassign_dummytask >> supervisor_assignement_task >> log_update_user_complete
        if_supervisor_mismatch >> rail.Label('No') >> log_update_user_complete

        log_update_user_complete >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
