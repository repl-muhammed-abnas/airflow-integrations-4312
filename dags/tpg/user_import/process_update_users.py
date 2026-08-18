from datetime import timedelta
from airflow.models import Variable
import rail

from tpg.user_import.utils import request_payload, response_filter
from tpg.user_import.tasks.process_supervisor import process_supervisor_assignment_task_group

null= None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users,
        description='TPG User Import - Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_user_info'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_user_info',
            end_task='catch_and_log_errors',
        )

        get_user_info = rail.RepliconServiceOperator(
            task_id='get_user_info',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data={
                "users": [
                    {
                        "uri": '{{ dag_run.conf.useruri }}',
                        "loginName": null,
                        "parameterCorrelationId": null
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            }
        )

        if_isloginenable_no = rail.IfOperator(
            task_id="if_isloginenable_no",
            test=lambda dag_run: dag_run.conf['isloginenable'] == 'No',
            yes_task="is_enddate_greater_than_start_date",
            no_task="is_user_disabled"
        )

        is_enddate_greater_than_start_date = rail.IfOperator(
            task_id ='is_enddate_greater_than_start_date',
            test = request_payload.validate_enddate,
            yes_task="update_employee_endate",
            no_task="log_endate_exception"
        )

        update_employee_endate = rail.RepliconServiceOperator(
            task_id='update_employee_endate',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['startdate']),
                    "endDate": request_payload.get_replicon_date(dag_run.conf['enddate'])
                }
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id = 'log_endate_exception',
            log = '{{ dag_run.conf.user_log }}',
            message = "User not Disabled,End date Prior to Start date",
            severity='Exception',
            properties ={
                'jobid': "{{ dag_run.conf.jobid }}",
                'lastname': "{{ dag_run.conf.lastname }}",
                'firstname': "{{ dag_run.conf.firstname }}",
                'loginname':  "{{ dag_run.conf.loginname }}",
                'employeeid': "{{ dag_run.conf.employeeid }}",
                'useruri': "{{ dag_run.conf.useruri }}",
                'manager': "{{ dag_run.conf.manager }}",
                'action': 'Validation',
                'status': "Exception",
                'details': "User not Disabled,End date Prior to Start date",
                'user_log': "{{ dag_run.conf.user_log }}"
            }
        )

        log_disabled_success = rail.WriteLogOperator(
            task_id = 'log_disabled_success',
            log = '{{ dag_run.conf.user_log }}',
            message = "User Disabled Successfully",
            severity='Success',
            properties = {
                'jobid': "{{ dag_run.conf.jobid }}",
                'lastname': "{{ dag_run.conf.lastname }}",
                'firstname': "{{ dag_run.conf.firstname }}",
                'loginname':  "{{ dag_run.conf.loginname }}",
                'employeeid': "{{ dag_run.conf.employeeid }}",
                'useruri': "{{ dag_run.conf.useruri }}",
                'manager': "{{ dag_run.conf.manager }}",
                'action': 'Disable',
                'status': "Success",
                'details': "User Disabled Successfully",
                'user_log': "{{ dag_run.conf.user_log }}"
            }
        )

        is_user_disabled =  rail.IfOperator(
            task_id="is_user_disabled",
            test=lambda dag_run : not bool(rail.result('get_user_info')[0]['userDetails']['isEnabled']) and dag_run.conf['isloginenable'] =='Yes',
            yes_task="enable_login",
            no_task="get_current_udf_values"
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        get_current_udf_values = rail.PythonOperator(
            task_id='get_current_udf_values',
            python_callable=lambda: rail.result('get_user_info')[0][
                'userDetails']['customFieldValues']
        )

        get_effective_user_groupmembership = rail.RepliconServiceOperator(
            task_id='get_effective_user_groupmembership',
            endpoint='/services/UserGroupService1.svc/GetEffectiveUserGroupMembership',
            data={
                "userUri": "{{dag_run.conf.useruri}}",
                "dateRange": null
            },
            data_handler=response_filter.get_effective_user_groupmembership_filter
        )

        apply_user_modifications = rail.RepliconServiceOperator(
            task_id='apply_user_modifications',
            endpoint='/services/ImportService1.svc/ApplyUserModifications3',
            data=request_payload.apply_user_modifications_payload,
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['manager']),
            yes_task='if_user_is_supervisor',
            no_task='log_user_completion'
        )

        if_user_is_supervisor = rail.IfOperator(
            task_id='if_user_is_supervisor',
            test=lambda dag_run: dag_run.conf['manager'] == dag_run.conf['employeeid'],
            yes_task='log_user_supervisor_same',
            no_task='search_supervisor_in_replicon'
        )

        log_user_supervisor_same = rail.SetVariableOperator(
            task_id='log_user_supervisor_same',
            name='sameusersupervisor',
            value='User and Supervisor is same'
        )

        process_supervisor_entry,  process_supervisor_exit= process_supervisor_assignment_task_group(
            'useruri', 'update_user')

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log = '{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                'jobid': dag_run.conf['jobid'],
                'lastname': dag_run.conf['lastname'],
                'firstname': dag_run.conf['firstname'],
                'loginname':  dag_run.conf['loginname'],
                'employeeid': dag_run.conf['employeeid'],
                'useruri': dag_run.conf['useruri'],
                'manager': dag_run.conf['manager'],
                'action': 'Update',
                'status': request_payload.get_update_user_severity(),
                'details': request_payload.get_update_user_message(),
                'user_log': dag_run.conf['user_log']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log = '{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'jobid': '{{dag_run.conf.jobid}}',
                'lastname': '{{dag_run.conf.lastname}}',
                'firstname': '{{dag_run.conf.firstname}}',
                'loginname': '{{dag_run.conf.loginname}}',
                'employeeid': '{{dag_run.conf.employeeid}}',
                'useruri': '{{dag_run.conf.useruri}}',
                'manager': '{{dag_run.conf.manager}}',
                'action': 'Update',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'user_log': '{{dag_run.conf.user_log}}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> get_user_info

        get_user_info >> if_isloginenable_no >> rail.Label("No") >> is_user_disabled
        if_isloginenable_no >> rail.Label("Yes") >> is_enddate_greater_than_start_date >> rail.Label(
            "No") >> log_endate_exception >> catch_and_log_errors
        is_enddate_greater_than_start_date >> rail.Label("No") >> update_employee_endate >> disable_login >> log_disabled_success >> catch_and_log_errors

        is_user_disabled >> rail.Label('Yes') >> enable_login >> get_current_udf_values
        is_user_disabled >> rail.Label('No') >> get_current_udf_values

        get_current_udf_values >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> apply_user_modifications >> is_supervisor_in_feed_file

        is_supervisor_in_feed_file >> rail.Label('No') >> log_user_completion
        is_supervisor_in_feed_file >> rail.Label('Yes') >> if_user_is_supervisor

        if_user_is_supervisor >> rail.Label('No') >> process_supervisor_entry
        if_user_is_supervisor >> rail.Label('Yes') >> log_user_supervisor_same >> log_user_completion
        process_supervisor_exit >> log_user_completion >> catch_and_log_errors

        catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
