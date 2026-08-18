from datetime import datetime, timedelta
from airflow.models import Variable
import rail

from moodys.user_sync.germany.utils import request_payload, response_filter, python_callable_methods
from moodys.user_sync.germany.tasks.process_supervisor import process_supervisor_assignment_task_group

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_update_users_dagid,
        description='Moodys User Sync - Process Update Users',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_update_users,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_valid_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_valid_data',
            end_task='catch_and_log_errors',
        )

        has_valid_data = rail.IfOperator(
            task_id='has_valid_data',
            test=request_payload.test_valid_fields_update,
            yes_task="is_rehire_present",
            no_task="log_invalid_data"
        )

        # CR-V1.0 Branch A: when rehire is present, clear stored end-date and skip
        # new-feed enddate processing. If the source system needs to set a future
        # enddate post-rehire, it must arrive on a subsequent feed (rehire blank,
        # enddate populated). This avoids the ambiguity of "rehire and termination
        # in the same record."
        is_rehire_present = rail.IfOperator(
            task_id='is_rehire_present',
            test=lambda dag_run: bool(dag_run.conf.get('rehire')),
            yes_task='is_rehire_after_or_equal_enddate',
            no_task='is_enddate_available'
        )

        is_rehire_after_or_equal_enddate = rail.IfOperator(
            task_id='is_rehire_after_or_equal_enddate',
            test=python_callable_methods._is_rehire_after_or_equal_enddate,
            yes_task='clear_end_date_for_rehire',
            no_task='log_rehire_before_enddate_exception'
        )

        log_rehire_before_enddate_exception = rail.WriteLogOperator(
            task_id='log_rehire_before_enddate_exception',
            log='{{ dag_run.conf.user_log }}',
            message='Rehire date earlier than End date - record skipped',
            severity='Exception',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Validation",
                "status": "Exception",
                'details': 'Rehire date earlier than End date - record skipped',
            }
        )

        clear_end_date_for_rehire = rail.RepliconServiceOperator(
            task_id='clear_end_date_for_rehire',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=lambda dag_run: {
                "userUri": dag_run.conf['useruri'],
                "dateRange": {
                    "startDate": request_payload.get_replicon_date(dag_run.conf['startdate']),
                    "endDate": null
                }
            }
        )

        log_invalid_data = rail.WriteLogOperator(
            task_id='log_invalid_data',
            log='{{ dag_run.conf.user_log }}',
            message=request_payload.get_invalid_fields_message_update,
            severity='Exception',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Validation",
                "status": "Exception",
                'details':  request_payload.get_invalid_fields_message_update(dag_run),
            }
        )

        is_enddate_available = rail.IfOperator(
            task_id='is_enddate_available',
            test=lambda dag_run: bool(dag_run.conf['enddate']),
            yes_task="is_enddate_greater_than_start_date",
            no_task="is_user_disabled"
        )

        is_enddate_greater_than_start_date = rail.IfOperator(
            task_id='is_enddate_greater_than_start_date',
            test=request_payload.validate_enddate,
            yes_task="update_employee_endate_and_timesheet_period",
            no_task="log_endate_exception"
        )

        update_employee_endate_and_timesheet_period = rail.RepliconServiceOperator(
            task_id='update_employee_endate_and_timesheet_period',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.update_employee_endate_and_timesheet_period_payload
        )

        log_endate_exception = rail.WriteLogOperator(
            task_id='log_endate_exception',
            log='{{ dag_run.conf.user_log }}',
            message="End date Prior to Start date",
            severity='Exception',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Validation",
                "status": "Exception",
                'details': "End date Prior to Start date",
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/securityservice1.svc/DisableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
        )

        is_enddate_in_future = rail.IfOperator(
            task_id="is_enddate_in_future",
            test=request_payload.is_enddate_in_future,
            yes_task="log_end_date_future",
            no_task="disable_login"
        )

        log_end_date_future = rail.WriteLogOperator(
            task_id='log_end_date_future',
            log='{{ dag_run.conf.user_log }}',
            message="User End date in Future, Endate updated but Profile will be disabled on end date",
            severity='Exception',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                "status": "Exception",
                'details': "User End date in Future, Endate updated but Profile will be disabled on end date",
            }
        )

        log_disabled_success = rail.WriteLogOperator(
            task_id='log_disabled_success',
            log='{{ dag_run.conf.user_log }}',
            message="User Disabled in Replicon",
            severity='Success',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                "status": "Success",
                'details': "User Disabled in Replicon",
            }
        )

        is_user_disabled = rail.IfOperator(
            task_id="is_user_disabled",
            test=lambda dag_run: dag_run.conf['userstatus'] in (
                False, 'False', 'false', 0, '0'),
            yes_task="clear_stored_enddate_reverse_term",
            no_task="get_user_info"
        )

        clear_stored_enddate_reverse_term = rail.RepliconServiceOperator(
            task_id='clear_stored_enddate_reverse_term',
            endpoint="/services/ImportService2.svc/CreateUserOrApplyModifications",
            data=request_payload.clear_stored_enddate_reverse_term_payload
        )

        enable_login = rail.RepliconServiceOperator(
            task_id='enable_login',
            endpoint='/services/securityservice1.svc/EnableLogin',
            data={
                "userUri": '{{ dag_run.conf.useruri }}'
            }
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
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            },
            data_handler=lambda response: response[0] if response else []
        )

        get_current_udf_values = rail.PythonOperator(
            task_id='get_current_udf_values',
            python_callable=lambda: rail.result('get_user_info')[
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
            data=lambda dag_run: request_payload.apply_user_modifications_payload(
                dag_run, config),
        )

        put_timeoff_assignment_for_user = rail.RepliconServiceOperator(
            task_id="put_timeoff_assignment_for_user",
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data=request_payload.put_timeoff_assignment_for_existing_user
        )

        is_supervisor_in_feed_file = rail.IfOperator(
            task_id='is_supervisor_in_feed_file',
            test=lambda dag_run: bool(dag_run.conf['supervisorid']),
            yes_task='search_supervisor_in_replicon',
            no_task='log_user_completion'
        )

        process_supervisor_entry,  process_supervisor_exit = process_supervisor_assignment_task_group(
            'useruri', 'update_user', config)

        log_user_completion = rail.WriteLogOperator(
            task_id='log_user_completion',
            log='{{ dag_run.conf.user_log }}',
            message=request_payload.get_update_user_message,
            severity=request_payload.get_update_user_severity,
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                "status": request_payload.get_update_user_severity(),
                'details': request_payload.get_update_user_message()
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.user_log }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "countryid": dag_run.conf['countryid'],
                "loginname": dag_run.conf['loginname'],
                "lastname": dag_run.conf['lastname'],
                "firstname": dag_run.conf['firstname'],
                "action": "Update",
                'status': 'Error',
                'details': ("User updated Partially" if rail.result(
                    'apply_user_modifications') else "") + " ; " + rail.render_template("{{ get_error_message() }}")
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> has_valid_data

        has_valid_data >> rail.Label(
            'No') >> log_invalid_data >> catch_and_log_errors
        has_valid_data >> rail.Label('Yes') >> is_rehire_present

        is_rehire_present >> rail.Label(
            'Yes') >> is_rehire_after_or_equal_enddate
        is_rehire_present >> rail.Label('No') >> is_enddate_available

        is_rehire_after_or_equal_enddate >> rail.Label(
            'Yes') >> clear_end_date_for_rehire >> is_user_disabled
        is_rehire_after_or_equal_enddate >> rail.Label(
            'No') >> log_rehire_before_enddate_exception >> catch_and_log_errors

        is_enddate_available >> rail.Label('No') >> is_user_disabled
        is_enddate_available >> rail.Label(
            'Yes') >> is_enddate_greater_than_start_date

        is_enddate_greater_than_start_date >> rail.Label(
            'Yes') >> update_employee_endate_and_timesheet_period >> is_enddate_in_future
        is_enddate_greater_than_start_date >> rail.Label(
            'No') >> log_endate_exception >> catch_and_log_errors

        is_enddate_in_future >> rail.Label(
            'Yes') >> log_end_date_future >> catch_and_log_errors
        is_enddate_in_future >> rail.Label(
            'No') >> disable_login >> log_disabled_success >> catch_and_log_errors

        is_user_disabled >> rail.Label(
            'Yes') >> clear_stored_enddate_reverse_term >> enable_login >> get_user_info
        is_user_disabled >> rail.Label(
            'No') >> get_user_info >> get_current_udf_values

        get_current_udf_values >> get_effective_user_groupmembership
        get_effective_user_groupmembership >> apply_user_modifications >> put_timeoff_assignment_for_user

        put_timeoff_assignment_for_user >> is_supervisor_in_feed_file

        is_supervisor_in_feed_file >> rail.Label(
            'No') >> log_user_completion >> catch_and_log_errors
        is_supervisor_in_feed_file >> rail.Label(
            'Yes') >> process_supervisor_entry
        process_supervisor_exit >> log_user_completion

    return dag


rail.for_each_instance(create_child_dag)
