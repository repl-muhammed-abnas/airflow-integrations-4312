from datetime import datetime, timedelta
import rail
from macquariegroup.user_import.utils.request_payload import get_today_date, update_employment_daterange_user
from macquariegroup.user_import.utils.custom_methods import bool_can_disable_user
from airflow.models import Variable
# pylint: disable=too-many-statements


def create_disableuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_user_import_disable_users_child_{config.instance}",
        description=f"Macquarie User Import Disable Users Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_disableuser_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id= "can_run_batch_task",
            test= lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task= "can_disable_user"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='can_disable_user',
            end_task="catch_and_log_errors",
        )

        can_disable_user = rail.IfOperator(
            task_id="can_disable_user",
            test=bool_can_disable_user,
            yes_task="get_my_actual_useridentity",
            no_task="log_users_group_is_not_allowed"
        )

        log_users_group_is_not_allowed = rail.WriteLogOperator(
            task_id="log_users_group_is_not_allowed",
            severity='Skipped',
            message='User group is not allowed. Hence, cannot be disabled',
            properties={
                'userloginname': '{{ dag_run.conf.userloginname }}',
                'action': 'Disable',
                'status': 'Skipped',
                'details': 'User group is not allowed. Hence, cannot be disabled',
                'employee_id': "{{dag_run.conf.emp_id}}",
                'user_name': "{{dag_run.conf.user_first_name}}" + "." + "{{dag_run.conf.user_last_name}}"
            }

        )

        get_my_actual_useridentity = rail.RepliconServiceOperator(
            task_id='get_my_actual_useridentity',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity'
        )

        is_integrationuser = rail.IfOperator(
            task_id='is_integrationuser',
            test="{{ result('get_my_actual_useridentity').loginName == dag_run.conf.userloginname }}",
            yes_task='write_exception_integrationuser',
            no_task='validate_userstartdate'
        )

        write_exception_integrationuser = rail.WriteLogOperator(
            task_id='write_exception_integrationuser',
            severity='Skipped',
            message='User is used for integration. Hence, cannot be disabled',
            properties={
                'userloginname': '{{ dag_run.conf.userloginname }}',
                'action': 'Disable',
                'status': 'Skipped',
                'details': 'User is used for integration. Hence, cannot be disabled',
                'employee_id': "{{dag_run.conf.emp_id}}",
                'user_name': "{{dag_run.conf.user_first_name}}" + "." + "{{dag_run.conf.user_last_name}}"
            }
        )

        validate_userstartdate = rail.EmptyOperator(
            task_id='validate_userstartdate'
        )

        is_startdate_future = rail.IfOperator(
            task_id='is_startdate_future',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], '%b %d, %Y') > datetime.now(),
            yes_task='write_exception_future_startdate',
            no_task='get_direct_reports'
        )

        write_exception_future_startdate = rail.WriteLogOperator(
            task_id='write_exception_future_startdate',
            severity='Exception',
            message='User\'s start date ({{ dag_run.conf.startdate }}) is in future',
            properties={
                'userloginname': '{{ dag_run.conf.userloginname }}',
                'action': 'Disable',
                'status': 'Exception',
                'details': 'User\'s start date ({{ dag_run.conf.startdate }}) is in future',
                'user_name': "{{dag_run.conf.user_first_name}}" + "." + "{{dag_run.conf.user_last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
            }
        )

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint='/services/UserService1.svc/GetDirectReportsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['user_uri'],
                'asOfDate': get_today_date(),
                'userStatusOptionUri': 'urn:replicon:user-status-option:include-only-enabled-users'
            }
        )

        get_users_current_timesheet_end_date = rail.RepliconServiceOperator(
            task_id="get_users_current_timesheet_end_date",
            endpoint="services/TimesheetService1.svc/GetNextTimesheetDueDate",
            data={
                "userUri": "{{ dag_run.conf.user_uri }}",
                "asOfDate": get_today_date()
            },
            data_handler=lambda response: response if response else get_today_date()
        )

        is_supervisor = rail.IfOperator(
            task_id='is_supervisor',
            test=lambda: len(rail.result("get_direct_reports")) > 0,
            yes_task='is_default_supervisor_present',
            no_task='update_employment_daterange'
        )

        is_default_supervisor_present = rail.IfOperator(
            task_id="is_default_supervisor_present",
            test="{{ dag_run.conf.default_supervisor_uri | is_truthy }}",
            yes_task="create_default_supervisor_log",
            no_task="log_exception_default_supervisor_not_present"
        )

        create_default_supervisor_log = rail.CreateLogOperator(
            task_id="create_default_supervisor_log"
        )

        log_exception_default_supervisor_not_present = rail.PythonOperator(
            task_id="log_exception_default_supervisor_not_present",
            python_callable=lambda: "User marked for disabled, however the default supervisor was not found. Please update manually",
        )

        change_supervisor_of_reportees = rail.TriggerDagRunForEachItemOperator(
            task_id="change_supervisor_of_reportees",
            items="{{ result('get_direct_reports') | to_json }}",
            trigger_dag_id=f"macquarie_user_import_disable_users_update_supervisor_child_{config.instance}",
            conf=lambda item, dag_run: {
                "file_name": dag_run.conf['file_name'],
                "user_uri": item['uri'],
                    "user_loginname": item['loginName'],
                    "default_supervisor_uri": dag_run.conf['default_supervisor_uri'],
                    "current_supervisor_uri": dag_run.conf['user_uri'],
                    "current_supervisor_login_name": dag_run.conf['userloginname'],
                    "default_supervisor_log": rail.result('create_default_supervisor_log'),
                    "default_supervisor_effective_date": rail.result("get_users_current_timesheet_end_date")
            },
            execution_timeout=timedelta(days=config.execution_timeout_days),
            retries=0
        )

        wait_for_change_supervisor_of_reportees = rail.WaitForDagRunsSensor(
            task_id="wait_for_change_supervisor_of_reportees",
            dag_runs="{{result('change_supervisor_of_reportees')}}",
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=update_employment_daterange_user
        )

        update_actual_end_date = rail.RepliconServiceOperator(
            task_id="update_actual_end_date",
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                    "user": {
                        "uri": "{{ dag_run.conf.user_uri }}"
                    },
                "modifications": {
                        "customFieldValuesToApply": [
                            {
                                "customField": {
                                    "uri": "{{dag_run.conf.actual_end_date_udf_uri}}",
                                },
                                "text": None,
                                "date": get_today_date(),
                                "dropDownOption": None,
                                "number": None
                            }
                        ]
                    },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        def get_log(dag_run):
            has_error = False
            has_exception = False
            log_message = "User marked for disabled Successfully."
            if rail.result("log_exception_default_supervisor_not_present"):
                has_exception = True
                log_message = rail.result(
                    "log_exception_default_supervisor_not_present")

            elif rail.result('create_default_supervisor_log'):
                supervisor_logs = rail.load_all_records(
                    rail.result('create_default_supervisor_log'))
                if rail.find_first_by_attr_and_get_attr(supervisor_logs, "message", "Error"):
                    has_error = True
                    log_message = "User marked for disabled Successfully. Default Supervisor assignment was not successful"
                else:
                    log_message = "User marked for disabled Successfully. reportees supervisor updated"
            else:
                pass

            return {
                'userloginname': dag_run.conf['userloginname'],
                'user_name': dag_run.conf['user_first_name'] + dag_run.conf['user_last_name'],
                'employee_id': dag_run.conf['emp_id'],
                'action': 'Disable',
                'status': 'Success' if not has_exception and not has_error else ("Exception" if has_exception else "Error"),
                'details': log_message,
            }

        write_disableduser_log = rail.WriteLogOperator(
            task_id='write_disableduser_log',
            severity='Success',
            message='User disabled successfully',
            properties=get_log
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'userloginname': '{{ dag_run.conf.userloginname }}',
                'user_name': "{{dag_run.conf.user_first_name}}" + "." + "{{dag_run.conf.user_last_name}}",
                'employee_id': "{{dag_run.conf.emp_id}}",
                'action': 'Disable',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> rail.Label("On Error") >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> can_disable_user
        can_disable_user >> rail.Label(
            'Yes') >> get_my_actual_useridentity

        can_disable_user >> rail.Label(
            'No') >> log_users_group_is_not_allowed >> catch_and_log_errors

        get_my_actual_useridentity >> is_integrationuser

        is_integrationuser >> rail.Label(
            'Yes') >> write_exception_integrationuser >> catch_and_log_errors

        is_integrationuser >> rail.Label(
            'No') >> validate_userstartdate >> is_startdate_future

        is_startdate_future >> rail.Label(
            'Yes') >> write_exception_future_startdate >> catch_and_log_errors

        is_startdate_future >> rail.Label(
            'No') >> get_direct_reports >> get_users_current_timesheet_end_date >> is_supervisor

        is_supervisor >> rail.Label(
            'Yes') >> is_default_supervisor_present >> rail.Label("Yes") >> create_default_supervisor_log \
            >> change_supervisor_of_reportees >> wait_for_change_supervisor_of_reportees >> update_employment_daterange

        is_default_supervisor_present >> rail.Label(
            "No") >> log_exception_default_supervisor_not_present >> update_employment_daterange

        is_supervisor >> rail.Label(
            'No') >> update_employment_daterange >> update_actual_end_date >> write_disableduser_log >> \
            catch_and_log_errors

        catch_and_log_errors >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_disableuser_child_dag)
