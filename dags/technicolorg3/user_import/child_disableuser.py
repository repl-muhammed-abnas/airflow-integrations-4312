from datetime import datetime, timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.utils.request_payload import get_today_date, update_employment_daterange_user


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_disableuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_disableuser_{config.instance}',
        description=f'Technicolor_Child_Workflow to disable user {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_disableuser_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_user_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_user_log',
            end_task='catch_and_log_errors',
        )

        create_user_log = rail.CreateLogOperator(
            task_id='create_user_log'
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
            log="{{ result('create_user_log') }}",
            severity='Skipped',
            message='User is used for integration. Hence, cannot be disabled',
            properties={
                'globalid': '{{ dag_run.conf.employeeid }}',
                'action': 'Disable',
                'status': 'Skipped',
                'details': 'User is used for integration. Hence, cannot be disabled',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        validate_userstartdate = rail.EmptyOperator(
            task_id='validate_userstartdate'
        )

        is_startdate_future = rail.IfOperator(
            task_id='is_startdate_future',
            test=lambda dag_run: datetime.strptime(
                dag_run.conf['startdate'], '%d %B %Y') > datetime.now(),
            yes_task='write_exception_future_startdate',
            no_task='get_direct_reports'
        )

        write_exception_future_startdate = rail.WriteLogOperator(
            task_id='write_exception_future_startdate',
            log="{{ result('create_user_log') }}",
            severity='Exception',
            message='User\'s start date ({{ dag_run.conf.startdate }}) is in future',
            properties={
                'globalid': '{{ dag_run.conf.employeeid }}',
                'action': 'Disable',
                'status': 'Exception',
                'details': 'User\'s start date ({{ dag_run.conf.startdate }}) is in future',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        get_direct_reports = rail.RepliconServiceOperator(
            task_id='get_direct_reports',
            endpoint='/services/UserService1.svc/GetDirectReportsForUser',
            data=lambda dag_run: {
                'userUri': dag_run.conf['useruri'],
                'asOfDate': get_today_date(),
                'userStatusOptionUri': 'urn:replicon:user-status-option:include-only-enabled-users'
            }
        )

        is_supervisor = rail.IfOperator(
            task_id='is_supervisor',
            test="{{ result('get_direct_reports') | map_to_attr('loginName') | length > 0 }}",
            yes_task='write_supervisor_exception',
            no_task='disable_login'
        )

        write_supervisor_exception = rail.WriteLogOperator(
            task_id='write_supervisor_exception',
            log="{{ result('create_user_log') }}",
            severity='Exception',
            message="User is supervisor for {{ result('get_direct_reports') | map_to_attr('loginName') | length }} users",
            properties={
                'globalid': '{{ dag_run.conf.employeeid }}',
                'action': 'Disable',
                'status': 'Exception',
                'details': "User is supervisor for {{ result('get_direct_reports') | map_to_attr('loginName') | length }} users",
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=update_employment_daterange_user
        )

        write_disableduser_log = rail.WriteLogOperator(
            task_id='write_disableduser_log',
            log="{{ result('create_user_log') }}",
            severity='Success',
            message='User disabled successfully',
            properties={
                'globalid': '{{ dag_run.conf.employeeid }}',
                'action': 'Disable',
                'status': 'Success',
                'details': 'User disabled successfully',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ result('create_user_log') }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'globalid': '{{ dag_run.conf.employeeid }}',
                'action': 'Disable',
                'status': 'Error',
                'details': '{{ get_error_message() }}',
                'username': '{{ dag_run.conf.username }}',
                'new_location': '',
                'location': ''
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.sumo_conn_id,
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> create_user_log

        create_user_log >> get_my_actual_useridentity >> is_integrationuser

        is_integrationuser >> rail.Label(
            'Yes') >> write_exception_integrationuser >> catch_and_log_errors

        is_integrationuser >> rail.Label(
            'No') >> validate_userstartdate >> is_startdate_future

        is_startdate_future >> rail.Label(
            'Yes') >> write_exception_future_startdate >> catch_and_log_errors

        is_startdate_future >> rail.Label(
            'No') >> get_direct_reports >> is_supervisor

        is_supervisor >> rail.Label(
            'Yes') >> write_supervisor_exception >> catch_and_log_errors

        is_supervisor >> rail.Label(
            'No') >> disable_login >> update_employment_daterange >> write_disableduser_log >> \
            catch_and_log_errors

        catch_and_log_errors >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_disableuser_child_dag)
