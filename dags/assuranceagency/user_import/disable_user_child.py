import rail
from assuranceagency.user_import.utils import python_callable
from assuranceagency.user_import.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'assuranceagency_user_import_disable_user_child_{config.instance}',
        description=f'assuranceagency_user_import_disable_user_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_disable_user_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_date_range_data = rail.PythonOperator(
            task_id = "get_date_range_data",
            python_callable=python_callable.get_daterange_data
        )

        is_date_range_less_than_0 = rail.IfOperator(
            task_id="is_date_range_less_than_0",
            test="{{ result('get_date_range_data') < 0}}",
            yes_task="log_skipped_disable",
            no_task="disable_user_login"
        )

        log_skipped_disable = rail.WriteLogOperator(
            task_id='log_skipped_disable',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties={
                "username" : "{{ dag_run.conf.username }}",
                "login_name": "{{ dag_run.conf.userloginname }}",
                "emplid" : "{{ dag_run.conf.emplid }}",
                "action" : "Disable",
                "status": "Skipped",
                "details": "User start date {{ dag_run.conf.startdate }} is in future to end date"
            }
        )

        disable_user_login = rail.RepliconServiceOperator(
            task_id='disable_user_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data={
                'userUri': '{{ dag_run.conf.useruri }}'
            }
        )

        update_employment_daterange = rail.RepliconServiceOperator(
            task_id='update_employment_daterange',
            endpoint="/services/UserService1.svc/UpdateEmploymentDateRange",
            data=request_payload.update_emp_date
        )

        log_sucess_disable = rail.WriteLogOperator(
            task_id='log_sucess_disable',
            log = "{{ dag_run.conf.logger }}",
            message="na",
            severity="Skipped",
            properties={
                "username" : "{{ dag_run.conf.username }}",
                "login_name": "{{ dag_run.conf.userloginname }}",
                "emplid" : "{{ dag_run.conf.emplid }}",
                "action" : "Disable",
                "status": "Success",
                "details": "User profile disabled successfully"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log = "{{ dag_run.conf.logger }}",
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'login_name': '{{ dag_run.conf.userloginname }}',
                'username': "{{ dag_run.conf.username }}",
                'emplid': "{{ dag_run.conf.emplid }}",
                'action': 'Disable',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            trigger_rule='all_done'
        )

        get_date_range_data >> is_date_range_less_than_0 >> rail.Label('Yes') >> log_skipped_disable >> catch_and_log_errors

        is_date_range_less_than_0 >> rail.Label('No') >> disable_user_login >> update_employment_daterange >> \
            log_sucess_disable >> catch_and_log_errors

        catch_and_log_errors >> log_dagrun_to_sumo


    return dag

rail.for_each_instance(create_dag)
