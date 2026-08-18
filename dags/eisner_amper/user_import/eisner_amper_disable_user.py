import rail
from eisner_amper.user_import.utils import response_filter, request_payload
from datetime import datetime, timedelta

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.disble_user_dag_id,
        description=f"Eisner Amper disable user Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        bulk_get_user = rail.RepliconServiceOperator(
            task_id='bulk_get_user',
            endpoint='/services/ImportService1.svc/BulkGetUsers3',
            data=request_payload.bulk_get_user_payload
        )

        is_user_enabled = rail.IfOperator(
            task_id='is_user_enabled',
            test=lambda dag_run: (rail.result('bulk_get_user')[
                                  0]['userDetails']['isEnabled'] and True if dag_run.conf['workagreementstatus'] == "0" else False),
            yes_task='update_employmentdaterange',
            no_task='is_user_disabled'
        )

        update_employmentdaterange = rail.RepliconServiceOperator(
            task_id='update_employmentdaterange',
            endpoint='/services/UserService1.svc/UpdateEmploymentDateRange',
            data=request_payload.update_employmentdaterange_payload
        )

        disable_login = rail.RepliconServiceOperator(
            task_id='disable_login',
            endpoint='/services/SecurityService1.svc/DisableLogin',
            data=request_payload.disable_login_payload
        )

        log_disable_log = rail.WriteLogOperator(
            task_id='log_disable_log',
            message="User disabled in Replicon",
            log='{{dag_run.conf.log}}',
            severity='Success',
            properties={
                'employeeid': "{{dag_run.conf.personexternalid}}",
                'loginname': "{{dag_run.conf.name}}",
                'action': "Disable",
                'status': "Success",
                'details': "User disabled in Replicon",
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            }
        )

        is_user_disabled = rail.IfOperator(
            task_id='is_user_disabled',
            test=lambda dag_run: (rail.result('bulk_get_user')[
                                  0]['userDetails']['isEnabled'] == False and True if dag_run.conf['workagreementstatus'] == "0" else False),
            yes_task='log_disabled_log'
        )

        log_disabled_log = rail.WriteLogOperator(
            task_id='log_disabled_log',
            message="User already disabled in Replicon",
            log='{{dag_run.conf.log}}',
            severity='Success',
            properties={
                'employeeid': "{{dag_run.conf.personexternalid}}",
                'loginname': "{{dag_run.conf.name}}",
                'action': "Disable",
                'status': "Skipped",
                'details': "User already disabled in Replicon",
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{dag_run.conf.log}}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'employeeid': "{{dag_run.conf.personexternalid}}",
                'loginname': "{{dag_run.conf.name}}",
                'action': "Disable",
                'status': "Error",
                'details': '{{ get_error_message() }}',
                'jobid': "{{dag_run_ecid()}}",
                'childjobid': '',
            },
        )

        bulk_get_user >> is_user_enabled >> rail.Label(
            "Yes") >> update_employmentdaterange >> disable_login >> log_disable_log >> catch_and_log_errors

        is_user_enabled >> rail.Label("No") >> is_user_disabled >> rail.Label(
            "Yes") >> log_disabled_log >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
