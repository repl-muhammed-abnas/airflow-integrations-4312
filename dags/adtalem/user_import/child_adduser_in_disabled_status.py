from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_datetime_obj


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_add_disableuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_child_add_user_disabledstatus_{config.instance}',
        description=f'Adtalem userimport Child_Add User in Disabled status_CR14.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_adduser_disabledstatus'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='process_adduser_disabledstatus',
            end_task='catch_and_log_errors',
        )

        process_adduser_disabledstatus = rail.EmptyOperator(
            task_id='process_adduser_disabledstatus'
        )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint="/services/ImportService1.svc/PutUser3",
            data=lambda dag_run: {
                "user": {
                    "target": {
                        "loginName": dag_run.conf['loginname']
                    },
                    "firstname": dag_run.conf['firstname'],
                    "lastname": dag_run.conf['lastname'],
                    "emailAddress": dag_run.conf['emailaddress'],
                    "employeeId": dag_run.conf['employeeid'],
                    "department": {
                        "name": "Adtalem",
                    },
                    "schedulePolicySchedule": [{
                        "schedulePolicy": {
                            "name": "Mon - Fri (8*5)",
                            "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                        },
                    }],
                    "employmentDateRange": {
                        "startDate": get_datetime_obj(dag_run.conf['startdate'])
                    },
                    "securityConfiguration": {
                        "enabledAuthenticationTypeUris": [
                            "urn:replicon:user-authentication-type:sso"
                        ],
                        "isLoginEnabled": "false",
                        "loginName": dag_run.conf['loginname'],
                        "SSOName": dag_run.conf['loginname']
                    },
                    "employeeType": {
                        "name": "Temp"
                    }
                }
            }
        )

        remove_timeofftypes = rail.RepliconServiceOperator(
            task_id='remove_timeofftypes',
            endpoint="/services/TimeOffService1.svc/PutTimeOffTypeAssignmentsForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "timeOffTypeUris": []
            }
        )

        updatedefaultschedule = rail.RepliconServiceOperator(
            task_id='updatedefaultschedule',
            endpoint="/services/SchedulingService2.svc/PutSchedulePolicyScheduleForUser",
            data={
                "userUri": "{{ result('create_user').uri }}",
                "scheduleEntries": [
                    {
                        "schedulePolicy": {
                            "name": "All Week"
                        }
                    }
                ]
            }
        )

        write_disableuser_log = rail.WriteLogOperator(
            task_id='write_disableuser_log',
            log='{{ dag_run.conf.log }}',
            message="User Added in Disabled status, part of excluded job code list",
            severity="Success",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': "User Added in Disabled status, part of excluded job code list",
                'failure_reason': ''
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf.log }}',
            trigger_rule='one_failed',
            severity='Error',
            message="{{ get_error_message() }}",
            properties={
                'login_name': '{{ dag_run.conf.loginname }}',
                'status': 'Error',
                # pylint: disable=line-too-long
                'failure_reason': "User \"{{ dag_run.conf.firstname }} {{ dag_run.conf.lastname }}\" not created: {{ get_error_message() }}"
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.sumo_conn_id
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
            'No') >> process_adduser_disabledstatus

        process_adduser_disabledstatus >> create_user >> remove_timeofftypes >> updatedefaultschedule >> \
            write_disableuser_log >> catch_and_log_errors

        catch_and_log_errors >> dagrun_log_to_sumo

    return dag


rail.for_each_instance(create_add_disableuser_child_dag)
