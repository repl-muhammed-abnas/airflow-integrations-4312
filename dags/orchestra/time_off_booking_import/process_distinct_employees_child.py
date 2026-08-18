from datetime import timedelta
from airflow.models import Variable
import rail

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_distinct_employees_dagid,
        description="orchestra Time Off Booking Import Process Distinct Employees",
        replicon_conn_id=config.replicon_conn_id,
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_employee_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_employee_data',
            end_task='catch_and_log_errors',
        )

        query_employee_data = rail.QueryCollectionOperator(
            task_id="query_employee_data",
            name='employee_data',
            query="""SELECT DISTINCT * FROM valid_records WHERE loginname='{{ dag_run.conf.loginname }}'"""
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/UserService1.svc/BulkGetUsers2",
            data={
                "users": [
                    {
                        "loginName": "{{ dag_run.conf.loginname }}"
                    }
                ]
            },
            data_handler=lambda data: data[0] if data else []
        )

        is_user_present = rail.IfOperator(
            task_id="is_user_present",
            test=lambda: bool(rail.result('get_user_details')),
            yes_task="process_each_timeoff",
            no_task="log_user_not_present"
        )

        log_user_not_present = rail.WriteLogOperator(
            task_id='log_user_not_present',
            log='{{ dag_run.conf.log }}',
            items='{{result("query_employee_data")}}',
            message="User with loginname - '{{ dag_run.conf.loginname }}' is not present/disabled in replicon",
            severity='Exception',
            properties={
                'booking_id': "{{item.booking_id}}",
                'loginname': "{{item.loginname}}",
                'time_off_type': "{{ item.timeofftype }}",
                'start_date': "{{ item.startdate }}",
                'end_date': "{{ item.enddate }}",
                'action':'Validation',
                'status': 'Exception',
                'details': "User with loginname - '{{dag_run.conf.loginname}}' is not present/disabled in replicon",
            }
        )

        process_each_timeoff = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_timeoff',
            items="{{ result('query_employee_data') }}",
            trigger_dag_id=config.process_each_timeoff_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item, dag_run:{
                'booking_id': item['booking_id'],
                'employee_id': item['employee_id'],
                'loginname': item['loginname'],
                'time_off_type': item['timeofftype'],
                'startdate': item['startdate'],
                'enddate': item['enddate'],
                'hours': item['hours'],
                'action': item['action'],
                'timeoff_uri': dag_run.conf['timeoff_uri'],
                'booking_id_oef_uri': dag_run.conf['booking_id_oef_uri'],
                'user_uri': rail.result("get_user_details")['uri'],
                'log': dag_run.conf['log']
            }
        )

        wait_for_process_each_timeoff = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_timeoff',
            dag_runs='{{ result("process_each_timeoff") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            items='{{result("query_employee_data")}}',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                'booking_id': "{{ item.booking_id }}",
                'loginname': "{{ item.loginname }}",
                'time_off_type': "{{ item.timeofftype }}",
                'start_date': "{{ item.startdate }}",
                'end_date': "{{ item.enddate }}",
                'action':'Validation',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> query_employee_data

        query_employee_data >> get_user_details >> is_user_present

        is_user_present >> rail.Label(
            'No') >> log_user_not_present >> catch_and_log_errors
        is_user_present >> rail.Label(
            'Yes') >> process_each_timeoff >> wait_for_process_each_timeoff >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)
