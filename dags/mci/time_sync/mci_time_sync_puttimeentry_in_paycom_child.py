
from datetime import timedelta, datetime
import json
import pendulum
from airflow.models import Variable
from mci.time_sync.utils import python_callable
from mci.time_sync.utils import request_payload
import rail

null = None

DATE_FORMAT = "%b %d, %Y"
def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.mci_time_sync_puttimeentry_in_paycom_child,
        description=f'MCIUSA_PutTimeEntry In PayCom_Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # schedule_interval=config.schedule_interval,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='process_time_sync_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_time_sync_log',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        process_time_sync_log = rail.CreateLogOperator(
            task_id="process_time_sync_log"
        )

        get_punch_history_payload = rail.PythonOperator(
            task_id='get_punch_history_payload',
            python_callable=lambda dag_run: request_payload.get_punch_history_data_payload(dag_run.conf['time_sync_user_data'][0]['employeeid'], dag_run)
        )

        def get_punch_history_data(dag_run):
            conf_data = json.loads(dag_run.conf['conf'])
            conf_data.pop('start_date', None)
            conf_data.pop('end_date', None)
            return conf_data

        punch_history_data = rail.PythonOperator(
            task_id='punch_history_data',
            python_callable=get_punch_history_data
            # python_callable=lambda: python_callable.get_punch_history()
        )

        # https://api.paycomonline.net/v4/rest/index.php
        # punch_history_data = SimpleHttpOperator(
        #     task_id='punch_history_data',
        #     method='GET',
        #     http_conn_id=config.http_conn_id,
        #     # pylint: disable=consider-using-f-string
        #     endpoint="/api/v1/employee/{{ dag_run.conf.time_sync_user_data[0].employeeid }}/punchhistory",
        #     # endpoint="/api/v1/employee/punchhistory",
        #     # data=lambda dag_run: request_payload.get_punch_history_data_payload(dag_run),
        #     data="{{ result('get_punch_history_payload') | to_json }}",
        #     extra_options={
        #         'verify': False
        #     },
        # )

        if_data_presence_present = rail.IfOperator(
            task_id='if_data_presence_present',
            test=lambda: bool(rail.result('punch_history_data')),
            # test=lambda: False,
            yes_task="get_punch_ids",
            no_task="map_time_schema",
        )

        get_punch_ids = rail.PythonOperator(
            task_id='get_punch_ids',
            python_callable=lambda: request_payload.get_punch_ids()
        )

        foreach_get_punch_ids = rail.ForEachOperator(
            task_id='foreach_get_punch_ids',
            items="{{ result('get_punch_ids') }}",
            start_task='delete_punch',
            end_task='foreach_get_punch_ids_end'
        )

        # delete_punch = SimpleHttpOperator(
        #     task_id='delete_punch',
        #     method='GET',
        #     http_conn_id=config.http_conn_id,
        #     endpoint="/api/v1/punchimport/{{result('foreach_get_punch_ids').id}}",
        #     headers={
        #         'Content-Type': 'application/json'
        #     },
        #     # data='{{ result("get_punch_ids") }}',
        #     extra_options={
        #         'verify': False
        #     }
        # )
        delete_punch = rail.PythonOperator(
            task_id='delete_punch',
            python_callable=lambda: rail.result('foreach_get_punch_ids')['id']
        )

        foreach_get_punch_ids_end = rail.EmptyOperator(
            task_id='foreach_get_punch_ids_end',
        )

        def get_punch_time(entrydate, tz):
            date = datetime.strptime(entrydate, DATE_FORMAT)
            tz_obj = pendulum.timezone(tz)
            std_offset = pendulum.datetime(date.year, 1, 15, tz=tz_obj).offset
            dt = pendulum.datetime(date.year, date.month, date.day, tz=pendulum.fixed_timezone(std_offset))
            return int(dt.timestamp())


        def get_time_sync_data(dag_run, tz):
            to_list_data = []
            user_timeoff_data = dag_run.conf['time_sync_user_data']
            for user_time in user_timeoff_data:
                to_list_data.append({
                    "Eecode": user_time['employeeid'],
                    "Entrytype": 2,
                    "Punchtype": None,
                    "Timezone": 'PST',
                    "Punchtime": get_punch_time(user_time['entrydate'], tz),
                    "Hours": user_time['hours'],
                    "Earncode": user_time['paycode']
                })
            return to_list_data

        map_time_schema = rail.PythonOperator(
            task_id='map_time_schema',
            python_callable=lambda dag_run: get_time_sync_data(dag_run, "America/Los_Angeles")
        )

        # https://api.paycomonline.net/v4/rest/index.php
        # punch_import = SimpleHttpOperator(
        #     task_id='punch_import',
        #     method='POST',
        #     http_conn_id=config.http_conn_id,
        #     endpoint="/api/v1.1/punchimport",
        #     data="{{ result('map_time_schema') | to_json}}",
        #     extra_options={
        #         'verify': False
        #     },
        # )

        punch_import = rail.PythonOperator(
            task_id='punch_import',
            python_callable=lambda: python_callable.punch_import()
        )

        log_success_entries = rail.WriteLogOperator(
            task_id='log_success_entries',
            message="Timesync update",
            log = '{{result("process_time_sync_log")}}',
            severity="Success",
            properties={
                "Employee_ID": "{{ dag_run.conf.time_sync_user_data[0].employeeid }}",
                "username": "{{ dag_run.conf.time_sync_user_data[0].username }}",
                "timesheetperiod": "{{ dag_run.conf.timesheetperiod }}",
                "employeetype": "{{ dag_run.conf.time_sync_user_data[0].employeetype }}",
                "status": "Success",
                "details": "completed successfully",
                "child_job_id": "{{ dag_run_ecid() }}"
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error ' +
            rail.render_template("{{get_error_message()}}")
        )

        if_error_is_descriptive = rail.IfOperator(
            task_id='if_error_is_descriptive',
            test=lambda: bool(rail.result('catch_error') and 'Cannot add entries in archived time' not in rail.result('catch_error')),
            yes_task="log_error_message",
            no_task="log_exception_message",
        )

        log_error_message = rail.WriteLogOperator(
            task_id='log_error_message',
            message="Error occurred",
            log = '{{result("process_time_sync_log")}}',
            severity="Error",
            properties={
                "Employee_ID": "{{ dag_run.conf.time_sync_user_data[0].employeeid }}",
                "username": "{{ dag_run.conf.time_sync_user_data[0].username }}",
                "timesheetperiod": "{{ dag_run.conf.timesheetperiod }}",
                "employeetype": "{{ dag_run.conf.time_sync_user_data[0].employeetype }}",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "child_job_id": "{{ dag_run_ecid() }}"
            }
        )

        log_exception_message = rail.WriteLogOperator(
            task_id='log_exception_message',
            message="na",
            log = '{{result("process_time_sync_log")}}',
            severity="Exception",
            properties={
                "Employee_ID": "{{ dag_run.conf.time_sync_user_data[0].employeeid }}",
                "username": "{{ dag_run.conf.time_sync_user_data[0].username }}",
                "timesheetperiod": "{{ dag_run.conf.timesheetperiod }}",
                "employeetype": "{{ dag_run.conf.time_sync_user_data[0].employeetype }}",
                "status": "Exception",
                "details": "{{get_error_message()}}",
                "child_job_id": "{{ dag_run_ecid() }}"
            }
        )

        stop = rail.EmptyOperator(
            task_id='stop',

        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> process_time_sync_log >> get_punch_history_payload >> punch_history_data
        punch_history_data >> if_data_presence_present
        if_data_presence_present >> rail.Label('Yes') >> get_punch_ids >> foreach_get_punch_ids
        foreach_get_punch_ids >> delete_punch >> foreach_get_punch_ids_end
        foreach_get_punch_ids >> foreach_get_punch_ids_end >> map_time_schema
        if_data_presence_present >> rail.Label(
            'No') >> map_time_schema >> punch_import >> log_success_entries >> catch_error >> if_error_is_descriptive
        if_error_is_descriptive >> rail.Label(
            'Yes') >> log_error_message >> stop
        if_error_is_descriptive >> rail.Label(
            'No') >> log_exception_message >> stop >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
