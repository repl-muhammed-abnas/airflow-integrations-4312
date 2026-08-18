import json
from datetime import datetime, timedelta
from airflow.models import Variable
import rail
from mci.timeoff_sync.utils import request_payload

null = None
DATE_FORMAT = "%b %d, %Y"

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.mci_timeoff_sync_puttimeentry_in_paycom_child,
        description=f'MCIUSA_PutTimeOffEntry In PayCom_Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
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
            no_task='process_timeoff_sync_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='process_timeoff_sync_log',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        process_timeoff_sync_log = rail.CreateLogOperator(
            task_id="process_timeoff_sync_log"
        )

        foreach_user_timeoff_data = rail.ForEachOperator(
            task_id='foreach_user_timeoff_data',
            items="{{ dag_run.conf.usertimeoffdata | to_json}}",
            start_task='get_punch_history_payload',
            end_task='foreach_user_timeoff_data_end'
        )

        get_punch_history_payload = rail.PythonOperator(
            task_id='get_punch_history_payload',
            python_callable=lambda: request_payload.get_punch_history_daterange_payload(
                rail.result('foreach_user_timeoff_data')['employeeid'],
                rail.result('foreach_user_timeoff_data')['entrydate'],
            )
        )

        # https://api.paycomonline.net/v4/rest/index.php
        # punch_history_for_daterange = SimpleHttpOperator(
        #     task_id='punch_history_for_daterange',
        #     method='GET',
        #     http_conn_id=config.http_conn_id,
        #     # pylint: disable=consider-using-f-string
        #     endpoint="/api/v1/employee/{{ result('foreach_user_timeoff_data').employeeid }}/punchhistory",
        #     # endpoint="/api/v1/employee/punchhistory",
        #     data="{{ result('get_punch_history_payload') | to_json }}",
        #     extra_options={
        #         'verify': False
        #     },
        # )

        punch_history_for_daterange = rail.PythonOperator(
            task_id='punch_history_for_daterange',
            # python_callable=lambda: python_callable.get_punch_history_daterange_data()
            python_callable=lambda dag_run: json.loads(dag_run.conf['conf'])
        )

        if_punch_history_data_presence = rail.IfOperator(
            task_id='if_punch_history_data_presence',
            test=lambda: bool(
                rail.result('punch_history_for_daterange') and
                rail.result('punch_history_for_daterange').get('response', {}).get('data', [])
            ),
            yes_task="get_punch_ids",
            no_task="foreach_user_timeoff_data_end",
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

        foreach_user_timeoff_data_end = rail.EmptyOperator(
            task_id='foreach_user_timeoff_data_end',
        )

        def get_sync_timeoffdata(dag_run):
            to_list_data = []
            user_timeoff_data = dag_run.conf['usertimeoffdata']
            for user_to in user_timeoff_data:
                to_list_data.append({
                    "Eecode": user_to['employeeid'],
                    "Entrytype": 2,
                    "Punchtype": None,
                    "Timezone": 'EST',
                    "Punchtime": int((datetime.strptime(user_to['entrydate'] + ' 1:00:00 PM', DATE_FORMAT + ' %I:%M:%S %p')).timestamp()),
                    "Hours": user_to['timeoffhours'],
                    "Earncode": user_to['paycode']
                })
            return {
                "Timedatalist": to_list_data
            }

        map_time_schema = rail.PythonOperator(
            task_id='map_time_schema',
            python_callable=lambda dag_run: get_sync_timeoffdata(dag_run)
        )

        send_request = rail.PythonOperator(
            task_id='send_request',
            python_callable=lambda dag_run: get_sync_timeoffdata(dag_run)
        )

        # send_request = SimpleHttpOperator(
        #     task_id='send_request',
        #     method='POST',
        #     endpoint='/api/v1.1/punchimport',
        #     http_conn_id=config.http_conn_id,
        #     response_filter=lambda response: response.json()['issues'],
        #     data="{{ result('map_time_schema') | to_json}}"
        # )

        write_log_success = rail.WriteLogOperator(
            task_id='write_log_success',
            log = '{{result("process_timeoff_sync_log")}}',
            message="na",
            severity="Success",
            properties={
                "Employee_ID": "{{ dag_run.conf.usertimeoffdata[0].employeeid }}",
                "username": "{{ dag_run.conf.usertimeoffdata[0].username }}",
                "status": "Success",
                "details": "time off added successfully for this user for the approval date",
                "child_job_id": "{{ dag_run_ecid() }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log = '{{result("process_timeoff_sync_log")}}',
            message="na",
            severity="Error",
            trigger_rule='one_failed',
            properties={
                "Employee_ID": "{{ dag_run.conf.usertimeoffdata[0].employeeid }}",
                "username": "{{ dag_run.conf.usertimeoffdata[0].username }}",
                "status": "Error",
                "details": "{{get_error_message()}}",
                "child_job_id": "{{ dag_run_ecid() }}"
            }
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> process_timeoff_sync_log >> foreach_user_timeoff_data
        foreach_user_timeoff_data >> get_punch_history_payload >> punch_history_for_daterange >> if_punch_history_data_presence
        if_punch_history_data_presence >> rail.Label(
            'Yes') >> get_punch_ids >> foreach_get_punch_ids
        foreach_get_punch_ids >> delete_punch >> foreach_get_punch_ids_end
        foreach_get_punch_ids >> foreach_get_punch_ids_end >> foreach_user_timeoff_data_end
        foreach_user_timeoff_data >> foreach_user_timeoff_data_end
        if_punch_history_data_presence >> rail.Label(
            'No') >> foreach_user_timeoff_data_end >> map_time_schema >> send_request >> write_log_success >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
