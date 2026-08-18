from datetime import timedelta
from uuid import uuid4
from pendulum import datetime
import rail
from mammoet.time_export_v1.utils.request_payload import get_all_timesheet_for_user


OPEN_BRACKETS = '{{'
CLOSE_BRACKETS = '}}'

#pylint: disable=too-many-statements

def create_main_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.time_export_process_timesheets_dag_id,
        description="Mammoet Time Export process time export",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.user_process_max_active_run,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        get_all_records_for_the_user = rail.QueryCollectionOperator(
            task_id = "get_all_records_for_the_user",
            query="""SELECT * FROM blank_replicon_ids WHERE employee_id = :_employee_id""",
            name="user_ts_data",
            query_params={
                "_employee_id": "{{dag_run.conf.employee_id}}"
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "employeeId": "{{ dag_run.conf.employee_id }}",
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:omit-data-if-insufficient-access-permission"
            },
            data_handler=lambda response: response[0]
        )

        process_per_timeentry = rail.trigger_parallel_dagrun(
            task_id = "process_per_timeentry",
            items="{{result('get_all_records_for_the_user')}}",
            trigger_dag_id=config.time_export_process_timesheets_time_entries_dag_id,
            parallel_count=5,
            conf=lambda item, dag_run: {
                **item,
                **dag_run.conf
                },
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )


        get_all_records_for_the_user >> get_user_details >> process_per_timeentry


    return dag

rail.for_each_instance(create_main_dag)
