from datetime import timedelta
import rail
from pendulum import datetime,now
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
from assuredpartnersinc.timeoff_balance_export_v1.utils import custom_methods
from assuredpartnersinc.timeoff_balance_export_v1.utils import request_payload
from assuredpartnersinc.timeoff_balance_export_v1.tasks.upload_users_timeoff_data import get_users_timeoff_data

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"assuredpartnersinc_timeoff_balance_export_master_dag_{config.instance}_v1",
        description=f"AssuredpartnersInc Timeoff Balance Export Master {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 10, 10, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        catchup=False,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs
    ) as dag:

        def get_run_date_schedule():
            _today = now(config.time_zone)
            run_date = Variable.get(config.next_run_date, default_var=_today.strftime("%d/%m/%Y"))
            if run_date == _today.strftime("%d/%m/%Y"):
                next_run_date = (_today + timedelta(days=14)).strftime("%d/%m/%Y")
                Variable.set(config.next_run_date, next_run_date)
                return run_date
            return False

        if_run_date_in_schedules = rail.IfOperator(
            task_id="if_run_date_in_schedules",
            test=get_run_date_schedule,
            yes_task="get_logging_details"
        )
        
        get_logging_details = rail.PythonOperator(
            task_id='get_logging_details',
            python_callable=custom_methods.logging_details,
            op_args=[config.time_zone]
        )

        get_enabled_users_timeoff_report = rail.RepliconReportDetailsOperator(
            task_id='get_enabled_users_timeoff_report',
            report_name=config.enabled_users_timeoff_report,
        )

        run_report_group_entry, run_report_group_exit = rail.run_report(
            group_id='run_enabled_users_report',
            report_params=request_payload.get_enabled_users_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        enabled_users_report_has_data, enabled_user_timeoff_data_upload_status = get_users_timeoff_data("enabled")

        get_disabled_users_timeoff_report = rail.RepliconReportDetailsOperator(
            task_id='get_disabled_users_timeoff_report',
            report_name=config.disabled_users_timeoff_report,
        )

        run_disabled_users_report_group_entry, run_disabled_users_report_group_exit = rail.run_report(
            group_id='run_disabled_users_report',
            report_params=request_payload.get_disabled_users_batch_payload,
            replicon_conn_id=config.replicon_conn_id,
        )

        disabled_users_report_has_data, disabled_user_timeoff_data_upload_status = get_users_timeoff_data("disabled")

        get_query_to_merge = rail.PythonOperator(
            task_id='get_query_to_merge',
            python_callable=custom_methods.get_query_merge_master
        )

        merge_all_master_timeoff_data = rail.QueryCollectionOperator(
            task_id='merge_all_master_timeoff_data',
            query='{{result("get_query_to_merge")}}',
            name='processed_master_timeoff_data'
        )

        processed_timeoff_data = rail.CreateCollectionOperator(
            task_id='processed_timeoff_data',
            source='{{ result("merge_all_master_timeoff_data") }}',
            name='processed_timeoff_data',
            columns={"employeeid": "employeeid", "companycode": "companycode", "timeofftype": "timeofftype",
                     "timeoffaccrued": "timeoffaccrued", "timeofftaken": "timeofftaken", "timeoffbalance": "timeoffbalance",
                     "headercode": "headercode", "ptocode": "ptocode"}
        )

        timeoff_balance_report_export = rail.TriggerDagRunOperator(
            task_id='timeoff_balance_report_export',
            trigger_dag_id=f'assuredpartnersinc_timeoff_balance_export_report_child_dag_{config.instance}_v1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "dag_run_ecid": get_dagrun_ecid(dag_run),
                "dateaccruedthru": rail.result('get_logging_details')['dateaccruedthru'],
                "periodenddate": rail.result('get_logging_details')['periodenddate'],
                "jobdateformatted": rail.result('get_logging_details')['jobdateformatted'],
                "dag_start_date": rail.result('get_logging_details')['dag_run_start_time']
            }
        )

        if_run_date_in_schedules >> rail.Label("Yes") >>\
        get_logging_details >> get_enabled_users_timeoff_report >> run_report_group_entry
        run_report_group_exit >> enabled_users_report_has_data
        enabled_user_timeoff_data_upload_status >> get_disabled_users_timeoff_report

        get_disabled_users_timeoff_report >> run_disabled_users_report_group_entry
        run_disabled_users_report_group_exit >> disabled_users_report_has_data
        disabled_user_timeoff_data_upload_status >> get_query_to_merge >> merge_all_master_timeoff_data \
            >> processed_timeoff_data >> timeoff_balance_report_export

    return dag


rail.for_each_instance(create_dag)
