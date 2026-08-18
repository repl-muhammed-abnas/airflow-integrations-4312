from pendulum import datetime, now
import rail
from mammoet.payroll_export_france.utils.custom_methods import EXPORT_DATE_FORMAT
from mammoet.payroll_export_france.utils.request_payload import get_payroll_location_uri_payload
from mammoet.payroll_export_france.utils.response_filters import get_payroll_location_uri_filter


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.payroll_export_monthly_master_dag_id,
        description="Mammoet Payroll Export monthly Master",
        start_date=datetime(2023, 12, 1, tz=config.time_zone),
        schedule_interval=config.monthly_run_schedule_interval,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.master_max_active_run,
    ) as dag:

        rail.ViewDagRunScheduleOperator(
            task_id='view_dag_run_schedule'
        )

        get_payroll_location_uri = rail.RepliconServiceOperator(
            task_id="get_payroll_location_uri",
            endpoint="/services/LocationService1.svc/GetPageOfAvailableLocationsByTextSearch",
            data=lambda: get_payroll_location_uri_payload(config),
            data_handler=lambda response: get_payroll_location_uri_filter(
                response, config)
        )

        trigger_monthly_export = rail.TriggerDagRunOperator(
            task_id="trigger_monthly_export",
            trigger_dag_id=config.payroll_export_process_payroll,
            conf=lambda: {
                    "payroll_export_run_type": "monthly",
                    "todays_date": now(tz=config.time_zone).strftime(EXPORT_DATE_FORMAT),
                    "timezone": config.time_zone,
                    "process_start_time": now(tz=config.time_zone).strftime('%Y-%m-%dT%H:%M:%S'),
                    "payroll_location_name": config.PAYROLL_LOCATION_NAME,
                    "payroll_location_uri": rail.result('get_payroll_location_uri')['location']['uri']
            }
        )

        get_payroll_location_uri >> trigger_monthly_export

    return dag


rail.for_each_instance(create_main_dag)
