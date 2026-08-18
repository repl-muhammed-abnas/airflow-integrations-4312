from datetime import timedelta
from functools import lru_cache
from pendulum import datetime
import rail

from dxctechnology.timesheet_autosubmission_loa.utils.request_payload import get_enabled_divisions_company_codes_payload
from dxctechnology.timesheet_autosubmission_loa.utils.response_filter import map_list_data_to_companycode_list
from dxctechnology.timesheet_autosubmission_loa.utils.custom_methods import get_report_dates_per_erp_each_month

null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'DXCTechnology TimeSheet Auto Submission LOA Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 4, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        get_enabled_divisions_company_codes = rail.RepliconServiceOperator(
            task_id="get_enabled_divisions_company_codes",
            endpoint="/services/DivisionListService1.svc/GetData",
            data=get_enabled_divisions_company_codes_payload,
            data_handler=map_list_data_to_companycode_list,
            target='artifact'
        )

        @lru_cache(maxsize=8)
        def get_required_details(erp):
            return {
                "report_uri": rail.result('get_report_details')['uri'],
                "timesheet_period_filter_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', 'TimesheetPeriodFilter', 'uri', null),
                "current_division_filter_uri": rail.find_first_by_attr_and_get_attr(
                    rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                    'displayText', 'CurrentDivisionFilter', 'uri', null),
                f'{erp}_company_code_uri_values': rail.result('get_enabled_divisions_company_codes', key=f'{erp}_company_code_values')
            }

        trigger_dagrun_for_each_month_c1 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dagrun_for_each_month_c1',
            items=get_report_dates_per_erp_each_month,
            trigger_dag_id=config.c1_chid_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "erp": "C1",
                "report_start_date": item['report_start_date'],
                "report_end_date":item['report_end_date'],
                **get_required_details("c1")
            }
        )

        trigger_dagrun_for_each_month_compass = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dagrun_for_each_month_compass',
            items=get_report_dates_per_erp_each_month,
            trigger_dag_id=config.compass_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "erp": "COMPASS",
                "report_start_date": item['report_start_date'],
                "report_end_date":item['report_end_date'],
                **get_required_details("compass")
            }
        )
       
        trigger_dagrun_for_each_month_gsap = rail.TriggerDagRunForEachItemOperator( 
            task_id='trigger_dagrun_for_each_month_gsap',
            items=get_report_dates_per_erp_each_month,
            trigger_dag_id=config.gsap_child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "erp": "GSAP",
                "report_start_date": item['report_start_date'],
                "report_end_date":item['report_end_date'],
                **get_required_details("gsap")
            }
        )

        get_report_details  >> get_enabled_divisions_company_codes >> [trigger_dagrun_for_each_month_c1, trigger_dagrun_for_each_month_compass, \
            trigger_dagrun_for_each_month_gsap]

    return dag


rail.for_each_instance(create_dag)
