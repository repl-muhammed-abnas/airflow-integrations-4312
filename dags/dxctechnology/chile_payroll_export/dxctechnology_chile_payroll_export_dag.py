from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import rail
from dxctechnology.chile_payroll_export.dxctechnology_chile_payroll_export_child import get_task_group, findItemByDisplayText


def create_main_airflow_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_chile_payroll_export_master_{config.instance}',
        description='DXC_chile_Payroll_Export_Master - V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        # runs at 7PM on every day-of-week from Monday through Friday
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        chile_name = config.chile_config['name']
        chile_payroll_format = config.chile_config['payroll_format']
        division_name = config.chile_config['division_name']

        end_dateObj = datetime.now().replace(day=1) - timedelta(days=1)
        start_dateObj = datetime.now().replace(day=1) - relativedelta(months=2)
        currentDate = {"year": datetime.now().year,
                       "month": datetime.now().month,
                       "day": datetime.now().day}
        startDate = {"year": start_dateObj.year,
                     "month": start_dateObj.month,
                     "day": start_dateObj.day}

        endDate = {"year": end_dateObj.year,
                   "month": end_dateObj.month,
                   "day": end_dateObj.day}

        start = rail.EmptyOperator(
            task_id="start"
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        authenticate_replicon = rail.RepliconServiceOperator(
            task_id="authenticate_replicon",
            endpoint="/services/UserAccessControlService1.svc/GetMyActualUserIdentity"
        )

        get_chile_calendarUri = rail.RepliconServiceOperator(
            task_id="get_chile_calendarUri",
            endpoint="/services/HolidayCalendarService1.svc/GetAllHolidayCalendars",
            response_filter=lambda response: findItemByDisplayText(
                response, chile_name)
        )

        payload = {
            "holidayCalendarUri": "{{ ti.xcom_pull(task_ids='get_chile_calendarUri')}}",
            "dateRange": {
                "startDate": currentDate,
                "endDate": currentDate,
                "relativeDateRangeUri": None,
                "relativeDateRangeAsOfDate": None
            }
        }
        get_holidays_indaterange = rail.RepliconServiceOperator(
            task_id="get_holidays_indaterange",
            data=payload,
            endpoint="/services/HolidayCalendarService2.svc/GetHolidaysInDateRange",
            response_filter=lambda response: response.json()['d']
        )

        has_holiday_data = rail.IfOperator(
            task_id='has_holiday_data',
            test="{{ result('get_holidays_indaterange') | length > 0  }}",
            yes_task='finish',
            no_task='get_chile_payroll_script',
        )

        get_chile_payroll_script = rail.RepliconServiceOperator(
            task_id="get_chile_payroll_script",
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
            response_filter=lambda response: findItemByDisplayText(
                response, chile_payroll_format)
        )

        get_chile_division_uri = rail.RepliconServiceOperator(
            task_id="get_chile_division_uri",
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
            response_filter=lambda response: findItemByDisplayText(
                response, division_name)
        )

        divisionUris = ["{{ result('get_chile_division_uri')}}"]

        child_recipe_task_group = get_task_group(
            startDate,
            endDate,
            divisionUris,
            division_name,
            "{{ result('get_chile_payroll_script')}}",
            config)

        start >> authenticate_replicon >> get_chile_calendarUri >> get_holidays_indaterange >> has_holiday_data
        has_holiday_data >> rail.Label('Yes') >> finish
        has_holiday_data >> rail.Label('No') >> get_chile_payroll_script
        get_chile_payroll_script >> get_chile_division_uri >> child_recipe_task_group >> finish

    return dag


rail.for_each_instance(create_main_airflow_dag)
