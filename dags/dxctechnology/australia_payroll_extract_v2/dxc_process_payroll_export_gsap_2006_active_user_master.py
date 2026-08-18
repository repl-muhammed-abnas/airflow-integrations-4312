from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v2.utils import request_payload,response_filter

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_gsap_2006_active_user_master_dag_v2_{config.instance}",
        description=f"DXC_AUS_PayrollExport_Active_User_GSAP_Master V2 {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval_active_users,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_all_holiday_calanders = rail.RepliconServiceOperator(
            task_id= 'get_all_holiday_calanders',
            endpoint= '/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            response_filter= lambda response: response_filter.get_all_holiday_calanders(response,config)
        )

        get_gsap_holiday_calander_for_daterange = rail.RepliconServiceOperator(
            task_id= 'get_gsap_holiday_calander_for_daterange',
            endpoint= '/services/HolidayCalendarService2.svc/GetHolidaysInDateRange',
            data=lambda: request_payload.get_holiday_calander_data('gsap_calander_uri')
        )

        is_holoday_calander_has_data = rail.IfOperator(
            task_id= 'is_holoday_calander_has_data',
            test= lambda: bool(rail.result("get_gsap_holiday_calander_for_daterange")),
            yes_task= 'process_gsap_active_user_payrolldata_export',
            no_task= 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.EmptyOperator(
            task_id= 'delete_this_dagrun'
        )

        process_gsap_active_user_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_gsap_active_user_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_active_gsap_user_conf(config)
        )

        finish_export = rail.EmptyOperator(
            task_id= 'finish_export'
        )

        get_all_holiday_calanders >> get_gsap_holiday_calander_for_daterange >> is_holoday_calander_has_data

        is_holoday_calander_has_data >> rail.Label(
            "Yes") >> process_gsap_active_user_payrolldata_export >> finish_export

        is_holoday_calander_has_data >> rail.Label(
            "No") >> delete_this_dagrun


    return dag


rail.for_each_instance(create_main_dag)
