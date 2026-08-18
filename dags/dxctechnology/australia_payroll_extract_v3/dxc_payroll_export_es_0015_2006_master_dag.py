from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v3.utils import request_payload, response_filter
from dxctechnology.australia_payroll_extract_v3.utils.python_callable_method import check_0015_infotype, \
    check_2006_infotype, check_last_day_of_month
from dxctechnology.australia_payroll_extract_v3.tasks import master_dag_task_group
from dxctechnology.australia_payroll_extract_v3.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_ES

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_es_0015_2006_master_v3_{config.instance}",
        description=f"DXC_AUS_PayrollExport_ES_0015_2006_Master V3 {config.instance}",
        company_key=config.company_key,
        schedule_interval=config.schedule_interval_es,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs
    ) as dag:

        get_all_holiday_calanders = rail.RepliconServiceOperator(
            task_id= 'get_all_holiday_calanders',
            endpoint= '/services/HolidayCalendarService1.svc/GetAllHolidayCalendars',
            response_filter= lambda response: response_filter.get_all_holiday_calanders(response,config)
        )

        get_es_holiday_calander_for_daterange = rail.RepliconServiceOperator(
            task_id= 'get_es_holiday_calander_for_daterange',
            endpoint= '/services/HolidayCalendarService2.svc/GetHolidaysInDateRange',
            data=lambda: request_payload.get_holiday_calander_data('es_calander_uri')
        )

        is_holoday_calander_has_data = rail.IfOperator(
            task_id= 'is_holoday_calander_has_data',
            test= lambda: bool(rail.result("get_es_holiday_calander_for_daterange")),
            yes_task= 'check_holiday_calander_has_0015',
            no_task= 'is_last_day_of_month_no_holiday'
        )

        # When no holiday data, check if last day of month to trigger 2006 alone
        is_last_day_of_month_no_holiday = rail.IfOperator(
            task_id='is_last_day_of_month_no_holiday',
            test=check_last_day_of_month,
            yes_task='process_es_active_user_payrolldata_export',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun = rail.EmptyOperator(
            task_id= 'delete_this_dagrun'
        )

        check_holiday_calander_has_0015 = rail.IfOperator(
            task_id = 'check_holiday_calander_has_0015',
            test=check_0015_infotype,
            yes_task= 'stat_processing_0015',
            no_task= 'should_run_2006'
        )

        stat_processing_0015 = rail.EmptyOperator(
            task_id= 'stat_processing_0015'
        )

        master_dag_task_group_entry, master_dag_task_group_exit = master_dag_task_group.get_master_dag_task_group(
            config.es_region, config.export, config.company_key, COMPANY_CODE_MAP_ES)

        # pylint: disable=unnecessary-lambda
        process_cashout_annual_payroll_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_cashout_annual_payroll_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_sellback_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_cashout_es_user_conf
        )

        # Run 2006 if holiday calendar has 2006 OR if last day of month
        should_run_2006 = rail.IfOperator(
            task_id = 'should_run_2006',
            test=lambda: check_2006_infotype() or check_last_day_of_month(),
            yes_task= 'process_es_active_user_payrolldata_export',
            no_task= 'finish'
        )

        process_es_active_user_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_es_active_user_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v3_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda: request_payload.process_active_es_user_conf(config)
        )

        finish = rail.EmptyOperator(
            task_id= 'finish'
        )

        get_all_holiday_calanders >> get_es_holiday_calander_for_daterange >> is_holoday_calander_has_data

        # No holiday data path: only run 2006 if last day of month
        is_holoday_calander_has_data >> rail.Label(
            "No") >> is_last_day_of_month_no_holiday

        is_last_day_of_month_no_holiday >> rail.Label(
            "Yes") >> process_es_active_user_payrolldata_export >> finish

        is_last_day_of_month_no_holiday >> rail.Label(
            "No") >> delete_this_dagrun

        # Holiday data exists path: check 0015 first, then 2006
        is_holoday_calander_has_data >> rail.Label(
            "Yes") >> check_holiday_calander_has_0015

        check_holiday_calander_has_0015 >> rail.Label(
            "Yes") >> stat_processing_0015 >> master_dag_task_group_entry >> master_dag_task_group_exit >> \
                 process_cashout_annual_payroll_export >> should_run_2006

        check_holiday_calander_has_0015 >> rail.Label(
            "No") >> should_run_2006

        should_run_2006 >> rail.Label(
            "Yes") >> process_es_active_user_payrolldata_export >> finish

        should_run_2006 >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
