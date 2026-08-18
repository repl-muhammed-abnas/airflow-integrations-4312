from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v2.utils import request_payload,response_filter
from dxctechnology.australia_payroll_extract_v2.utils.python_callable_method import check_2010_infotype,check_0015_infotype,\
    check_0007_infotype,check_2006_infotype
from dxctechnology.australia_payroll_extract_v2.tasks import master_dag_task_group
from dxctechnology.australia_payroll_extract_v2.mapper.company_code_mapper_usles_uscsc import COMPANY_CODE_MAP_ES

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"dxctechnology_australia_payrollexport_es_2010_2001_0015_0007_2006_master_dag_v2_{config.instance}",
        description=f"DXC_AUS_PayrollExport_ES_Master V2 {config.instance}",
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
            yes_task= 'check_holiday_calander_has_2010',
            no_task= 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.EmptyOperator(
            task_id= 'delete_this_dagrun'
        )

        check_holiday_calander_has_2010 = rail.IfOperator(
            task_id = 'check_holiday_calander_has_2010',
            test=lambda: check_2010_infotype()['condition'],
            yes_task= 'stat_processsing_2010',
            no_task= 'check_holiday_calander_has_0015'
        )

        stat_processsing_2010 = rail.EmptyOperator(
            task_id= 'stat_processsing_2010'
        )

        master_dag_task_group_entry, master_dag_task_group_exit =master_dag_task_group.get_master_dag_task_group(
            config.es_region, config.export, config.company_key, COMPANY_CODE_MAP_ES)

        process_payrolldata_export = rail.TriggerDagRunForEachItemOperator(
            task_id='process_payrolldata_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_es_child_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_payrolldata_export_es_conf
        )

        check_holiday_calander_has_0015 = rail.IfOperator(
            task_id = 'check_holiday_calander_has_0015',
            test=check_0015_infotype,
            yes_task= 'process_cashout_annual_payroll_export',
            no_task= 'check_holiday_calander_has_0007'
        )

        # pylint: disable=unnecessary-lambda
        process_cashout_annual_payroll_export= rail.TriggerDagRunForEachItemOperator(
            task_id='process_cashout_annual_payroll_export',
            retries=0,
            items=lambda: rail.result('search_entries_companycode_mapper'),
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_sellback_child_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.process_cashout_es_user_conf
        )

        check_holiday_calander_has_0007 = rail.IfOperator(
            task_id = 'check_holiday_calander_has_0007',
            test=check_0007_infotype,
            yes_task= 'process_user_schedule_payrolldata_export',
            no_task= 'check_holiday_calander_has_2006'
        )

        process_user_schedule_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_user_schedule_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_user_schedule_child_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_es_user_schedule_conf(config)
        )

        check_holiday_calander_has_2006 = rail.IfOperator(
            task_id = 'check_holiday_calander_has_2006',
            test=check_2006_infotype,
            yes_task= 'process_es_active_user_payrolldata_export',
            no_task= 'finish'
        )

        process_es_active_user_payrolldata_export = rail.TriggerDagRunOperator(
            task_id='process_es_active_user_payrolldata_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_australia_payrolldata_export_active_user_child_v2_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:  request_payload.process_active_es_user_conf(config)
        )

        finish = rail.EmptyOperator(
            task_id= 'finish'
        )

        get_all_holiday_calanders >> get_es_holiday_calander_for_daterange >> is_holoday_calander_has_data

        is_holoday_calander_has_data >> rail.Label(
            "Yes") >> check_holiday_calander_has_2010

        is_holoday_calander_has_data >> rail.Label(
            "No") >> delete_this_dagrun

        check_holiday_calander_has_2010 >> rail.Label(
            "Yes") >> stat_processsing_2010 >> master_dag_task_group_entry >> master_dag_task_group_exit >>\
                 process_payrolldata_export >> check_holiday_calander_has_0015

        check_holiday_calander_has_2010 >> rail.Label(
            "No") >> check_holiday_calander_has_0015

        check_holiday_calander_has_0015 >> rail.Label(
            "Yes") >> process_cashout_annual_payroll_export >> check_holiday_calander_has_0007

        check_holiday_calander_has_0015 >> rail.Label(
            "No") >> check_holiday_calander_has_0007

        check_holiday_calander_has_0007 >> rail.Label(
            "Yes") >> process_user_schedule_payrolldata_export >> check_holiday_calander_has_2006

        check_holiday_calander_has_0007 >> rail.Label(
            "No") >> check_holiday_calander_has_2006

        check_holiday_calander_has_2006 >> rail.Label(
            "Yes") >> process_es_active_user_payrolldata_export >> finish

        check_holiday_calander_has_2006 >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
