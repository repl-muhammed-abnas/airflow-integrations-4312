
from datetime import timedelta, datetime
from pendulum import datetime as pendulum_datetime
from airflow.models import Variable
from sigroup.payroll_export_belgium.mappers import sigroup_belgium_calendar_mapper
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'sigroup_payroll_export_belgium_master_{config.instance}',
        description=f'SiGroup_Payroll_Export_Belgium_Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=pendulum_datetime(2025, 9, 1, tz=config.time_zone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_valid_run'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_valid_run',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def check_schedule_present():
            """
            Production-ready schedule validation for Belgium.
            Checks today's date against Belgium calendar mapper entries.
            """
            today = datetime.now().strftime("%d-%m-%Y")
            
            return list(filter(
                lambda schedule: (
                    schedule['repliconextractdate'] == today and
                    schedule['fileformat'] == "Belgium" and
                    schedule['export'] == "Yes"
                ),
                sigroup_belgium_calendar_mapper.sigroup_belgium_calendar
            ))
        is_valid_run = rail.IfOperator(
            task_id = 'is_valid_run',
            test = check_schedule_present,
            yes_task = 'get_all_scripts',
            no_task = 'delete_this_dagrun'
        )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        get_all_scripts=rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        get_enabled_divisions=rail.RepliconServiceOperator(
            task_id='get_enabled_divisions',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_schedule_from_calendar=rail.PythonOperator(
            task_id='get_schedule_from_calendar',
            python_callable= check_schedule_present
        )

        filter_valid_entries_from_schedule = rail.PythonOperator(
            task_id = 'filter_valid_entries_from_schedule',
            python_callable = lambda: list(filter(lambda entry: ( rail.result('get_all_scripts') and
                                        rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'),'displayText',entry['fileformat'],'uri','') ) and
                                        ( rail.result('get_enabled_divisions') and rail.find_first_by_attr_and_get_attr(
                                        rail.result('get_enabled_divisions'),'displayText',entry['businessunit'],'uri','')) and
                                        ( entry['fileformat'] == 'Belgium' ),rail.result('get_schedule_from_calendar')))
        )

        trigger_belgium_business_unit_payroll_data_export_child=rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_belgium_business_unit_payroll_data_export_child',
            retries=0,
            items=lambda: rail.result('filter_valid_entries_from_schedule'),
            trigger_dag_id=f'sigroup_payroll_export_belgium_business_unit_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item:{
                "businessunit": item['businessunit'],
                "businessunituri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisions'),'displayText',item['businessunit'],'uri','') if
                    rail.result('get_enabled_divisions') and rail.result('get_enabled_divisions')[0]['displayText'] else null,
                "fileformat": item['fileformat'],
                "fileformaturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'),'displayText',item['fileformat'],'uri','') if
                    rail.result('get_all_scripts') and rail.result('get_all_scripts')[0]['displayText'] else null,
                "startdate": item['periodstartdate'],
                "enddate": item['periodenddate'],
                "startdateday": datetime.strptime(item['periodstartdate'],'%d-%m-%Y').day,
                "startdatemonth": datetime.strptime(item['periodstartdate'],'%d-%m-%Y').month,
                "startdateyear": datetime.strptime(item['periodstartdate'],'%d-%m-%Y').year,
                "enddateday": datetime.strptime(item['periodenddate'],'%d-%m-%Y').day,
                "enddatemonth": datetime.strptime(item['periodenddate'],'%d-%m-%Y').month,
                "enddateyear": datetime.strptime(item['periodenddate'],'%d-%m-%Y').year,
                "filenamecounter": item['filenamecounter']
            }
        )

        wait_for_trigger_belgium_business_unit_payroll_data_export_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_trigger_belgium_business_unit_payroll_data_export_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_belgium_business_unit_payroll_data_export_child") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> is_valid_run >> rail.Label('Yes') >> get_all_scripts
        is_valid_run >> rail.Label('No') >> delete_this_dagrun >> finish
        get_all_scripts >> get_enabled_divisions >> get_schedule_from_calendar >> filter_valid_entries_from_schedule
        filter_valid_entries_from_schedule >> trigger_belgium_business_unit_payroll_data_export_child
        trigger_belgium_business_unit_payroll_data_export_child >> wait_for_trigger_belgium_business_unit_payroll_data_export_child
        wait_for_trigger_belgium_business_unit_payroll_data_export_child >> finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
