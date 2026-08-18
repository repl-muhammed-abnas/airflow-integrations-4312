
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
from pendulum import datetime as pendulum_datetime
from airflow.models import Variable
import rail
from dxctechnology.india_earned_leave_export.monthly_export.payroll_extract_mapper import payroll_extract_mapper

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_india_earned_leave_export_monthly_export_master_{config.instance}',
        description=f'dxctechnology_india_earned_leave_export_monthly_export_master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=pendulum_datetime(
            2022, 10, 10, tz=config.schedule_time_zone),
        max_active_runs=1,
        default_args={
        },
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_cut_off_date_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_cut_off_date_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        log_cut_off_date_3 = rail.PythonOperator(
            task_id='log_cut_off_date_3',
            python_callable=lambda: {"month": 1,
                                     "day": 4, "year": 2022}  # "01/04/2022"
        )

        get_all_scripts_5 = rail.RepliconServiceOperator(
            task_id='get_all_scripts_5',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        get_enabled_divisionscompanycodes_6 = rail.RepliconServiceOperator(
            task_id='get_enabled_divisionscompanycodes_6',
            endpoint="/services/DivisionService1.svc/GetEnabledDivisions",
        )

        get_mapper_entries_7 = rail.PythonOperator(
            task_id='get_mapper_entries_7',
            python_callable=lambda:  list(
                filter(lambda x: x['export'] == 'Yes', payroll_extract_mapper))
        )

        invoke_custom_ruby_code_8 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_8',
            python_callable=lambda: list(map(lambda x: {
                "name": x['companycode'],
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_enabled_divisionscompanycodes_6'), 'displayText', x['companycode'], 'uri'),
                "script_uri": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts_5'), 'displayText', x['fileformat name'], 'uri'),
                "type": x['type'],
            }, rail.result('get_mapper_entries_7'))),
        )

        invoke_custom_ruby_code_9 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_9',
            python_callable=lambda: {
                "companycodejson": list(set(map(lambda x: x['uri'], rail.result('invoke_custom_ruby_code_8')))),
                "startdate": rail.get_replicon_date(datetime.strptime(Variable.get(config.startdate_test_var_name, default_var=''), '%m/%d/%Y')) if Variable.get(config.startdate_test_var_name, default_var='') else rail.get_replicon_date(rail.result('log_cut_off_date_3') if (datetime.utcnow() - relativedelta(months=3)) - timedelta(days=(datetime.utcnow() - relativedelta(months=3)).weekday()) < datetime(**rail.result('log_cut_off_date_3')) else (datetime.utcnow() - relativedelta(months=3)) - timedelta(days=(datetime.utcnow() - relativedelta(months=3)).weekday())),
                "enddate": rail.get_replicon_date(datetime.strptime(Variable.get(config.enddate_test_var_name, default_var=''), '%m/%d/%Y')) if Variable.get(config.enddate_test_var_name, default_var='') else rail.get_replicon_date(datetime.utcnow().replace(day=1)-timedelta(days=1)),
                "companycode": list(set(map(lambda x: x['name'], rail.result('invoke_custom_ruby_code_8')))),
            }
        )

        if_pluckuri_smart_joinnil_present_10 = rail.IfOperator(
            task_id='if_pluckuri_smart_joinnil_present_10',
            test=lambda: bool(list(filter(
                lambda x:  x['script_uri'] and x['type'] == 'Compass', rail.result('invoke_custom_ruby_code_8')))),
            yes_task="trigger_dag_run_download_and_validateasync_11",
            no_task="stop_13",
        )

        trigger_dag_run_download_and_validateasync_11 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_download_and_validateasync_11',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_validate_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "fileformaturi": rail.result('invoke_custom_ruby_code_8')[0]['script_uri'],
                "startdate": rail.result('invoke_custom_ruby_code_9')['startdate'],
                "enddate": rail.result('invoke_custom_ruby_code_9')['enddate'],
                "division": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INES", 'name'),
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INES", 'uri'),
                "timenow": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "rundateinYYYYMMDDformat": datetime.utcnow().strftime("%Y%m%d"),
                "runtimeinHHMMSSformat": datetime.utcnow().strftime("%H%M%S"),
                "filename": f"PP3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL03_DUT8G2I" if str(config.company_key).lower() == 'dxctechnology' \
                    else f"PQ3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL03_DUT8G2I"
            }
        )

        wait_for_completion_trigger_dag_run_download_and_validateasync_11 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_download_and_validateasync_11',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_download_and_validateasync_11") }}'
        )

        stop_13 = rail.FailOperator(
            task_id='stop_13',
            message='''Required File format "{{ result('get_mapper_entries_7')[0]['fileformat name'] }}" is not available in Replicon'''
        )

        trigger_dag_run_earned_leave_export_async_14 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_earned_leave_export_async_14',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_earned_leave_export_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "division": list(set(map(lambda x: x['name'], rail.result('invoke_custom_ruby_code_8')))),
                "timenow": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "rundateinYYYYMMDDformat": datetime.utcnow().strftime("%Y%m%d"),
                "runtimeinHHMMSSformat": datetime.utcnow().strftime("%H%M%S"),
                "filename": f"PP3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL02_DUT8G2I" if str(config.company_key).lower() == 'dxctechnology' \
                    else f"PQ3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL02_DUT8G2I"
            }
        )

        wait_for_completion_trigger_dag_run_earned_leave_export_async_14 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_earned_leave_export_async_14',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_earned_leave_export_async_14") }}'
        )

        trigger_dag_run_download_and_validateasync_16 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_download_and_validateasync_16',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_validate_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda: {
                "fileformaturi": rail.result('invoke_custom_ruby_code_8')[0]['script_uri'],
                "startdate": rail.result('invoke_custom_ruby_code_9')['startdate'],
                "enddate": rail.result('invoke_custom_ruby_code_9')['enddate'],
                "division": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INET", 'name'),
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INET", 'uri'),
                "timenow": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "rundateinYYYYMMDDformat": datetime.utcnow().strftime("%Y%m%d"),
                "runtimeinHHMMSSformat": datetime.utcnow().strftime("%H%M%S"),
                "filename": f"PP3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL04_DUT8G2I" if str(config.company_key).lower() == 'dxctechnology' \
                    else f"PQ3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL04_DUT8G2I"
            }
        )

        wait_for_completion_trigger_dag_run_download_and_validateasync_16 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_download_and_validateasync_16',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_download_and_validateasync_16") }}'
        )

        trigger_dag_run_download_and_validateasync_18 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_download_and_validateasync_18',
            retries=0,
            items=[1],
            trigger_dag_id=f'dxctechnology_india_earned_leave_export_monthly_download_and_validate_child_{config.instance}',
            execution_timeout=timedelta(days=14),
            conf=lambda item: {
                "fileformaturi": rail.result('invoke_custom_ruby_code_8')[0]['script_uri'],
                "startdate": rail.result('invoke_custom_ruby_code_9')['startdate'],
                "enddate": rail.result('invoke_custom_ruby_code_9')['enddate'],
                "division": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INA7", 'name'),
                "divisionuri": rail.find_first_by_attr_and_get_attr(rail.result('invoke_custom_ruby_code_8'), 'name', "INA7", 'uri'),
                "timenow": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S"),
                "rundateinYYYYMMDDformat": datetime.utcnow().strftime("%Y%m%d"),
                "runtimeinHHMMSSformat": datetime.utcnow().strftime("%H%M%S"),
                "filename": f"PP3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL05_DUT8G2I" if str(config.company_key).lower() == 'dxctechnology' \
                    else f"PQ3220_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}_INREPL_REPL05_DUT8G2I"
            }
        )

        wait_for_completion_trigger_dag_run_download_and_validateasync_18 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_download_and_validateasync_18',
            execution_timeout=timedelta(days=14),
            dag_runs='{{ result("trigger_dag_run_download_and_validateasync_18") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> log_cut_off_date_3
        log_cut_off_date_3 >> get_all_scripts_5 >> get_enabled_divisionscompanycodes_6 >> get_mapper_entries_7 >> invoke_custom_ruby_code_8 >> invoke_custom_ruby_code_9 >> if_pluckuri_smart_joinnil_present_10
        if_pluckuri_smart_joinnil_present_10 >> rail.Label(
            'Yes') >> trigger_dag_run_download_and_validateasync_11 >> wait_for_completion_trigger_dag_run_download_and_validateasync_11 >> trigger_dag_run_earned_leave_export_async_14
        if_pluckuri_smart_joinnil_present_10 >> rail.Label(
            'No') >> stop_13 >> trigger_dag_run_earned_leave_export_async_14 >> wait_for_completion_trigger_dag_run_earned_leave_export_async_14 >> trigger_dag_run_download_and_validateasync_16 >> wait_for_completion_trigger_dag_run_download_and_validateasync_16 >> trigger_dag_run_download_and_validateasync_18 >> wait_for_completion_trigger_dag_run_download_and_validateasync_18 >> finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
