import ast
from collections import defaultdict
from datetime import datetime, timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_timesheet_sync_for_employee_child_{config.instance}',
        description='Syncs the time data for an Employee to Vantagepoint as timesheets',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_per_employee,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_employee_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_employee_data',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_employee_data = rail.QueryCollectionOperator(
            task_id='get_employee_data',
            query='SELECT * FROM all_data WHERE Login_Name =:loginname',
            name='employee_data',
            query_params={
                'loginname': '{{ dag_run.conf.loginname }}'
            }
        )

        def is_budget_labor_code_enabled():
            raw = getattr(config, 'enable_budget_labor_codes_level', False)
            if isinstance(raw, str):
                raw = raw.strip().lower() == 'true'
            return bool(raw) and getattr(config, 'budget_labor_codes_level', '') in ('Task', 'TimesheetFields')

        def find_closest_active_period(active_periods, timesheet_enddate):
            closest_active_period = None
            closest_end_date = None
            for period in active_periods:
                start_date = datetime.fromisoformat(period['AccountPdStart'])
                end_date = datetime.fromisoformat(period['AccountPdEnd'])
                # If timesheet_enddate falls in range of an active period
                if start_date <= timesheet_enddate <= end_date:
                    return period['Period']
                # If not, check if it is the closest period to it
                if closest_end_date is None or abs((end_date - timesheet_enddate).days) < abs((closest_end_date - timesheet_enddate).days):
                    closest_end_date = end_date
                    closest_active_period = period['Period']
            return closest_active_period

        def segregate_by_period(dag_run):
            data = rail.load_all_records(rail.result('get_employee_data'))
            categorized_data = defaultdict(list)
            for item in data:
                period = item['Timesheet_Period']
                categorized_data[period].append(item)
            segregated_data = []
            for period, entries in categorized_data.items():
                home_company = entries[0][config.department_name]
                start_date, end_date = map(lambda date: datetime.strptime(
                    date, config.replicon_date_format), period.split(' - '))
                active_period = find_closest_active_period(
                    dag_run.conf.get('activeperiods',[]), end_date)
                segregated_data.append({
                    'entries': entries,
                    'loginname': dag_run.conf['loginname'],
                    'start_date': start_date.isoformat(timespec='milliseconds'),
                    'end_date': end_date.isoformat(timespec='milliseconds'),
                    'active_period': active_period,
                    'timesheetperiod': period,
                    'home_company': home_company,
                    'timecategories': ast.literal_eval(dag_run.conf['timecategories']) if dag_run.conf['timecategories'] else [],
                    'allow_lc_update': False if is_budget_labor_code_enabled() else (
                        (not(entries[0].get(config.allow_lc_update_caption)) or (
                            entries[0].get(config.allow_lc_update_caption) == 'Y')) if config.allow_lc_update_caption else True),
                })
            return segregated_data
        segregate_data_by_period = rail.PythonOperator(
            task_id='segregate_data_by_period',
            python_callable=segregate_by_period
        )

        process_timesheet_data_foreach_period = rail.TriggerDagRunForEachItemOperator(
            task_id='process_timesheet_data_foreach_period',
            items=lambda: rail.result('segregate_data_by_period'),
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_timesheet_sync_for_employee_per_period_child_{config.instance}',
            conf=lambda dag_run, item: {
                **item,
                'export_time': dag_run.conf['export_time'],
            }
        )

        wait_for_processing_each_timesheet_for_user = rail.WaitForDagRunsSensor(
            task_id='wait_for_processing_each_timesheet_for_user',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_timesheet_data_foreach_period") }}'
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error/Exception",
            properties={
                "loginname": "{{ dag_run.conf.loginname }}",
                "status": "Error",
                "details": "Failed to sync any timesheet data for user with Employee Number {{ dag_run.conf.loginname }}. {{ get_error_message() }}",
                "batch": ""
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error

        can_run_batch_task >> rail.Label(
            'No') >> get_employee_data >> segregate_data_by_period >> process_timesheet_data_foreach_period
        process_timesheet_data_foreach_period >> wait_for_processing_each_timesheet_for_user >> catch_error >> log_to_sumo
        return dag


rail.for_each_instance(create_dag)
