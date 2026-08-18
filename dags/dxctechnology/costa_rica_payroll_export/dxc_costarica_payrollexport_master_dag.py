
from datetime import timedelta, datetime
from pendulum import datetime as dt
from dateutil.relativedelta import relativedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_costa_rica_payroll_export_master_{config.instance}',
        description=f'DXC_CostaRica_PayrollExport_Master - {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=dt(2023, 1, 1, tz=config.timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_scripts'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_scripts',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_scripts=rail.RepliconServiceOperator(
            task_id='get_all_scripts',
            endpoint="/services/PayrollDownloadScriptAdministrationService1.svc/GetAllScripts",
        )

        def get_dates():
            startdate = datetime.now().replace(day=1)-relativedelta(months=3)
            enddate =  datetime.now().replace(day=1)-timedelta(days=1)
            return {
                'startdate': startdate.strftime("%Y-%m-%d"),
                'enddate': enddate.strftime("%Y-%m-%d"),
                'startdateday': startdate.day,
                'startdatemonth': startdate.month,
                'startdateyear': startdate.year,
                'enddateday': enddate.day,
                'enddatemonth': enddate.month,
                'enddateyear': enddate.year
            }

        get_startdate_enddate = rail.PythonOperator(
            task_id = 'get_startdate_enddate',
            python_callable=get_dates
        )

        trigger_child_dag_costa_rica_payroll_export=rail.TriggerDagRunOperator(
            task_id='trigger_child_dag_costa_rica_payroll_export',
            retries=0,
            trigger_dag_id=f'dxctechnology_costa_rica_payroll_export_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda:{
                "fileformat": "Costa Rica payroll export",
                "fileformaturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_scripts'),
                                    'displayText','Costa Rica payroll export','uri',null) if rail.result('get_all_scripts')[0]['displayText'] else null,
                "startdate": rail.result('get_startdate_enddate')['startdate'],
                "startdateday": rail.result('get_startdate_enddate')['startdateday'],
                "startdatemonth": rail.result('get_startdate_enddate')['startdatemonth'],
                "startdateyear": rail.result('get_startdate_enddate')['startdateyear'],
                "enddate": rail.result('get_startdate_enddate')['enddate'],
                "enddateday": rail.result('get_startdate_enddate')['enddateday'],
                "enddatemonth": rail.result('get_startdate_enddate')['enddatemonth'],
                "enddateyear": rail.result('get_startdate_enddate')['enddateyear'],
            }
        )

        wait_for_child_dag_costa_rica_payroll_export = rail.WaitForDagRunsSensor(
            task_id='wait_for_child_dag_costa_rica_payroll_export',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_child_dag_costa_rica_payroll_export") }}'
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_all_scripts
        get_all_scripts >> get_startdate_enddate >> trigger_child_dag_costa_rica_payroll_export
        trigger_child_dag_costa_rica_payroll_export >> wait_for_child_dag_costa_rica_payroll_export >> finish

    return dag

rail.for_each_instance(create_dag)
