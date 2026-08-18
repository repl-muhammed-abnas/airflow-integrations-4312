from datetime import timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_timesheet_sync_foreach_company_child_{config.instance}',
        description='Syncs the time data for a Employees belonging to a Company to Vantagepoint as timesheets',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,# Can't be increased, since we can only process timesheets for one company at a time
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_distinct_employees'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_distinct_employees',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        set_company_as_active = rail.VantagepointAPIOperator(
            task_id='set_company_as_active',
            endpoint='/Settings/ActiveCompany/{{dag_run.conf.company}}',
            request_method='PUT'
        )

        get_active_periods = rail.VantagepointAPIOperator(
            task_id='get_active_periods',
            endpoint='/Settings/Period',
            request_method='GET'
        )

        query_distinct_employees = rail.QueryCollectionOperator(
            task_id='query_distinct_employees',
            query='SELECT DISTINCT Login_Name FROM all_data WHERE ' + config.department_name + ' =:company',
            name = 'employees',
            query_params={
                'company': '{{ dag_run.conf.company }}'
            }
        )

        query_timecategories_for_required_employees = rail.QueryCollectionOperator(
            task_id = 'query_timecategories_for_required_employees',
            name = 'required_timecategories',
            query='SELECT * from users_timecategory_values WHERE loginname IN (SELECT Login_Name FROM employees)'
        )

        load_required_timecategories = rail.PythonOperator(
            task_id = 'load_required_timecategories',
            python_callable=lambda : rail.load_all_records(rail.result('query_timecategories_for_required_employees'))
        )

        process_each_employee_timedata = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_employee_timedata',
            items="{{result('query_distinct_employees')}}",
            retries=0,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'deltek_vantagepoint_timesheet_sync_for_employee_child_{config.instance}',
            conf=lambda dag_run, item: {
                'loginname': item['Login_Name'],
                'timecategories': rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_required_timecategories'), 'loginname', item['Login_Name'], 'timecategory','[]'),
                'activeperiods': list(filter(lambda period: period['ActiveCompanyClosed'] == 'N', rail.result('get_active_periods'))),
                'export_time': dag_run.conf['export_time']
            }
        )

        wait_foreach_employee_timedata_to_process = rail.WaitForDagRunsSensor(
            task_id='wait_foreach_employee_timedata_to_process',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_employee_timedata") }}'
        )


        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error/Exception",
            properties={
                "loginname": "None",
                "status": "Error",
                "details": "Failed to sync any timesheet data for employees belonging to Company: {{ dag_run.conf.company }}. {{ get_error_message() }}",
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
            'No') >> query_distinct_employees >> query_timecategories_for_required_employees >> load_required_timecategories >> set_company_as_active
        set_company_as_active >> get_active_periods >> process_each_employee_timedata
        process_each_employee_timedata >> wait_foreach_employee_timedata_to_process >> catch_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
