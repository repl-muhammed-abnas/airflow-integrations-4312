from datetime import timedelta
from airflow.models import Variable
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timesheet_per_company_dag_id,
        description=f'{config.company_key} Syncs the time data for Employees belonging to a Company to Vantagepoint as timesheets',
        company_key=config.company_key,
        max_active_runs=10,
        multi_tenant=True
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
            request_method='PUT',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        get_active_periods = rail.VantagepointAPIOperator(
            task_id='get_active_periods',
            endpoint='/Settings/Period',
            request_method='GET',
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
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
            trigger_dag_id=config.timesheet_for_employee_dag_id,
            conf=lambda dag_run, item: {
                'loginname': item['Login_Name'],
                'timecategories': rail.find_first_by_attr_and_get_attr(rail.result(
                    'load_required_timecategories'), 'loginname', item['Login_Name'], 'timecategory','[]'),
                'activeperiods': list(filter(lambda period: period['ActiveCompanyClosed'] == 'N', rail.result('get_active_periods'))),
                'export_time': dag_run.conf['export_time'],
                'company_key': dag_run.conf['company_key'],
                'vantagepoint_conn_id': dag_run.conf['vantagepoint_conn_id'],
                'replicon_conn_id': dag_run.conf['replicon_conn_id'],
                'laborcodelevels': dag_run.conf.get('laborcodelevels', [])
            }
        )

        wait_foreach_employee_timedata_to_process = rail.WaitForDagRunsSensor(
            task_id='wait_foreach_employee_timedata_to_process',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_employee_timedata") }}'
        )

        gather_employee_errors = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_employee_errors',
            dag_runs="{{ result('process_each_employee_timedata') }}",
            dagrun_task_id='catch_error',
            flatten=True
        )

        is_employee_error = rail.IfOperator(
            task_id='is_employee_error',
            test="{{ get_task_state('gather_employee_errors') == 'success' and result('gather_employee_errors') | length > 0 }}",
            yes_task='fail_employee_error',
            no_task='catch_error'
        )

        fail_employee_error = rail.FailOperator(
            task_id='fail_employee_error',
            message="{{ result('gather_employee_errors') | map_to_attr('error') | join('\n') }}"
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in timesheet sync per company - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error

        can_run_batch_task >> rail.Label(
            'No') >> query_distinct_employees >> query_timecategories_for_required_employees >> load_required_timecategories >> set_company_as_active
        set_company_as_active >> get_active_periods >> process_each_employee_timedata
        process_each_employee_timedata >> wait_foreach_employee_timedata_to_process >> gather_employee_errors >> is_employee_error
        is_employee_error >> rail.Label('Yes') >> fail_employee_error >> catch_error
        is_employee_error >> rail.Label('No') >> catch_error

        return dag


rail.for_each_instance(create_dag)
