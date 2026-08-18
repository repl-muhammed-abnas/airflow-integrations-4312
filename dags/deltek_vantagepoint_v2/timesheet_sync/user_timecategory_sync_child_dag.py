from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.timecategory_sync_child_dag_id,
        description=f"{config.company_key} Retrieves Time Category for a user from Deltek Vantagepoint API",
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_time_category_sync_for_user,
        multi_tenant=True
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_time_category_by_tkgroup',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        fetch_time_category_by_tkgroup = rail.VantagepointAPIOperator(
            task_id = 'fetch_time_category_by_tkgroup',
            request_method='GET',
            pagination=False,
            endpoint="/settings/time/categories/{{dag_run.conf.homecompany}}/{{dag_run.conf.tkgroup}}?employee={{dag_run.conf.loginname}}",
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        def check_for_failure():
            error = rail.render_template("{{get_error_message()}}")
            return error and ('The requested records could not be found.' in error)

        if_records_not_found_by_tkgroup = rail.IfOperator(
            task_id = 'if_records_not_found_by_tkgroup',
            trigger_rule = 'all_done',
            test=check_for_failure,
            yes_task='fetch_time_category_by_allgroup',
            no_task='add_time_category_by_employee'
        )

        fetch_time_category_by_allgroup = rail.VantagepointAPIOperator(
            task_id = 'fetch_time_category_by_allgroup',
            request_method='GET',
            pagination=False,
            endpoint="/settings/time/categories/{{dag_run.conf.homecompany}}/<allgroup>?employee={{dag_run.conf.loginname}}",
            vp_conn_id='{{ dag_run.conf.vantagepoint_conn_id }}'
        )

        def time_category_by_employee():
            return rail.result('fetch_time_category_by_tkgroup') or rail.result('fetch_time_category_by_allgroup') or []

        add_time_category_by_employee = rail.PythonOperator(
            task_id = 'add_time_category_by_employee',
            trigger_rule = 'all_done',
            python_callable = lambda dag_run: {
                'loginname': dag_run.conf['loginname'],
                'timecategory': time_category_by_employee()
            }
        )

        def get_downstreamtasks_error(error_message):
            return {
                'error': f'Error in user timecategory sync child - {error_message}'
            }

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=get_downstreamtasks_error,
            op_args=['{{ get_error_message() }}']
        )

        batch_task >> fetch_time_category_by_tkgroup
        batch_task >> catch_error
        fetch_time_category_by_tkgroup >> if_records_not_found_by_tkgroup
        if_records_not_found_by_tkgroup >> rail.Label('Yes') >> fetch_time_category_by_allgroup >> add_time_category_by_employee
        if_records_not_found_by_tkgroup >> rail.Label('No') >> add_time_category_by_employee >> catch_error

        return dag


rail.for_each_instance(create_dag)
