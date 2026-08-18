from datetime import timedelta
import rail
null = None


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_vantagepoint_user_timecategory_sync_child_{config.instance}',
        description="Retrieves Time Category for a user from Deltek Vantagepoint API",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_time_category_sync_for_user,
        default_args={
            'vp_conn_id': config.deltek_vantagepoint_conn_id
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='fetch_time_category_by_tkgroup',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        fetch_time_category_by_tkgroup = rail.VantagepointAPIOperator(
            task_id = 'fetch_time_category_by_tkgroup',
            request_method='GET',
            pagination=False,
            endpoint="/settings/time/categories/{{dag_run.conf.homecompany}}/{{dag_run.conf.tkgroup}}?employee={{dag_run.conf.loginname}}"
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
            endpoint="/settings/time/categories/{{dag_run.conf.homecompany}}/<allgroup>?employee={{dag_run.conf.loginname}}"
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

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        batch_task >> fetch_time_category_by_tkgroup
        batch_task >> log_to_sumo
        fetch_time_category_by_tkgroup >> if_records_not_found_by_tkgroup
        if_records_not_found_by_tkgroup >> rail.Label('Yes') >> fetch_time_category_by_allgroup >> add_time_category_by_employee
        if_records_not_found_by_tkgroup >> rail.Label('No') >> add_time_category_by_employee >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
