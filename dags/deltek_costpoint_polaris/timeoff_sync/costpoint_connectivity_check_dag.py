import rail

# Both Costpoint timeoff query paths take a "modified since" placeholder that
# must be supplied. A fixed, ancient date keeps the probe wide open regardless
# of when this DAG last ran, so a real row count -- not just a live
# connection -- proves data is actually fetchable. Matches the
# "%Y-%m-%d %H:%M:%S" shape both config.odbc_query and config.sql_query expect
# (see timeoff_sync_main_dag.py's do_get_last_run_date).
STATIC_PROBE_DATE = '1900-01-01 00:00:00'


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timeoff_connectivity_check_{config.instance}',
        description=f'deltek_costpoint_timeoff_connectivity_check_{config.instance}',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        # timeoff_sync_main_dag.py picks between two Costpoint data sources
        # based on config.isFromSql -- a SQL Server connection
        # (deltek_cospoint_sql_conn_id) or the Costpoint ODBC connection
        # (odbc_conn_id). Probe whichever this tenant is actually configured
        # to use.
        choose_data_source = rail.IfOperator(
            task_id='choose_data_source',
            test=lambda: bool(config.isFromSql),
            yes_task='build_probe_sql_query',
            no_task='probe_odbc_timeoff_bookings',
        )

        build_probe_sql_query = rail.PythonOperator(
            task_id='build_probe_sql_query',
            python_callable=lambda: config.sql_query.replace(
                '> ?', f"> '{STATIC_PROBE_DATE}'")
        )

        probe_sql_timeoff_bookings = rail.MsSqlEncryptedOperator(
            task_id='probe_sql_timeoff_bookings',
            mssql_conn_id=config.deltek_cospoint_sql_conn_id,
            sql="{{ result('build_probe_sql_query') }}",
        )

        probe_odbc_timeoff_bookings = rail.DeltekCostPointODBCOperator(
            task_id='probe_odbc_timeoff_bookings',
            deltek_costpoint_odbc_conn_id=config.odbc_conn_id,
            query=config.odbc_query,
            query_params=[STATIC_PROBE_DATE, STATIC_PROBE_DATE, STATIC_PROBE_DATE],
        )

        def do_summarize_probe_result():
            source_task = 'probe_sql_timeoff_bookings' if config.isFromSql else 'probe_odbc_timeoff_bookings'
            rows = rail.result(source_task) or []
            return {
                'source': source_task,
                'row_count': len(rows),
            }

        summarize_probe_result = rail.PythonOperator(
            task_id='summarize_probe_result',
            trigger_rule='none_failed_min_one_success',
            python_callable=do_summarize_probe_result,
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "costpoint_timeoff_connectivity_check",
                "action": "Probe",
                "status": "Error",
                "reason": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        choose_data_source >> rail.Label(
            'Yes') >> build_probe_sql_query >> probe_sql_timeoff_bookings >> summarize_probe_result
        choose_data_source >> rail.Label(
            'No') >> probe_odbc_timeoff_bookings >> summarize_probe_result
        [build_probe_sql_query, probe_sql_timeoff_bookings, probe_odbc_timeoff_bookings, summarize_probe_result] \
            >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
