import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_budget_connectivity_check_{config.instance}',
        description=f'deltek_costpoint_budget_connectivity_check_{config.instance}',
        schedule_interval=None,
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'deltek_costpoint_conn_id': config.deltek_cospoint_conn_id,
        }
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        def summarize_costpoint_response(response):
            # data_handler runs once per company on that company's raw
            # document -- the multi-company list with '_company' keys is
            # assembled afterward by dict_to_list, so this must return a
            # single summary dict, not iterate response as if it were
            # already the aggregated list.
            return {
                'row_count': len(((response or {}).get('document') or {}).get('rows') or []),
            }

        # allocation_sync_master_dag.py's get_sub_period_info pulls the full
        # accounting sub-period list unfiltered every run, so mirror that
        # exactly -- there's no LAST_MODIFIED field on this rsId to probe with.
        fetch_periods = rail.DeltekCostPointServiceOperator(
            task_id='fetch_periods',
            endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
            company=config.deltek_cospoint_company_ids,
            data={
                "filter": {
                    "id": "polaris_exp_periods",
                    "where": [
                        {
                            "rsWhere": {
                                "rsId": "BNP_BAMMAM8",
                                "conditions": [
                                ],
                                "children": [
                                ]
                            }
                        }
                    ]
                }
            },
            data_handler=summarize_costpoint_response,
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "costpoint_budget_connectivity_check",
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

        fetch_periods >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
