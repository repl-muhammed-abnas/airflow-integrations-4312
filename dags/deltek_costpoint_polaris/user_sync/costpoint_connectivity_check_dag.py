import rail

# Costpoint LAST_MODIFIED conditions require a value; a fixed, ancient date
# keeps the probe's "modified since" filter wide open regardless of when
# this DAG last ran, so a real row count -- not just an HTTP 200 -- proves
# data is actually fetchable.
STATIC_PROBE_DATE = '1900-01-01T00:00:00'


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_user_connectivity_check_{config.instance}',
        description=f'deltek_costpoint_user_connectivity_check_{config.instance}',
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

        def modified_since(field_name):
            return [
                {
                    "joinWithParent": "N",
                    "relations": [
                        {
                            "name": field_name,
                            "relation": "gt=",
                            "value": STATIC_PROBE_DATE,
                        }
                    ]
                }
            ]

        def costpoint_probe(task_id, filter_id, rs_id, condition_field=None, children=None):
            return rail.DeltekCostPointServiceOperator(
                task_id=task_id,
                endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
                company=config.deltek_cospoint_company_ids,
                data={
                    "filter": {
                        "id": filter_id,
                        "where": [
                            {
                                "rsWhere": {
                                    "rsId": rs_id,
                                    "conditions": modified_since(condition_field) if condition_field else [],
                                    "children": children or [],
                                }
                            }
                        ]
                    }
                },
                data_handler=summarize_costpoint_response,
            )

        # user_sync_master_dag.py queries this exact filter/rsId (both
        # unchunked via get_modified_users and chunked via
        # get_modified_users_in_chunks) to pull employee records for sync.
        fetch_users = costpoint_probe(
            'fetch_users', 'polaris_exp_user_details', 'LDMEINFO_EMPL',
            'LDMEINFO_EMPL_LAST_MODIFIED')

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "costpoint_user_connectivity_check",
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

        fetch_users >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
