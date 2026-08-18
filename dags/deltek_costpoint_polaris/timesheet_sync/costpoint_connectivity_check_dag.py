import rail

# timesheet_sync_dag.py is entirely webhook-driven: every Costpoint read it
# makes is scoped to a specific EMPL_ID/TS_DT/PROJ_ID taken from the inbound
# webhook payload, and "company" is resolved per-run from the Replicon user's
# "Company" OEF value -- this instance config has no static company id list
# to fall back on (see instances/*.py, none define deltek_cospoint_company_ids).
# Default to '1', the company id every other deltek_costpoint_polaris
# integration in this repo defaults to; override via
# dag_run.conf.company_ids = ["<id>"] if that doesn't apply to this tenant.
#
# Deliberately excludes the "polaris_imp_*" entities (push_time_to_costpoint,
# revert_existing_time, update_assignments_in_costpoint) -- those write/import
# into Costpoint, so running them here would risk mutating real timesheet data
# instead of just proving connectivity.
DEFAULT_COMPANY_IDS = ['1']


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_timesheet_connectivity_check_{config.instance}',
        description=f'deltek_costpoint_timesheet_connectivity_check_{config.instance}',
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

        def get_probe_company_ids():
            return rail.get_dag_run_conf().get('company_ids') or DEFAULT_COMPANY_IDS

        def summarize_costpoint_response(response):
            # data_handler runs once per company on that company's raw
            # document -- the multi-company list with '_company' keys is
            # assembled afterward by dict_to_list, so this must return a
            # single summary dict, not iterate response as if it were
            # already the aggregated list.
            return {
                'row_count': len(((response or {}).get('document') or {}).get('rows') or []),
            }

        def costpoint_probe(task_id, filter_id, rs_id):
            return rail.DeltekCostPointServiceOperator(
                task_id=task_id,
                endpoint='cpweb/cprestfulws/cpwwsgenericexport.cps',
                company=get_probe_company_ids,
                data={
                    "filter": {
                        "id": filter_id,
                        "where": [
                            {
                                "rsWhere": {
                                    "rsId": rs_id,
                                    "conditions": [],
                                    "children": [],
                                }
                            }
                        ]
                    }
                },
                data_handler=summarize_costpoint_response,
            )

        # Normally filtered to a single EMPL_ID + TS_DT from the webhook
        # payload (get_existing_deltek_timesheet); probed unfiltered here.
        fetch_timesheet_headers = costpoint_probe(
            'fetch_timesheet_headers', 'polaris_exp_ldmtime', 'LDMTIME_TSHDR')
        # Normally filtered to a single PROJ_ID (get_costpotint_work_force).
        fetch_project_workforce = costpoint_probe(
            'fetch_project_workforce', 'polaris_exp_pjm_work', 'PJM_PROJEMPL_HDR')
        # Normally filtered to a single PROJ_ID (get_costpoint_project_plcs).
        fetch_project_labor_categories = costpoint_probe(
            'fetch_project_labor_categories', 'polaris_exp_plc_prj', 'PJM_PROJLABCAT_HDR')

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "costpoint_timesheet_connectivity_check",
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

        probes = [fetch_timesheet_headers, fetch_project_workforce, fetch_project_labor_categories]
        probes >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
