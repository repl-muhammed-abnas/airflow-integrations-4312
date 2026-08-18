import rail

# Costpoint LAST_MODIFIED conditions require a value; a fixed, ancient date
# keeps every probe's "modified since" filter wide open regardless of when
# this DAG last ran, so a real row count -- not just an HTTP 200 -- proves
# data is actually fetchable.
STATIC_PROBE_DATE = '1900-01-01T00:00:00'


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'deltek_costpoint_connectivity_check_{config.instance}',
        description=f'deltek_costpoint_connectivity_check_{config.instance}',
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
            return [
                {
                    'company': (company_result or {}).get('_company'),
                    'row_count': len(((company_result or {}).get('document') or {}).get('rows') or []),
                }
                for company_result in (response or [])
            ]

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

        fetch_countries = costpoint_probe(
            'fetch_countries', 'replicon_exp_country', 'SYMCNTRY_COUNTRY',
            'SYMCNTRY_COUNTRY_LAST_MODIFIED')
        fetch_labor_locations = costpoint_probe(
            'fetch_labor_locations', 'replicon_exp_labor_location', 'LDMLLOC_LABLOCATION',
            'LDMLLOC_LABLOCATION_LAST_MODIFIED')
        fetch_organizations = costpoint_probe(
            'fetch_organizations', 'replicon_exp_org', 'GLMORMNT_ORG_PARENT',
            'GLMORMNT_ORG_PARENT_LAST_MODIFIED')
        fetch_accounts = costpoint_probe(
            'fetch_accounts', 'replicon_exp_act', 'GLMACT_ACCT_HDR',
            'GLMACT_ACCT_HDR_LAST_MODIFIED')
        fetch_general_labor_categories = costpoint_probe(
            'fetch_general_labor_categories', 'replicon_exp_glc', 'LDMGLC_GENLLABCAT_HDR',
            'LDMGLC_GENLLABCAT_HDR_LAST_MODIFIED')
        fetch_project_labor_categories = costpoint_probe(
            'fetch_project_labor_categories', 'replicon_exp_plc', 'PJMPLC_BILLLABCAT_PLC',
            'PJMPLC_BILLLABCAT_PLC_LAST_MODIFIED')
        fetch_employee_classes = costpoint_probe(
            'fetch_employee_classes', 'replicon_exp_empclass', 'LDMCLASS_EMPLCLASS_HDR',
            'LDMCLASS_EMPLCLASS_HDR_LAST_MODIFIED')
        fetch_pay_types = costpoint_probe(
            'fetch_pay_types', 'replicon_exp_paytype', 'LDMPAYTP_PAYTYPE',
            'LDMPAYTP_PAYTYPE_LAST_MODIFIED')
        fetch_taxable_entities = costpoint_probe(
            'fetch_taxable_entities', 'replicon_exp_taxable', 'GLMCOMP_TAXBLEENTITY',
            'GLMCOMP_TAXBLEENTITY_LAST_MODIFIED')
        fetch_leave_types = costpoint_probe(
            'fetch_leave_types', 'replicon_exp_leavetype', 'LDMLVTP_LVTYPE_HDR',
            'LDMLVTP_LVTYPE_HDR_LAST_MODIFIED')
        fetch_timesheet_periods = costpoint_probe(
            'fetch_timesheet_periods', 'replicon_exp_tsperiod', 'LDMTSPD_TSPD_HDR',
            'LDMTSPD_TSPD_HDR_LAST_MODIFIED')
        fetch_work_schedules = costpoint_probe(
            'fetch_work_schedules', 'replicon_exp_tmmworkschedule', 'TMMWORKSCHEDULE_HDR',
            children=[
                {
                    "rsWhere": {
                        "rsId": "TMMWORKSCHEDULE_DATE",
                        "conditions": modified_since('TMMWORKSCHEDULE_DATE_LAST_MODIFIED'),
                        "children": [],
                    }
                }
            ])

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            message="{{ get_error_message() }}",
            severity="Error",
            properties={
                "entity": "costpoint_connectivity_check",
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

        probes = [
            fetch_countries, fetch_labor_locations, fetch_organizations, fetch_accounts,
            fetch_general_labor_categories, fetch_project_labor_categories, fetch_employee_classes,
            fetch_pay_types, fetch_taxable_entities, fetch_leave_types, fetch_timesheet_periods,
            fetch_work_schedules,
        ]
        probes >> catch_and_log_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
