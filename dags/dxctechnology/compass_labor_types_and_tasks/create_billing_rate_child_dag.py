
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_compass_labor_types_and_tasks_child_createbillingrate_{config.sub_erp_name}_{config.instance}',
        description=f'DXC_Compass_Labour_Type_and_Task_Automation- child_create billing rate_V2.0 - B1 {config.sub_erp_name}_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_dag_run_child_process,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config
        )

        put_company_billing_ratewith_billableextention = rail.RepliconServiceOperator(
            task_id='put_company_billing_ratewith_billableextention',
            endpoint="/services/BillingRateService1.svc/PutCompanyBillingRate",
            data={
                "billingRate": {
                    "target": {
                        "uri": null,
                        "name": "{{dag_run.conf.name}}|Billable"
                    },
                    "name": "{{dag_run.conf.name}}|Billable",
                    "description": null,
                    "isEnabled": "true",
                    "rateSchedule": null
                }
            }
        )

        put_company_billing_ratewith_non_billableextention = rail.RepliconServiceOperator(
            task_id='put_company_billing_ratewith_non_billableextention',
            endpoint="/services/BillingRateService1.svc/PutCompanyBillingRate",
            data={
                "billingRate": {
                    "target": {
                        "uri": null,
                        "name": "{{ dag_run.conf.name }}|Non-Billable"
                    },
                    "name": "{{ dag_run.conf.name }}|Non-Billable",
                    "description": null,
                    "isEnabled": "true",
                    "rateSchedule": null
                }
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        put_company_billing_ratewith_billableextention >> put_company_billing_ratewith_non_billableextention >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
