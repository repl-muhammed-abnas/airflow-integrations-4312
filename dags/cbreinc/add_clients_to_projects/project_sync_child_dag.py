
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'cbreinc_add_clients_to_projects_project_sync_child_{config.instance}',
        description=f'CBREInc_Projectsync_Child - V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        create_project_info_modification_batch = rail.RepliconServiceOperator(
            task_id='create_project_info_modification_batch',
            endpoint="/services/ProjectService1.svc/CreateProjectInfoModificationBatch",
            data=lambda: {
                "projectUris": rail.get_dag_run_conf()['projecturis'],
                "modificationParameter": {
                    "currentClients": {
                        "clients": rail.get_dag_run_conf()['clients']
                    },
                    "clientBillingAllocationMethodUri": "urn:replicon:client-billing-allocation-method:user-specified",
                    "customFields": []
                }
            }
        )

        batch_entry, batch_exit = rail.batch_execution(
            group_id='execute_batch_management',
            creation_task_id='create_project_info_modification_batch'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_project_info_modification_batch >> batch_entry
        batch_exit >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
