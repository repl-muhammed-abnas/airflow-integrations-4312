import rail

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.disable_dagid,
        description=f'PwC Supervisory Org Custom Import Disable Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:
        
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        disable_supervisory_org = rail.RepliconServiceOperator(
            task_id='disable_supervisory_org',
            endpoint="/services/CostCenterService1.svc/Disable",
            data={
                "costCenterUri": "{{ dag_run.conf.costcenter_path.uri }}"
            }
        )

        success_disable_log = rail.WriteLogOperator(
            task_id='success_disable_log',
            log="{{ dag_run.conf.lookuptable }}",
            message="Success",
            severity="Success",
            properties={
                "Supervisory Org": "{{ dag_run.conf.costcenter_path.full_path }}",
                "Action": "Disable",
                "Status": "Success",
                "Details": "Disabled Supervisory Org."
            }
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        disable_supervisory_org >> success_disable_log >> finish

    return dag

rail.for_each_instance(create_dag)
