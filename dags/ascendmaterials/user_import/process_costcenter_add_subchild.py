import rail
import uuid

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.cost_center_add_dag_id,
        description=f'Ascend_Sub Child_cost center add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        # Create the one missing cost center. Parent URI was resolved by the parent DAG.
        create_costcenter = rail.RepliconServiceOperator(
            task_id='create_costcenter',
            endpoint="/services/CostCenterService1.svc/CreateCostCenterOrApplyModification",
            data=lambda dag_run: {
                "costCenter": {
                    "parent": {
                        "uri": dag_run.conf["parent_costcenter_uri"],
                    } if dag_run.conf.get('parent_costcenter_uri') else null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf["costcenter"].split('/')[-1].strip(),
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ dag_run.conf["create_dept_log"] }}',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                "username": "",
                "userloginname": "",
                "action": "Cost Center Add",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        create_costcenter >> catch_and_log_errors

    return dag


rail.for_each_instance(create_dag)
