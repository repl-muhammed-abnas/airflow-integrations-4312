import rail

null = None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.department_add_dag_id,
        description=f'Ascend_Sub Child_department add {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_seconday_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        # Create the one missing department. Parent URI was resolved by the parent DAG.
        create_dept = rail.RepliconServiceOperator(
            task_id='create_dept',
            endpoint="/services/DepartmentService1.svc/PutDepartment",
            data=lambda dag_run: {
                "department": {
                    "target": {
                        "uri": null,
                        "name": dag_run.conf["department"].split('/')[-1].strip(),
                        "parent": {
                            "uri": dag_run.conf["parent_dept_uri"]
                        } if dag_run.conf.get('parent_dept_uri') else null,
                        "parameterCorrelationId": null
                    },
                    "name": dag_run.conf["department"].split('/')[-1].strip(),
                    "code": null,
                    "comments": null,
                    "isEnabled": "true",
                    "customFieldValues": []
                }
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
                "action": "Department Add",
                "status": "Error",
                "details": rail.render_template("{{ get_error_message() }}")
            }
        )

        create_dept >> catch_and_log_errors


    return dag


rail.for_each_instance(create_dag)
