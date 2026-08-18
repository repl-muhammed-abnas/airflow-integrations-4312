import rail


def create_disableuser_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"macquarie_user_import_disable_users_update_supervisor_child_{config.instance}",
        description=f"Macquarie User Import Disable Users update supervisor Child {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_disableuser_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        update_users_supervisor = rail.RepliconServiceOperator(
            task_id="update_users_supervisor",
            endpoint="/services/UserService1.svc/UpdateSupervisorAssignmentScheduleOverDateRange",
            data=lambda dag_run:{
                "userUri": dag_run.conf['user_uri'],
                "supervisorUri": dag_run.conf['default_supervisor_uri'],
                "dateRange": {
                    "startDate": dag_run.conf['default_supervisor_effective_date']
                }
            }
        )


        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule="one_failed",
            severity="Error",
            message='{{ get_error_message() }}',
            properties={
                "user_name": "{{dag_run.conf.user_loginname}}",
                "message": '{{ get_error_message() }}'
            }
        )

        update_users_supervisor >> rail.Label("On Error") >> catch_and_log_error

    return dag


rail.for_each_instance(create_disableuser_child_dag)
