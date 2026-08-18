import rail


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'cbrefcgproduction_Newhire_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        for_each_project= rail.ForEachOperator(
            task_id='for_each_project',
            items="{{ dag_run.conf.projects | to_json }}",
            start_task = 'assign_user_to_project',
            end_task = 'for_each_project_end'
        )

        assign_user_to_project= rail.RepliconServiceOperator(
            task_id='assign_user_to_project',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                    "projectUri": "{{ result('for_each_project').projecturi }}",
                    "resourceUri": "{{ dag_run.conf.useruri }}",
                    "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
                }
        )

        assign_users_billing_rate= rail.RepliconServiceOperator(
            task_id='assign_users_billing_rate',
            endpoint="/services/TimeAndMaterialsProjectService1.svc/PutProjectTeamMemberBillingRatesAllowedForBillingTime",
            data={
                    "projectUri": "{{ result('for_each_project').projecturi }}",
                    "resourceUri": "{{ dag_run.conf.useruri }}",
                    "billingRateUris":  [
                        "urn:replicon:user-specific-billing-rate"
                    ]
                }
        )

        for_each_project_end= rail.EmptyOperator(
            task_id='for_each_project_end',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        for_each_project >> assign_user_to_project >> assign_users_billing_rate >> for_each_project_end

        for_each_project >> for_each_project_end >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
