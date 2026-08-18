import rail


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_assign_substitute_user_dag_id,
        description=f'strayeruniversity_usersync_assign_substitute_user_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.assign_sub_user_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        def map_impersonate_and_create_interactive_session(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ dag_run.conf.actualuri }}"
            },
            response_filter=map_impersonate_and_create_interactive_session
        )

        create_new_draft = rail.RepliconServiceOperator(
            task_id='create_new_draft',
            endpoint="/services/SubstituteUserAssignmentService1.svc/CreateNewDraft",
            data={
                "userUri": "{{ dag_run.conf.actualuri }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        update_substitute_user = rail.RepliconServiceOperator(
            task_id='update_substitute_user',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateSubstituteUser",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft') }}",
                "substituteUser": {
                    "uri": "{{ dag_run.conf.suburi }}",
                    "loginName": None,
                    "parameterCorrelationId": "{{ dag_run_ecid() }}"
                }
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        put_access_levels = rail.RepliconServiceOperator(
            task_id='put_access_levels',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PutAccessLevels",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft') }}",
                "accessLevelUris": [
                    "urn:replicon:substitute-user-access-level:full-access"
                ]
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft') }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session'),
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Assign Substitute User",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        impersonate_and_create_interactive_session >> create_new_draft >> update_substitute_user >> put_access_levels >> \
            publish_draft >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
