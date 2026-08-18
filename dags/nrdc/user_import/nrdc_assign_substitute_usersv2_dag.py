
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'nrdc_assignsubstituteusersv2_{config.instance}',
        description=f'Live|NRDC_Assign Substitute usersv2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_3',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_3 = rail.SetVariableOperator(
            task_id='declare_variable_3',
            append=False,
            name='status',
            value=None
        )

        def map_impersonate_and_create_interactive_session(res):
            data = res.json()['d']
            auth_token = list(
                filter(lambda x: x['name'] == 'AUTHTOKEN', data['sessionCookies']))[0]['value']
            tenant = list(
                filter(lambda x: x['name'] == 'TENANT', data['sessionCookies']))[0]['value']
            return {'cookie': f'AUTHTOKEN={auth_token};TENANT={tenant}', 'Path': '/'}

        impersonate_and_create_interactive_session_6 = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session_6',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data={
                "impersonatedUserUri": "{{ dag_run.conf.actualuri }}"
            },
            response_filter=map_impersonate_and_create_interactive_session
        )

        create_new_draft_6 = rail.RepliconServiceOperator(
            task_id='create_new_draft_6',
            endpoint="/services/SubstituteUserAssignmentService1.svc/CreateNewDraft",
            data={
                "userUri": "{{ dag_run.conf.actualuri }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session_6'),
        )

        update_substitute_user_7 = rail.RepliconServiceOperator(
            task_id='update_substitute_user_7',
            endpoint="/services/SubstituteUserAssignmentService1.svc/UpdateSubstituteUser",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_6') }}",
                "substituteUser": {
                    "uri": "{{ dag_run.conf.suburi }}",
                    "loginName": null,
                    "parameterCorrelationId": "{{ dag_run_ecid() }}"
                }
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session_6'),
        )

        put_access_levels_8 = rail.RepliconServiceOperator(
            task_id='put_access_levels_8',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PutAccessLevels",
            data={
                "substituteUserAssignmentUri": "{{ result('create_new_draft_6') }}",
                "accessLevelUris": [
                    "urn:replicon:substitute-user-access-level:full-access"
                ]
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session_6'),
        )

        publish_draft_9 = rail.RepliconServiceOperator(
            task_id='publish_draft_9',
            endpoint="/services/SubstituteUserAssignmentService1.svc/PublishDraft",
            data={
                "draftUri": "{{ result('create_new_draft_6') }}"
            },
            headers=lambda: rail.result(
                'impersonate_and_create_interactive_session_6'),
        )

        update_variable_10 = rail.SetVariableOperator(
            task_id='update_variable_10',
            append=False,
            name='{{ result("declare_variable_3").name }}',
            value="Success"
        )

        stop_13 = rail.EmptyOperator(
            task_id='stop_13',

        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_variable_3
        declare_variable_3 >> impersonate_and_create_interactive_session_6 >> create_new_draft_6 >> update_substitute_user_7 >> put_access_levels_8 >> \
            publish_draft_9 >> update_variable_10 >> stop_13 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
