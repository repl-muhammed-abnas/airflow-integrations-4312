
from datetime import timedelta
import uuid
import json
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.location_add_child_dagid,
        description=f'Arctic wolf Location add child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_group,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_location_level_1_uri'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_location_level_1_uri',
            end_task='catch_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_location_level_1_uri = rail.PythonOperator(
            task_id='get_location_level_1_uri',
            python_callable=lambda dag_run:  next((item['locationuri'] for item in json.loads(dag_run.conf['all_existing_locations'].strip(
                '"').replace("'", '\"')) if item['locationname'] == dag_run.conf['location_level_1']), None)
        )

        if_location_level_1_uri_not_present = rail.IfOperator(
            task_id='if_location_level_1_uri_present',
            test=lambda: not bool(rail.result('get_location_level_1_uri')),
            yes_task='create_location_level_1',
            no_task='create_location_level_2'
        )

        create_location_level_1 = rail.RepliconServiceOperator(
            task_id='create_location_level_1',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda dag_run: {
                "location": null,
                "modifications": {
                    "name": dag_run.conf['location_level_1'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_location_level_2 = rail.RepliconServiceOperator(
            task_id='create_location_level_2',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=lambda dag_run: {
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": null,
                        "uri": rail.result('get_location_level_1_uri') if rail.result('get_location_level_1_uri')
                        else rail.result('create_location_level_1')['uri'],
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": dag_run.conf['location_level_2'],
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        catch_error = rail.PythonOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            python_callable=lambda: 'Error' +
            rail.render_template("{{get_error_message()}}")
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_error
        can_run_batch_task >> rail.Label(
            'No') >> get_location_level_1_uri
        get_location_level_1_uri >> if_location_level_1_uri_not_present

        if_location_level_1_uri_not_present >> rail.Label(
            'Yes') >> create_location_level_1 >> create_location_level_2
        if_location_level_1_uri_not_present >> rail.Label(
            'No') >> create_location_level_2 >> catch_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
