from datetime import timedelta
import rail
from pwcglobal.time_export_v3.request_payload import get_current_past_period_conf


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/time_extract_v3/config.py


# pylint:disable = too-many-statements
def create_child_process_batch_dag(config):
    batch_dags = []

    for location in config.location_codes:

        with rail.create_airflow_dag(
            dag_id=f'pwc_time_export_user_batch_child_{location}_{config.instance}_v3',
            description=f'Timeexport process user list formatted {location} {config.instance}',
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.child_user_batch_max_active_runs,
            max_active_tasks=config.dag_max_active_tasks
        ) as dag:

            rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

            create_user_object_set = rail.RepliconServiceOperator(
                task_id='create_user_object_set',
                endpoint='/services/UserService1.svc/CreateObjectSet',
                data=lambda dag_run: {
                    "userUris": [x['uri'] for x in dag_run.conf['user_uri_batch']]
                }
            )

            is_valid_export_period = rail.IfOperator(
                task_id='is_valid_export_period',
                test="{{ dag_run.conf.export_period == 'past' or dag_run.conf.export_period == 'current' }}",
                yes_task='trigger_current_past_period_imports',
                no_task='finish'
            )

            trigger_current_past_period_imports = rail.TriggerDagRunForEachItemOperator(
                task_id='trigger_current_past_period_imports',
                retries=0,
                items=lambda dag_run: [dag_run.conf['export_period']],
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                trigger_dag_id=f'pwc_time_export_child_current_past_period_{location}_{config.instance}_v3',
                conf=lambda dag_run: get_current_past_period_conf(dag_run, config)
            )

            finish = rail.EmptyOperator(
                task_id='finish'
            )

            create_user_object_set >> is_valid_export_period

            is_valid_export_period >> rail.Label(
                "Yes") >> trigger_current_past_period_imports >> finish

            is_valid_export_period >> rail.Label(
                "No") >> finish

        batch_dags.append(dag)

    return batch_dags


rail.for_each_instance(create_child_process_batch_dag)
