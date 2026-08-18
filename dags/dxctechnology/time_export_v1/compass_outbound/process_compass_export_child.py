from datetime import timedelta
from airflow.models import Variable
import rail

null = None
def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.compass_process_time_export_child_dagid,
        description=f"DXC - Compass Time Export Process Compass Export Child - {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='can_export_compass_timedata'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_export_compass_timedata',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_export_compass_timedata = rail.IfOperator(
            task_id='can_export_compass_timedata',
            test=lambda: Variable.get(config.time_data_posting_mapper, deserialize_json=True)["COMPASS"]["posting"].lower() == "yes",
            yes_task='is_twb_name_starts_with_REG',
            no_task='batch_end'
        )

        is_twb_name_starts_with_REG = rail.IfOperator(
            task_id='is_twb_name_starts_with_REG',
            test='{{ dag_run.conf.twbname | starts_with("REG") }}',
            yes_task='create_time_export_outbound',
            no_task='is_twb_name_starts_with_IWO'
        )

        create_time_export_outbound = rail.TriggerDagRunOperator(
            task_id='create_time_export_outbound',
            retries=0,
            trigger_dag_id=config.compass_regular_create_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: dag_run.conf
        )

        is_twb_name_starts_with_IWO = rail.IfOperator(
            task_id='is_twb_name_starts_with_IWO',
            test='{{ dag_run.conf.twbname | starts_with("IWO") }}',
            yes_task='create_time_export_iwo',
            no_task='batch_end'
        )

        create_time_export_iwo = rail.TriggerDagRunOperator(
            task_id='create_time_export_iwo',
            retries=0,
            trigger_dag_id=config.compass_iwo_create_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: dag_run.conf
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> can_export_compass_timedata

        can_export_compass_timedata >> rail.Label("Yes") >> is_twb_name_starts_with_REG
        is_twb_name_starts_with_REG >> rail.Label("Yes") >> create_time_export_outbound >> is_twb_name_starts_with_IWO
        is_twb_name_starts_with_REG >> rail.Label("No") >> is_twb_name_starts_with_IWO

        is_twb_name_starts_with_IWO >> rail.Label("Yes") >> create_time_export_iwo >> batch_end
        is_twb_name_starts_with_IWO >> rail.Label("No") >> batch_end
        can_export_compass_timedata >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
