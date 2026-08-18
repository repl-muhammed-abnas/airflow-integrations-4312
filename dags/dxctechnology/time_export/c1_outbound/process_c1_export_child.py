from datetime import timedelta
from airflow.models import Variable
from dxctechnology.time_export.c1_outbound.utils import request_payload
import rail

null = None
def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.c1_process_time_export_child_dagid,
        description=f"DXC - C1 Time Export Process C1 Child - {config.instance}",
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
            no_task='can_export_c1_timedata'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_export_c1_timedata',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_export_c1_timedata = rail.IfOperator(
            task_id='can_export_c1_timedata',
            test=lambda: Variable.get(config.time_data_posting_mapper, deserialize_json=True)["C1"]["posting"].lower() == "yes",
            yes_task='process_acknowledgement_not_received',
            no_task='batch_end'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            trigger_dag_id=config.c1_acknowledgement_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: request_payload.get_conf_for_process_ack_payload(dag_run, config)
        )

        wait_to_process_acknowledgement_not_received = rail.WaitForDagRunsSensor(
            task_id='wait_to_process_acknowledgement_not_received',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_acknowledgement_not_received") }}'
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
            trigger_dag_id=config.c1_regular_create_time_export_child_dagid,
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
            trigger_dag_id=config.c1_iwo_create_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: dag_run.conf
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> can_export_c1_timedata

        can_export_c1_timedata >> rail.Label("Yes") >> process_acknowledgement_not_received \
            >> wait_to_process_acknowledgement_not_received >> is_twb_name_starts_with_REG
        can_export_c1_timedata >> rail.Label("No") >> batch_end
        is_twb_name_starts_with_REG >> rail.Label("Yes") >> create_time_export_outbound >> is_twb_name_starts_with_IWO
        is_twb_name_starts_with_REG >> rail.Label("No") >> is_twb_name_starts_with_IWO
        is_twb_name_starts_with_IWO >> rail.Label("Yes") >> create_time_export_iwo >> batch_end
        is_twb_name_starts_with_IWO >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
