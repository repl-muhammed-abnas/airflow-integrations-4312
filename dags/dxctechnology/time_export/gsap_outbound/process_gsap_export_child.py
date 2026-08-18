from datetime import timedelta
from airflow.models import Variable
from dxctechnology.time_export.gsap_outbound.utils import request_payload, response_filters
import rail

null = None
def create_child_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.gsap_process_time_export_child_dagid,
        description=f"DXC - GSAP Outbound Time Export Process GSAP Child - {config.instance}",
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
            no_task='can_export_gsap_timedata'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='can_export_gsap_timedata',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        can_export_gsap_timedata = rail.IfOperator(
            task_id='can_export_gsap_timedata',
            test=lambda: Variable.get(config.time_data_posting_mapper, deserialize_json=True)["GSAP"]["posting"].lower() == "yes",
            yes_task='get_last_time_export_gsap_details',
            no_task='batch_end'
        )

        get_last_time_export_gsap_details = rail.RepliconServiceOperator(
            task_id='get_last_time_export_gsap_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ dag_run.conf.lasttwburi }}",
                    "name": null
                }
            }
        )

        is_unckn_export_extension_field_value_present_for_gsap = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present_for_gsap",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_last_time_export_gsap_details")['extensionFieldValues'],
                    dag_run.conf["oefname"]),
            yes_task='process_acknowledgement_not_received',
            no_task='is_twb_name_starts_with_REG'
        )

        process_acknowledgement_not_received = rail.TriggerDagRunOperator(
            task_id='process_acknowledgement_not_received',
            retries=0,
            trigger_dag_id=config.gsap_acknowledgement_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=request_payload.get_conf_for_process_ack_payload
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
            trigger_dag_id=config.gsap_regular_create_time_export_child_dagid,
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
            trigger_dag_id=config.gsap_iwo_create_time_export_child_dagid,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: dag_run.conf
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> can_export_gsap_timedata

        can_export_gsap_timedata >> rail.Label("Yes") >> get_last_time_export_gsap_details \
            >> is_unckn_export_extension_field_value_present_for_gsap
        can_export_gsap_timedata >> rail.Label("No") >> batch_end
        is_unckn_export_extension_field_value_present_for_gsap >> rail.Label("Yes") >> process_acknowledgement_not_received \
            >> is_twb_name_starts_with_REG
        is_unckn_export_extension_field_value_present_for_gsap >> rail.Label("No") >> is_twb_name_starts_with_REG
        is_twb_name_starts_with_REG >> rail.Label("Yes") >> create_time_export_outbound >> is_twb_name_starts_with_IWO
        is_twb_name_starts_with_REG >> rail.Label("No") >> is_twb_name_starts_with_IWO
        is_twb_name_starts_with_IWO >> rail.Label("Yes") >> create_time_export_iwo >> batch_end
        is_twb_name_starts_with_IWO >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
