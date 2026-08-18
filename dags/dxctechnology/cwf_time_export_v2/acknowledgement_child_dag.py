import rail
from dxctechnology.cwf_time_export_v2.utils import response_filter
from dxctechnology.cwf_time_export_v2.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_acknowledgement_not_received_notification_v2_{config.instance}',
        description=f'DXCTechnology_CWF Time export Acknowledgement Notification Not Received V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.compass_sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        check_compass_erp = rail.IfOperator(
            task_id='check_compass_erp',
            test='{{ True if dag_run.conf.erp=="compass" else False }}',
            yes_task='condition_check_for_compass',
            no_task='get_specific_time_export_details'
        )

        condition_check_for_compass = rail.EmptyOperator(
            task_id='condition_check_for_compass'
        )

        check_export_time_and_is_available_previously = rail.IfOperator(
            task_id='check_export_time_and_is_available_previously',
            test=request_payload.check_compass_ack,
            yes_task='get_specific_time_export_details'
        )

        get_specific_time_export_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_details',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data={
                    "target": {
                        "uri": '{{ dag_run.conf.uri }}',
                    }
            },
            response_filter=response_filter.get_last_time_export_details
        )

        is_unckn_export_extension_feild_value_present = rail.IfOperator(
            task_id="is_unckn_export_extension_feild_value_present",
            test='{{ result("get_specific_time_export_details") }}',
            no_task="time_export_details_output",
        )

        time_export_details_output = rail.RenderTemplateOperator(
            task_id='time_export_details_output',
            template="""
                {% set users = [] %}
                {% do users.append({ 'Identifier': dag_run.conf.name+'|'+dag_run.conf.sender,
                    'createdatetime': dag_run.conf.createdatetime }) %}
                {{ users | tojson }}
                """,
            target='result',
            json=True,
        )

        check_compass_erp >> rail.Label(
            "Yes") >> condition_check_for_compass >> check_export_time_and_is_available_previously >> rail.Label(
                "Yes") >> get_specific_time_export_details

        check_compass_erp >> rail.Label(
            "No") >> get_specific_time_export_details

        get_specific_time_export_details >> is_unckn_export_extension_feild_value_present >> rail.Label(
            "No") >> time_export_details_output

    return dag


rail.for_each_instance(create_dag)
