import rail
from dxctechnology.ftp_time_export_v4.utils import request_payload
from dxctechnology.ftp_time_export_v4.utils import custom_method


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ftp_export_child_v4_process_all_unackn_export_{config.instance}',
        description=f'DXC_FTP_Export_Automation Client Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_specific_time_export_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_details',
            endpoint='/services/TimeDataExportService1.svc/GetTimeDataExportDetails',
            data=request_payload.get_specific_time_export_details_payload,
        )

        is_unckn_export_extension_feild_value_present = rail.IfOperator(
            task_id="is_unckn_export_extension_feild_value_present",
            test=lambda: custom_method.is_extension_feild(
                "get_specific_time_export_details"),
            no_task="time_export_details_output"
        )

        time_export_details_output = rail.RenderTemplateOperator(
            task_id='time_export_details_output',
            template="""
                {% set users = [] %}
                {% do users.append({ 'Identifier': dag_run.conf.name+'|FTP', 'createdatetime': dag_run.conf.createdatetime }) %}
                {{ users | tojson }}
                """,
            target='result',
            json=True,
        )

        get_specific_time_export_details >> is_unckn_export_extension_feild_value_present
        is_unckn_export_extension_feild_value_present >> rail.Label(
            "No") >> time_export_details_output

    return dag


rail.for_each_instance(create_child_dag)
