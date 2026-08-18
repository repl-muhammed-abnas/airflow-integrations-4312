import rail
from dxctechnology.cwf_time_export_v7.utils import response_filter
from dxctechnology.cwf_time_export_v7.utils import request_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_acknowledgement_not_received_notification_{config.instance}_v7',
        description=f'DXCTechnology_CWF Time export Acknowledgement Notification Not Received v7 {config.instance}',
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
            no_task='check_gsap_erp'
        )

        condition_check_for_compass = rail.EmptyOperator(
            task_id='condition_check_for_compass'
        )

        check_export_time_and_is_available_previously_compass = rail.IfOperator(
            task_id='check_export_time_and_is_available_previously_compass',
            test=request_payload.check_ack_date_and_name,
            yes_task='get_specific_time_export_details',
        )

        check_gsap_erp = rail.IfOperator(
            task_id='check_gsap_erp',
            test='{{ True if dag_run.conf.erp=="GSAP" else False }}',
            yes_task='condition_check_for_gsap',
            no_task='check_psa_erp'
        )

        condition_check_for_gsap = rail.EmptyOperator(
            task_id='condition_check_for_gsap'
        )

        check_export_time_and_is_available_previously_gsap = rail.IfOperator(
            task_id='check_export_time_and_is_available_previously_gsap',
            test=request_payload.check_ack_date_and_name,
            yes_task='get_specific_time_export_details'
        )

        check_psa_erp = rail.IfOperator(
            task_id='check_psa_erp',
            test='{{ True if dag_run.conf.erp=="PSA" else False }}',
            yes_task='condition_check_for_psa',
            no_task='get_specific_time_export_details'
        )

        condition_check_for_psa = rail.EmptyOperator(
            task_id='condition_check_for_psa'
        )

        check_export_time_and_is_available_previously_psa = rail.IfOperator(
            task_id='check_export_time_and_is_available_previously_psa',
            test=request_payload.check_ack_date_and_name,
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
            "Yes") >> condition_check_for_compass >> check_export_time_and_is_available_previously_compass

        check_compass_erp >> rail.Label(
            "No") >> check_gsap_erp

        check_export_time_and_is_available_previously_compass >> rail.Label(
            "Yes") >> get_specific_time_export_details

        check_export_time_and_is_available_previously_compass >> rail.Label(
            "No") >> check_gsap_erp

        check_gsap_erp >> rail.Label(
            "Yes") >> condition_check_for_gsap >> check_export_time_and_is_available_previously_gsap

        check_gsap_erp >> rail.Label(
            "No") >> check_psa_erp

        check_export_time_and_is_available_previously_gsap >> rail.Label(
            "Yes") >> get_specific_time_export_details

        check_export_time_and_is_available_previously_gsap >> rail.Label(
            "No") >> check_psa_erp

        check_psa_erp >> rail.Label(
            "Yes") >> condition_check_for_psa >> check_export_time_and_is_available_previously_psa

        check_psa_erp >> rail.Label(
            "No") >> get_specific_time_export_details

        check_export_time_and_is_available_previously_psa >> rail.Label(
            "Yes") >> get_specific_time_export_details

        get_specific_time_export_details >> is_unckn_export_extension_feild_value_present >> rail.Label(
            "No") >> time_export_details_output

    return dag


rail.for_each_instance(create_dag)
