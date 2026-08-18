from datetime import datetime
import rail
from dxctechnology.time_export_webhook_ackn.utils import request_payload
from dxctechnology.time_export_webhook_ackn.utils import response_filter
from dxctechnology.time_export_webhook_ackn.utils import custom_method

# pylint: disable=too-many-statements


def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_timeexport_status_update{config.instance}',
        description='dxctechnology status check webhooks',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date=datetime(2022, 1, 1),
        max_active_runs=config.master_dag_max_active_runs,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var)
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_past_time_export = rail.RepliconServiceOperator(
            task_id='get_all_past_time_export',
            endpoint='/services/TimeDataExportListService1.svc/GetData',
            data=request_payload.get_all_past_time_export_payload,
            response_filter=response_filter.map_time_export
        )

        is_time_export_present_and_complete = rail.IfOperator(
            task_id='is_time_export_present_and_complete',
            test=lambda: rail.result('get_all_past_time_export') != [] and rail.result(
                'get_all_past_time_export')[0]['cells'][1]['textValue'] == "Complete",
            yes_task="get_all_time_export_oef_bindings",
            no_task="fail_acknowledgement"
        )

        fail_acknowledgement = rail.FailOperator(
            task_id='fail_acknowledgement',
            message='{{"Required identifier not available" if result("get_all_past_time_export") | \
                is_falsy else "Required identifier is not in complete status"}}'
        )

        get_all_time_export_oef_bindings = rail.RepliconServiceOperator(
            task_id='get_all_time_export_oef_bindings',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldBindings',
            data={
                    "bindingContextUri": "urn:replicon:object-type:time-data-export"
            }
        )

        get_sender_erp_details = rail.PythonOperator(
            task_id="get_sender_erp_details",
            python_callable=custom_method.get_sender_erp_details
        )

        is_sender_c1 = rail.IfOperator(
            task_id='is_sender_c1',
            test=lambda: rail.result("get_sender_erp_details")[
                'sender'].lower() == "c1",
            yes_task="empty_is_sender_c1",
            no_task="is_sender_ftp"
        )

        empty_is_sender_c1 = rail.EmptyOperator(
            task_id="empty_is_sender_c1")

        is_c1_starts_with_iwo = rail.IfOperator(
            task_id='is_c1_starts_with_iwo',
            test=lambda: rail.result("get_sender_erp_details")[
                'time_export_name'].lower().startswith("iwo"),
            yes_task="is_iwo_ends_with_c1",
            no_task="update_c1_oef_value"
        )

        is_iwo_ends_with_c1 = rail.IfOperator(
            task_id='is_iwo_ends_with_c1',
            test=lambda: rail.result("get_sender_erp_details")[
                'erp'].lower() == "c1",
            yes_task="update_iwo_ends_with_c1_oef_value",
            no_task="is_c1_pn1_or_nt1_erp"
        )

        update_iwo_ends_with_c1_oef_value = rail.RepliconServiceOperator(
            task_id='update_iwo_ends_with_c1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'C1_Payload_Processed')
        )

        is_c1_pn1_or_nt1_erp = rail.IfOperator(
            task_id='is_c1_pn1_or_nt1_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pn1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt1",
            yes_task="update_c1_pn1_nt1_oef_value",
            no_task="is_c1_pj1_or_nt3_erp"
        )

        update_c1_pn1_nt1_oef_value = rail.RepliconServiceOperator(
            task_id='update_c1_pn1_nt1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PN1/NT1_Payload_Processed')
        )

        is_c1_pj1_or_nt3_erp = rail.IfOperator(
            task_id='is_c1_pj1_or_nt3_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pj1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt3",
            yes_task="update_c1_pj1_nt3_oef_value",
            no_task="is_c1_p01_or_nt2_erp"
        )

        update_c1_pj1_nt3_oef_value = rail.RepliconServiceOperator(
            task_id='update_c1_pj1_nt3_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PJ1/NT3_Payload_Processed')
        )

        is_c1_p01_or_nt2_erp = rail.IfOperator(
            task_id='is_c1_p01_or_nt2_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "p01" or rail.result("get_sender_erp_details")['erp'].lower() == "nt2",
            yes_task="update_c1_p01_nt2_oef_value",
            no_task="is_sender_ftp"
        )

        update_c1_p01_nt2_oef_value = rail.RepliconServiceOperator(
            task_id='update_c1_p01_nt2_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_P01/NT2_Payload_Processed')
        )

        update_c1_oef_value = rail.RepliconServiceOperator(
            task_id='update_c1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'C1_Payload_Processed')
        )

        is_sender_ftp = rail.IfOperator(
            task_id='is_sender_ftp',
            test=lambda: rail.result("get_sender_erp_details")[
                'sender'].lower() == "ftp",
            yes_task="update_ftp_oef_value",
            no_task="is_sender_compass"
        )

        update_ftp_oef_value = rail.RepliconServiceOperator(
            task_id='update_ftp_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'FTP_Payload_Processed')
        )

        is_sender_compass = rail.IfOperator(
            task_id='is_sender_compass',
            test=lambda: rail.result("get_sender_erp_details")[
                'sender'].lower() == "compass",
            yes_task="empty_is_sender_compass",
            no_task="is_sender_cwf"
        )

        empty_is_sender_compass = rail.EmptyOperator(
            task_id="empty_is_sender_compass")

        is_compass_starts_with_iwo = rail.IfOperator(
            task_id='is_compass_starts_with_iwo',
            test=lambda: rail.result("get_sender_erp_details")[
                'time_export_name'].lower().startswith("iwo"),
            yes_task="is_compass_iwo_ends_with_c1",
            no_task="is_pn1_or_nt1_erp"
        )

        is_compass_iwo_ends_with_c1 = rail.IfOperator(
            task_id='is_compass_iwo_ends_with_c1',
            test=lambda: rail.result("get_sender_erp_details")[
                'erp'].lower() == "c1",
            yes_task="update_compass_iwo_ends_with_c1_oef_value",
            no_task="is_compass_iwo_pn1_or_nt1_erp"
        )

        update_compass_iwo_ends_with_c1_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_iwo_ends_with_c1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'C1_Payload_Processed')
        )

        is_compass_iwo_pn1_or_nt1_erp = rail.IfOperator(
            task_id='is_compass_iwo_pn1_or_nt1_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pn1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt1",
            yes_task="update_compass_iwo_pn1_nt1_oef_value",
            no_task="is_compass_iwo_pj1_or_nt3_erp"
        )

        update_compass_iwo_pn1_nt1_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_iwo_pn1_nt1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PN1/NT1_Payload_Processed')
        )

        is_compass_iwo_pj1_or_nt3_erp = rail.IfOperator(
            task_id='is_compass_iwo_pj1_or_nt3_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pj1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt3",
            yes_task="update_compass_iwo_pj1_nt3_oef_value",
            no_task="is_compass_iwo_p01_or_nt2_erp"
        )

        update_compass_iwo_pj1_nt3_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_iwo_pj1_nt3_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PJ1/NT3_Payload_Processed')
        )

        is_compass_iwo_p01_or_nt2_erp = rail.IfOperator(
            task_id='is_compass_iwo_p01_or_nt2_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "p01" or rail.result("get_sender_erp_details")['erp'].lower() == "nt2",
            yes_task="update_compass_iwo_p01_nt2_oef_value",
            no_task="is_sender_cwf"
        )

        update_compass_iwo_p01_nt2_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_iwo_p01_nt2_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_P01/NT2_Payload_Processed')
        )

        is_pn1_or_nt1_erp = rail.IfOperator(
            task_id='is_pn1_or_nt1_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pn1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt1",
            yes_task="update_compass_pn1_nt1_oef_value",
            no_task="is_pj1_or_nt3_erp"
        )

        update_compass_pn1_nt1_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_pn1_nt1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PN1/NT1_Payload_Processed')
        )

        is_pj1_or_nt3_erp = rail.IfOperator(
            task_id='is_pj1_or_nt3_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pj1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt3",
            yes_task="update_compass_pj1_nt3_oef_value",
            no_task="is_p01_or_nt2_erp"
        )

        update_compass_pj1_nt3_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_pj1_nt3_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PJ1/NT3_Payload_Processed')
        )

        is_p01_or_nt2_erp = rail.IfOperator(
            task_id='is_p01_or_nt2_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "p01" or rail.result("get_sender_erp_details")['erp'].lower() == "nt2",
            yes_task="update_compass_p01_nt2_oef_value",
            no_task="is_sender_cwf"
        )

        update_compass_p01_nt2_oef_value = rail.RepliconServiceOperator(
            task_id='update_compass_p01_nt2_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_P01/NT2_Payload_Processed')
        )

        is_sender_cwf = rail.IfOperator(
            task_id='is_sender_cwf',
            test=lambda: rail.result("get_sender_erp_details")[
                'sender'].lower() == "cwf",
            yes_task="empty_is_sender_cwf"
        )

        empty_is_sender_cwf = rail.EmptyOperator(
            task_id="empty_is_sender_cwf")

        is_cwf_c1_erp = rail.IfOperator(
            task_id='is_cwf_c1_erp',
            test=lambda: rail.result("get_sender_erp_details")[
                'erp'].lower() == "c1",
            yes_task="update_cwf_c1_oef_value",
            no_task="is_cwf_pn1_or_nt1_erp"
        )

        update_cwf_c1_oef_value = rail.RepliconServiceOperator(
            task_id='update_cwf_c1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'C1_Payload_Processed')
        )

        is_cwf_pn1_or_nt1_erp = rail.IfOperator(
            task_id='is_cwf_pn1_or_nt1_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pn1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt1",
            yes_task="update_cwf_pn1_nt1_oef_value",
            no_task="is_cwf_pj1_or_nt3_erp"
        )

        update_cwf_pn1_nt1_oef_value = rail.RepliconServiceOperator(
            task_id='update_cwf_pn1_nt1_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PN1/NT1_Payload_Processed')
        )

        is_cwf_pj1_or_nt3_erp = rail.IfOperator(
            task_id='is_cwf_pj1_or_nt3_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "pj1" or rail.result("get_sender_erp_details")['erp'].lower() == "nt3",
            yes_task="update_cwf_pj1_nt3_oef_value",
            no_task="is_cwf_p01_or_nt2_erp"
        )

        update_cwf_pj1_nt3_oef_value = rail.RepliconServiceOperator(
            task_id='update_cwf_pj1_nt3_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_PJ1/NT3_Payload_Processed')
        )

        is_cwf_p01_or_nt2_erp = rail.IfOperator(
            task_id='is_cwf_p01_or_nt2_erp',
            test=lambda: rail.result("get_sender_erp_details")['erp'].lower(
            ) == "p01" or rail.result("get_sender_erp_details")['erp'].lower() == "nt2",
            yes_task="update_cwf_p01_nt2_oef_value",
        )

        update_cwf_p01_nt2_oef_value = rail.RepliconServiceOperator(
            task_id='update_cwf_p01_nt2_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=lambda: request_payload.get_update_oef_value_payload(
                'Compass_P01/NT2_Payload_Processed')
        )

    get_all_past_time_export >> is_time_export_present_and_complete
    is_time_export_present_and_complete >> rail.Label(
        "Yes") >> get_all_time_export_oef_bindings
    is_time_export_present_and_complete >> rail.Label(
        "No") >> fail_acknowledgement
    get_all_time_export_oef_bindings >> get_sender_erp_details >> is_sender_c1
    is_sender_c1 >> rail.Label(
        "Yes") >> empty_is_sender_c1 >> is_c1_starts_with_iwo
    is_c1_starts_with_iwo >> rail.Label("Yes") >> is_iwo_ends_with_c1
    is_iwo_ends_with_c1 >> rail.Label(
        "Yes") >> update_iwo_ends_with_c1_oef_value >> is_c1_pn1_or_nt1_erp
    is_iwo_ends_with_c1 >> rail.Label("No") >> is_c1_pn1_or_nt1_erp
    is_c1_pn1_or_nt1_erp >> rail.Label(
        "Yes") >> update_c1_pn1_nt1_oef_value >> is_c1_pj1_or_nt3_erp
    is_c1_pn1_or_nt1_erp >> rail.Label("No") >> is_c1_pj1_or_nt3_erp
    is_c1_pj1_or_nt3_erp >> rail.Label(
        "Yes") >> update_c1_pj1_nt3_oef_value >> is_c1_p01_or_nt2_erp
    is_c1_pj1_or_nt3_erp >> rail.Label("No") >> is_c1_p01_or_nt2_erp
    is_c1_p01_or_nt2_erp >> rail.Label(
        "Yes") >> update_c1_p01_nt2_oef_value >> is_sender_ftp
    is_c1_p01_or_nt2_erp >> rail.Label("No") >> is_sender_ftp
    is_c1_starts_with_iwo >> rail.Label(
        "No") >> update_c1_oef_value >> is_sender_ftp
    is_sender_c1 >> rail.Label("No") >> is_sender_ftp
    is_sender_ftp >> rail.Label("Yes") >> update_ftp_oef_value
    update_ftp_oef_value >> is_sender_compass
    is_sender_ftp >> rail.Label("No") >> is_sender_compass
    is_sender_compass >> rail.Label("Yes") >> empty_is_sender_compass
    empty_is_sender_compass >> is_compass_starts_with_iwo
    is_compass_starts_with_iwo >> rail.Label(
        "Yes") >> is_compass_iwo_ends_with_c1
    is_compass_iwo_ends_with_c1 >> rail.Label(
        "Yes") >> update_compass_iwo_ends_with_c1_oef_value >> is_compass_iwo_pn1_or_nt1_erp
    is_compass_iwo_ends_with_c1 >> rail.Label(
        "No") >> is_compass_iwo_pn1_or_nt1_erp
    is_compass_iwo_pn1_or_nt1_erp >> rail.Label(
        "Yes") >> update_compass_iwo_pn1_nt1_oef_value >> is_compass_iwo_pj1_or_nt3_erp
    is_compass_iwo_pn1_or_nt1_erp >> rail.Label(
        "No") >> is_compass_iwo_pj1_or_nt3_erp
    is_compass_iwo_pj1_or_nt3_erp >> rail.Label(
        "Yes") >> update_compass_iwo_pj1_nt3_oef_value >> is_compass_iwo_p01_or_nt2_erp
    is_compass_iwo_pj1_or_nt3_erp >> rail.Label(
        "No") >> is_compass_iwo_p01_or_nt2_erp
    is_compass_iwo_p01_or_nt2_erp >> rail.Label(
        "Yes") >> update_compass_iwo_p01_nt2_oef_value >> is_sender_cwf
    is_compass_iwo_p01_or_nt2_erp >> rail.Label("No") >> is_sender_cwf
    is_compass_starts_with_iwo >> rail.Label("No") >> is_pn1_or_nt1_erp
    is_pn1_or_nt1_erp >> rail.Label("Yes") >> update_compass_pn1_nt1_oef_value
    update_compass_pn1_nt1_oef_value >> is_pj1_or_nt3_erp
    is_pn1_or_nt1_erp >> rail.Label("No") >> is_pj1_or_nt3_erp
    is_pj1_or_nt3_erp >> rail.Label("Yes") >> update_compass_pj1_nt3_oef_value
    update_compass_pj1_nt3_oef_value >> is_p01_or_nt2_erp
    is_pj1_or_nt3_erp >> rail.Label("No") >> is_p01_or_nt2_erp
    is_p01_or_nt2_erp >> rail.Label("Yes") >> update_compass_p01_nt2_oef_value
    update_compass_p01_nt2_oef_value >> is_sender_cwf
    is_p01_or_nt2_erp >> rail.Label("No") >> is_sender_cwf
    is_sender_compass >> rail.Label("No") >> is_sender_cwf
    is_sender_cwf >> rail.Label("Yes") >> empty_is_sender_cwf >> is_cwf_c1_erp
    is_cwf_c1_erp >> rail.Label(
        "Yes") >> update_cwf_c1_oef_value >> is_cwf_pn1_or_nt1_erp
    is_cwf_c1_erp >> rail.Label("No") >> is_cwf_pn1_or_nt1_erp
    is_cwf_pn1_or_nt1_erp >> rail.Label(
        "Yes") >> update_cwf_pn1_nt1_oef_value >> is_cwf_pj1_or_nt3_erp
    is_cwf_pn1_or_nt1_erp >> rail.Label("No") >> is_cwf_pj1_or_nt3_erp
    is_cwf_pj1_or_nt3_erp >> rail.Label(
        "Yes") >> update_cwf_pj1_nt3_oef_value >> is_cwf_p01_or_nt2_erp
    is_cwf_pj1_or_nt3_erp >> rail.Label("No") >> is_cwf_p01_or_nt2_erp
    is_cwf_p01_or_nt2_erp >> rail.Label("Yes") >> update_cwf_p01_nt2_oef_value

    return dag


rail.for_each_instance(create_main_airflow_dag)
