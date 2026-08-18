from datetime import timedelta
import rail
from dxctechnology.time_export_v1.compass_outbound.utils import response_filters, custom_methods
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.compass_acknowledgement_child_dagid,
        description=f'DXC - Compass Regular Time Export Acknowledgement Notification Not Received Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='log_twb_creation_time'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='log_twb_creation_time',
            end_task='batch_end',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        log_twb_creation_time = rail.PythonOperator(
            task_id='log_twb_creation_time',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                dag_run.conf["twblist"], "timeexport", dag_run.conf["twbname"], "creationdate")
        )

        create_log_twb_without_acknowledge = rail.SetVariableOperator(
            task_id='create_log_twb_without_acknowledge',
            name='twb_without_acknowledge',
            value=[]
        )

        is_erp_equals_compass = rail.IfOperator(
            task_id='is_erp_equals_compass',
            test='{{ dag_run.conf.erp == "COMPASS" }}',
            yes_task='for_each_time_export_compass',
            no_task='for_each_time_export_other_erp'
        )

        for_each_time_export_compass = rail.ForEachOperator(
            task_id='for_each_time_export_compass',
            items=lambda dag_run: dag_run.conf["twblist"],
            start_task='check_twb_and_createdate',
            end_task='for_each_end_compass'
        )

        check_twb_and_createdate = rail.IfOperator(
            task_id='check_twb_and_createdate',
            test=lambda dag_run: custom_methods.check_ack_date_and_name(dag_run, config.utc_timezone),
            yes_task='get_specific_time_export_compass_details',
            no_task='for_each_end_compass'
        )

        get_specific_time_export_compass_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_compass_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ result('for_each_time_export_compass').uri }}",
                    "name": null
                }
            }
        )

        is_unckn_export_extension_field_value_present_for_compass = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present_for_compass",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_specific_time_export_compass_details")['extensionFieldValues'],
                    dag_run.conf["oefname"]),
            yes_task='add_twb_compass_without_acknowledge',
            no_task='for_each_end_compass'
        )

        add_twb_compass_without_acknowledge = rail.SetVariableOperator(
            task_id='add_twb_compass_without_acknowledge',
            name='twb_without_acknowledge',
            value={
                "identifier": '{{ result("for_each_time_export_compass").timeexport }}|{{ dag_run.conf.sender }}',
                "creationdatetime": '{{ result("for_each_time_export_compass").creationdate }}'
            },
            append=True
        )

        for_each_end_compass = rail.EmptyOperator(
            task_id='for_each_end_compass'
        )

        get_twb_compass_without_acknowledge_data_var = rail.GetVariableOperator(
            task_id='get_twb_compass_without_acknowledge_data_var',
            name='twb_without_acknowledge'
        )

        get_unackn_email_content_for_compass = rail.RenderTemplateOperator(
            task_id='get_unackn_email_content_for_compass',
            dataset='{{ result("get_twb_compass_without_acknowledge_data_var").value | to_json }}',
            template_file='templates/emails/compass_ackn_template.html',
            target='result'
        )

        send_unackn_email_for_compass = rail.EmailOperator(
            task_id='send_unackn_email_for_compass',
            to=config.compass_acknowledgement_email,
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for" }} {{ dag_run.conf.sender }}',
            html_content='{{ result("get_unackn_email_content_for_compass")}}',
        )

        for_each_time_export_other_erp = rail.ForEachOperator(
            task_id='for_each_time_export_other_erp',
            items=lambda dag_run: dag_run.conf["twblist"],
            start_task='get_specific_time_export_other_erp_details',
            end_task='for_each_end_other_erp'
        )

        get_specific_time_export_other_erp_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_other_erp_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ result('for_each_time_export_other_erp').uri }}",
                    "name": null
                }
            }
        )

        is_unckn_export_extension_field_value_present_other_erp = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present_other_erp",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_specific_time_export_other_erp_details")['extensionFieldValues'],
                    dag_run.conf["oefname"]),
            yes_task='add_twb_other_erp_without_acknowledge',
            no_task='for_each_end_other_erp'
        )

        add_twb_other_erp_without_acknowledge = rail.SetVariableOperator(
            task_id='add_twb_other_erp_without_acknowledge',
            name='twb_without_acknowledge',
            value={
                "identifier": '{{ result("for_each_time_export_other_erp").timeexport }}|{{ dag_run.conf.sender }}',
                "creationdatetime": '{{ result("for_each_time_export_other_erp").creationdate }}'
            },
            append=True
        )

        for_each_end_other_erp = rail.EmptyOperator(
            task_id='for_each_end_other_erp'
        )

        get_twb_other_erp_without_acknowledge_data_var = rail.GetVariableOperator(
            task_id='get_twb_other_erp_without_acknowledge_data_var',
            name='twb_without_acknowledge'
        )

        get_unackn_email_content_for_other_erp = rail.RenderTemplateOperator(
            task_id='get_unackn_email_content_for_other_erp',
            dataset='{{ result("get_twb_other_erp_without_acknowledge_data_var").value | to_json }}',
            template_file='templates/emails/compass_ackn_template.html',
            target='result'
        )

        send_unackn_email_for_other_erp = rail.EmailOperator(
            task_id='send_unackn_email_for_other_erp',
            to=config.cwf_ftp_acknowledgement_email,
            bcc= config.alert_email,
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for" }} {{ dag_run.conf.sender }}',
            html_content='{{ result("get_unackn_email_content_for_other_erp")}}',
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> log_twb_creation_time

        log_twb_creation_time >> create_log_twb_without_acknowledge >> is_erp_equals_compass

        is_erp_equals_compass >> rail.Label("Yes") >> for_each_time_export_compass >> check_twb_and_createdate
        is_erp_equals_compass >> rail.Label("No") >> for_each_time_export_other_erp

        check_twb_and_createdate >> rail.Label("Yes") >> get_specific_time_export_compass_details
        check_twb_and_createdate >> rail.Label("No") >> for_each_end_compass

        get_specific_time_export_compass_details >> is_unckn_export_extension_field_value_present_for_compass

        is_unckn_export_extension_field_value_present_for_compass >> rail.Label("Yes") >> add_twb_compass_without_acknowledge >> for_each_end_compass \
            >> get_twb_compass_without_acknowledge_data_var
        is_unckn_export_extension_field_value_present_for_compass >> rail.Label("No") >> for_each_end_compass
        get_twb_compass_without_acknowledge_data_var >> get_unackn_email_content_for_compass

        get_unackn_email_content_for_compass >> send_unackn_email_for_compass >> batch_end

        for_each_time_export_compass >> for_each_end_compass

        for_each_time_export_other_erp >> get_specific_time_export_other_erp_details >> is_unckn_export_extension_field_value_present_other_erp

        is_unckn_export_extension_field_value_present_other_erp >> rail.Label("Yes") >> add_twb_other_erp_without_acknowledge >> for_each_end_other_erp \
            >> get_twb_other_erp_without_acknowledge_data_var
        is_unckn_export_extension_field_value_present_other_erp >> rail.Label("No") >> for_each_end_other_erp
        get_twb_other_erp_without_acknowledge_data_var >> get_unackn_email_content_for_other_erp >> send_unackn_email_for_other_erp >> batch_end

        for_each_time_export_other_erp >> for_each_end_other_erp



    return dag

rail.for_each_instance(create_child_dag)
