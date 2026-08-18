from datetime import timedelta
import rail
from airflow.models import Variable
from dxctechnology.time_export_v1.psa_outbound.utils import response_filters, custom_methods

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.psa_outbound_acknowledgement_child_dagid,
        description=f'DXC - PSA Outbound Time Export Acknowledgement Notification Not Received Child {config.instance}',
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
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(dag_run.conf["twblist"],
                "timeexport", dag_run.conf["twbname"], "creationdate")
        )

        if_erp_equals_psa = rail.IfOperator(
            task_id='if_erp_equals_psa',
            test=lambda dag_run: dag_run.conf["erp"].lower() == "psa",
            yes_task='create_log_twb_without_acknowledge',
            no_task='batch_end'
        )

        create_log_twb_without_acknowledge = rail.SetVariableOperator(
            task_id='create_log_twb_without_acknowledge',
            name='twb_without_acknowledge',
            value=[]
        )

        for_each_time_export = rail.ForEachOperator(
            task_id='for_each_time_export',
            items=lambda dag_run: dag_run.conf["twblist"],
            start_task='check_twb_and_createdate',
            end_task='for_each_end'
        )

        check_twb_and_createdate = rail.IfOperator(
            task_id='check_twb_and_createdate',
            test=lambda dag_run: custom_methods.check_ack_date_and_name(dag_run, config.utc_timezone),
            yes_task='get_specific_time_export_details',
            no_task='for_each_end'
        )

        get_specific_time_export_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ result('for_each_time_export').uri }}",
                    "name": null
                }
            }
        )

        is_unckn_export_extension_field_value_present = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present",
            test=lambda dag_run: response_filters.get_specific_time_export_details(
                rail.result("get_specific_time_export_details")['extensionFieldValues'],
                    dag_run.conf["oefname"]),
            yes_task='add_twb_without_acknowledge',
            no_task='for_each_end'
        )

        add_twb_without_acknowledge = rail.SetVariableOperator(
            task_id='add_twb_without_acknowledge',
            name='twb_without_acknowledge',
            value={
                "identifier": '{{ result("for_each_time_export").timeexport }}|{{ dag_run.conf.sender }}',
                "creationdatetime": '{{ result("for_each_time_export").creationdate }}'
            },
            append=True
        )

        for_each_end = rail.EmptyOperator(
            task_id='for_each_end'
        )

        get_twb_without_acknowledge_data_var = rail.GetVariableOperator(
            task_id='get_twb_without_acknowledge_data_var',
            name='twb_without_acknowledge'
        )

        get_all_twb_without_acknowledge = rail.PythonOperator(
            task_id='get_all_twb_without_acknowledge',
            python_callable=custom_methods.get_all_twb_without_acknowledge
        )

        check_twb_without_acknowledge_exists = rail.IfOperator(
            task_id='check_twb_without_acknowledge_exists',
            test='{{ result("get_all_twb_without_acknowledge") | is_truthy }}',
            yes_task='get_unackn_email_content',
            no_task='batch_end'
        )

        get_unackn_email_content = rail.RenderTemplateOperator(
            task_id='get_unackn_email_content',
            dataset='{{ result("get_all_twb_without_acknowledge") | to_json }}',
            template_file='templates/emails/psa_ackn_template.html',
            target='result'
        )

        send_unackn_email = rail.EmailOperator(
            task_id='send_unackn_email',
            to=config.psa_acknowledgement_email,
            #bcc= config.alert_email,
            subject='{{ get_company_key() }} | Priority 2 : Payload acknowledgement not received for {{ dag_run.conf.sender }}',
            html_content='{{ result("get_unackn_email_content")}}',
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> batch_end
        can_run_batch_task >> rail.Label('No') >> log_twb_creation_time

        log_twb_creation_time >> if_erp_equals_psa
        if_erp_equals_psa >> rail.Label("Yes") >> create_log_twb_without_acknowledge >> for_each_time_export >> check_twb_and_createdate
        if_erp_equals_psa >> rail.Label("No") >> batch_end

        check_twb_and_createdate >> rail.Label("Yes") >> get_specific_time_export_details
        check_twb_and_createdate >> rail.Label("No") >> for_each_end

        get_specific_time_export_details >> is_unckn_export_extension_field_value_present

        is_unckn_export_extension_field_value_present >> rail.Label("Yes") >> add_twb_without_acknowledge >> for_each_end \
            >> get_twb_without_acknowledge_data_var >> get_all_twb_without_acknowledge >> check_twb_without_acknowledge_exists
        is_unckn_export_extension_field_value_present >> rail.Label("No") >> for_each_end

        for_each_time_export >> for_each_end

        check_twb_without_acknowledge_exists >> rail.Label("Yes") >> get_unackn_email_content >> send_unackn_email >> batch_end
        check_twb_without_acknowledge_exists >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
