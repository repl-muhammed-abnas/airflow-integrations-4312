from datetime import timedelta
import rail
from dxctechnology.time_export_v1.c1_outbound.utils import request_payload, response_filters, custom_methods
from airflow.models import Variable

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.c1_acknowledgement_child_dagid,
        description=f'DXC - C1 Regular Time Export Acknowledgement Notification Not Received Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task="batch_task",
            no_task="get_past_14days_time_exports_for_C1"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id = "batch_task",
            start_task="get_past_14days_time_exports_for_C1",
            end_task="batch_end",
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        get_past_14days_time_exports_for_C1 = rail.RepliconServiceOperator(
            task_id='get_past_14days_time_exports_for_C1',
            endpoint='/services/TimeDataExportListService1.svc/GetData',
            data=request_payload.get_past_14days_time_exports_for_C1_payload,
            data_handler=response_filters.past_14days_time_exports_for_C1
        )

        create_past_time_exports_collection = rail.CreateCollectionOperator(
            task_id='create_past_time_exports_collection',
            source='{{ result("get_past_14days_time_exports_for_C1") | to_json }}',
            columns=["twbname", "status", "creationdate", "twburi"],
            name='past_time_exports_data'
        )

        query_c1_exports = rail.QueryCollectionOperator(
            task_id='query_c1_exports',
            query="""SELECT * FROM past_time_exports_data
                WHERE twbname LIKE 'IWO-C1%'
                OR twbname LIKE 'IWO-GS%'
                OR twbname LIKE 'IWOP-GS%'
                OR twbname LIKE 'IWO-CP%'
                OR twbname LIKE 'REG-C1%'
                OR twbname LIKE 'REGP-C1%'
                OR twbname LIKE 'IWOP-C1%'
                OR twbname LIKE 'IWOP-CP%'
            """
        )

        log_twb_creation_time = rail.PythonOperator(
            task_id='log_twb_creation_time',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.load_all_records(
                rail.result("query_c1_exports")), "twbname", dag_run.conf["twbname"], "creationdate")
        )

        create_log_twb_without_acknowledge = rail.SetVariableOperator(
            task_id='create_log_twb_without_acknowledge',
            name='twb_without_acknowledge',
            value=[]
        )

        for_each_time_export = rail.ForEachOperator(
            task_id='for_each_time_export',
            items='{{ result("query_c1_exports") }}',
            start_task='check_twb_and_createdate',
            end_task='for_each_end'
        )

        check_twb_and_createdate = rail.IfOperator(
            task_id='check_twb_and_createdate',
            test=custom_methods.check_ack_date_and_name,
            yes_task='is_status_not_cancelled',
            no_task='for_each_end'
        )

        is_status_not_cancelled = rail.IfOperator(
            task_id='is_status_not_cancelled',
            test='{{ result("for_each_time_export").status != "Canceled" }}',
            yes_task='get_specific_time_export_details',
            no_task='for_each_end'
        )

        get_specific_time_export_details = rail.RepliconServiceOperator(
            task_id='get_specific_time_export_details',
            endpoint="/services/TimeDataExportService1.svc/GetTimeDataExportDetails",
            data={
                "target": {
                    "uri": "{{ result('for_each_time_export').twburi }}",
                    "name": null
                }
            },
            data_handler=lambda response: response
        )

        is_unckn_export_extension_field_value_present = rail.IfOperator(
            task_id="is_unckn_export_extension_field_value_present",
            test=response_filters.get_unacknowledged_time_export_details,
            yes_task='add_twb_without_acknowledge',
            no_task='for_each_end'
        )

        add_twb_without_acknowledge = rail.SetVariableOperator(
            task_id='add_twb_without_acknowledge',
            name='twb_without_acknowledge',
            value={
                "identifier": '{{ result("for_each_time_export").twbname }}|C1',
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
            template_file='templates/emails/c1_ackn_template.html',
            target='result'
        )

        send_unackn_email = rail.EmailOperator(
            task_id='send_unackn_email',
            to=config.c1_acknowledgement_email,
            bcc= config.alert_email,
            subject='{{ get_company_key() + " | Priority 2 : Payload acknowledgement not received for C1" }}',
            html_content='{{ result("get_unackn_email_content")}}',
        )

        fail_for_no_ackn = rail.FailOperator(
            task_id='fail_for_no_ackn',
            message='Acknowledgement pending for {{ result("get_all_twb_without_acknowledge") | length }} previous export',
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> get_past_14days_time_exports_for_C1

        get_past_14days_time_exports_for_C1 >> create_past_time_exports_collection \
            >> query_c1_exports >> log_twb_creation_time >> create_log_twb_without_acknowledge >> for_each_time_export >> check_twb_and_createdate

        check_twb_and_createdate >> rail.Label("Yes") >> is_status_not_cancelled
        check_twb_and_createdate >> rail.Label("No") >> for_each_end

        is_status_not_cancelled >> rail.Label("Yes") >> get_specific_time_export_details
        is_status_not_cancelled >> rail.Label("No") >> for_each_end

        get_specific_time_export_details >> is_unckn_export_extension_field_value_present

        is_unckn_export_extension_field_value_present >> rail.Label("Yes") >> add_twb_without_acknowledge >> for_each_end \
            >> get_twb_without_acknowledge_data_var >> get_all_twb_without_acknowledge >> check_twb_without_acknowledge_exists
        is_unckn_export_extension_field_value_present >> rail.Label("No") >> for_each_end

        for_each_time_export >> for_each_end

        check_twb_without_acknowledge_exists >> rail.Label("Yes") >> get_unackn_email_content >> send_unackn_email >> fail_for_no_ackn >> batch_end
        check_twb_without_acknowledge_exists >> rail.Label("No") >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
