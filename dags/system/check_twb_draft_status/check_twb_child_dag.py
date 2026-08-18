from datetime import datetime
import rail
import airflow
from system.check_twb_draft_status import config
from system.check_twb_draft_status import request_payload
from system.check_twb_draft_status import response_filter

with airflow.DAG(
    dag_id='system_timeworkbench_draft_status_monitor_child',
    description='System Time Workbench Draft Status monitoring alerts Child v0.1',
    max_active_runs=config.max_active_runs_child_dag,
    tags=['system'],
    schedule=None,
    start_date=datetime(2022, 1, 1),
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    default_args={
        'owner': 'system',
    }
) as dag:

    rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

    get_timeexport_details = rail.RepliconServiceOperator(
        task_id="get_timeexport_details",
        replicon_conn_id="{{ dag_run.conf.connection_id }}",
        endpoint="services/TimeDataExportListService1.svc/GetData",
        data=request_payload.get_timeexportdata_payload(),
        response_filter=response_filter.get_draft_timeexports
    )

    is_draft_present = rail.IfOperator(
        task_id='is_draft_present',
        test='{{ result("get_timeexport_details") | length > 0 }}',
        yes_task='get_alert_email_content',
    )

    get_alert_email_content = rail.RenderTemplateOperator(
        task_id='get_alert_email_content',
        target='result',
        template_file='output_template.html',
        dataset=request_payload.output_payload,
    )

    send_alert_email = rail.EmailOperator(
        task_id='send_alert_email',
        to=config.tenant_email,
        subject='{{ dag_run.conf.company_key }} | Alert - New Draft Found For Time Export - {{ current_time() }}',
        html_content='{{ result("get_alert_email_content")}}',
    )

    get_timeexport_details >> is_draft_present >> rail.Label(
        "YES") >> get_alert_email_content >> send_alert_email
