from datetime import timedelta
import rail
from crjg3.payroll_export.utils import python_callable, response_filter

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'crj_custom_payroll_report_v2_master_{config.instance}',
        description=f'CRJ_Custom Payroll Report V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
        webhook_conf=rail.WebhookConf(
            bearer_token_var=config.bearer_token_var),
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint="/services/UserService1.svc/GetUserDetails",
            data=lambda dag_run: {
                "userUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid']
            }
        )

        is_email_present = rail.IfOperator(
            task_id='is_email_present',
            test='''{{ result("get_user_details")["emailAddress"] | is_truthy }}''',
            yes_task="email_ids_for_logs",
            no_task="finish",
        )

        email_ids_for_logs = rail.PythonOperator(
            task_id="email_ids_for_logs",
            python_callable=python_callable.email_ids_for_logs
        )

        is_daterange_does_not_contains_null = rail.IfOperator(
            task_id='is_daterange_does_not_contains_null',
            test= python_callable.check_if_daterange_does_not_contains_null,
            yes_task="get_assigned_permission_sets",
            no_task="send_no_date_range_mail"
        )

        send_no_date_range_mail = rail.EmailOperator(
            task_id='send_no_date_range_mail',
            to="{{result('email_ids_for_logs')}}",
            bcc=config.bcc_tenant_email,
            subject='{{ get_company_key() }} | Custom Payroll Report - No Data - {{ current_time_in_specified_tz(fmt="%m/%d/%YT%H:%M:%S") }}',
            html_content="templates/emails/send_no_date_range_mail.html"
        )

        get_assigned_permission_sets = rail.RepliconServiceOperator(
            task_id='get_assigned_permission_sets',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data=lambda dag_run: {
                "userUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid']
            },
            data_handler=response_filter.get_permission_set
        )

        if_required_permission_available = rail.IfOperator(
            task_id='if_required_permission_available',
            test=lambda: (rail.result('get_assigned_permission_sets') and rail.result(
                'get_assigned_permission_sets') == 'Custom Report Access'),
            yes_task="get_report_details",
            no_task="finish",
        )

        get_report_details = rail.RepliconReportDetailsOperator(
            task_id='get_report_details',
            report_name=config.report_name,
        )

        match_column_configuration = rail.IfOperator(
            task_id='match_column_configuration',
            test=lambda: python_callable.match_column_configuration,
            yes_task="impersonate_and_create_interactive_session",
            no_task="finish",
        )

        impersonate_and_create_interactive_session = rail.RepliconServiceOperator(
            task_id='impersonate_and_create_interactive_session',
            endpoint="/services/UserImpersonationService1.svc/AdministrativeImpersonationAndCreateInteractiveSession",
            data=lambda dag_run: {
                "impersonatedUserUri": f"urn:replicon-tenant:{rail.get_tenant_slug()}:user:" + dag_run.conf['webhook']['data']['requestorid']
            },
            data_handler=response_filter.get_auth_token
        )

        crj_custom_payroll_report_dag_run = rail.TriggerDagRunOperator(
            task_id='crj_custom_payroll_report_dag_run',
            trigger_dag_id=f'crj_custom_payroll_report_child_{config.instance}',
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            conf=lambda dag_run: {
                "date_field": python_callable.get_date_range(dag_run),
                "userid": dag_run.conf['webhook']['data']['requestorid'],
                "username": rail.result('get_user_details')['firstName'],
                "email": rail.result('email_ids_for_logs'),
                "reportUri": rail.result('get_report_details')['uri'],
                "enabledFilters": rail.result('get_report_details')['filterConfiguration']['enabledFilters'],
                "dagran": dag_run.conf
            }
        )

        wait_for_crj_custom_payroll_report_dag_run = rail.WaitForDagRunsSensor(
            task_id='wait_for_crj_custom_payroll_report_dag_run',
            dag_runs='{{ result("crj_custom_payroll_report_dag_run") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_user_details >> is_email_present >> rail.Label(
            "Yes") >> email_ids_for_logs >> is_daterange_does_not_contains_null
        is_email_present >> rail.Label("No") >> finish
        is_daterange_does_not_contains_null >> rail.Label(
            "Yes") >> get_assigned_permission_sets
        is_daterange_does_not_contains_null >> rail.Label(
            "No") >> send_no_date_range_mail >> finish
        get_assigned_permission_sets >> if_required_permission_available >> rail.Label(
            "Yes") >> get_report_details
        if_required_permission_available >> rail.Label("No") >> finish
        get_report_details >> match_column_configuration >> rail.Label(
            "Yes") >> impersonate_and_create_interactive_session >> crj_custom_payroll_report_dag_run >> \
            wait_for_crj_custom_payroll_report_dag_run >> finish
        match_column_configuration >> rail.Label(
            "No") >> finish

        return dag


rail.for_each_instance(create_dag)
