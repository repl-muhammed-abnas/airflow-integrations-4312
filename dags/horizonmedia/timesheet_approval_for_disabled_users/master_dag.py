from datetime import timedelta
from pendulum import datetime
import rail
from horizonmedia.timesheet_approval_for_disabled_users.tasks.send_logs import get_send_logs

null = None


def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'horizonmedia_timesheet_approval_for_disabled_users_master_{config.instance}',
        description=f'Horizonmedia_timesheet_approval_for_disabled_users {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        start_date=datetime(2023, 1, 1, tz=config.time_zone),
        max_active_runs=config.max_active_runs_master,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:


        generate_report = rail.RepliconReportDetailsOperator(
            task_id='generate_report',
            report_name=config.disable_user_timesheets_report_name,
        )

        run_my_report_entry, run_my_report_exit = rail.run_report(
            group_id='run_report_disabled_users',
            report_params={
                "reportParameters": [
                    {
                        "filterValues": [],
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                        "reportUri": "{{result('generate_report').uri}}"
                    }
                ]
            },
            replicon_conn_id=config.replicon_conn_id,
        )

        load_report_data = rail.LoadCSVFileOperator(
            task_id="load_report_data",
            document="{{result('run_report_disabled_users.get_report_result').reportGenerationResults[0].payload }}",
        )

        create_disabled_user_timesheet_list = rail.CreateCollectionOperator(
            task_id='create_disabled_user_timesheet_list',
            source="{{ result('load_report_data') }}",
            name="disabled_user_timesheet",
            # todo update this map from actual csv header for key name
            columns={
                'Timesheet Period': 'timesheetperiod',
                'User Name': 'username',
                'Approval Status': 'approvalstatus',
                'User Status': 'userstatus',
                'TimesheetPeriodUri': 'timesheetperioduri',
                'Employee ID': 'employeeid',
            }
        )

        has_disabled_user_timesheet_data= rail.IfOperator(
            task_id='has_disabled_user_timesheet_data',
            test="{{ result('create_disabled_user_timesheet_list', 'length') > 0 }}",
            yes_task="query_disabled_user_timesheet",
            no_task="send_blank_mail",
        )

        send_blank_mail = rail.EmailOperator(
            task_id='send_blank_mail',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''{{ get_company_key() }} | Force Approve Timesheets For Disabled Users - {{ current_time_in_specified_tz("America/New_York") }}''',
            html_content='templates/emails/blank_email.html',
        )

        query_disabled_user_timesheet = rail.QueryCollectionOperator(
            task_id='query_disabled_user_timesheet',
            query="""SELECT * FROM disabled_user_timesheet""",
        )

        process_each_disabled_user_timesheet = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_disabled_user_timesheet',
            retries=0,
            items='{{ result("query_disabled_user_timesheet") }}',
            batch_size=config.batch_size,
            trigger_dag_id=f'horizonmedia_timesheet_approval_for_disabled_users_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                "timesheet_batch_items":item
            }
        )

        wait_for_process_each_disabled_user_timesheet = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_disabled_user_timesheet',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_each_disabled_user_timesheet") }}'
        )

        send_logs = get_send_logs(config)

        generate_report >> run_my_report_entry
        run_my_report_exit >> load_report_data >> create_disabled_user_timesheet_list >> has_disabled_user_timesheet_data
        has_disabled_user_timesheet_data >> rail.Label(
            'No') >> send_blank_mail
        has_disabled_user_timesheet_data >> rail.Label(
            'Yes') >> query_disabled_user_timesheet >> process_each_disabled_user_timesheet >> wait_for_process_each_disabled_user_timesheet >> send_logs

        return dag


rail.for_each_instance(create_dag)
