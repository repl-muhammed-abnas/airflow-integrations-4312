
from datetime import timedelta
import rail
from daimlertrucks.custom_email_notification.utils import request_payload, data_formatting

def create_dag(config):
    # pylint: disable=too-many-statements,line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_notificiation_master_recipe_{config.instance}',
        description=f'Daimlertrucks_notificiation_Master_Recipe {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.master_dag_schedule_interval,
        max_active_runs=config.max_active_runs,
    ) as dag:
        start = rail.EmptyOperator(
            task_id="start"
        )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        gettimesheetwaitingforapproval_2=rail.RepliconServiceOperator(
            task_id='gettimesheetwaitingforapproval_2',
            endpoint="/services/TimesheetListService1.svc/GetData",
            data=request_payload.get_timesheetwaitingforapproval_payload()
        )

        process_timesheetwaitingforapproval = rail.PythonOperator(
            task_id='process_timesheetwaitingforapproval',
            python_callable = data_formatting.process_timesheetwaitingforapproval,
            op_args=['{{ result("gettimesheetwaitingforapproval_2") | tojson }}']
        )

        check_approver = rail.PythonOperator(
            task_id='check_approver',
            python_callable = data_formatting.check_approver,
            op_args=['{{ result("process_timesheetwaitingforapproval") | tojson }}']
        )

        def format_approver(response, tsuri):
            ts_uri_obj = {tsuri:[]}
            for data in response:
                ts_uri_obj[tsuri].append(data["slug"])
            return ts_uri_obj

        get_expected_approvers = rail.RepliconServiceCallForEachItemOperator(
            task_id='get_expected_approvers',
            items=lambda: [rail.result('check_approver')[x] for x in rail.result('check_approver') if rail.result('check_approver')[x]["get_approver"]],
            endpoint='/services/TimesheetApprovalService1.svc/GetExpectedApprovers',
            data={"timesheetUri": "{{ item.timesheeturi }}"},
            response_filter=lambda response, item: format_approver(response.json()['d'], item["timesheeturi"])
        )

        format_timesheet_data_with_approvers = rail.PythonOperator(
            task_id='format_timesheet_data_with_approvers',
            python_callable = data_formatting.format_timesheet_data_with_approvers,
            op_args=['{{ result("check_approver") | tojson }}', '{{ result("get_expected_approvers") | tojson }}']
        )

        process_email_body_and_subject = rail.TriggerDagRunForEachItemOperator(
            task_id='process_email_body_and_subject',
            retries=0,
            items=lambda: [rail.result('format_timesheet_data_with_approvers')[x] for x in rail.result('format_timesheet_data_with_approvers') if rail.result('format_timesheet_data_with_approvers')[x]["get_approver"]],
            trigger_dag_id=f'daimlertrucks_custom_notification_send_email_notification_child_dag_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda item: {
                'supervisoroftimesheetowner': item['supervisoroftimesheetowner'],
                'supervisoroftimesheetowneruri': item['supervisoroftimesheetowneruri'],
                "timesheet_owner":item['timesheetowner'],
                "timesheet_period":item['timesheetperiod'],
                "approvalduedate":item['approvalduedate'],
                "approvers":item['approvers'],
                "opensince":item['opensince']
            }
        )

        wait_for_process_email_body_and_subject = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_email_body_and_subject',
            dag_runs='{{ result("process_email_body_and_subject") }}',
            execution_timeout=timedelta(days=14),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
        )

        start >> gettimesheetwaitingforapproval_2 >> process_timesheetwaitingforapproval >> check_approver >> get_expected_approvers >> \
        format_timesheet_data_with_approvers >> process_email_body_and_subject >> wait_for_process_email_body_and_subject >> finish >> catch_and_log_errors
    return dag

rail.for_each_instance(create_dag)
