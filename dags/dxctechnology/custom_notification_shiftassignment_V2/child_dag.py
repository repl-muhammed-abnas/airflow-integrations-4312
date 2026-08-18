from datetime import timedelta
import rail
from dxctechnology.custom_notification_shiftassignment_V2.utils import request_payload, response_payload


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_custom_notification_shiftassignment_child_V2_{config.instance}',
        description=f'DXC Custom notification for shiftassignment - Child V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=None,
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        query_userdata = rail.QueryCollectionOperator(
            task_id='query_userdata',
            query="""SELECT * FROM shiftassignment_basereportdata WHERE scheduletype= 'Shift Schedule' AND supervisoruri=:supervisoruri""",
            query_params={
                "supervisoruri": "{{ dag_run.conf['supervisoruri'] }}"
            }
        )

        get_user_shift_details = rail.RepliconServiceCallForEachItemOperator(
            task_id="get_user_shift_details",
            endpoint="/services/ShiftAssignmentService1.svc/GetShiftAssignmentTotalsByDate2",
            items=lambda: rail.result('query_userdata'),
            log_response=True,
            execution_timeout=timedelta(hours=config.execution_timeout_hours),
            data=request_payload.get_shift_details,
            data_handler=response_payload.convert_data_to_target_format
        )

        list_of_usernames = rail.PythonOperator(
            task_id="list_of_usernames",
            python_callable=request_payload.parse_list_of_users,
            op_args=['get_user_shift_details']
        )

        get_dates_for_email = rail.PythonOperator(
            task_id='get_dates_for_email',
            python_callable=request_payload.get_date_range,
            op_args=['get_user_shift_details']
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("list_of_usernames") | length > 0 }}',
            yes_task='render_logs_csv',
            no_task='finish',
        )

        render_logs_csv = rail.WriteCSVFileOperator(
            task_id='render_logs_csv',
            source="{{ result('list_of_usernames') | to_json }}",
            header=["User Names"],
            row=["{{ item.user }}"]
        )

        generate_download_link = rail.GeneratePresignedDownloadUrlOperator(
            task_id='generate_download_link',
            artifact_name="{{ result('render_logs_csv')}}",
            output_file_name='{{ dag_run_ecid() | replace(":", "-") }}.csv',
            expires_in_seconds=7*24*60*60,
        )

        get_email_body = rail.RenderTemplateOperator(
            task_id='get_email_body',
            template_file='templates/send_email.html',
            target='result',
            dataset="{{ result('list_of_usernames') | to_json }}",
        )

        get_final_payload = rail.PythonOperator(
            task_id='get_final_payload',
            python_callable=request_payload.get_final_payload_sendemail,
            op_args=[
                "{{ dag_run.conf['supervisoruri'] }}",
                'get_dates_for_email',
                "{{ result('get_email_body') }}"]
        )

        send_email_supervisor = rail.RepliconServiceOperator(
            task_id='send_email_supervisor',
            endpoint="/services/NotificationService1.svc/SendEmail2",
            data='{{ result("get_final_payload") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        query_userdata >> get_user_shift_details >> list_of_usernames >> get_dates_for_email >> has_data >> rail.Label(
            "Yes ") >> render_logs_csv >> generate_download_link >> get_email_body >> get_final_payload >> send_email_supervisor >> finish

        has_data >> rail.Label("No") >> finish

    return dag


rail.for_each_instance(create_dag)
