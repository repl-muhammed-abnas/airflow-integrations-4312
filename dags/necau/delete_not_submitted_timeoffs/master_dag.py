from pendulum import datetime
import rail
from necau.delete_not_submitted_timeoffs.utils import custom_method, request_payload, response_filter

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_delete_not_submitted_timeoffs_master_{config.instance}',
        description=f'NECAU_Delete Not Submitted Time Offs Master {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        start_date= datetime(2022, 4, 1, tz=config.brisbane_timezone),
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs
    ) as dag:

        create_timeoff_log= rail.SetVariableOperator(
            task_id='create_timeoff_log',
            append=False,
            name='logs',
            value=[]
        )

        get_all_not_submitted_time_offs= rail.RepliconServiceOperator(
            task_id='get_all_not_submitted_time_offs',
            endpoint="/services/TimeOffListService1.svc/GetData",
            data= request_payload.get_timeoff_payload,
            data_handler= response_filter.timeoff_off_details
        )

        has_timeoff_data_for_delete = rail.IfOperator(
            task_id = 'has_timeoff_data_for_delete',
            test= '{{ result("get_all_not_submitted_time_offs") | is_truthy }}',
            yes_task= 'for_each_timeoff',
            no_task= 'get_time_off_log_data'
        )

        for_each_timeoff= rail.ForEachOperator(
            task_id='for_each_timeoff',
            items="{{ result('get_all_not_submitted_time_offs') | to_json }}",
            start_task = 'delete_time_off',
            end_task = 'for_each_timeoff_end'
        )

        delete_time_off= rail.RepliconServiceOperator(
            task_id='delete_time_off',
            endpoint="/services/TimeOffService1.svc/DeleteTimeOff",
            data={
                "timeOffUri": "{{ result('for_each_timeoff').timeoffuri }}"
            }
        )

        add_timeoff_data_to_logs= rail.SetVariableOperator(
            task_id='add_timeoff_data_to_logs',
            append=True,
            name='{{ result("create_timeoff_log").name }}',
            value= custom_method.add_timeoff_log_data
        )

        for_each_timeoff_end=rail.EmptyOperator(
            task_id='for_each_timeoff_end',
        )

        get_time_off_log_data = rail.GetVariableOperator(
            task_id = 'get_time_off_log_data',
            name= '{{ result("create_timeoff_log").name }}'
        )

        has_timeoff_log_data= rail.IfOperator(
            task_id='has_timeoff_log_data',
            test="{{ result('get_time_off_log_data').value | is_truthy }}",
            yes_task="create_csv_lines",
            no_task="send_mail_for_no_timeoffs",
        )

        create_csv_lines= rail.WriteCSVFileOperator(
            task_id='create_csv_lines',
            source="{{ result('get_time_off_log_data').value | to_json }}",
            header=['User URI',
                'Timeoff booking uri',
                'Start date',
                'End date',
                'Timeoff type name',
                'Execution status'],
            row=[
                "{{ item.useruri }}",
                "{{ item.timeoffbookinguri }}",
                "{{ item.startdate }}",
                "{{ item.enddate }}",
                "{{ item.timeofftypename }}",
                "{{ item.executionstatus }}"
            ]
        )

        send_mail_for_timeoff_deletion= rail.EmailOperator(
            task_id='send_mail_for_timeoff_deletion',
            to= config.tenant_email,
            subject='NECAU | Delete Not Submitted Timeoff Completed - {{ current_time_in_specified_tz() }}',
            html_content= 'templates/email/timeoff_deleted.html',
            files=[
                    ("{{ result('create_csv_lines') }}")
                ]
        )

        send_mail_for_no_timeoffs= rail.EmailOperator(
            task_id='send_mail_for_no_timeoffs',
            to= config.tenant_email,
            subject='NECAU | Delete Not Submitted Timeoff Completed - {{ current_time_in_specified_tz() }}',
            html_content= 'templates/email/no_timeoff_deleted.html',
            params=None,
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_timeoff_log >> get_all_not_submitted_time_offs >> has_timeoff_data_for_delete

        has_timeoff_data_for_delete >> rail.Label(
            "Yes") >> for_each_timeoff >> delete_time_off >> add_timeoff_data_to_logs >> for_each_timeoff_end

        has_timeoff_data_for_delete >> rail.Label(
            "No") >> get_time_off_log_data

        for_each_timeoff >> for_each_timeoff_end >> get_time_off_log_data >> has_timeoff_log_data

        has_timeoff_log_data >> rail.Label(
            'Yes')  >> create_csv_lines >> send_mail_for_timeoff_deletion >> log_to_sumo

        has_timeoff_log_data >> rail.Label(
            'No') >> send_mail_for_no_timeoffs >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
