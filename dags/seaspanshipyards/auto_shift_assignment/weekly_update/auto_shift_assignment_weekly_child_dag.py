from datetime import datetime, timedelta
import rail

from seaspanshipyards.auto_shift_assignment.weekly_update.utils import request_payload, python_callable_method


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'seaspanshipyards_weekly_shift_assignment_per_existing_user_V3.0_{config.instance}',
        description=f'Seaspanshipyards_Weekly shift assignment_per existing user V3.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        start_date = "{{ dag_run.conf.Startdate }}"
        end_date = "{{ dag_run.conf.Enddate }}"
        shift_name = "{{ dag_run.conf.Shiftname }}"
        user_uri = "{{ dag_run.conf.Useruri }}"

        def get_dag_run_conf():
            return rail.get_current_context()['dag_run'].conf

        def get_csv_rows(item):
            dag_conf = get_dag_run_conf()
            start_date_str = dag_conf['Startdate']
            start_date_time = datetime.strptime(start_date_str, "%Y-%m-%d")
            day_seq = (start_date_time + timedelta(days=item['seq'])) -  timedelta(days=1)
            row_data = [
                item['seq'],
                day_seq,
                day_seq.weekday(),
                day_seq.strftime("%d"),
                day_seq.strftime("%m"),
                day_seq.strftime("%Y"),
                day_seq.isocalendar()[1]
            ]
            return row_data

        has_shift_name = rail.IfOperator(
            task_id="has_shift_name",
            test='{{ dag_run.conf.Shiftname | is_truthy }}',
            yes_task='create_date_range_seq',
            no_task='finish'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        create_date_range_seq = rail.PythonOperator(
            task_id = 'create_date_range_seq',
            python_callable=python_callable_method.create_date_range_seq,
            op_args=[end_date, start_date]
        )

        get_shift_schedule_summary = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary',
            endpoint='/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary',
            data=request_payload.get_shift_schedule_summary_data
        )

        get_assigned_shift_dates = rail.PythonOperator(
            task_id = 'get_assigned_shift_dates',
            python_callable=python_callable_method.get_assigned_shift_dates
        )

        has_shift_assigned_data = rail.IfOperator(
            task_id = 'has_shift_assigned_data',
            test="{{ result('get_assigned_shift_dates') | length > 0 }}",
            yes_task='bulk_delete_shifts',
            no_task='get_upcoming_weekdays'
        )

        bulk_delete_shifts = rail.RepliconServiceOperator(
            task_id="bulk_delete_shifts",
            endpoint="/services/ShiftAssignmentService1.svc/BulkDelete",
            data=request_payload.get_assignment_uris
        )

        get_upcoming_weekdays = rail.WriteCSVFileOperator(
            task_id = 'get_upcoming_weekdays',
            source="{{result('create_date_range_seq')  | to_json }}",
            header=[
                    'seq',
                    'date',
                    'day',
                    'dateday',
                    'datemonth',
                    'dateyear',
                    'week'
                    ],
            row=get_csv_rows
        )

        create_collection_of_weekday_data = rail.CreateCollectionOperator(
            task_id='create_collection_of_weekday_data',
            name='datestoconsider',
            source=lambda: rail.result('get_upcoming_weekdays'),
        )

        query_shift_assignment = rail.QueryCollectionOperator(
            task_id = 'query_shift_assignment',
            query= 'SELECT * FROM datestoconsider WHERE day != 5 AND day != 6'
        )

        has_shift_data = rail.IfOperator(
            task_id = 'has_shift_data',
            test= "{{ result('query_shift_assignment') | length > 0 }}",
            yes_task='add_shift_assignment_to_list',
            no_task='has_shift_list'
        )

        add_shift_assignment_to_list = rail.PythonOperator(
            task_id='add_shift_assignment_to_list',
            python_callable=python_callable_method.get_shift_assignment_list,
            op_args=[shift_name, user_uri]
        )

        has_shift_list = rail.IfOperator(
            task_id='has_shift_list',
            test = "{{result('add_shift_assignment_to_list') | length > 0}}",
            yes_task='bulk_put_shift_assignment',
            no_task='finish'
        )

        bulk_put_shift_assignment = rail.RepliconServiceOperator(
            task_id="bulk_put_shift_assignment",
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data = request_payload.get_put_shift_payload
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info={
                'User | Login Name ': "'{{ dag_run.conf.Username }}' | '{{ dag_run.conf.Loginname }}'",
                'Type': '{{dag_run.conf.Type}}',
                'Shiftname': '{{ dag_run.conf.Shiftname }}',
                'Number of working days': "{{result('add_shift_assignment_to_list') | length}}",
                'Start Date - End Date': "'{{ dag_run.conf.Startdate }}' - '{{ dag_run.conf.Enddate }}'",
                'username': '{{ dag_run.conf.Username }}'
            }
        )

        has_shift_name
        has_shift_name >> rail.Label(
            "No") >> finish
        has_shift_name >> rail.Label(
            "Yes") >> create_date_range_seq >> get_shift_schedule_summary >> get_assigned_shift_dates >>  has_shift_assigned_data
        has_shift_assigned_data >> rail.Label(
            "Yes") >> bulk_delete_shifts >> get_upcoming_weekdays
        has_shift_assigned_data >> rail.Label(
            "No") >> get_upcoming_weekdays >> create_collection_of_weekday_data >>  query_shift_assignment >> has_shift_data
        has_shift_data >> rail.Label(
            "Yes") >> add_shift_assignment_to_list >> has_shift_list
        has_shift_data >> rail.Label(
            "No") >> has_shift_list
        has_shift_list >> rail.Label(
            "No") >> finish
        has_shift_list >> rail.Label(
            "Yes")  >> bulk_put_shift_assignment >> log_to_sumo >> finish
    return dag

rail.for_each_instance(create_dag)
