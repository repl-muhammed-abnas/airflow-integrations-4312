from datetime import datetime, timedelta
import rail

from seaspanshipyards.auto_shift_assignment.new_user.utils import request_payload, python_callable_method

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'seaspanshipyards_default_shift_assignment_per_new_user_V3.0_{config.instance}',
        description=f'Seaspanshipyards_Default shift assignment_per new user V3.0 {config.instance}',
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
        shift_type = "{{dag_run.conf.Type}}"

        def get_dag_run_conf():
            return rail.get_current_context()['dag_run'].conf

        def get_csv_rows(item):
            if item:
                dag_conf = get_dag_run_conf()
                start_date_str = dag_conf['Startdate']
                start_date_time = datetime.strptime(start_date_str, "%Y-%m-%d")
                day_seq = (start_date_time + timedelta(days=item['seq'])) -  timedelta(days=1)
                row_data = {
                    'seq': item['seq'],
                    'date': day_seq.strftime("%Y-%m-%d"),
                    'day': day_seq.weekday(),
                    'dateday': day_seq.strftime("%d"),
                    'datemonth': day_seq.strftime("%m"),
                    'dateyear': day_seq.strftime("%Y"),
                    'week': day_seq.isocalendar()[1],
                    'yday': int(day_seq.strftime("%j"))
                }
                return row_data
            return {}

        def check_days_present_till_friday():
            dates_till_friday = rail.result(
        "create_dates_till_friday")
            return bool(dates_till_friday['daystillfriday'] > 0)


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
            python_callable=python_callable_method.get_assigned_shift_dates,
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

        get_upcoming_weekdays = rail.DataAdaptorOperator(
            task_id = 'get_upcoming_weekdays',
            source="{{result('create_date_range_seq')  | to_json }}",
            columns=[
                    'seq',
                    'date',
                    'day',
                    'dateday',
                    'datemonth',
                    'dateyear',
                    'week',
                    'yday'
                    ],
            data=get_csv_rows
        )

        create_collection_of_weekday_data = rail.CreateCollectionOperator(
            task_id='create_collection_of_weekday_data',
            name='datestoconsider',
            source=lambda: rail.result('get_upcoming_weekdays'),
        )

        create_dates_till_friday = rail.PythonOperator(
            task_id='create_dates_till_friday',
            python_callable=python_callable_method.create_dates_till_friday,
            op_args=[start_date]
        )

        has_days_till_friday = rail.IfOperator(
            task_id = 'has_days_till_friday',
            test= check_days_present_till_friday,
            yes_task='query_shift_assignment_till_friday',
            no_task='query_shift_data_for_next_week'
        )

        query_shift_assignment_till_friday = rail.QueryCollectionOperator(
            task_id = 'query_shift_assignment_till_friday',
            # pylint: disable=line-too-long
            query= 'SELECT * FROM datestoconsider WHERE day != 5 AND day != 6 AND yday < :day_of_year AND week = :week_of_year',
            query_params = {
                "day_of_year" : '{{result("create_dates_till_friday")["yday"]}}',
                "week_of_year" : '{{result("create_dates_till_friday")["week"]}}'
            }
        )

        add_shift_assignments_till_friday = rail.PythonOperator(
            task_id='add_shift_assignments_till_friday',
            python_callable=python_callable_method.add_shift_assignments_for_next_week,
            op_args=[shift_type, user_uri, 'query_shift_assignment_till_friday', shift_name]
        )

        query_shift_assignment_for_next_week = rail.QueryCollectionOperator(
            task_id = 'query_shift_assignment_for_next_week',
            query= 'SELECT * FROM datestoconsider WHERE day != 5 AND day != 6 AND week != :week_of_year',
            query_params = {
                "week_of_year" : '{{result("create_dates_till_friday")["week"]}}'
            }
        )

        add_shift_assignment_for_next_week = rail.PythonOperator(
            task_id='add_shift_assignment_for_next_week',
            python_callable=python_callable_method.add_shift_assignments_for_next_week,
            op_args=[shift_type, user_uri, 'query_shift_assignment_for_next_week']
        )

        query_shift_data_for_next_week =  rail.QueryCollectionOperator(
            task_id = 'query_shift_data_for_next_week',
            query= 'SELECT * FROM datestoconsider WHERE day != 5 AND day != 6',
        )

        add_shift_assignment_for_week = rail.PythonOperator(
            task_id='add_shift_assignment_for_week',
            python_callable=python_callable_method.add_shift_assignments_for_next_week,
            op_args=[shift_type, user_uri,  'query_shift_data_for_next_week']
        )

        get_final_shift_assignment_list =  rail.PythonOperator(
            task_id='get_final_shift_assignment_list',
            python_callable=python_callable_method.get_final_shift_assignment_list,
        )

        has_shift_assignment_data = rail.IfOperator(
            task_id = 'has_shift_assignment_data',
            test= "{{ result('get_final_shift_assignment_list') | length > 0 }}",
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
                'Number of working days': "{{result('get_final_shift_assignment_list') | length}}",
                'Start Date - End Date': "'{{ dag_run.conf.Startdate }}' - '{{ dag_run.conf.Enddate }}'"
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        # pylint: disable=line-too-long
        create_date_range_seq >> get_shift_schedule_summary >> get_assigned_shift_dates >> has_shift_assigned_data
        has_shift_assigned_data >> rail.Label(
            "Yes") >> bulk_delete_shifts >> get_upcoming_weekdays
        has_shift_assigned_data >> rail.Label(
            "No") >> get_upcoming_weekdays >> create_collection_of_weekday_data >> create_dates_till_friday >> has_days_till_friday
        has_days_till_friday >>  rail.Label(
            "Yes") >>  query_shift_assignment_till_friday >> add_shift_assignments_till_friday >> query_shift_assignment_for_next_week >> add_shift_assignment_for_next_week >> get_final_shift_assignment_list
        has_days_till_friday >>  rail.Label(
            "No") >> query_shift_data_for_next_week >> add_shift_assignment_for_week >> get_final_shift_assignment_list >> has_shift_assignment_data
        has_shift_assignment_data >> rail.Label(
            "Yes") >> bulk_put_shift_assignment >> log_to_sumo >> finish
        has_shift_assignment_data >> rail.Label(
            "No") >> finish
    return dag

rail.for_each_instance(create_dag)
