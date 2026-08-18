from datetime import timedelta
from airflow.models import Variable
import rail
from moodys.monthly_shift_assignment.utils import request_payload
from moodys.monthly_shift_assignment.utils import response_filter
from moodys.monthly_shift_assignment.utils import python_callable_method
from moodys.monthly_shift_assignment.utils import custom_methods

null = None


def create_moodys_default_shift_assignment_monthly_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'moodysemea_default_shift_assignment_monthly_child_{config.instance}',
        description=f'Moodys_Default Shift assignment_monthly V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id='view_dagrun_config')
        start_date = '{{ dag_run.conf.startdate }}'
        end_date = '{{ dag_run.conf.enddate }}'
        shift_name = '{{ dag_run.conf.shiftname }}'
        user_uri = '{{ dag_run.conf.useruri }}'

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_monthly_shift_assignment_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='create_monthly_shift_assignment_log',
            end_task='catch_and_log_errors',
        )

        create_monthly_shift_assignment_log = rail.CreateLogOperator(
            task_id='create_monthly_shift_assignment_log'
        )

        get_data_shift_details = rail.RepliconServicePageOperator(
            task_id='get_data_shift_details',
            endpoint='/services/ShiftListService1.svc/GetData',
            data=request_payload.get_shift_details_payload,
            page_handler=response_filter.page_handler,
            all_result_data_handler=response_filter.all_result_data_handler
        )

        has_shift_name = rail.IfOperator(
            task_id='has_shift_name',
            test=lambda: rail.result('get_data_shift_details') is True,
            yes_task='create_date_range_seq',
            no_task='log_schedule_not_populated',
        )

        log_schedule_not_populated = rail.WriteLogOperator(
            task_id='log_schedule_not_populated',
            log='{{ result("create_monthly_shift_assignment_log") }}',
            message='Shift schedule was not populated. The shift - "{{dag_run.conf.shiftname}}" is not present/disabled in Replicon',
            properties={
                'parentjobid': '{{ dag_run.conf.parentjobid }}',
                'childjobid': '{{ ecid() }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'shiftname': '{{ dag_run.conf.shiftname }}',
                'status': 'Ignored',
                'details': 'Shift schedule was not populated. The shift - "{{dag_run.conf.shiftname}}" is not present/disabled in Replicon'
            }
        )

        create_date_range_seq = rail.PythonOperator(
            task_id='create_date_range_seq',
            python_callable=python_callable_method.create_date_range_seq,
            op_args=[end_date, start_date]
        )

        get_shift_schedule_summary_for_user = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary_for_user',
            endpoint='/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary',
            data=request_payload.get_shift_schedule_summary_for_user_payload,
            data_handler=response_filter.get_assigned_shift_dates
        )

        has_assigned_shift_dates = rail.IfOperator(
            task_id='has_assigned_shift_dates',
            test=lambda: bool(
                len(rail.result('get_shift_schedule_summary_for_user')) > 0),
            yes_task='bulk_delete_for_user',
            no_task='get_weekdays_for_upcoming_week',
        )

        bulk_delete_for_user = rail.RepliconServiceOperator(
            task_id='bulk_delete_for_user',
            endpoint='/services/ShiftAssignmentService1.svc/BulkDelete',
            data=request_payload.get_bulk_delete_for_user_payload
        )

        get_weekdays_for_upcoming_week = rail.WriteCSVFileOperator(
            task_id='get_weekdays_for_upcoming_week',
            source='{{result("create_date_range_seq") | to_json }}',
            header=['seq',
                    'date',
                    'day',
                    'dateday',
                    'datemonth',
                    'dateyear',
                    'week'],
            row=custom_methods.get_csv_rows
        )

        create_collection_of_weekday_data = rail.CreateCollectionOperator(
            task_id='create_collection_of_weekday_data',
            name='datestoconsider',
            source=lambda: rail.result('get_weekdays_for_upcoming_week'),
        )

        query_shift_assignment = rail.QueryCollectionOperator(
            task_id='query_shift_assignment',
            query='SELECT * FROM datestoconsider WHERE day != 5 AND day != 6'
        )

        has_shift_data = rail.IfOperator(
            task_id='has_shift_data',
            test="{{ result('query_shift_assignment') | length > 0 }}",
            yes_task='add_shift_assignment_to_list',
            no_task='has_shift_list'
        )

        add_shift_assignment_to_list = rail.PythonOperator(
            task_id='add_shift_assignment_to_list',
            python_callable=request_payload.get_shift_assignment_list,
            op_args=[shift_name, user_uri]
        )

        has_shift_list = rail.IfOperator(
            task_id='has_shift_list',
            test="{{result('add_shift_assignment_to_list') | length > 0}}",
            yes_task='bulk_put_shift_assignment',
            no_task='finish'
        )

        bulk_put_shift_assignment = rail.RepliconServiceOperator(
            task_id="bulk_put_shift_assignment",
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=request_payload.get_put_shift_payload
        )

        log_shift_update_sucess = rail.WriteLogOperator(
            task_id='log_shift_update_sucess',
            log='{{ result("create_monthly_shift_assignment_log") }}',
            message='The shift is updated for date: {{ dag_run.conf.startdate }} - {{ dag_run.conf.enddate }}',
            properties={
                'parentjobid': '{{ dag_run.conf.parentjobid }}',
                'childjobid': '{{ ecid() }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'shiftname': '{{ dag_run.conf.shiftname }}',
                'status': 'Success',
                'details': 'The shift is updated for date: {{ dag_run.conf.startdate }} - {{ dag_run.conf.enddate }}'
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_monthly_shift_assignment_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message=config.error_template,
            properties={
                'parentjobid': '{{ dag_run.conf.parentjobid }}',
                'childjobid': '{{ ecid() }}',
                'loginname': '{{ dag_run.conf.loginname }}',
                'shiftname': '{{ dag_run.conf.shiftname }}',
                'status': 'Error',
                'details': {config.error_template}
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
            extra_info={
                'user': '{{ dag_run.conf.username }}',
                'login_name': '{{ dag_run.conf.loginname }}',
                'shift': '{{ dag_run.conf.shiftname }}',
                'number_of_working_days': '{{ result("add_shift_assignment_to_list") | length if result("add_shift_assignment_to_list") }}',
                'start_date': '{{ dag_run.conf.startdate }}',
                'end_date': '{{ dag_run.conf.enddate }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> create_monthly_shift_assignment_log

        create_monthly_shift_assignment_log >> get_data_shift_details >> has_shift_name
        has_shift_name >> rail.Label(
            'Yes') >> create_date_range_seq >> get_shift_schedule_summary_for_user >> has_assigned_shift_dates
        has_shift_name >> rail.Label(
            'No') >> log_schedule_not_populated >> finish

        has_assigned_shift_dates >> rail.Label(
            'Yes') >> bulk_delete_for_user >> get_weekdays_for_upcoming_week >> create_collection_of_weekday_data \
            >> query_shift_assignment >> has_shift_data
        has_assigned_shift_dates >> rail.Label(
            'No') >> get_weekdays_for_upcoming_week

        has_shift_data >> rail.Label(
            'Yes') >> add_shift_assignment_to_list >> has_shift_list
        has_shift_data >> rail.Label(
            'No') >> has_shift_list

        has_shift_list >> rail.Label(
            'Yes') >> bulk_put_shift_assignment >> log_shift_update_sucess >> finish
        has_shift_list >> rail.Label(
            'No') >> finish

        finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_moodys_default_shift_assignment_monthly_dag)
