# pylint: disable=too-many-statements
from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract_v2.utils import python_callable_method
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_payroll_export_process_each_user_child_v2_{config.instance}',
        description=f'DXC_Australia_PayrollData_Export_Process_Each_User_Child V2 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'false',
            yes_task='batch_task',
            no_task='query_distinct_schedules_for_employee_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_distinct_schedules_for_employee_id',
            end_task='finish',
        )

        query_distinct_schedules_for_employee_id = rail.QueryCollectionOperator(
            task_id= 'query_distinct_schedules_for_employee_id',
            query="""SELECT DISTINCT Employee_Id,Shift_Description,Office_Schedule,Actual_Employee_ID,Schedule_Name
            FROM active_userbalance WHERE Employee_Id == :employee_id AND ((Office_Schedule == 'Shift Schedule' AND
            NULLIF(Shift_Description, '') IS NOT NULL) OR Office_Schedule != 'Shift Schedule')""",
            query_params={
                "employee_id": "{{dag_run.conf.employee_id}}"
            },
            name= 'distinctschedules'
        )

        check_the_length = rail.IfOperator(
            task_id = 'check_the_length',
            test = '{{ result("query_distinct_schedules_for_employee_id", "length") == 1 }}',
            yes_task = 'get_data_for_single_schedule',
            no_task = 'query_data_for_employee_id'
        )

        get_data_for_single_schedule = rail.DataAdaptorOperator(
            task_id="get_data_for_single_schedule",
            source='{{ result("query_distinct_schedules_for_employee_id") }}',
            columns=['Employee_Id', 'Shift_Description', 'Office_Schedule', 'Actual_Employee_ID', 'Schedule_Name', 'start_date','end_date'],
            data=python_callable_method.get_converted_query_data,
        )

        query_data_for_employee_id= rail.QueryCollectionOperator(
            task_id= 'query_data_for_employee_id',
            query="""SELECT * FROM active_userbalance WHERE Employee_Id == :employee_id """,
            query_params={
                "employee_id": "{{dag_run.conf.employee_id}}"
            },
            name= 'rawdatawithempid'
        )

        calculate_start_date_and_end_date=rail.PythonOperator(
            task_id= 'calculate_start_date_and_end_date',
            python_callable= python_callable_method.get_multiple_schedule_dates
        )

        create_final_dates_collection = rail.CreateCollectionOperator(
            task_id="create_final_dates_collection",
            source='{{result("calculate_start_date_and_end_date") | to_json }}',
            name= 'finaldata'
        )

        query_final_dates = rail.QueryCollectionOperator(
            task_id= 'query_final_dates',
            query="""SELECT DISTINCT Employee_Id,Shift_Description,start_date,end_date,Schedule_Name
            FROM finaldata WHERE NULLIF(Shift_Description,'') IS NOT NULL OR Shift_Description != '' """
        )

        get_dates_for_schedule = rail.PythonOperator(
            task_id= 'get_dates_for_schedule',
            python_callable= python_callable_method.get_multiple_schedule_dates_withmd5
        )

        finish = rail.EmptyOperator(
            task_id= 'finish'
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> finish

        can_run_batch_task >> rail.Label(
            "No") >> query_distinct_schedules_for_employee_id

        query_distinct_schedules_for_employee_id >> check_the_length

        check_the_length >> rail.Label(
            "Yes") >> get_data_for_single_schedule >> get_dates_for_schedule >> finish

        check_the_length >> rail.Label(
            "No") >> query_data_for_employee_id >> calculate_start_date_and_end_date >> create_final_dates_collection >>\
                query_final_dates >> get_dates_for_schedule >> finish

    return dag

rail.for_each_instance(create_child_dag)
