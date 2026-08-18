# pylint: disable=too-many-statements
from datetime import timedelta
import rail
from dxctechnology.australia_payroll_extract.utils import python_callable_method
from airflow.models import Variable


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_australia_payroll_export_process_each_user_child_{config.instance}',
        description=f'DXC_Australia_PayrollData_Export_Process_Each_User_Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_data_for_employee_id'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='query_data_for_employee_id',
            end_task='finish',
        )

        # pylint: disable=line-too-long
        query_data_for_employee_id= rail.QueryCollectionOperator(
            task_id= 'query_data_for_employee_id',
            query="""SELECT * FROM active_userbalance WHERE Employee_Id == :employee_id AND Shift_Description == :shift_description AND Office_Schedule == :schedule_name""",
            query_params={
                "employee_id": "{{dag_run.conf.employee_id}}",
                "shift_description": "{{dag_run.conf.shift_description}}",
                "schedule_name": "{{dag_run.conf.schedule_name}}"
            },
            name= 'rawdatawithempid'
        )

        get_required_query_data = rail.DataAdaptorOperator(
            task_id="get_required_query_data",
            source='{{ result("query_data_for_employee_id") }}',
            columns=['Entry_Date', 'Shift_Description', 'Schedule_Name', 'Employee_Id', 'Actual_Employee_ID', 'Office_Schedule'],
            data=python_callable_method.get_converted_query_data,
        )

        query_final_list_data_collection = rail.CreateCollectionOperator(
            task_id='query_final_list_data_collection',
            source="{{ result('get_required_query_data') }}",
            name='finalquerydata',
        )

        query_for_dates= rail.QueryCollectionOperator(
            task_id= 'query_for_dates',
            query="""SELECT MIN(Entry_Date),MAX(Entry_Date),Shift_Description,Schedule_Name,Employee_Id,Actual_Employee_ID,Office_Schedule FROM finalquerydata""",
        )

        get_query_data= rail.PythonOperator(
            task_id= 'get_query_data',
            python_callable= lambda: rail.load_all_records(rail.result("query_for_dates"))
        )

        calculate_start_date_and_end_date=rail.PythonOperator(
            task_id= 'calculate_start_date_and_end_date',
            python_callable= python_callable_method.calculate_dates
        )

        get_required_data_for_empid= rail.PythonOperator(
            task_id ='get_required_data_for_empid',
            python_callable= python_callable_method.get_required_data
        )

        finish = rail.EmptyOperator(
            task_id= 'finish'
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> finish

        query_data_for_employee_id >> get_required_query_data >> query_final_list_data_collection >> query_for_dates >> get_query_data >> \
            calculate_start_date_and_end_date >> get_required_data_for_empid >> finish

        can_run_batch_task >> rail.Label(
            "No") >> query_data_for_employee_id

    return dag

rail.for_each_instance(create_child_dag)
