from datetime import timedelta
import rail
from airflow.models import Variable
from ttecholdingsinc.schedule_creation_v1.utils import request_payload, custom_methods
from ttecholdingsinc.schedule_creation_v1.task.create_schedule import create_schedule_task
from ttecholdingsinc.schedule_creation_v1.task.update_schedule import update_schedule_task

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_schedule_dag_id,
        description=f'TTEC Schedule Creation child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_shift_schedule_details'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_shift_schedule_details',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_shift_schedule_details = rail.QueryCollectionOperator(
            task_id = 'query_shift_schedule_details',
            name='getscheduledata',
            query="""SELECT * FROM inputdatacollection WHERE schedulename == :Schedule_Name """,
            query_params = {
                'Schedule_Name': '{{ dag_run.conf.schedulename }}'
            }
        )

        get_query_data = rail.PythonOperator(
            task_id = 'get_query_data',
            python_callable= custom_methods.get_query_data
        )

        shift_details_in_replicon = rail.RepliconServiceOperator(
            task_id = 'shift_details_in_replicon',
            endpoint= '/services/ShiftListService1.svc/GetData',
            data= request_payload.get_shift_data,
            data_handler= custom_methods.get_shift_data
        )

        is_shift_available = rail.IfOperator(
            task_id = 'is_shift_available',
            test= '{{ result("shift_details_in_replicon") | is_truthy }}',
            yes_task= 'empty_task_for_update',
            no_task= 'empty_task_for_create'
        )

        empty_task_for_create = rail.EmptyOperator(
            task_id = 'empty_task_for_create'
        )

        create_schedule = create_schedule_task()

        empty_task_for_update = rail.EmptyOperator(
            task_id = 'empty_task_for_update'
        )

        update_schedule = update_schedule_task()

        empty_success = rail.EmptyOperator(
            task_id='empty_success'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log= "{{ dag_run.conf.log }}",
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda: {
                "employeeid": rail.result("get_query_data")['empid'],
                "schedulename": rail.result("get_query_data")['name'],
                "startdate": rail.result("get_query_data")['startdate'],
                "status": "Error",
                "action": "Add",
                "details": "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> query_shift_schedule_details

        query_shift_schedule_details >> get_query_data >> shift_details_in_replicon >> is_shift_available

        is_shift_available >> rail.Label(
            "Yes") >> empty_task_for_create >> create_schedule >> empty_success >> catch_and_log_errors

        is_shift_available >> rail.Label(
            "No") >> empty_task_for_update >> update_schedule >> empty_success >> catch_and_log_errors

        return dag

rail.for_each_instance(create_dag)
