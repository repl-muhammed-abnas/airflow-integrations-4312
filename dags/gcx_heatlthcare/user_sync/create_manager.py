import rail
from gcx_heatlthcare.user_sync.utils import request_payload,response_payload
from datetime import datetime, timedelta
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable

# pylint: disable=too-many-statements


def create_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.create_manager_child_dag_id,
        description=f"GCX CREATE MANAGER {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        can_run_batch_task = rail.IfOperator(
                task_id='can_run_batch_task',
                test=lambda: Variable.get(
                    config.can_run_batch_task_name, default_var='true').lower() == 'true',
                yes_task='batch_task',
                no_task='query_user_records'
            )

        batch_task = rail.BatchTaskRunOperator(
                task_id='batch_task',
                execution_timeout=timedelta(
                    days=config.execution_timeout_days),
                start_task='query_user_records',
                end_task='catch_and_log_errors',
            )

        query_user_records = rail.QueryCollectionOperator(
            task_id='query_user_records',
            query="""SELECT * FROM  inputfile WHERE  inputfile.employee_id = '{{dag_run.conf.manager}}'"""
        )

        if_manager_details_present = rail.IfOperator(
                task_id='if_manager_details_present',
                test='{{ result("query_user_records", "length") > 0 }}',
                yes_task='create_user',
                no_task='update_to_log_exception'
            )
        
        update_to_log_exception = rail.WriteLogOperator(
                task_id='update_to_log_exception',
                message="Supervisior user not available in the paycore",
                log='{{dag_run.conf.log}}',
                severity='Exception',
                properties=lambda dag_run:{
                    'employeeid': dag_run.conf['manager'],
                    'first_name': '',
                    'last_name': '',
                    'action': "Add",
                    'status': "Exception",
                    'details': "Supervisior user not available in the paycore",
                    'jobid': get_dagrun_ecid(dag_run)
                }
            )

        create_user = rail.RepliconServiceOperator(
            task_id='create_user',
            endpoint='/services/ImportService2.svc/CreateUserOrApplyModifications',
            data=lambda :request_payload.get_create_user_payload()
        )

        update_to_log_success = rail.WriteLogOperator(
                task_id='update_to_log_success',
                message="User created successfully",
                log='{{dag_run.conf.log}}',
                items=lambda: rail.load_all_records(rail.result('query_user_records')),
                severity='Success',
                properties=lambda dag_run,item : {
                    'employeeid': item['employee_id'],
                    'first_name': item['employee_first_name'],
                    'last_name': item['employee_last_name'],
                    'action': "Add",
                    'status': "Success",
                    'details': "User created successfully",
                    'jobid': get_dagrun_ecid(dag_run)
                }
            )

        catch_and_log_errors = rail.WriteLogOperator(
                task_id='catch_and_log_errors',
                trigger_rule='one_failed',
                log='{{dag_run.conf.log}}',
                items=lambda:rail.load_all_records(rail.result('query_user_records')),
                severity='Error',
                message='{{ get_error_message() }}',
                properties=lambda dag_run,item:{
                    'employeeid': item['employee_id'] if item else dag_run.conf['manager'],
                    'first_name': item['employee_first_name'] if item else '',
                    'last_name': item['employee_last_name'] if item  else '',
                    'action': "Add",
                    'status': "Error",
                    'details': '{{ get_error_message() }}',
                    'jobid': get_dagrun_ecid(dag_run)
                },
            )




        can_run_batch_task >> rail.Label(
                'Yes') >> batch_task >> catch_and_log_errors

        can_run_batch_task >> rail.Label(
                'No') >> query_user_records
        query_user_records >> if_manager_details_present >> rail.Label("Yes") >> create_user >> update_to_log_success >> catch_and_log_errors

        if_manager_details_present >> rail.Label("No") >> update_to_log_exception >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_dag)