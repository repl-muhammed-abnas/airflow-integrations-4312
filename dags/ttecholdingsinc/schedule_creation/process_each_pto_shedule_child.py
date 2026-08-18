from datetime import timedelta
import uuid
import rail
from airflow.models import Variable
from ttecholdingsinc.schedule_creation.utils import request_payload,custom_methods

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.pto_child_dag_id,
        description=f'TTEC Process Each PTO Schedule child {config.instance}',
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
            no_task='query_user_shift_data'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_user_shift_data',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_user_shift_data = rail.QueryCollectionOperator(
            task_id = 'query_user_shift_data',
            query= '''SELECT * FROM finaldata WHERE empid ==:Empid AND description == "PTO" ''',
            name= 'userdata',
            query_params= {
                'Empid': '{{ dag_run.conf.empid }}'
            }
        )

        query_dates_for_shift = rail.QueryCollectionOperator(
            task_id = 'query_dates_for_shift',
            query= '''SELECT MIN(startdate), MAX(startdate), useruri FROM userdata ''',
        )

        get_query_data = rail.PythonOperator(
            task_id = 'get_query_data',
            python_callable=lambda: rail.load_all_records(rail.result("query_user_shift_data"))
        )

        get_shift_schedule_summary = rail.RepliconServiceOperator(
            task_id='get_shift_schedule_summary',
            endpoint='/services/ShiftAssignmentService1.svc/GetShiftScheduleSummary',
            data=request_payload.get_shift_schedule_summary_data,
            data_handler= custom_methods.filter_shifts
        )

        get_assigned_shift_dates = rail.PythonOperator(
            task_id = 'get_assigned_shift_dates',
            python_callable=custom_methods.get_assigned_shift_dates
        )

        has_shift_to_create = rail.IfOperator(
            task_id = 'has_shift_to_create',
            test= custom_methods.check_any_shifts_to_be_created,
            yes_task='bulk_put_shift_assignments',
            no_task='assign_pto_timeoff'
        )

        bulk_put_shift_assignments = rail.RepliconServiceOperator(
            task_id='bulk_put_shift_assignments',
            endpoint="/services/ShiftAssignmentService1.svc/BulkPutShiftAssignments",
            data=request_payload.get_default_pto_shift_payload
        )

        assign_pto_timeoff = rail.RepliconServiceCallForEachItemOperator(
            task_id = 'assign_pto_timeoff',
            items= '{{ result("get_query_data") | to_json }}',
            endpoint="/services/TimeOffApprovalService1.svc/PutAndSubmitTimeOff",
            data=request_payload.get_put_holiday_payload
        )

        force_approve = rail.RepliconServiceOperator(
            task_id='force_approve',
            endpoint='/services/TimeOffApprovalService1.svc/ForceApprove',
            data={
                "timeOffUri": '{{ result("assign_pto_timeoff")[0].uri }}',
                    "unitOfWorkId": str(uuid.uuid4()),
                    "comments": "Auto approving as part of Schedule Integration"
            }
        )

        log_success = rail.WriteLogOperator(
            task_id='log_success',
            message="pto timeoff added successfully",
            items= '{{ result("get_query_data") | to_json }}',
            severity="Success",
            properties=lambda: {
                "employeeid": '{{ item.empid }}',
                "schedulename": '{{ item.schedulename }}',
                "startdate": '{{ item.startdate }}',
                "status": "Success",
                "action": "Add",
                "details": "pto timeoff added successfully",
                "ecid": '{{ dag_run_ecid() }}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            items= '{{ result("get_query_data") | to_json }}',
            message="{{ get_error_message() }}",
            severity="Error",
            properties=lambda dag_run:{
                "employeeid": '{{ item.empid }}',
                "schedulename": '{{ item.schedulename }}',
                "startdate": '{{ item.startdate }}',
                "status": "Error",
                "action": "PTO timeoff Assignment",
                "details": "{{ get_error_message() }}",
                "ecid": '{{ dag_run_ecid() }}'
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> query_user_shift_data

        query_user_shift_data >> query_dates_for_shift >> get_query_data >> get_shift_schedule_summary >> get_assigned_shift_dates >> \
            has_shift_to_create

        has_shift_to_create >> rail.Label(
            "Yes") >> bulk_put_shift_assignments >> assign_pto_timeoff

        has_shift_to_create >> rail.Label(
            "No") >> assign_pto_timeoff >> force_approve >> log_success >> catch_and_log_errors

        return dag

rail.for_each_instance(create_dag)
