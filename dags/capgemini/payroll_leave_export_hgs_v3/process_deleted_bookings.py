
from datetime import timedelta
from capgemini.payroll_leave_export_hgs_v3.utils import custom_methods
from airflow.models import Variable
import rail

null=None

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_deleted_bookings_child_dag_id,
        description=f'HGS Payroll Export - Capgemini {config.instance} V3',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_approved_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_log'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_log',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_keyvalue_for_user_timeoff = rail.RepliconServiceOperator(
            task_id='get_keyvalue_for_user_timeoff',
            endpoint='/services/GenericKeyValueStoreService1.svc/GetKeyValue',
            data={
                "keyNamespace": "Capgemini_HGSTimeoffExportData",
                "key": "{{ dag_run.conf.timeoff_booking_details.employee_id }}_{{ dag_run.conf.timeoff_booking_details.leave_request_id }}"
            }
        )

        if_key_value_json_present = rail.IfOperator(
            task_id='if_key_value_json_present',
            test='{{ result("get_keyvalue_for_user_timeoff") | is_truthy and result("get_keyvalue_for_user_timeoff").jsonValue | is_truthy }}',
            yes_task='log_R_timeoff_record_to_reverse',
            no_task='dagrun_log_to_sumo'
        )

        log_R_timeoff_record_to_reverse = rail.WriteLogOperator(
            task_id='log_R_timeoff_record_to_reverse',
            log='{{ result("create_log") }}',
            message='Added "R" Record for Delete',
            severity='Success',
            properties=custom_methods.log_R_timeoff_record_to_delete
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> dagrun_log_to_sumo
        can_run_batch_task >> rail.Label('No') >> create_log

        create_log >> get_keyvalue_for_user_timeoff >> if_key_value_json_present

        if_key_value_json_present >> rail.Label("No") >> dagrun_log_to_sumo
        if_key_value_json_present >> rail.Label("Yes") >> log_R_timeoff_record_to_reverse >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
