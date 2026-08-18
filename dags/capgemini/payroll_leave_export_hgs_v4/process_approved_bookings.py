
from datetime import timedelta
from capgemini.payroll_leave_export_hgs_v4.utils import custom_methods
import rail

null=None

def create_child_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=config.process_approved_bookings_child_dag_id,
        description=f'HGS Payroll Export - Capgemini {config.instance} V4',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_approved_runs,
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='view_dagrun_config',
            end_task='dagrun_log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        view_dagrun_config = rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

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
            yes_task='sort_and_parse_json_value',
            no_task='create_new_timeoff_record_to_add_in_generickeystore'
        )

        create_new_timeoff_record_to_add_in_generickeystore = rail.PythonOperator(
            task_id='create_new_timeoff_record_to_add_in_generickeystore',
            python_callable=custom_methods.get_new_timeoff_record_to_add
        )

        put_keyvalue_for_user_timeoff = rail.RepliconServiceOperator(
            task_id='put_keyvalue_for_user_timeoff',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data={
                "keyNamespace": "Capgemini_HGSTimeoffExportData",
                "keyValue": {
                    "key": "{{ dag_run.conf.timeoff_booking_details.employee_id }}_{{ dag_run.conf.timeoff_booking_details.leave_request_id }}",
                    "jsonValue": "{{ result('create_new_timeoff_record_to_add_in_generickeystore') }}"
                }
            }
        )

        log_L_timeoff_record_to_add = rail.WriteLogOperator(
            task_id='log_L_timeoff_record_to_add',
            log='{{ result("create_log") }}',
            message='Added "L" Record for New',
            severity='Success',
            properties=custom_methods.log_L_timeoff_record_to_add
        )

        sort_and_parse_json_value = rail.PythonOperator(
            task_id='sort_and_parse_json_value',
            python_callable=custom_methods.parse_and_sort_json_data
        )

        check_startdates_and_enddates_equal = rail.IfOperator(
            task_id='check_startdates_and_enddates_equal',
            test=custom_methods.check_startdates_and_enddates_equal,
            yes_task='dagrun_log_to_sumo',
            no_task='log_R_timeoff_record_to_reverse'
        )

        log_R_timeoff_record_to_reverse = rail.WriteLogOperator(
            task_id='log_R_timeoff_record_to_reverse',
            log='{{ result("create_log") }}',
            message='Added "R" Record for Reversal',
            severity='Success',
            properties=custom_methods.log_R_timeoff_record_to_reverse
        )

        create_new_and_existing_timeoff_records_to_add_in_generickeystore = rail.PythonOperator(
            task_id='create_new_and_existing_timeoff_records_to_add_in_generickeystore',
            python_callable=custom_methods.get_new_and_existing_timeoff_record_to_add
        )

        put_new_existing_keyvalue_for_user_timeoff = rail.RepliconServiceOperator(
            task_id='put_new_existing_keyvalue_for_user_timeoff',
            endpoint='/services/GenericKeyValueStoreService1.svc/PutKeyValue',
            data={
                "keyNamespace": "Capgemini_HGSTimeoffExportData",
                "keyValue": {
                    "key": "{{ dag_run.conf.timeoff_booking_details.employee_id }}_{{ dag_run.conf.timeoff_booking_details.leave_request_id }}",
                    "jsonValue": "{{ result('create_new_and_existing_timeoff_records_to_add_in_generickeystore') }}"
                }
            }
        )

        dagrun_log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='dagrun_log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id=config.dagrun_log_sumo_conn_id
        )

        batch_task >> dagrun_log_to_sumo
        batch_task >> view_dagrun_config >> create_log

        create_log >> get_keyvalue_for_user_timeoff >> if_key_value_json_present

        if_key_value_json_present >> rail.Label("No") >> create_new_timeoff_record_to_add_in_generickeystore \
            >> put_keyvalue_for_user_timeoff >> log_L_timeoff_record_to_add >> dagrun_log_to_sumo

        if_key_value_json_present >> rail.Label("Yes") >> sort_and_parse_json_value >> check_startdates_and_enddates_equal

        check_startdates_and_enddates_equal >> rail.Label("Yes") >> dagrun_log_to_sumo
        check_startdates_and_enddates_equal >> rail.Label("No") >> log_R_timeoff_record_to_reverse \
            >> create_new_and_existing_timeoff_records_to_add_in_generickeystore \
                >> put_new_existing_keyvalue_for_user_timeoff >> log_L_timeoff_record_to_add >> dagrun_log_to_sumo

    return dag

rail.for_each_instance(create_child_dag)
