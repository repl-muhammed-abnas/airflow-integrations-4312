from datetime import timedelta
import rail
from airflow.models import Variable
from necau.time_off_import.task.delete_timeoff_booking import delete_timeoff_booking_process
from necau.time_off_import.task.reopen_timesheet import get_timesheet_open_process
from necau.time_off_import.utils import custom_method
from necau.time_off_import.utils import request_payload
from rail.lib.ecid import get_dagrun_ecid
null = None

# pylint: disable=too-many-statements


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'necau_modify_booking_child_{config.instance}',
        description=f'NECAU - modify_booking_child_v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='has_same_request_key'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='has_same_request_key',
            end_task='finish',
        )

        has_same_request_key = rail.IfOperator(
            task_id='has_same_request_key',
            test=custom_method.is_request_keys_same,
            yes_task='dummy_operator_1',
            no_task='finish'
        )

        is_timesheet_not_approved_waiting = rail.IfOperator(
            task_id='is_timesheet_not_approved_waiting',
            test=custom_method.get_timesheet_reopen_status,
            yes_task='dummy_operator_2',
            no_task='dummy_operator_10'
        )

        is_form_code_lvc = rail.IfOperator(
            task_id='is_form_code_lvc',
            test=custom_method.lvc_form_code_validation,
            yes_task='delete_time_off_booking_lvc',
            no_task='dummy_operator_3'
        )

        delete_lvc_timeoff = delete_timeoff_booking_process("lvc")

        add_log_entry_1 = rail.WriteLogOperator(
            task_id='add_log_entry_1',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Deleted",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_form_code_lvc_not_approved = rail.IfOperator(
            task_id='is_form_code_lvc_not_approved',
            test=custom_method.lvc_timesheet_status,
            yes_task='add_log_entry_2',
            no_task='dummy_operator_4'
        )

        add_log_entry_2 = rail.WriteLogOperator(
            task_id='add_log_entry_2',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Form Code is LVC and Action status is not equal to Approved",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_form_code_lap = rail.IfOperator(
            task_id='is_form_code_lap',
            test=custom_method.is_lap_form_code,
            yes_task='dummy_operator_5',
            no_task='dummy_operator_10'
        )

        is_action_approved = rail.IfOperator(
            task_id='is_action_approved',
            test=custom_method.get_action_status,
            yes_task='approve_timeoff_booking',
            no_task='dummy_operator_6'
        )

        approve_timeoff_booking = rail.RepliconServiceOperator(
            task_id='approve_timeoff_booking',
            endpoint="/services/TimeOffApprovalService1.svc/ForceApprove",
            data=request_payload.get_timeoff_approve_request
        )

        add_log_entry_3 = rail.WriteLogOperator(
            task_id='add_log_entry_3',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Time Off Approved",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_action_requested1 = rail.IfOperator(
            task_id='is_action_requested1',
            test=custom_method.get_action_request,
            yes_task='add_log_entry_4',
            no_task='dummy_operator_7'
        )

        add_log_entry_4 = rail.WriteLogOperator(
            task_id='add_log_entry_4',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Time Off already exists in Replicon, Form Code LAP and Action status Request/Requested",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_timeoff_approved = rail.IfOperator(
            task_id='is_timeoff_approved',
            test=custom_method.get_timeoff_status,
            yes_task='add_log_entry_5',
            no_task='dummy_operator_8'
        )

        add_log_entry_5 = rail.WriteLogOperator(
            task_id='add_log_entry_5',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Time Off Already Approved",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_action_deleted = rail.IfOperator(
            task_id='is_action_deleted',
            test=custom_method.get_action_deleted,
            yes_task='delete_time_off_booking_lap1',
            no_task='dummy_operator_9'
        )

        delete_lap_booking = delete_timeoff_booking_process("lap1")

        add_log_entry_6 = rail.WriteLogOperator(
            task_id='add_log_entry_6',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Deleted",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_action_missing = rail.IfOperator(
            task_id='is_action_missing',
            test=custom_method.actions_present,
            yes_task='add_log_entry_7',
            no_task='dummy_operator_10'
        )

        add_log_entry_7 = rail.WriteLogOperator(
            task_id='add_log_entry_7',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Form Code is LAP, Action Status is not present and Time Off already exists",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_timesheet_approved_waiting = rail.IfOperator(
            task_id='is_timesheet_approved_waiting',
            test=custom_method.get_timesheet_approved_status,
            yes_task='is_action_requested',
            no_task='finish'
        )

        is_action_requested = rail.IfOperator(
            task_id='is_action_requested',
            test=custom_method.get_timesheet_reopen_status,
            yes_task='add_log_entry_8',
            no_task='dummy_operator_11'
        )

        add_log_entry_8 = rail.WriteLogOperator(
            task_id='add_log_entry_8',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Time Off already exists in Replicon, Form Code LAP and Action status Request/Requested",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_lvc_approved = rail.IfOperator(
            task_id='is_lvc_approved',
            test=custom_method.lvc_form_code_validation,
            yes_task='initiate_lvc_reopen_delete',
            no_task='dummy_operator_12'
        )

        re_open_timesheet1 = get_timesheet_open_process(
            'lvc', config, True, True)

        initiate_lvc_reopen_delete = rail.EmptyOperator(
            task_id="initiate_lvc_reopen_delete"
        )

        initiate_lap_reopen_approved = rail.EmptyOperator(
            task_id="initiate_lap_reopen_approved"
        )

        initiate_lap_reopen_delete = rail.EmptyOperator(
            task_id="initiate_lap_reopen_delete"
        )

        add_log_entry_9 = rail.WriteLogOperator(
            task_id='add_log_entry_9',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Deleted",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        is_lvc_not_approved = rail.IfOperator(
            task_id='is_lvc_not_approved',
            test=custom_method.lvc_timesheet_status,
            yes_task='add_log_entry_10',
            no_task='dummy_operator_13'
        )

        add_log_entry_10 = rail.WriteLogOperator(
            task_id='add_log_entry_10',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "No Action required",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        lap_reopen_timesheet = rail.IfOperator(
            task_id='lap_reopen_timesheet',
            test=custom_method.get_lap_action_aproved,
            yes_task='initiate_lap_reopen_approved',
            no_task='dummy_operator_14'
        )

        re_open_timesheet2 = get_timesheet_open_process(
            'lap', config, False, True)

        add_log_entry_11 = rail.WriteLogOperator(
            task_id='add_log_entry_11',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Time Off Approved",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        lap_timeoff_approved = rail.IfOperator(
            task_id='lap_timeoff_approved',
            test=custom_method.get_lap_timeoff_approved,
            yes_task='add_log_entry_12',
            no_task='dummy_operator_15'
        )

        add_log_entry_12 = rail.WriteLogOperator(
            task_id='add_log_entry_12',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Time Off Already Approved",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        lap_action_deleted = rail.IfOperator(
            task_id='lap_action_deleted',
            test=custom_method.get_lap_action_delete,
            yes_task='initiate_lap_reopen_delete',
            no_task='dummy_operator_16'
        )

        re_open_timesheet3 = get_timesheet_open_process(
            'lap1', config, True, True)

        add_log_entry_13 = rail.WriteLogOperator(
            task_id='add_log_entry_13',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Success",
                'reason': "Deleted",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        lap_action_status = rail.IfOperator(
            task_id='lap_action_status',
            test=custom_method.lap_actions_present,
            yes_task='add_log_entry_14',
            no_task='finish'
        )

        add_log_entry_14 = rail.WriteLogOperator(
            task_id='add_log_entry_14',
            log="{{ dag_run.conf.create_file_processing_log }}",
            severity="Success",
            properties=lambda dag_run: {
                'jobid': dag_run.conf['master_ecid'] + '|' + get_dagrun_ecid(dag_run),
                'Staff Member': dag_run.conf['staff_member'],
                'Status': "Ignored",
                'reason': "Form Code is LAP, Action Status is not present and Time Off already exists",
                'Request Key': dag_run.conf['request_key']
            },
            message="Success"
        )

        # log_to_sumo = rail.DagRunLogToSumoOperator(
        #     task_id='log_to_sumo',
        #     sumo_conn_id='sumologic-dagrunlogger',
        #     trigger_rule='all_done',
        #     extra_info={
        #         'staff_member': '{{ dag_run.conf.staff_member }}',
        #         'request_key': '{{ dag_run.conf.request_key }}'
        #     }
        # )

        finish = rail.EmptyOperator(
            task_id="finish"
        )

        dummy_operator_1 = rail.EmptyOperator(
            task_id='dummy_operator_1'
        )

        dummy_operator_2 = rail.EmptyOperator(
            task_id='dummy_operator_2'
        )

        dummy_operator_3 = rail.EmptyOperator(
            task_id='dummy_operator_3'
        )

        dummy_operator_4 = rail.EmptyOperator(
            task_id='dummy_operator_4'
        )

        dummy_operator_5 = rail.EmptyOperator(
            task_id='dummy_operator_5'
        )

        dummy_operator_6 = rail.EmptyOperator(
            task_id='dummy_operator_6'
        )

        dummy_operator_7 = rail.EmptyOperator(
            task_id='dummy_operator_7'
        )

        dummy_operator_8 = rail.EmptyOperator(
            task_id='dummy_operator_8'
        )

        dummy_operator_9 = rail.EmptyOperator(
            task_id='dummy_operator_9'
        )

        dummy_operator_10 = rail.EmptyOperator(
            task_id='dummy_operator_10'
        )

        dummy_operator_11 = rail.EmptyOperator(
            task_id='dummy_operator_11'
        )

        dummy_operator_12 = rail.EmptyOperator(
            task_id='dummy_operator_12'
        )

        dummy_operator_13 = rail.EmptyOperator(
            task_id='dummy_operator_13'
        )

        dummy_operator_14 = rail.EmptyOperator(
            task_id='dummy_operator_14'
        )

        dummy_operator_15 = rail.EmptyOperator(
            task_id='dummy_operator_15'
        )

        dummy_operator_16 = rail.EmptyOperator(
            task_id='dummy_operator_16'
        )

        can_run_batch_task
        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            "No") >> has_same_request_key
        has_same_request_key >> rail.Label(
            "Yes") >> dummy_operator_1 >> is_timesheet_not_approved_waiting
        has_same_request_key >> rail.Label("No") >> finish
        is_timesheet_not_approved_waiting >> rail.Label(
            "Yes") >> dummy_operator_2 >> is_form_code_lvc
        is_form_code_lvc >> rail.Label(
            "Yes") >> delete_lvc_timeoff >> add_log_entry_1 >> dummy_operator_3 >> is_form_code_lvc_not_approved
        is_form_code_lvc_not_approved >> rail.Label(
            "Yes") >> add_log_entry_2 >> dummy_operator_4 >> is_form_code_lap
        is_form_code_lap >> rail.Label(
            "Yes") >> dummy_operator_5 >> is_action_approved
        is_action_approved >> rail.Label(
            "Yes") >> approve_timeoff_booking >> add_log_entry_3 >> dummy_operator_6 >> is_action_requested1
        is_action_requested1 >> rail.Label(
            "Yes") >> add_log_entry_4 >> finish
        is_timeoff_approved >> rail.Label(
            "Yes") >> add_log_entry_5 >> dummy_operator_8 >> is_action_deleted
        is_timeoff_approved >> rail.Label("No") >> dummy_operator_8
        is_action_requested1 >> rail.Label(
            "No") >> dummy_operator_7 >> is_timeoff_approved
        is_action_deleted >> rail.Label(
            "Yes") >> delete_lap_booking >> add_log_entry_6 >> dummy_operator_9 >> is_action_missing
        is_action_missing >> rail.Label(
            "Yes") >> add_log_entry_7 >> dummy_operator_10
        is_action_missing >> rail.Label("No") >> dummy_operator_10
        is_action_deleted >> rail.Label("No") >> dummy_operator_9
        is_action_approved >> rail.Label("No") >> dummy_operator_6
        is_form_code_lap >> rail.Label("No") >> dummy_operator_10
        is_form_code_lvc_not_approved >> rail.Label("No") >> dummy_operator_4
        is_form_code_lvc >> rail.Label("No") >> dummy_operator_3
        is_timesheet_not_approved_waiting >> rail.Label(
            "No") >> dummy_operator_10 >> is_timesheet_approved_waiting
        is_timesheet_approved_waiting >> rail.Label(
            "Yes") >> is_action_requested
        is_action_requested >> rail.Label(
            "Yes") >> add_log_entry_8 >> finish
        is_lvc_approved >> rail.Label(
            "Yes") >> initiate_lvc_reopen_delete >> re_open_timesheet1 >> add_log_entry_9 >> dummy_operator_12 >> is_lvc_not_approved
        is_lvc_not_approved >> rail.Label(
            "Yes") >> add_log_entry_10 >> dummy_operator_13 >> lap_reopen_timesheet
        lap_reopen_timesheet >> rail.Label(
            "Yes") >> initiate_lap_reopen_approved >> re_open_timesheet2 >> add_log_entry_11 >> dummy_operator_14 >> lap_timeoff_approved
        lap_timeoff_approved >> rail.Label(
            "Yes") >> add_log_entry_12 >> dummy_operator_15 >> lap_action_deleted
        lap_timeoff_approved >> rail.Label("No") >> dummy_operator_15
        lap_action_deleted >> rail.Label(
            "Yes") >> initiate_lap_reopen_delete >> re_open_timesheet3 >> add_log_entry_13 >> dummy_operator_16 >> lap_action_status
        lap_action_status >> rail.Label(
            "Yes") >> add_log_entry_14 >> finish
        lap_action_status >> rail.Label("No") >> finish
        lap_action_deleted >> rail.Label("No") >> dummy_operator_16
        lap_reopen_timesheet >> rail.Label("No") >> dummy_operator_14
        is_lvc_not_approved >> rail.Label("No") >> dummy_operator_13
        is_lvc_approved >> rail.Label("No") >> dummy_operator_12
        is_action_requested >> rail.Label(
            "No") >> dummy_operator_11 >> is_lvc_approved
        is_timesheet_approved_waiting >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_dag)
