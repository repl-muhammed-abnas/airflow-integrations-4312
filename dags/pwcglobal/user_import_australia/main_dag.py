from datetime import timedelta
import rail
from pwcglobal.user_import_australia import custom_methods


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_master_dag_{config.instance}",
        description=f"PWC Global USer Import Australia Master Dag {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs

    ) as dag:

        list_user_import_dict = rail.SFTPListFilesOperator(
            task_id="list_user_import_dict",
            paths=[config.user_import_input_path],
        )
        has_any_user_import_files = rail.IfOperator(
            task_id="has_any_user_import_files",
            test=lambda: custom_methods.has_any_file(
                result_task_id="list_user_import_dict", input_file_path=config.user_import_input_path),
            yes_task="process_each_user_import_file",
            no_task=["list_user_allowance_dict", "can_delete_dag"]
        )
        process_each_user_import_file = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_user_import_file",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_import_process_each_file_child_{config.instance}",
            items=lambda: rail.result("list_user_import_dict")[
                config.user_import_input_path],
            conf={
                "file_name": "{{item.name}}",
                "file_path": config.user_import_input_path,
                "log_file_path": config.user_import_log_path,
                "log_file_name_postfix": "_{{dag_run_ecid()}}_log_"+'{{item.name | file_name}}'
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_process_each_user_import_file = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_user_import_file",
            dag_runs="{{result('process_each_user_import_file')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        list_user_allowance_dict = rail.SFTPListFilesOperator(
            task_id="list_user_allowance_dict",
            paths=[config.user_allowance_input_path],
        )

        has_any_allowance_import_files = rail.IfOperator(
            task_id="has_any_allowance_import_files",
            test=lambda: custom_methods.has_any_file(
                result_task_id="list_user_allowance_dict", input_file_path=config.user_allowance_input_path),
            yes_task="process_each_allowance_file",
            no_task=["list_termination_details_dict", "can_delete_dag"]
        )

        process_each_allowance_file = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_allowance_file",
            trigger_dag_id=f"pwcglobal_user_import_australia_user_allowance_process_each_file_child_{config.instance}",
            items=lambda: rail.result("list_user_allowance_dict")[
                config.user_allowance_input_path],
            conf={
                "file_name": "{{item.name}}",
                "file_path": config.user_allowance_input_path,
                "log_file_path": config.user_allowance_log_path,
                "log_file_name_postfix": "_PwCGlobal_userallowance_log_"+'{{current_time_in_specified_tz("Australia/Sydney","%m-%d-%Y")}}.csv'
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_process_each_allowance_file = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_allowance_file",
            dag_runs="{{result('process_each_allowance_file')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        list_termination_details_dict = rail.SFTPListFilesOperator(
            task_id="list_termination_details_dict",
            paths=[config.termination_details_input_path],
        )

        has_any_termination_files = rail.IfOperator(
            task_id="has_any_termination_files",
            test=lambda: custom_methods.has_any_file(
                result_task_id="list_termination_details_dict", input_file_path=config.termination_details_input_path),
            yes_task="process_each_termination_file",
            no_task="can_delete_dag"
        )

        process_each_termination_file = rail.TriggerDagRunForEachItemOperator(
            task_id="process_each_termination_file",
            trigger_dag_id=f"pwcglobal_user_import_australia_termination_details_process_each_file_child_{config.instance}",
            items=lambda: rail.result("list_termination_details_dict")[
                config.termination_details_input_path],
            conf={
                "file_name": "{{item.name}}",
                "file_path": config.termination_details_input_path,
                "log_file_path": config.termination_details_log_filepath,
                "log_file_name_postfix": "_PwCGlobal_usertermination_log_"+'{{current_time_in_specified_tz("Australia/Sydney","%m-%d-%Y")}}.csv'
            },
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_process_each_termination_file = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_each_termination_file",
            dag_runs="{{result('process_each_termination_file')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        can_delete_dag = rail.IfOperator(
            task_id="can_delete_dag",
            test=lambda: not (custom_methods.has_any_file(
                result_task_id="list_user_import_dict", input_file_path=config.user_import_input_path) or
                custom_methods.has_any_file(
                result_task_id="list_user_allowance_dict", input_file_path=config.user_allowance_input_path) or
                custom_methods.has_any_file(
                result_task_id="list_termination_details_dict", input_file_path=config.termination_details_input_path)),
            yes_task="delete_this_dagrun"
        )
        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        can_log_to_sumo = rail.IfOperator(
            task_id="can_log_to_sumo",
            trigger_rule="all_done",
            test=lambda:  rail.get_current_context()['dag_run'].get_task_instance(
                delete_this_dagrun.task_id).current_state().lower() != "success",
            yes_task="log_to_sumo",
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule="all_done",
            sumo_conn_id='sumologic-dagrunlogger',
            extra_info=lambda: {
                "user_import_files": rail.result("list_user_import_dict")[config.user_import_input_path] if rail.result("list_user_import_dict") else {},
                "allowance_files": rail.result("list_user_allowance_dict")[config.user_allowance_input_path]
                if rail.result("list_user_allowance_dict") else {},
                "termiantion_details_files": rail.result("list_termination_details_dict")[config.termination_details_input_path]
                if rail.result("list_termination_details_dict") else {}
            }
        )

        list_user_import_dict >> has_any_user_import_files >> rail.Label("Yes") >> process_each_user_import_file >> wait_for_process_each_user_import_file >>\
            list_user_allowance_dict >> has_any_allowance_import_files >> rail.Label(
            "Yes") >> process_each_allowance_file >> wait_for_process_each_allowance_file >> \
            list_termination_details_dict >> has_any_termination_files >> rail.Label(
            "Yes") >> process_each_termination_file >> wait_for_process_each_termination_file >> can_log_to_sumo >> rail.Label("Yes") >> log_to_sumo

        has_any_user_import_files >> rail.Label(
            "No") >> [list_user_allowance_dict, can_delete_dag]
        has_any_allowance_import_files >> rail.Label(
            "No") >> [list_termination_details_dict, can_delete_dag]
        has_any_termination_files >> rail.Label("No") >> can_delete_dag

        can_delete_dag >> rail.Label(
            "Yes") >> delete_this_dagrun >> can_log_to_sumo

    return dag


rail.for_each_instance(create_main_dag)
