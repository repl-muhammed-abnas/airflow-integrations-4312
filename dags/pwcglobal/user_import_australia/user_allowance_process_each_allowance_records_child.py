from datetime import timedelta
import rail
from pwcglobal.user_import_australia import custom_methods


def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_records_{config.instance}",
        description=f"PwCGlobal User Import Australia User Allowance child process each records {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.child_max_active_runs
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        get_mapper_value = rail.PythonOperator(
            task_id="get_mapper_value",
            python_callable=custom_methods.get_allowance_mapper_value
        )

        is_mapper_value_present = rail.IfOperator(
            task_id="is_mapper_value_present",
            test="{{result('get_mapper_value') != None}}",
            yes_task="is_replicon_group_cost_center",
            no_task="log_no_mapper_value"
        )

        is_replicon_group_cost_center = rail.IfOperator(
            task_id="is_replicon_group_cost_center",
            test="{{result('get_mapper_value')['replicongroup'] == 'Cost Center'}}",
            yes_task="process_cost_center",
            no_task="is_replicon_group_business_unit"
        )

        process_cost_center = rail.TriggerDagRunForEachItemOperator(
            task_id="process_cost_center",
            items=['1'],
            trigger_dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_cost_center_records_{config.instance}",
            conf=custom_methods.get_allowance_generic_child_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_cost_center = rail.WaitForDagRunsSensor(
            task_id="wait_for_cost_center",
            dag_runs="{{result('process_cost_center')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        is_replicon_group_business_unit = rail.IfOperator(
            task_id="is_replicon_group_business_unit",
            test="{{result('get_mapper_value')['replicongroup'] == 'Business Unit'}}",
            yes_task="process_business_unit",
            no_task="is_replicon_group_classification"
        )

        process_business_unit = rail.TriggerDagRunForEachItemOperator(
            task_id="process_business_unit",
            items=[1],
            trigger_dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_business_units_records_{config.instance}",
            conf=custom_methods.get_allowance_generic_child_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_business_unit = rail.WaitForDagRunsSensor(
            task_id="wait_for_business_unit",
            dag_runs="{{result('process_business_unit')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        is_replicon_group_classification = rail.IfOperator(
            task_id="is_replicon_group_classification",
            test="{{result('get_mapper_value')['replicongroup'] == 'Classification'}}",
            yes_task="process_classification"
        )

        process_classification = rail.TriggerDagRunForEachItemOperator(
            task_id="process_classification",
            items=[1],
            trigger_dag_id=f"pwcglobal_user_import_australia_user_allowance_child_process_each_classifications_records_{config.instance}",
            conf=custom_methods.get_allowance_generic_child_conf,
            retries=0,
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        wait_for_process_classification = rail.WaitForDagRunsSensor(
            task_id="wait_for_process_classification",
            dag_runs="{{result('process_classification')}}",
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )
        log_no_mapper_value = rail.WriteLogOperator(
            task_id="log_no_mapper_value",
            log="{{dag_run.conf.log}}",
            message="{{dag_run.conf.compensation_element}} is not allowed",
            severity="Ignored",
            properties={
                "employeeid": "{{dag_run.conf.employee_id}}",
                "guid": "{{dag_run.conf.guid}}",
                "status": "Ignored",
                "details": "{{dag_run.conf.compensation_element}} is not allowed"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "status": "Error",
                "details": '{{ get_error_message() }}',
                "employeeid": "{{dag_run.conf.employee_id}}"
            },
        )
        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        get_mapper_value >> is_mapper_value_present >> rail.Label(
            "No") >> log_no_mapper_value >> rail.Label("On Error") >> catch_and_log_errors
        is_mapper_value_present >> rail.Label("Yes") >> is_replicon_group_cost_center >> rail.Label("Yes") \
            >> process_cost_center >> wait_for_cost_center >> rail.Label("On Error") >> catch_and_log_errors
        is_replicon_group_cost_center >> rail.Label("No") >> is_replicon_group_business_unit >> rail.Label("Yes") \
            >> process_business_unit >> wait_for_business_unit >> rail.Label("On Error") >> catch_and_log_errors
        is_replicon_group_business_unit >> rail.Label("No") >> is_replicon_group_classification >> rail.Label("Yes") \
            >> process_classification >> wait_for_process_classification >> rail.Label("On Error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
