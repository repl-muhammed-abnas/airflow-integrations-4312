import rail
from pendulum import datetime
from datetime import timedelta


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.post_to_bamboohr_dag_id,
        description="TransparentBPO Schedule Sync post to bamboohr",
        company_key=config.company_key,
        max_active_runs=config.max_active_subchild_runs,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        if_employee_id_blank = rail.IfOperator(
            task_id="if_employee_id_blank",
            test=lambda dag_run: not (dag_run.conf['employee_id']),
            yes_task='log_entry_blank_employee_id',
            no_task='post_to_bamboohr'
        )

        log_entry_blank_employee_id = rail.WriteLogOperator(
            task_id='log_entry_blank_employee_id',
            log="{{ dag_run.conf.schedule_update_logs}}",
            severity="na",
            message="exception",
            properties={
                'empid': "{{dag_run.conf.employee_id}}",
                'schedule': "{{dag_run.conf.current_schedule}}",
                'status': "Ignored",
                'details': "Employeeid is not present",
                'username': "{{dag_run.conf.user_name}}",
            }
        )

        post_to_bamboohr = rail.BambooHROperator(
            task_id='post_to_bamboohr',
            request_method='POST',
            company_domain="",
            endpoint="/employees/{{dag_run.conf.bamboohr_id}}",
            bamboohr_conn_id=config.bamboohr_conn_id,
            data={
                'customSchedule': '{{dag_run.conf.current_schedule}}'
            }
        )
        

        log_successful_user_schedule_sync = rail.WriteLogOperator(
            task_id='log_successful_user_schedule_sync',
            log="{{ dag_run.conf.schedule_update_logs}}",
            severity="Success",
            message="na",
            properties={
                'empid': "{{dag_run.conf.employee_id}}",
                'schedule': "{{dag_run.conf.current_schedule}}",
                'status': "Success",
                'details': "",
                'username': "{{dag_run.conf.user_name}}",

            }
        )

        log_error = rail.WriteLogOperator(
            task_id='log_error',
            log="{{ dag_run.conf.schedule_update_logs}}",
            trigger_rule="one_failed",
            severity="Error",
            message="na",
            properties={
                'empid': "{{dag_run.conf.employee_id}}",
                'schedule': "{{dag_run.conf.current_schedule}}",
                'status': "Error",
                'details': "{{get_error_message()}}",
                'username': "{{dag_run.conf.user_name}}",

            }
        )

        if_employee_id_blank >> rail.Label(
            "Yes") >> log_entry_blank_employee_id >> log_error
        if_employee_id_blank >> rail.Label(
            "No") >> post_to_bamboohr >> log_successful_user_schedule_sync >> log_error

    return dag


rail.for_each_instance(create_child_dag)
