import rail
from dxctechnology.gsap_task_import.tasks.update_tasks import update_tasks
from dxctechnology.gsap_task_import.utils import custom_methods


def create_update_gsap_task_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.gsap_wbs_update_task_dagid,
        description=f"DXCTechnology GSAP task import update gsap task {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_tasks_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_run")

        is_date_range_valid, can_update_task, update_task, finish = update_tasks(
            config, "gsap", config.gsap_wbs_create_task_dagid)

        log_date_outside_project_date = rail.WriteLogOperator(
            task_id='log_date_outside_project_date',
            message="{{dag_run.conf.task_name}}'s given date is outside of project start, end date",
            items='[{{dag_run.conf | to_json}}]',
            severity="Skipped",
            properties=custom_methods.get_log_out_of_range,
        )

        log_unchanged_record = rail.WriteLogOperator(
            task_id='log_unchanged_record',
            message='{{dag_run.conf.task_name}} No change to task record',
            severity="Skipped",
            properties={
                    'wbs': '{{ dag_run.conf.project_name }}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': 'Skipped',
                    'details': "No change to task record for {{dag_run.conf.billingkey_task_name}}"
            },
        )

        log_successful_update_completion = rail.WriteLogOperator(
            task_id='log_successful_update_completion',
            message='{{dag_run.conf.task_name}} Updated successfully',
            severity="Success",
            properties={
                    'wbs': '{{ dag_run.conf.project_name}}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': 'Success',
                    'details': "Updated successfully for {{dag_run.conf.billingkey_task_name}}"
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                    'wbs': '{{ dag_run.conf.project_name }}',
                    'task': '{{ dag_run.conf.task_name }}',
                    'status': "Error",
                    # pylint: disable= line-too-long
                    'details': '{{ get_error_message()}}' + " for {{dag_run.conf.billingkey_task_name}}"
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        is_date_range_valid
        update_task >> log_successful_update_completion >> rail.Label(
            "On error") >> catch_and_log_errors
        can_update_task >> rail.Label("No") >> log_unchanged_record >> rail.Label(
            "On error") >> catch_and_log_errors
        finish >> rail.Label("On error") >> catch_and_log_errors
        is_date_range_valid >> rail.Label("No") >> log_date_outside_project_date >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_update_gsap_task_dag)
