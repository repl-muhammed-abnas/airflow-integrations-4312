from datetime import timedelta
from airflow.models import Variable
import rail
from tsystems.jira_time_import_v2.utils import custom_methods

def create_child_dag(config):
    """
    Creates the Child DAG for log generation and reporting.
    
    This DAG handles the final processing step including log formatting,
    CSV report generation, file upload, and email notification.

    Args:
        config: Configuration module containing instance-specific settings,
                email configurations, and file path settings
    
    Returns:
        Airflow DAG: The configured child DAG for log generation and reporting
    """
    with rail.create_airflow_dag(
        dag_id=config.process_log_generation,
        description=f'T-Systems Jira Time Import Child - Process Log Generation',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_log_gen_child
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Task: Check if batch processing mode is enabled
        # Controls execution flow for debugging vs production processing
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='format_logs'
        )

        # Task: Execute entry processing pipeline in batch mode
        # Wraps individual entry processing for monitoring and error handling
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='format_logs',
            end_task='batch_end',
        )

        # Task: Process and consolidate logs from all processing activities
        # Aggregates logs from record validation and time entry processing
        format_logs = rail.PythonOperator(
            task_id='format_logs',
            python_callable=custom_methods.do_format_logs
        )

        get_email_log_details = rail.PythonOperator(
            task_id='get_email_log_details',
            python_callable=lambda dag_run: custom_methods.get_email_log_details(
                config.log_filepath, dag_run, config.STANDARD_EMAIL_DATE_FORMAT)
        )

        # Task: Trigger main email notification with all logs
        trigger_all_logs_email_notification = rail.TriggerDagRunOperator(
            task_id='trigger_all_logs_email_notification',
            trigger_dag_id=config.send_email_notification_child,
            conf=lambda dag_run: {
                **rail.result('get_email_log_details'),
                'log_file_name': f"log_{dag_run.conf['log_file_suffix']}",
                'email': config.tenant_email,
                'log_details': rail.write_json_artifact(rail.load_json_artifact(rail.result('format_logs'))['all_logs']),
                'notification_type': 'all_logs',
                'email_template': 'import_complete.html'
            }
        )
        
        # Task: Trigger email notifications for each user
        trigger_user_email_notifications = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_user_email_notifications',
            items=lambda: [{'email': email, 'log_details': user_data}
                for email, user_data in rail.load_json_artifact(rail.result('format_logs'))['users'].items()],
            trigger_dag_id=config.send_email_notification_child,
            conf=lambda item, dag_run, index: {
                **rail.result('get_email_log_details'),
                'log_file_name': f"log_EMP_{index}_{dag_run.conf['log_file_suffix']}",
                'email': item['email'],
                'log_details': rail.write_json_artifact(item['log_details']),
                'notification_type': 'end_user',
                'email_template': 'import_complete_users_pm.html'
            }
        )
        
        # Task: Trigger email notifications for each project manager
        trigger_pm_email_notifications = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_pm_email_notifications',
            items=lambda: [{'email': email, 'log_details': pm_data}
                for email, pm_data in rail.load_json_artifact(rail.result('format_logs'))['project_managers'].items()],
            trigger_dag_id=config.send_email_notification_child,
            conf=lambda item, dag_run, index: {
                **rail.result('get_email_log_details'),
                'log_file_name': f"log_PM_{index}_{dag_run.conf['log_file_suffix']}",
                'email': item['email'],
                'log_details': rail.write_json_artifact(item['log_details']),
                'notification_type': 'project_manager',
                'email_template': 'import_complete_users_pm.html'
            }
        )

        batch_end = rail.EmptyOperator(
            task_id='batch_end'
        )

        can_run_batch_task >> rail.Label("Yes") >> batch_task >> batch_end
        can_run_batch_task >> rail.Label("No") >> format_logs
        format_logs >> get_email_log_details >> trigger_all_logs_email_notification >> trigger_user_email_notifications >> trigger_pm_email_notifications >> batch_end

    return dag

rail.for_each_instance(create_child_dag)
