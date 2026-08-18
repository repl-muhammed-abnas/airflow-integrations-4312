"""
CRL Office Schedule Sync - Create Schedule Child DAG
Creates individual office schedules in Replicon
"""
from datetime import timedelta
import rail
from airflow.models import Variable
from crl.office_schedule_import_v1.utils import custom_methods, request_payload


def create_dag(config):
    """Create the child DAG for individual schedule creation"""
    with rail.create_airflow_dag(
        dag_id=config.create_schedule_dag_id,
        description=f'CRL office schedule import create child {config.dag_id_suffix}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        # View the DAG run configuration
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        # Check if batch task execution is enabled via Airflow variable
        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_schedule_log'
        )

        # Wrap schedule creation tasks in batch for error isolation
        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_schedule_log',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        # Create log for tracking this schedule creation
        create_schedule_log = rail.CreateLogOperator(
            task_id="create_schedule_log"
        )

        # Validate pattern format and date format
        validate_schedule_data = rail.PythonOperator(
            task_id='validate_schedule_data',
            python_callable=lambda dag_run: custom_methods.validate_schedule_data(
                dag_run.conf.get('pattern', ''),
                dag_run.conf.get('start_date', ''),
                config.start_date_format
            )
        )

        # Check validation result
        is_valid_schedule_data = rail.IfOperator(
            task_id='is_valid_schedule_data',
            test=lambda: rail.result('validate_schedule_data')[0],
            yes_task='parse_pattern',
            no_task='log_invalid_schedule_data'
        )

        # Log invalid schedule data (pattern or date format) as exception
        log_invalid_schedule_data = rail.WriteLogOperator(
            task_id='log_invalid_schedule_data',
            log='{{ result("create_schedule_log") }}',
            message='Invalid schedule data',
            severity='Exception',
            properties=lambda dag_run: {
                'schedule_name': dag_run.conf.get('schedule_name'),
                'pattern': dag_run.conf.get('pattern'),
                'start_date': dag_run.conf.get('start_date', ''),
                'action': 'Validation',
                'status': 'Exception',
                'details': f"Office Schedule not processed - {rail.result('validate_schedule_data')[1]}"
            }
        )

        # Parse pattern string into array of float values (X becomes 0.0)
        parse_pattern = rail.PythonOperator(
            task_id='parse_pattern',
            python_callable=lambda dag_run: custom_methods.parse_pattern_to_array(
                dag_run.conf.get('pattern')
            )
        )

        # Create new draft office schedule in Replicon
        create_new_draft = rail.RepliconServiceOperator(
            task_id='create_new_draft',
            endpoint='/services/OfficeScheduleService1.svc/CreateNewDraft'
        )

        # Update the draft schedule with the schedule name
        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint='/services/OfficeScheduleService1.svc/UpdateName',
            data=lambda dag_run: {
                "officeScheduleUri": rail.result('create_new_draft'),
                "name": dag_run.conf.get('schedule_name', '')
            }
        )

        # Check if schedule description is provided
        has_description = rail.IfOperator(
            task_id='has_description',
            test=lambda dag_run: bool(
                (dag_run.conf.get('schedule_description') or '').strip()),
            yes_task='update_description',
            no_task='is_non_standard_pattern'
        )

        # Update the draft schedule with description (if provided)
        update_description = rail.RepliconServiceOperator(
            task_id='update_description',
            endpoint='/services/OfficeScheduleService1.svc/UpdateDescription',
            data=lambda dag_run: {
                "officeScheduleUri": rail.result('create_new_draft'),
                "description": dag_run.conf.get('schedule_description', '')
            }
        )

        # Check if pattern is non-standard (length != 7) or standard (length == 7)
        is_non_standard_pattern = rail.IfOperator(
            task_id='is_non_standard_pattern',
            test=lambda dag_run: custom_methods.is_non_standard_pattern(
                dag_run.conf.get('pattern', '')
            ),
            yes_task='apply_recurring_schedule_pattern',
            no_task='apply_simple_schedule_pattern'
        )

        # Apply 7-day work pattern to the schedule (standard pattern)
        apply_simple_schedule_pattern = rail.RepliconServiceOperator(
            task_id='apply_simple_schedule_pattern',
            endpoint='/services/OfficeScheduleService1.svc/PutSimpleSchedulePattern',
            data=request_payload.get_simple_schedule_pattern_payload
        )

        # Apply non-7-day work pattern to the schedule (recurring pattern)
        apply_recurring_schedule_pattern = rail.RepliconServiceOperator(
            task_id='apply_recurring_schedule_pattern',
            endpoint='/services/OfficeScheduleService1.svc/PutRecurringSchedulePattern',
            data=lambda dag_run: request_payload.get_recurring_schedule_pattern_payload(
                dag_run.conf.get('start_date', ''), config.start_date_format)
        )

        # Publish the draft schedule to make it available for use
        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint='/services/OfficeScheduleService1.svc/PublishDraft',
            data=lambda: {
                "officeScheduleDraftUri": rail.result('create_new_draft')
            }
        )

        # Log successful schedule creation
        log_success = rail.WriteLogOperator(
            task_id='log_success',
            log='{{ result("create_schedule_log") }}',
            message='Office schedule created successfully',
            severity='Success',
            properties=lambda dag_run: {
                'schedule_name': dag_run.conf.get('schedule_name', ''),
                'pattern': dag_run.conf.get('pattern'),
                'start_date': dag_run.conf.get('start_date', ''),
                'action': 'Add',
                'status': 'Success',
                'details': 'Office schedule created and published successfully'
            }
        )

        # Catch and log any errors that occurred during schedule creation
        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log='{{ result("create_schedule_log") }}',
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties=lambda dag_run: {
                'schedule_name': dag_run.conf.get('schedule_name', ''),
                'pattern': dag_run.conf.get('pattern', ''),
                'start_date': dag_run.conf.get('start_date', ''),
                'action': 'Add',
                'status': 'Error',
                'details': '{{ get_error_message() }}'
            },
        )

        # Define task dependencies

        # Batch task flow
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> create_schedule_log

        # Validation and creation flow
        create_schedule_log >> validate_schedule_data >> is_valid_schedule_data
        is_valid_schedule_data >> rail.Label(
            'Yes') >> parse_pattern >> create_new_draft >> update_name >> has_description
        is_valid_schedule_data >> rail.Label(
            'No') >> log_invalid_schedule_data >> catch_and_log_errors

        # Description handling - routes to pattern type check
        has_description >> rail.Label(
            'Yes') >> update_description >> is_non_standard_pattern
        has_description >> rail.Label('No') >> is_non_standard_pattern

        # Pattern type branching (length == 7 vs length != 7)
        is_non_standard_pattern >> rail.Label(
            'Yes') >> apply_recurring_schedule_pattern >> publish_draft
        is_non_standard_pattern >> rail.Label(
            'No') >> apply_simple_schedule_pattern >> publish_draft

        # Final steps
        publish_draft >> log_success >> catch_and_log_errors

    return dag


# Create DAG for each instance
rail.for_each_instance(create_dag)
