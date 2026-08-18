from datetime import timedelta
from airflow.models import Variable
from sunovion.project_task_import.utils import request_payload
from sunovion.project_task_import.utils import response_filter
from sunovion.project_task_import.utils import custom_methods
import rail

# pylint: disable=too-many-statements


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'sunovion_project_sync_update_project_child_{config.instance}',
        description='Sunovion Project and Task Sync - Update Project',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_each_code,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id="can_run_batch_task",
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='false').lower() == 'true',
            yes_task="batch_task",
            no_task="get_project_records"
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_project_records',
            end_task="catch_and_log_errors",
        )

        get_project_records = rail.PythonOperator(
            task_id="get_project_records",
            python_callable=custom_methods.get_project_records
        )

        get_project_data = rail.RepliconServiceOperator(
            task_id="get_project_data",
            endpoint="services/ProjectService1.svc/GetProjectDetails",
            data=request_payload.get_project_data,
        )

        is_name_updated = rail.IfOperator(
            task_id='is_name_updated',
            test=request_payload.is_name_updated,
            yes_task='update_name',
            no_task='is_start_date_present'
        )

        update_name = rail.RepliconServiceOperator(
            task_id="update_name",
            endpoint="services/ProjectService1.svc/UpdateName",
            data=request_payload.get_update_name,
        )

        log_project_name_update_success = rail.WriteLogOperator(
            task_id="log_project_name_update_success",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Project Name "{{ result("get_project_data").name }}" updated to ' +
                '"{{ result("get_project_records")[0].projectname }} - {{ dag_run.conf.projectcode }}"',
            severity='Updated',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Updated',
                'details': '{{ dag_run_ecid() }} - Project Name "{{ result("get_project_data").name }}" updated to ' +
                    '"{{ result("get_project_records")[0].projectname }} - {{ dag_run.conf.projectcode }}"'
            }
        )

        is_start_date_present = rail.IfOperator(
            task_id='is_start_date_present',
            test=request_payload.is_start_date_present,
            yes_task='is_start_date_correct',
            no_task='is_end_date_present'
        )

        is_start_date_correct = rail.IfOperator(
            task_id='is_start_date_correct',
            test=request_payload.is_start_date_correct,
            yes_task='is_end_date_present',
            no_task='log_start_date_incorrect'
        )

        log_start_date_incorrect = rail.WriteLogOperator(
            task_id="log_start_date_incorrect",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Invalid Start Date',
            severity='Failed',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Invalid Start Date'
            }
        )

        is_end_date_present = rail.IfOperator(
            task_id='is_end_date_present',
            test=request_payload.is_end_date_present,
            yes_task='is_end_date_correct',
            no_task='is_project_date_present'
        )

        is_end_date_correct = rail.IfOperator(
            task_id='is_end_date_correct',
            test=request_payload.is_end_date_correct,
            yes_task='is_project_date_present',
            no_task='log_end_date_incorrect'
        )

        log_end_date_incorrect = rail.WriteLogOperator(
            task_id="log_end_date_incorrect",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Invalid End Date',
            severity='Failed',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Invalid End Date'
            }
        )

        is_project_date_present = rail.IfOperator(
            task_id='is_project_date_present',
            test=request_payload.is_project_date_present,
            yes_task='is_start_end_in_feed_file',
            no_task='is_project_description_present'
        )

        is_start_end_in_feed_file = rail.IfOperator(
            task_id='is_start_end_in_feed_file',
            test=request_payload.is_start_end_in_feed_file,
            yes_task='update_date_range_project',
            no_task='is_start_date_feed_file'
        )

        update_date_range_project = rail.RepliconServiceOperator(
            task_id="update_date_range_project",
            endpoint="services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=request_payload.update_date_range_project,
        )

        is_start_date_feed_file = rail.IfOperator(
            task_id='is_start_date_feed_file',
            test=request_payload.is_start_date_feed_file,
            yes_task='update_start_date_project',
            no_task='is_project_description_present'
        )

        update_start_date_project = rail.RepliconServiceOperator(
            task_id="update_start_date_project",
            endpoint="services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=request_payload.update_start_date_project,
        )

        is_project_description_present = rail.IfOperator(
            task_id='is_project_description_present',
            test=request_payload.is_project_description_present,
            yes_task='update_description',
            no_task='get_custom_field_groups'
        )

        update_description = rail.RepliconServiceOperator(
            task_id="update_description",
            endpoint="services/ProjectService1.svc/UpdateDescription",
            data=request_payload.update_description,
        )

        get_custom_field_groups = rail.RepliconServiceOperator(
            task_id="get_custom_field_groups",
            endpoint="services/CustomFieldService1.svc/GetCustomFieldGroups",
            data={},
            response_filter=response_filter.map_custom_field_groups
        )

        get_registration_udf_uri = rail.RepliconServiceOperator(
            task_id="get_registration_udf_uri",
            endpoint="services/CustomFieldService1.svc/GetAllCustomFields",
            data=lambda: {
                "objectUri": rail.result('get_custom_field_groups')[0]['uri']
            },
            response_filter=response_filter.map_registration_udf_uri
        )

        get_enabled_custom_field_dropdown_option = rail.RepliconServiceOperator(
            task_id="get_enabled_custom_field_dropdown_option",
            endpoint="services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions",
            data=lambda: {
                "customFieldUri": rail.result('get_registration_udf_uri')[0]['uri']
            }
        )

        is_custom_field_registered = rail.IfOperator(
            task_id='is_custom_field_registered',
            test=request_payload.is_cutsom_field_registered,
            yes_task='update_dropdown_registered',
            no_task='is_custom_field_non_registered'
        )

        update_dropdown_registered = rail.RepliconServiceOperator(
            task_id="update_dropdown_registered",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.update_dropdown_registered
        )

        is_custom_field_non_registered = rail.IfOperator(
            task_id='is_custom_field_non_registered',
            test=request_payload.is_cutsom_field_non_registered,
            yes_task='update_dropdown_non_registered',
            no_task='is_custom_field_non_present'
        )

        update_dropdown_non_registered = rail.RepliconServiceOperator(
            task_id="update_dropdown_non_registered",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.update_dropdown_non_registered
        )

        is_custom_field_non_present = rail.IfOperator(
            task_id='is_custom_field_non_present',
            test=request_payload.is_custom_field_non_present,
            yes_task='update_dropdown_non_present',
            no_task='is_end_date_present_feed_file'
        )

        update_dropdown_non_present = rail.RepliconServiceOperator(
            task_id="update_dropdown_non_present",
            endpoint="services/CustomFieldService1.svc/UpdateDropdownValue",
            data=request_payload.update_dropdown_non_present
        )

        is_end_date_present_feed_file = rail.IfOperator(
            task_id='is_end_date_present_feed_file',
            test=request_payload.is_end_date_present,
            yes_task='update_project_status',
            no_task='log_project_updated'
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id="update_project_status",
            endpoint="services/ProjectService1.svc/UpdateStatus",
            data=request_payload.update_project_status
        )

        log_project_status_update_success = rail.WriteLogOperator(
            task_id="log_project_status_update_success",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Project marked Completed',
            severity='Success',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Project marked Completed'
            }
        )

        log_project_updated = rail.WriteLogOperator(
            task_id="log_project_updated",
            log='{{ dag_run.conf.log }}',
            message='{{ dag_run_ecid() }} - Project Updated',
            severity='Success',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Success',
                'details': '{{ dag_run_ecid() }} - Project Updated'
            }
        )

        get_children_task_details = rail.RepliconServiceOperator(
            task_id="get_children_task_details",
            endpoint="services/TaskService1.svc/GetChildrenTaskDetails",
            data=request_payload.get_children_task_details
        )

        process_each_task = rail.TriggerDagRunForEachItemOperator(
            task_id='process_each_task',
            items='{{ result("get_project_records") | to_json }}',
            trigger_dag_id=f'sunovion_project_sync_process_each_task_child_{config.instance}',
            conf=request_payload.process_each_task_conf,
            execution_timeout=timedelta(hours=config.execution_timeout_days),
            retries=0,
        )

        wait_for_process_each_task = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_each_task',
            dag_runs='{{ result("process_each_task") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            log='{{ dag_run.conf.log }}',
            severity='Failed',
            message='{{ dag_run_ecid() }} - Project Update - {{ get_error_message() }}',
            properties={
                'projectcode': '{{ result("get_project_records")[-1].projectcode }} / {{ result("get_project_records")[-1].projectname }}',
                'taskcode': '-',
                'status': 'Failed',
                'details': '{{ dag_run_ecid() }} - Project Update - {{ get_error_message() }}',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label(
            "Yes") >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label("No") >> get_project_records
        get_project_records >> get_project_data >> is_name_updated
        is_name_updated >> rail.Label(
            "Yes") >> update_name >> log_project_name_update_success >> is_start_date_present
        is_name_updated >> rail.Label("No") >> is_start_date_present
        is_start_date_present >> rail.Label("Yes") >> is_start_date_correct
        is_start_date_present >> rail.Label("No") >> is_end_date_present
        is_start_date_correct >> rail.Label("Yes") >> is_end_date_present
        is_start_date_correct >> rail.Label(
            "No") >> log_start_date_incorrect >> catch_and_log_errors >> log_to_sumo
        is_end_date_present >> rail.Label("No") >> is_project_date_present
        is_end_date_present >> rail.Label("Yes") >> is_end_date_correct
        is_end_date_correct >> rail.Label("Yes") >> is_project_date_present
        is_end_date_correct >> rail.Label(
            "No") >> log_end_date_incorrect >> catch_and_log_errors
        is_project_date_present >> rail.Label(
            "Yes") >> is_start_end_in_feed_file
        is_project_date_present >> rail.Label(
            "No") >> is_project_description_present
        is_start_end_in_feed_file >> rail.Label(
            "Yes") >> update_date_range_project >> is_start_date_feed_file
        is_start_end_in_feed_file >> rail.Label(
            "No") >> is_start_date_feed_file
        is_start_date_feed_file >> rail.Label(
            "Yes") >> update_start_date_project >> is_project_description_present
        is_start_date_feed_file >> rail.Label(
            "No") >> is_project_description_present
        is_project_description_present >> rail.Label(
            "Yes") >> update_description >> get_custom_field_groups
        is_project_description_present >> rail.Label(
            "No") >> get_custom_field_groups >> get_registration_udf_uri >> get_enabled_custom_field_dropdown_option
        get_enabled_custom_field_dropdown_option >> is_custom_field_registered
        is_custom_field_registered >> rail.Label(
            "Yes") >> update_dropdown_registered >> is_custom_field_non_registered
        is_custom_field_registered >> rail.Label(
            "No") >> is_custom_field_non_registered
        is_custom_field_non_registered >> rail.Label(
            "Yes") >> update_dropdown_non_registered >> is_custom_field_non_present
        is_custom_field_non_registered >> rail.Label(
            "No") >> is_custom_field_non_present
        is_custom_field_non_present >> rail.Label(
            "Yes") >> update_dropdown_non_present >> is_end_date_present_feed_file
        is_custom_field_non_present >> rail.Label(
            "No") >> is_end_date_present_feed_file
        is_end_date_present_feed_file >> rail.Label(
            "Yes") >> update_project_status >> log_project_status_update_success >> log_project_updated
        is_end_date_present_feed_file >> rail.Label(
            "No") >> log_project_updated >> get_children_task_details >> process_each_task >> wait_for_process_each_task >> catch_and_log_errors
    return dag


rail.for_each_instance(create_child_dag_wbs)
