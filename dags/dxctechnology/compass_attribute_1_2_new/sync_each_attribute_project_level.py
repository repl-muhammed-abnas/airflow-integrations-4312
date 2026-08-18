import rail
from dxctechnology.compass_attribute_1_2_new import request_payload
from dxctechnology.compass_attribute_1_2_new import response_filter


def create_child_sync_each_attribute_project_level(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_child_sync_each_attribute_project_level{dag_id_postfix}_{config.sub_erp}_{config.attribute}',
        description=f'Sync Each Attribute At Project Level {config.instance} {config.sub_erp} {config.attribute}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_sync_attribute_1_2_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_end_date_prior_to_start_date = rail.IfOperator(
            task_id="is_end_date_prior_to_start_date",
            test=request_payload.is_end_date_before_start_date,
            yes_task='log_end_date_prior_to_start_date',
            no_task='get_specific_attribute_uri_system_level',
        )

        log_end_date_prior_to_start_date = rail.WriteLogOperator(
            task_id='log_end_date_prior_to_start_date',
            message="Attribute Failed to Sync As End Date is Prior to Start Date of WBS",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.WBS }}",
                'attributename': "{{ dag_run.conf.attribute_value }}" + " - " + "{{ dag_run.conf.Description }}",
                'attributenumber': "{{ dag_run.conf.AttributeNumber }}",
                'action': 'Update',
                'status': "Ignored",
                'recordcount': '1',
            }
        )

        get_specific_attribute_uri_system_level = rail.RepliconServiceOperator(
            task_id="get_specific_attribute_uri_system_level",
            endpoint="services/ObjectExtensionTagListService1.svc/GetData",
            data=request_payload.get_specific_attribute_system_level_payload,
            response_filter=response_filter.map_get_attribute_system_level_project
        )

        is_system_attribute_present = rail.IfOperator(
            task_id="is_system_attribute_present",
            test="{{ result('get_specific_attribute_uri_system_level') | length > 0 }}",
            yes_task='get_specific_attribute_project_level',
            no_task='log_failure_attribute_system',
        )

        log_failure_attribute_system = rail.WriteLogOperator(
            task_id='log_failure_attribute_system',
            message="Attribute Failed To Retrieve From System Level",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.WBS }}",
                'attributename': "{{ dag_run.conf.attribute_value }}" + " - " + "{{ dag_run.conf.Description }}",
                'attributenumber': "{{ dag_run.conf.AttributeNumber }}",
                'action': 'Update',
                'status': "Error",
                'recordcount': '1',
            }
        )

        get_specific_attribute_project_level = rail.RepliconServiceOperator(
            task_id="get_specific_attribute_project_level",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/GetPageOfProjectDependentTimeEntryObjectExtensionTags",
            data=request_payload.get_specific_attribute_project_level,
            response_filter=response_filter.map_get_specific_attribute_project_level
        )

        is_attribute_present_project = rail.IfOperator(
            task_id="is_attribute_present_project",
            test="{{ result('get_specific_attribute_project_level') | length > 0 }}",
            yes_task='update_attribute_end_date_project',
            no_task='add_attribute_end_date_project',
        )

        update_attribute_end_date_project = rail.RepliconServiceOperator(
            task_id="update_attribute_end_date_project",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.get_update_attribute_end_date_project
        )

        log_update_success_record = rail.WriteLogOperator(
            task_id='log_update_success_record',
            message="Attribute Updated Successfully",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.WBS }}",
                'attributename': "{{ dag_run.conf.attribute_value }}" + " - " + "{{ dag_run.conf.Description }}",
                'attributenumber': "{{ dag_run.conf.AttributeNumber }}",
                'action': 'Updated',
                'status': "Success",
                'recordcount': '1',
            }
        )

        add_attribute_end_date_project = rail.RepliconServiceOperator(
            task_id="add_attribute_end_date_project",
            endpoint="services/ProjectDependentTimeEntryObjectExtensionFieldService1.svc/ApplyModificationsForProjectTimeEntryDependentObjectExtensionTags",
            data=request_payload.get_update_attribute_end_date_project
        )

        log_add_success_record = rail.WriteLogOperator(
            task_id='log_add_success_record',
            message="Attribute Added Successfully",
            properties={
                'Level': "Project",
                'wbs': "{{ dag_run.conf.WBS }}",
                'attributename': "{{ dag_run.conf.attribute_value }}" + " - " + "{{ dag_run.conf.Description }}",
                'attributenumber': "{{ dag_run.conf.AttributeNumber }}",
                'action': 'Added',
                'status': "Success",
                'recordcount': '1',
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'Level': "Project",
                'wbs': "{{dag_run.conf.WBS}}",
                'attributename': "{{ dag_run.conf.attribute_value }}" + " - " + "{{ dag_run.conf.Description }}",
                'attributenumber': "{{dag_run.conf.AttributeNumber}}",
                'action': 'na',
                'status': "Error",
                'recordcount': '1',
            })

        is_end_date_prior_to_start_date >> rail.Label(
            "YES") >> log_end_date_prior_to_start_date >> catch_and_log_errors
        is_end_date_prior_to_start_date >> rail.Label(
            "NO") >> get_specific_attribute_uri_system_level
        get_specific_attribute_uri_system_level >> is_system_attribute_present >> rail.Label(
            "YES") >> get_specific_attribute_project_level
        is_system_attribute_present >> rail.Label(
            "NO") >> log_failure_attribute_system >> catch_and_log_errors
        get_specific_attribute_project_level >> is_attribute_present_project
        is_attribute_present_project >> rail.Label(
            "YES") >> update_attribute_end_date_project >> log_update_success_record >> catch_and_log_errors
        is_attribute_present_project >> rail.Label(
            "NO") >> add_attribute_end_date_project >> log_add_success_record >> catch_and_log_errors

    return dag


rail.for_each_instance(create_child_sync_each_attribute_project_level)
