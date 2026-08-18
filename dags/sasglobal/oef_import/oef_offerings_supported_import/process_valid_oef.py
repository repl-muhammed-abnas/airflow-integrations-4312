from sasglobal.oef_import.oef_offerings_supported_import.tasks.update_status import update_oef_status_task
from sasglobal.oef_import.oef_offerings_supported_import.utils import request_payload
from sasglobal.oef_import.oef_offerings_supported_import.utils import custom_methods
import rail

null = None

def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'sasglobal_oef_offerings_supported_import_process_valid_oef_child_{config.instance}',
        description=f'SaSGlobal OEF Offerings Supported Import Process Valid OEF Child DAG {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_input_status_exists = rail.IfOperator(
            task_id='is_input_status_exists',
            test='{{ dag_run.conf.object_data.status | is_truthy }}',
            yes_task='is_input_oef_name_length_exceed_50',
            no_task='log_status_not_present'
        )

        is_input_oef_name_length_exceed_50 = rail.IfOperator(
            task_id='is_input_oef_name_length_exceed_50',
            test=lambda dag_run: bool(len(dag_run.conf["object_data"]["value"]) > 50),
            yes_task='log_input_oef_name_len_exceed',
            no_task='get_existed_oef_uri'
        )

        get_existed_oef_uri = rail.PythonOperator(
            task_id='get_existed_oef_uri',
            python_callable=custom_methods.get_value_uri_already_exists
        )

        is_value_already_exists = rail.IfOperator(
            task_id='is_value_already_exists',
            test='{{ result("get_existed_oef_uri") | is_truthy }}',
            yes_task='are_status_same',
            no_task='create_oef_draft'
        )

        are_status_same = rail.IfOperator(
            task_id='are_status_same',
            test=custom_methods.check_statuses,
            yes_task='log_already_available',
            no_task='update_oef_status'
        )

        update_oef_status = rail.EmptyOperator(
            task_id='update_oef_status'
        )

        process_input_status, enable_or_disable_existed_oef = update_oef_status_task("existed", '{{result("get_existed_oef_uri")}}')

        create_oef_draft = rail.RepliconServiceOperator(
            task_id='create_oef_draft',
            endpoint='/services/ObjectExtensionTagService1.svc/CreateNewDraft',
            data={
                "objectExtensionTagDefinitionUri": "{{ dag_run.conf.offer_supported_oef_uri }}"
            }
        )

        update_name = rail.RepliconServiceOperator(
            task_id='update_name',
            endpoint='/services/ObjectExtensionTagService1.svc/UpdateName',
            data=request_payload.get_update_oef_name_payload
        )

        update_code = rail.RepliconServiceOperator(
            task_id='update_code',
            endpoint='/services/ObjectExtensionTagService1.svc/UpdateCode',
            data=request_payload.get_update_oef_code_payload
        )

        get_input_status, enable_or_disable_new_oef = update_oef_status_task("new", '{{result("create_oef_draft")}}')

        publish_draft = rail.RepliconServiceOperator(
            task_id='publish_draft',
            endpoint='/services/ObjectExtensionTagService1.svc/PublishDraft',
            data={
                "objectExtensionTagUri": "{{ result('create_oef_draft') }}"
            }
        )

        log_status_update = rail.WriteLogOperator(
            task_id='log_status_update',
            message="Tag value is available and updated as {{ dag_run.conf.object_data.status }} successfully",
            severity='Success',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Success",
                "details": "Tag value is available and updated as {{ dag_run.conf.object_data.status }} successfully"
            }
        )

        log_already_available = rail.WriteLogOperator(
            task_id='log_already_available',
            message="Tag value is available and the status is already {{ dag_run.conf.object_data.status }}",
            severity='Skipped',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Skipped",
                "details": "Tag value is available and the status is already {{ dag_run.conf.object_data.status }}"
            }
        )

        log_object_created = rail.WriteLogOperator(
            task_id='log_object_created',
            message="Created Successfully",
            severity='Success',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Success",
                "details": "Created Successfully"
            }
        )

        log_input_oef_name_len_exceed = rail.WriteLogOperator(
            task_id='log_input_oef_name_len_exceed',
            message="{{ dag_run.conf.object_data.value }} not created as the character length exceeds 50 characters",
            severity='Skipped',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Skipped",
                "details": "{{ dag_run.conf.object_data.value }} not created as the character length exceeds 50 characters"
            }
        )

        log_status_not_present = rail.WriteLogOperator(
            task_id='log_status_not_present',
            message="Staus is not present",
            severity='Skipped',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Skipped",
                "details": "Staus is not present"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity="Error",
            message='{{ get_error_message() }}',
            properties={
                "name": "{{ dag_run.conf.object_data.name }}",
                "value": "{{ dag_run.conf.object_data.value }}",
                "status": "{{ dag_run.conf.object_data.status }}",
                "processing_status": "Error",
                "details": '{{ get_error_message() }}'
            }
        )

        is_input_status_exists >> rail.Label("Yes") >> is_input_oef_name_length_exceed_50
        is_input_status_exists >> rail.Label("No") >> log_status_not_present >> catch_and_log_errors
        is_input_oef_name_length_exceed_50 >> rail.Label("No") >> get_existed_oef_uri >> is_value_already_exists >> rail.Label("Yes") >> are_status_same
        is_input_oef_name_length_exceed_50 >> rail.Label("Yes") >> log_input_oef_name_len_exceed >> catch_and_log_errors

        are_status_same >> rail.Label("Yes") >> log_already_available
        are_status_same >> rail.Label("No") >> update_oef_status >> process_input_status,enable_or_disable_existed_oef >> log_status_update

        log_status_update >> catch_and_log_errors

        is_value_already_exists >> rail.Label("No") >> create_oef_draft >> update_name >> update_code >> get_input_status,enable_or_disable_new_oef \
            >> publish_draft

        publish_draft >> log_object_created >> catch_and_log_errors

    return dag

rail.for_each_instance(create_main_dag)
