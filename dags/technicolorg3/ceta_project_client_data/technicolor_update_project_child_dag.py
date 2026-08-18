from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.ceta_project_client_data.utils import request_payload
from technicolorg3.ceta_project_client_data.utils import response_filter
from technicolorg3.ceta_project_client_data.tasks.update_project_manager import get_update_project_manager_id
from technicolorg3.ceta_project_client_data.tasks.project_mandatory_fields import get_project_mandatory_fields
from technicolorg3.ceta_project_client_data.tasks.project_logs import get_project_logs
from technicolorg3.ceta_project_client_data.tasks.project_catch_sumo_logs import get_project_catch_sumo_logs

null = None

# pylint: disable=too-many-statements


def create_update_project_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_udpate_project_{config.instance}',
        description=f'Technicolor CETA Update project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        action = 'update_project'

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='start_project_update'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='start_project_update',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task=f'catch_and_log_errors_{action}',
        )

        start_project_update = rail.EmptyOperator(
            task_id='start_project_update'
        )

        client_project_logs, project_mandatory_fields_end, log_project_fields_missing = get_project_mandatory_fields(
            action, config)

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint='/services/ProjectService1.svc/GetProjectDetails',
            data={"projectUri": '{{ dag_run.conf.projecturi }}'},
            response_filter=response_filter.get_project_details_response
        )

        is_project_status = rail.IfOperator(
            task_id='is_project_status',
            test=lambda dag_run: bool(dag_run.conf['projectstatus'].lower(
            ) != 'confirmed' and dag_run.conf['projectstatus'].lower() != 'new'),
            yes_task='get_project_status',
            no_task='projectmanager_id_start'
        )

        get_project_status = rail.IfOperator(
            task_id='get_project_status',
            test=lambda: bool(rail.result("get_project_details")[
                              'statusname'] == 'In Progress'),
            yes_task='update_project_status',
            no_task='projectmanager_id_start'
        )

        update_project_status = rail.RepliconServiceOperator(
            task_id="update_project_status",
            endpoint="/services/ProjectService1.svc/UpdateStatus",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi'],
                "projectStatusUri": "urn:replicon:project-status-type:completed"
            }
        )

        log_project_status_update_completed = rail.WriteLogOperator(
            task_id='log_project_status_update_completed',
            log='{{ result("client_project_logs_'+action+'") }}',
            message='The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" is completed successfully',
            properties={
                'db': '{{ dag_run.conf.millmpc }}',
                'client': '{{ dag_run.conf.clientname }}',
                'project': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'action': 'Update Project',
                'details': 'The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" is completed successfully',
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        projectmanager_id_start = rail.EmptyOperator(
            task_id='projectmanager_id_start',
        )

        process_update_project_manager = get_update_project_manager_id(
            action)

        projectmanager_id_finish = rail.EmptyOperator(
            task_id='projectmanager_id_finish'
        )

        is_project_type_updated = rail.IfOperator(
            task_id='is_project_type_updated',
            test=lambda dag_run: bool(dag_run.conf['projecttype'] !=
                                      rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                           'Project Type', 'value')),
            yes_task='is_project_type_uri_present',
            no_task='is_project_classification_updated'
        )

        is_project_type_uri_present = rail.IfOperator(
            task_id='is_project_type_uri_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                   'Project Type', 'customfielduri')),
            yes_task='get_dropdownoptions_project_type',
            no_task='is_project_classification_updated'
        )

        get_dropdownoptions_project_type = rail.RepliconServiceOperator(
            task_id='get_dropdownoptions_project_type',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data=lambda: {"customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                                 'Project Type', 'customfielduri')},
            data_handler=response_filter.get_customfields_project_type
        )

        is_project_type_dropdown_present = rail.IfOperator(
            task_id='is_project_type_dropdown_present',
            test=lambda dag_run: bool(dag_run.conf['projecttype'] and not rail.result(
                'get_dropdownoptions_project_type')),
            yes_task='process_dropdown_add_project_type',
            no_task='update_dropdown_value_projecttype'
        )

        process_dropdown_add_project_type = rail.TriggerDagRunOperator(
            task_id='process_dropdown_add_project_type',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_dropdown_option_addition_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'dropdownoption': dag_run.conf['projecttype'],
                'customfielduri': rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                       'Project Type', 'customfielduri')
            }
        )

        wait_for_process_dropdown_add_project_type = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_dropdown_add_project_type',
            dag_runs='{{ result("process_dropdown_add_project_type") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_dropdown_result_for_projecttype = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_dropdown_result_for_projecttype',
            dag_runs='{{ result("process_dropdown_add_project_type") }}',
            dagrun_task_id='put_dropdown_options',
            flatten=True
        )

        is_dropdown_add_for_projecttype_success = rail.IfOperator(
            task_id='is_dropdown_add_for_projecttype_success',
            test=lambda: not bool(rail.result(
                'gather_dropdown_result_for_projecttype', key='error')),
            yes_task='get_enabled_dropdown_project_buckets',
            no_task='update_dropdown_value_projecttype'
        )

        get_enabled_dropdown_project_buckets = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_project_buckets',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data=lambda: {"customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                                 'Project Type', 'customfielduri')},
            data_handler=response_filter.get_dropdown_options_project_buckets
        )

        update_dropdown_value_projecttype = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_projecttype',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data=request_payload.get_update_dropdown_value_projecttype,
        )

        is_project_classification_updated = rail.IfOperator(
            task_id='is_project_classification_updated',
            test=lambda dag_run: bool(dag_run.conf['projectclassification'] !=
                                      rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                           'Project Classification', 'value')),
            yes_task='is_project_classification_uri_present',
            no_task='is_project_product_name_updated'
        )

        is_project_classification_uri_present = rail.IfOperator(
            task_id='is_project_classification_uri_present',
            test=lambda: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                   'Project Classification', 'customfielduri')),
            yes_task='get_dropdownoptions_project_classification',
            no_task='is_project_product_name_updated'
        )

        get_dropdownoptions_project_classification = rail.RepliconServiceOperator(
            task_id='get_dropdownoptions_project_classification',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data=lambda: {"customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                                 'Project Classification', 'customfielduri')},
            data_handler=response_filter.get_customfields_project_classification
        )

        is_project_classification_dropdown_present = rail.IfOperator(
            task_id='is_project_classification_dropdown_present',
            test=lambda dag_run: bool(dag_run.conf['projectclassification'] and not rail.result(
                'get_dropdownoptions_project_classification')),
            yes_task='process_dropdown_add_project_classification',
            no_task='update_dropdown_value_project_classification'
        )

        process_dropdown_add_project_classification = rail.TriggerDagRunOperator(
            task_id='process_dropdown_add_project_classification',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_dropdown_option_addition_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'dropdownoption': dag_run.conf['projecttype'],
                'customfielduri': rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                       'Project Classification', 'customfielduri')
            }
        )

        wait_for_process_dropdown_add_project_classification = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_dropdown_option_addition_project_classification',
            dag_runs='{{ result("process_dropdown_add_project_classification") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_dropdown_result_for_project_classification = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_dropdown_result_for_project_classification',
            dag_runs='{{ result("process_dropdown_add_project_classification") }}',
            dagrun_task_id='put_dropdown_options',
            flatten=True
        )

        is_dropdown_add_for_project_classification_success = rail.IfOperator(
            task_id='is_dropdown_add_for_project_classification_success',
            test=lambda: not bool(rail.result(
                'gather_dropdown_result_for_project_classification', key='error')),
            yes_task='get_enabled_dropdown_project_classification',
            no_task='update_dropdown_value_project_classification'
        )

        get_enabled_dropdown_project_classification = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_project_classification',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data=lambda: {"customFieldUri": rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name',
                                                                                 'Project Classification', 'customfielduri')},
            data_handler=response_filter.get_dropdown_options_project_classification
        )

        update_dropdown_value_project_classification = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_project_classification',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data=request_payload.get_update_dropdown_value_project_classification,
        )

        is_project_product_name_updated = rail.IfOperator(
            task_id='is_project_product_name_updated',
            # pylint: disable=line-too-long
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(rail.result('get_project_details')['customfields'], 'name', 'Product Name', 'value') != dag_run.conf['productname']
                                      or rail.result('get_project_details')['projectname'].split('|')[0] != dag_run.conf['projectname']),
            yes_task='is_product_name_present',
            no_task='exception_messages_start'
        )

        is_product_name_present = rail.IfOperator(
            task_id='is_product_name_present',
            test=lambda dag_run: bool(dag_run.conf['productname']),
            yes_task='update_product_name',
            no_task='is_project_name_different'
        )

        update_product_name = rail.RepliconServiceOperator(
            task_id='update_product_name',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data=request_payload.get_update_product_name_payload,
        )

        is_project_name_different = rail.IfOperator(
            task_id='is_project_name_different',
            test=lambda dag_run: bool(rail.result('get_project_details')[
                                      'projectname'] != dag_run.conf['projectname']),
            yes_task='update_project_name',
            no_task='exception_messages_start'
        )

        update_project_name = rail.RepliconServiceOperator(
            task_id='update_project_name',
            endpoint='/services/ProjectService1.svc/UpdateName',
            data=request_payload.get_update_project_name_payload,
        )

        exception_messages_start = rail.EmptyOperator(
            task_id='exception_messages_start'
        )

        process_exception_messages = get_project_logs(
            action)

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors, log_to_sumo = get_project_catch_sumo_logs(
            action, config)

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label(
            'No') >> start_project_update >> client_project_logs

        project_mandatory_fields_end >> get_project_details >> is_project_status
        log_project_fields_missing >> finish

        is_project_status >> rail.Label(
            'Yes') >> get_project_status
        is_project_status >> rail.Label(
            'No') >> projectmanager_id_start

        get_project_status >> rail.Label(
            'Yes') >> update_project_status >> log_project_status_update_completed >> finish
        get_project_status >> rail.Label(
            'No') >> projectmanager_id_start

        projectmanager_id_start >> process_update_project_manager >> projectmanager_id_finish >> is_project_type_updated

        is_project_type_updated >> rail.Label(
            'Yes') >> is_project_type_uri_present
        is_project_type_updated >> rail.Label(
            'No') >> is_project_classification_updated

        is_project_type_uri_present >> rail.Label(
            'Yes') >> get_dropdownoptions_project_type >> is_project_type_dropdown_present
        is_project_type_uri_present >> rail.Label(
            'No') >> is_project_classification_updated

        is_project_type_dropdown_present >> rail.Label(
            'Yes') >> process_dropdown_add_project_type >> wait_for_process_dropdown_add_project_type \
            >> gather_dropdown_result_for_projecttype >> is_dropdown_add_for_projecttype_success
        is_project_type_dropdown_present >> rail.Label(
            'No') >> update_dropdown_value_projecttype

        is_dropdown_add_for_projecttype_success >> rail.Label(
            'Yes') >> get_enabled_dropdown_project_buckets >> update_dropdown_value_projecttype
        is_dropdown_add_for_projecttype_success >> rail.Label(
            'No') >> update_dropdown_value_projecttype >> is_project_classification_updated

        is_project_classification_updated >> rail.Label(
            'Yes') >> is_project_classification_uri_present
        is_project_classification_updated >> rail.Label(
            'No') >> is_project_product_name_updated

        is_project_classification_uri_present >> rail.Label(
            'Yes') >> get_dropdownoptions_project_classification >> is_project_classification_dropdown_present
        is_project_classification_uri_present >> rail.Label(
            'No') >> is_project_product_name_updated

        is_project_classification_dropdown_present >> rail.Label(
            'Yes') >> process_dropdown_add_project_classification >> wait_for_process_dropdown_add_project_classification \
            >> gather_dropdown_result_for_project_classification >> is_dropdown_add_for_project_classification_success
        is_project_classification_dropdown_present >> rail.Label(
            'No') >> update_dropdown_value_project_classification

        is_dropdown_add_for_project_classification_success >> rail.Label(
            'Yes') >> get_enabled_dropdown_project_classification >> update_dropdown_value_project_classification
        is_dropdown_add_for_project_classification_success >> rail.Label(
            'No') >> update_dropdown_value_project_classification >> is_project_product_name_updated

        is_project_product_name_updated >> rail.Label(
            'Yes') >> is_product_name_present
        is_project_product_name_updated >> rail.Label(
            'No') >> exception_messages_start

        is_product_name_present >> rail.Label(
            'Yes') >> update_product_name >> is_project_name_different
        is_product_name_present >> rail.Label(
            'No') >> is_project_name_different

        is_project_name_different >> rail.Label(
            'Yes') >> update_project_name >> exception_messages_start
        is_project_name_different >> rail.Label(
            'No') >> exception_messages_start >> process_exception_messages >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_update_project_child_dag)
