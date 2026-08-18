from datetime import timedelta
from airflow.models import Variable
import rail
from technicolorg3.ceta_project_client_data.utils import request_payload
from technicolorg3.ceta_project_client_data.utils import response_filter
from technicolorg3.ceta_project_client_data.utils import python_callable_method
from technicolorg3.ceta_project_client_data.tasks.update_project_manager import get_update_project_manager_id
from technicolorg3.ceta_project_client_data.tasks.project_mandatory_fields import get_project_mandatory_fields
from technicolorg3.ceta_project_client_data.tasks.project_logs import get_project_logs
from technicolorg3.ceta_project_client_data.tasks.project_catch_sumo_logs import get_project_catch_sumo_logs

null = None

# pylint: disable=too-many-statements


def create_add_project_child_dag(config):

    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_project_client_details_add_project_{config.instance}',
        description=f'Technicolor CETA add project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        action = 'add_project'

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='').lower() == 'true',
            yes_task='batch_task',
            no_task='start_project_add'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='start_project_add',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            end_task=f'catch_and_log_errors_{action}',
        )

        start_project_add = rail.EmptyOperator(
            task_id='start_project_add'
        )

        client_project_logs, project_mandatory_fields_end, log_project_fields_missing = get_project_mandatory_fields(
            action, config)

        is_project_status_completed = rail.IfOperator(
            task_id='is_project_status_completed',
            test=lambda dag_run: bool(
                dag_run.conf['projectstatus'].lower() != 'confirmed' and dag_run.conf['projectstatus'].lower() != 'new'),
            yes_task='log_project_status_not_allowed',
            no_task='is_project_status_valid'
        )

        log_project_status_not_allowed = rail.WriteLogOperator(
            task_id='log_project_status_not_allowed',
            log='{{ result("client_project_logs_'+action+'") }}',
            # pylint: disable=line-too-long
            message='The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has not been completed since the status received in the request payload is not in allowed list.',
            properties={
                'db': '{{ dag_run.conf.millmpc }}',
                'client': '{{ dag_run.conf.clientname }}',
                'project': '{{ dag_run.conf.projectname }}',
                'status': 'Exception',
                'action': 'Add Project',
                # pylint: disable=line-too-long
                'details': 'The Client_Project transfer from CETA to Replicon with job reference "{{ dag_run_ecid() }}" has not been completed since the status received in the request payload is not in allowed list.',
                'reference': '{{ dag_run_ecid() }}',
                'exported': 'No'
            }
        )

        is_project_status_valid = rail.IfOperator(
            task_id='is_project_status_valid',
            test=lambda dag_run: bool(
                dag_run.conf['projectstatus'].lower() == 'confirmed' or dag_run.conf['projectstatus'].lower() == 'new'),
            yes_task='projectmanager_id_start',
            no_task='exception_messages_start'
        )

        projectmanager_id_start = rail.EmptyOperator(
            task_id='projectmanager_id_start',
        )

        process_update_project_manager = get_update_project_manager_id(
            action)

        projectmanager_id_finish = rail.EmptyOperator(
            task_id='projectmanager_id_finish'
        )

        get_department_uri_for_teammember_assignment = rail.RepliconServiceOperator(
            task_id='get_department_uri_for_teammember_assignment',
            endpoint='/services/DepartmentGroupListService1.svc/GetData',
            data=request_payload.get_department_uri_payload,
            data_handler=response_filter.get_departmentlist
        )

        get_required_customfields = rail.RepliconServiceOperator(
            task_id='get_required_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"},
            response_filter=response_filter.get_required_customfields
        )

        is_mill_mpc_uri_present = rail.IfOperator(
            task_id='is_mill_mpc_uri_present',
            test=lambda: bool(rail.result(
                'get_required_customfields')['mill_mpc_uri']),
            yes_task='get_enabled_dropdown_mill_mpc',
            no_task='is_project_buckets_uri_present'
        )

        get_enabled_dropdown_mill_mpc = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_mill_mpc',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["mill_mpc_uri"]}}'},
            data_handler=response_filter.get_customfields_mill_mpc
        )

        is_project_buckets_uri_present = rail.IfOperator(
            task_id='is_project_buckets_uri_present',
            test=lambda: bool(rail.result('get_required_customfields')[
                              'project_buckets_uri']),
            yes_task='get_enabled_dropdown_project_buckets',
            no_task='get_enabled_dropdown_project_id'
        )

        get_enabled_dropdown_project_buckets = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_project_buckets',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["project_buckets_uri"]}}'},
            response_filter=response_filter.get_customfields_project_buckets
        )

        get_enabled_dropdown_project_id = rail.PythonOperator(
            task_id='get_enabled_dropdown_project_id',
            python_callable=python_callable_method.get_customfields_project_id
        )

        get_enabled_dropdown_product_name = rail.PythonOperator(
            task_id='get_enabled_dropdown_product_name',
            python_callable=python_callable_method.get_customfields_product_name
        )

        is_project_type_uri_present = rail.IfOperator(
            task_id='is_project_type_uri_present',
            test=lambda: bool(rail.result('get_required_customfields')[
                              'project_type_uri']),
            yes_task='get_enabled_dropdown_project_type',
            no_task='is_project_classification_uri_present'
        )

        get_enabled_dropdown_project_type = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_project_type',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["project_type_uri"]}}'},
            data_handler=response_filter.get_customfields_project_type
        )

        is_project_type_present = rail.IfOperator(
            task_id='is_project_type_present',
            test=lambda dag_run: bool(dag_run.conf['projecttype'] and not rail.result(
                'get_enabled_dropdown_project_type')),
            yes_task='process_dropdown_projecttype_add',
            no_task='add_project_type_customfields'
        )

        process_dropdown_projecttype_add = rail.TriggerDagRunOperator(
            task_id='process_dropdown_projecttype_add',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_dropdown_option_addition_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'dropdownoption': dag_run.conf['projecttype'],
                'customfielduri': rail.result("get_required_customfields")["project_type_uri"]
            }
        )

        wait_for_process_dropdown_projecttype_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_dropdown_projecttype_add',
            dag_runs='{{ result("process_dropdown_projecttype_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_dropdown_result_for_projecttype = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_dropdown_result_for_projecttype',
            dag_runs='{{ result("process_dropdown_projecttype_add") }}',
            dagrun_task_id='put_dropdown_options',
            flatten=True
        )

        is_projecttype_dropdown_success = rail.IfOperator(
            task_id='is_projecttype_dropdown_success',
            test=lambda: not bool(rail.result(
                'gather_dropdown_result_for_projecttype'), key='error'),
            yes_task='get_enabled_dropdown_options_project_buckets',
            no_task='add_project_type_customfields'
        )

        get_enabled_dropdown_options_project_buckets = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_options_project_buckets',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["project_type_uri"]}}'},
            data_handler=response_filter.get_dropdown_options_project_buckets
        )

        add_project_type_customfields = rail.PythonOperator(
            task_id='add_project_type_customfields',
            python_callable=python_callable_method.add_customfields_project_type,
        )

        is_project_classification_uri_present = rail.IfOperator(
            task_id='is_project_classification_uri_present',
            test=lambda: bool(rail.result('get_required_customfields')[
                              'project_classification_uri']),
            yes_task='get_enabled_customfield_dropdown_project_classification',
            no_task='create_project'
        )

        get_enabled_customfield_dropdown_project_classification = rail.RepliconServiceOperator(
            task_id='get_enabled_customfield_dropdown_project_classification',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["project_classification_uri"]}}'},
            data_handler=response_filter.get_customfields_project_classification
        )

        is_project_classification_present = rail.IfOperator(
            task_id='is_project_classification_present',
            test=lambda dag_run: bool(dag_run.conf['projectclassification'] and not rail.result(
                'get_enabled_customfield_dropdown_project_classification')),
            yes_task='process_dropdown_project_classification_add',
            no_task='add_project_classification_customfields'
        )

        process_dropdown_project_classification_add = rail.TriggerDagRunOperator(
            task_id='process_dropdown_project_classification_add',
            retries=0,
            trigger_dag_id=f'technicolorg3_project_client_details_dropdown_option_addition_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                'dropdownoption': dag_run.conf['projectclassification'],
                'customfielduri': rail.result("get_required_customfields")["project_classification_uri"]
            }
        )

        wait_for_process_dropdown_project_classification_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_dropdown_project_classification_add',
            dag_runs='{{ result("process_dropdown_project_classification_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        gather_dropdown_result_for_projectclassification = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_dropdown_result_for_projectclassification',
            dag_runs='{{ result("process_dropdown_project_classification_add") }}',
            dagrun_task_id='put_dropdown_options',
            flatten=True
        )

        is_projectclassification_dropdown_success = rail.IfOperator(
            task_id='is_projectclassification_dropdown_success',
            test=lambda: not bool(rail.result(
                'gather_dropdown_result_for_projectclassification', key='error')),
            yes_task='get_enabled_dropdown_options_project_classification',
            no_task='add_project_classification_customfields'
        )

        get_enabled_dropdown_options_project_classification = rail.RepliconServiceOperator(
            task_id='get_enabled_dropdown_options_project_classification',
            endpoint='/services/CustomFieldService1.svc/GetEnabledCustomFieldDropDownOptions',
            data={
                "customFieldUri": '{{result("get_required_customfields")["project_classification_uri"]}}'},
            data_handler=response_filter.get_dropdown_options_project_classification
        )

        add_project_classification_customfields = rail.PythonOperator(
            task_id='add_project_classification_customfields',
            python_callable=python_callable_method.add_customfields_project_classification,
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=lambda dag_run: request_payload.get_create_project_payload(
                dag_run, action)
        )

        update_department_group = rail.RepliconServiceOperator(
            task_id='update_department_group',
            endpoint='/services/ProjectService1.svc/UpdateDepartmentGroup',
            data=request_payload.get_update_department_payload
        )

        get_entries_from_projecttasks_mapper = rail.PythonOperator(
            task_id='get_entries_from_projecttasks_mapper',
            python_callable=python_callable_method.get_default_tasks,
            op_args=[config.project_tasks_mapper]
        )

        process_non_billable_task_add = rail.TriggerDagRunForEachItemOperator(
            task_id='process_non_billable_task_add',
            retries=0,
            items='{{ result("get_entries_from_projecttasks_mapper") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            trigger_dag_id=f'technicolorg3_project_client_details_non_billable_task_add_{config.instance}',
            conf=request_payload.get_non_billable_task_add_payload
        )

        wait_for_process_non_billable_task_add = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_non_billable_task_add',
            dag_runs='{{ result("process_non_billable_task_add") }}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
        )

        is_clienturi_present = rail.IfOperator(
            task_id='is_clienturi_present',
            test=lambda dag_run: bool(dag_run.conf['clienturi']),
            yes_task='apply_new_client',
            no_task='add_client_not_found_exception'
        )

        apply_new_client = rail.RepliconServiceOperator(
            task_id='apply_new_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient',
            data=request_payload.get_apply_new_client_payload
        )

        add_client_not_found_exception = rail.PythonOperator(
            task_id='add_client_not_found_exception',
            python_callable=lambda dag_run: f'Client not associated with the Project since client { dag_run.conf["clientname"] } not found'
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
            'No') >> start_project_add >> client_project_logs

        project_mandatory_fields_end >> is_project_status_completed
        log_project_fields_missing >> finish

        is_project_status_completed >> rail.Label(
            'Yes') >> log_project_status_not_allowed >> finish
        is_project_status_completed >> rail.Label(
            'No') >> is_project_status_valid

        is_project_status_valid >> rail.Label(
            'Yes') >> projectmanager_id_start
        is_project_status_valid >> rail.Label(
            'No') >> exception_messages_start

        projectmanager_id_start >> process_update_project_manager >> projectmanager_id_finish

        projectmanager_id_finish >> get_department_uri_for_teammember_assignment >> get_required_customfields >> is_mill_mpc_uri_present

        is_mill_mpc_uri_present >> rail.Label(
            'Yes') >> get_enabled_dropdown_mill_mpc >> is_project_buckets_uri_present
        is_mill_mpc_uri_present >> rail.Label(
            'No') >> is_project_buckets_uri_present

        is_project_buckets_uri_present >> rail.Label(
            'Yes') >> get_enabled_dropdown_project_buckets >> get_enabled_dropdown_project_id
        is_project_buckets_uri_present >> rail.Label(
            'No') >> get_enabled_dropdown_project_id >> get_enabled_dropdown_product_name \
            >> is_project_type_uri_present

        is_project_type_uri_present >> rail.Label(
            'Yes') >> get_enabled_dropdown_project_type >> is_project_type_present
        is_project_type_uri_present >> rail.Label(
            'No') >> is_project_classification_uri_present

        is_project_type_present >> rail.Label(
            'Yes') >> process_dropdown_projecttype_add >> wait_for_process_dropdown_projecttype_add \
            >> gather_dropdown_result_for_projecttype >> is_projecttype_dropdown_success
        is_project_type_present >> rail.Label(
            'No') >> add_project_type_customfields >> is_project_classification_uri_present

        is_projecttype_dropdown_success >> rail.Label(
            'Yes') >> get_enabled_dropdown_options_project_buckets >> add_project_type_customfields
        is_projecttype_dropdown_success >> rail.Label(
            'No') >> add_project_type_customfields

        is_project_classification_uri_present >> rail.Label(
            'Yes') >> get_enabled_customfield_dropdown_project_classification >> is_project_classification_present
        is_project_classification_uri_present >> rail.Label(
            'No') >> create_project

        is_project_classification_present >> rail.Label(
            'Yes') >> process_dropdown_project_classification_add >> wait_for_process_dropdown_project_classification_add \
            >> gather_dropdown_result_for_projectclassification >> is_projectclassification_dropdown_success
        is_project_classification_present >> rail.Label(
            'No') >> add_project_classification_customfields

        is_projectclassification_dropdown_success >> rail.Label(
            'Yes') >> get_enabled_dropdown_options_project_classification >> add_project_classification_customfields
        is_projectclassification_dropdown_success >> rail.Label(
            'No') >> add_project_classification_customfields >> create_project >> update_department_group >> get_entries_from_projecttasks_mapper \
            >> process_non_billable_task_add >> wait_for_process_non_billable_task_add >> is_clienturi_present

        is_clienturi_present >> rail.Label(
            'Yes') >> apply_new_client >> exception_messages_start
        is_clienturi_present >> rail.Label(
            'No') >> add_client_not_found_exception >> exception_messages_start \
            >> process_exception_messages >> finish >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_add_project_child_dag)
