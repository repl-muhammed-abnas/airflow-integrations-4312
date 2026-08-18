import datetime
from airflow.models import Variable
import rail
from pwcglobal.project_import_api_v6.custom_method import get_source_input_reference, compare_payload_with_sourceinputkey
from pwcglobal.project_import_api_v6.request_payload import get_project_uri, get_add_update_project_conf
from pwcglobal.project_import_api_v6.task.log_mandatory_field_exception import log_mandatory_field_exception_task


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_api_v6/config.py


# pylint:disable = too-many-statements
def create_child_process_project_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.project_import_api_process_project_child_dag_id,
        description=f'Process Project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_process_project_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            start_task='create_log',
            end_task='log_dagrun_to_sumo',
        )

        create_log = rail.CreateLogOperator(
            task_id='create_log'
        )

        get_md5_from_payload = rail.PythonOperator(
            task_id="get_md5_from_payload",
            python_callable=get_source_input_reference
        )

        get_project_details_from_code = rail.RepliconServiceOperator(
            task_id='get_project_details_from_code',
            endpoint='/services/ProjectService1.svc/BulkGetProjects2',
            data=lambda dag_run: {
                'projects': [
                    {
                        'code': dag_run.conf.get('chargecode')
                    }
                ]
            }
        )

        is_project_exists = rail.IfOperator(
            task_id="is_project_exists",
            test=lambda dag_run: bool(get_project_uri(
                dag_run.conf.get('chargecode'))),
            yes_task="is_mandatory_fields_not_present_update",
            no_task="is_project_not_open_for_time"
        )

        is_mandatory_fields_not_present_update = rail.IfOperator(
            task_id="is_mandatory_fields_not_present_update",
            test=lambda dag_run: not dag_run.conf['chargecode'] or
            not dag_run.conf.get('chargecodename') or
            not dag_run.conf.get('chargecodetype') or
            not dag_run.conf.get('chargecodestartdate') or
            not dag_run.conf.get('openfortime'),
            yes_task='log_mandatory_field_exception_update',
            no_task='load_project'
        )

        log_mandatory_field_exception_update = log_mandatory_field_exception_task(
            "Update")

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=lambda dag_run: {
                "projects": [
                    {
                        "uri": get_project_uri(dag_run.conf.get('chargecode', ''))
                    }
                ]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                {"projectDetails": None}])[0]['projectDetails'],
        )

        is_same_project_payload = rail.IfOperator(
            task_id="is_same_project_payload",
            test=lambda: compare_payload_with_sourceinputkey(
                rail.result('load_project')['keyValues']),
            yes_task="log_same_project_payload",
            no_task="update_project"
        )

        log_same_project_payload = rail.WriteLogOperator(
            task_id="log_same_project_payload",
            log="{{ result('create_log') }}",
            message="No change in the project payload",
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Skipped',
                'details': "No change in the project payload",
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Update'
            }
        )

        update_project = rail.TriggerDagRunForEachItemOperator(
            task_id='update_project',
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.project_import_api_update_project_child_dag_id,
            conf=get_add_update_project_conf
        )

        wait_for_update_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_update_project',
            dag_runs='{{ result("update_project") }}',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days)
        )

        is_project_not_open_for_time = rail.IfOperator(
            task_id="is_project_not_open_for_time",
            test=lambda dag_run: dag_run.conf['openfortime'] and dag_run.conf['openfortime'] == 'false',
            yes_task="log_project_as_completed",
            no_task="is_mandatory_fields_not_present_add"
        )

        log_project_as_completed = rail.WriteLogOperator(
            task_id="log_project_as_completed",
            log="{{ result('create_log') }}",
            message="Project not created because of following reasons: Project status received as completed",
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': '{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}',
                'Client Name|Client Code': '{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}',
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': 'Project not created because of following reasons: Project status received as completed',
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Add'
            }
        )

        is_mandatory_fields_not_present_add = rail.IfOperator(
            task_id="is_mandatory_fields_not_present_add",
            test=lambda dag_run: not dag_run.conf.get('chargecode') or
            not dag_run.conf.get('chargecodename') or
            not dag_run.conf.get('chargecodetype') or
            not dag_run.conf.get('chargecodestartdate') or
            not dag_run.conf.get('openfortime'),
            yes_task='log_mandatory_field_exception_add',
            no_task='is_project_type_not_found_in_replicon'
        )

        log_mandatory_field_exception_add = log_mandatory_field_exception_task(
            "Add")

        is_project_type_not_found_in_replicon = rail.IfOperator(
            task_id="is_project_type_not_found_in_replicon",
            test="{{ dag_run.conf.project_type | is_falsy }}",
            yes_task='log_project_type_not_found',
            no_task='is_cost_center_not_provided'
        )

        log_project_type_not_found = rail.WriteLogOperator(
            task_id="log_project_type_not_found",
            log="{{ result('create_log') }}",
            severity='Exception',
            message="Project not created as project type {{ dag_run.conf.chargecodetype }} not found in Replicon",
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': "Project not created as project type {{ dag_run.conf.chargecodetype }} not found in Replicon",
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Add'
            }
        )

        is_cost_center_not_provided = rail.IfOperator(
            task_id="is_cost_center_not_provided",
            test=lambda dag_run: not bool(dag_run.conf['costcentre'].get('CostCentreCode')
                                          if dag_run.conf.get('costcentre') else False),
            yes_task='log_cost_center_not_provided',
            no_task='create_project'
        )

        log_cost_center_not_provided = rail.WriteLogOperator(
            task_id="log_cost_center_not_provided",
            log="{{ result('create_log') }}",
            severity='Exception',
            message="Project not created as cost center not provided",
            properties={
                'SenderID': "{{ dag_run.conf.sender }} | Project",
                'Project Name|Project Code': "{{ dag_run.conf.chargecodename }} | {{ dag_run.conf.chargecode }}",
                'Client Name|Client Code': "{{ dag_run.conf.client_name }} | {{ dag_run.conf.client_code }}",
                'Task Name|Task Code': 'nil',
                'status': 'Exception',
                'details': "Project not created as cost center not provided",
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': 'Add'
            }
        )

        create_project = rail.TriggerDagRunForEachItemOperator(
            task_id='create_project',
            items=lambda dag_run: [dag_run.conf],
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=config.project_import_api_create_project_child_dag_id,
            conf=get_add_update_project_conf
        )

        wait_for_create_project = rail.WaitForDagRunsSensor(
            task_id='wait_for_create_project',
            dag_runs='{{ result("create_project") }}',
            execution_timeout=datetime.timedelta(
                days=config.execution_timeout_days)
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'MD5': "{{ result('get_md5_from_payload') }}",
                'Name': '{{ dag_run.conf.chargecodename }}',
                'Code': '{{ dag_run.conf.chargecode }}',
                'Projecttype': '{{ dag_run.conf.project_type }}',
                'Payloadidentifier': '{{ dag_run.conf.identifier }}',
                'Sender': '{{ dag_run.conf.sender }}'
            }
        )

        batch_task >> log_dagrun_to_sumo

        batch_task >> create_log >> get_md5_from_payload >> get_project_details_from_code >> is_project_exists

        is_project_exists >> rail.Label(
            "Yes") >> is_mandatory_fields_not_present_update

        is_mandatory_fields_not_present_update >> rail.Label(
            "Yes") >> log_mandatory_field_exception_update >> log_dagrun_to_sumo

        is_mandatory_fields_not_present_update >> rail.Label(
            "No") >> load_project >> is_same_project_payload

        is_same_project_payload >> rail.Label(
            "Yes") >> log_same_project_payload >> log_dagrun_to_sumo

        is_same_project_payload >> rail.Label(
            "No") >> update_project >> wait_for_update_project >> log_dagrun_to_sumo

        is_project_exists >> rail.Label(
            "No") >> is_project_not_open_for_time

        is_project_not_open_for_time >> rail.Label(
            "Yes") >> log_project_as_completed >> log_dagrun_to_sumo

        is_project_not_open_for_time >> rail.Label(
            "No") >> is_mandatory_fields_not_present_add

        is_mandatory_fields_not_present_add >> rail.Label(
            "Yes") >> log_mandatory_field_exception_add >> log_dagrun_to_sumo

        is_mandatory_fields_not_present_add >> rail.Label(
            "No") >> is_project_type_not_found_in_replicon

        is_project_type_not_found_in_replicon >> rail.Label(
            "Yes") >> log_project_type_not_found >> log_dagrun_to_sumo

        is_project_type_not_found_in_replicon >> rail.Label(
            "No") >> is_cost_center_not_provided

        is_cost_center_not_provided >> rail.Label(
            "Yes") >> log_cost_center_not_provided >> log_dagrun_to_sumo

        is_cost_center_not_provided >> rail.Label(
            "No") >> create_project >> wait_for_create_project >> log_dagrun_to_sumo

        return dag


rail.for_each_instance(create_child_process_project_dag)
