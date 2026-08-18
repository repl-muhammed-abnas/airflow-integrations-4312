from datetime import timedelta
import os
import rail
from rail.lib.log import get_master_log_artifact_name
from pwcglobal.project_import_file_based_v3 import python_callable_method
from pwcglobal.project_import_file_based_v3 import request_payload
from pwcglobal.project_import_file_based_v3 import response_filter
from airflow.models import Variable

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/pwcglobal/project_import_file_based_v3/config.py


# pylint:disable = too-many-statements
def create_main_airflow_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pwc_project_client_master_flat_file_based_{config.instance}_v3',
        description=f'Project Client data sync_Master V8 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs=config.master_dag_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dag_run_conf")


        get_file_manually = rail.IfOperator(
            task_id = "get_file_manually",
            test=lambda: Variable.get(f"pwc_project_client_master_flat_file_based_get_file_manually_variable_{config.instance}_v3", default_var="false") == "true",
            yes_task="dummy_get_file_manually_yes_task",
            no_task="new_file_sensor"
        )

        dummy_get_file_manually_yes_task = rail.EmptyOperator(
            task_id = "dummy_get_file_manually_yes_task"
        )

        new_file_sensor = rail.SFTPAnyFileSensor(
            task_id='new_file_sensor',
            path=config.input_filepath,
            soft_fail_timeout=timedelta(seconds=10)
        )

        def update_variable(previous_file_data, current_running_file, dag_run):
            _data= previous_file_data.get("fileprocessed", [])
            _data.append(
                {
                    "FILE_NAME": current_running_file,
                    "DAG_ID": dag_run.dag_id,
                    "RUN_ID": dag_run.run_id
                }
            )
            data_to_update = {
                "fileprocessed" : _data
            }
            Variable.set(key=config.pwc_project_client_master_flat_file_based_processed_file_data_variable, value=data_to_update, serialize_json=True)

        def validate_if_file_is_process_callable(dag_run):
            current_running_file = os.path.split(rail.result("new_file_sensor"))[1]
            previous_file_data = Variable.get(key=config.pwc_project_client_master_flat_file_based_processed_file_data_variable, deserialize_json=True, default_var={})

            if not previous_file_data:
                update_variable({}, current_running_file, dag_run)
                return "File processed log is not found"

            file_names = [file['FILE_NAME'] for file in previous_file_data.get("fileprocessed")]

            if current_running_file in file_names:
                return "File is already processed"

            update_variable(previous_file_data, current_running_file, dag_run)
            return "File processed log is not found"

        validate_if_file_is_process = rail.PythonOperator(
            task_id = "validate_if_file_is_process",
            python_callable=validate_if_file_is_process_callable
        )

        can_process_file = rail.IfOperator(
            task_id = "can_process_file",
            test=lambda: rail.result("validate_if_file_is_process") == "File processed log is not found",
            yes_task="is_xml"
        )

        is_xml = rail.IfOperator(
            task_id='is_xml',
            test='{{ result("new_file_sensor") | file_ext | lower == "xml" }}',
            yes_task='is_xml_yes_dummy',
            no_task='should_fail_dag'
        )

        is_xml_yes_dummy = rail.EmptyOperator(
            task_id = "is_xml_yes_dummy"
        )

        def get_download_file_name(dag_run):
            if Variable.get(f"pwc_project_client_master_flat_file_based_get_file_manually_variable_{config.instance}_v3", default_var="false") == "true":
                if dag_run.conf.get("manual_file_name"):
                    return f"{config.input_filepath}/{dag_run.conf['manual_file_name']}"
            return rail.result('new_file_sensor')

        download_file_name = rail.PythonOperator(
            task_id = "download_file_name",
            python_callable=get_download_file_name
        )

        download_file = rail.SFTPDownloadFileOperator(
            task_id='download_file',
            remote_filepath="{{ result('download_file_name') }}",
        )

        def was_new_file_found_test():
            if Variable.get(f"pwc_project_client_master_flat_file_based_get_file_manually_variable_{config.instance}_v3", default_var="false") == "true":
                return True
            if rail.render_template('{{get_task_state("new_file_sensor")}}') == "success":
                return True
            return False

        was_new_file_found = rail.IfOperator(
            task_id='was_new_file_found',
            trigger_rule='all_done',
            test=was_new_file_found_test,
            yes_task='archive_file' if config.should_archive else 'finish',
            no_task='delete_this_dagrun',
        )

        if config.should_archive:
            archive_file = rail.SFTPMoveFileOperator(
                task_id='archive_file',
                existing_filename="{{ result('new_file_sensor') }}",
                new_filename=config.archive_filepath + "/{{ dag_run_ecid() | \
                    replace(':', '-') }}_{{ result('new_file_sensor') | file_name }}"
            )
        else:
            finish = rail.EmptyOperator(
                task_id='finish'
            )

        delete_this_dagrun = rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun')

        parse_xml = rail.LoadXMLFileOperator(
            task_id='parse_xml',
            document="{{ result('download_file') }}"
        )

        has_data = rail.IfOperator(
            task_id='has_data',
            test='{{ result("parse_xml") | xpath("WorkManagement") | length > 0 }}',
            yes_task='get_sender_identifier',
            no_task='should_fail_dag'
        )

        get_sender_identifier = rail.XMLAdaptorOperator(
            task_id="get_sender_identifier",
            source='{{ result("parse_xml") }}',
            target='result',
            adaptor={
                'Sender': 'Sender/text()',
                'SenderEnvironment': 'SenderEnvironment/text()',
                'Identifier': 'Identifier/text()'
            }
        )

        get_all_permission_sets = rail.RepliconServiceOperator(
            task_id='get_all_permission_sets',
            endpoint='/services/PermissionSetService1.svc/GetAllPermissionSets'
        )

        get_all_legal_entities = rail.RepliconServiceOperator(
            task_id='get_all_legal_entities',
            endpoint='/services/DivisionListService1.svc/GetData',
            data=request_payload.get_enabled_division_list,
            response_filter=response_filter.map_legal_entities_list
        )

        get_all_locations = rail.RepliconServiceOperator(
            task_id='get_all_locations',
            endpoint='/services/LocationService1.svc/GetAllLocations'
        )

        get_all_client_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_client_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:client"}
        )

        get_all_project_custom_fields = rail.RepliconServiceOperator(
            task_id='get_all_project_custom_fields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={"objectUri": "urn:replicon:object-type:project"}
        )

        get_all_project_object_extension_field_details = rail.RepliconServiceOperator(
            task_id='get_all_project_object_extension_field_details',
            endpoint='/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails',
            data={"bindingContextUri": "urn:replicon:object-type:project"}
        )

        get_object_extension_tag_definition_details = rail.RepliconServiceOperator(
            task_id='get_object_extension_tag_definition_details',
            endpoint='/services/ObjectExtensionTagDefinitionService1.svc/GetObjectExtensionTagDefinitionDetails',
            data={"objectExtensionTagDefinitionUri": "{{ result('get_all_project_object_extension_field_details') | \
                find_first_by_attr_and_get_attr('name', 'Type', 'uri') }}"}
        )

        get_confidential_project_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_confidential_project_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'Confidential Project', 'uri') }}"}
        )

        get_mandatory_text_custom_field_dropdown_options = rail.RepliconServiceOperator(
            task_id='get_mandatory_text_custom_field_dropdown_options',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={"customFieldUri": "{{ result('get_all_project_custom_fields') | \
                find_first_by_attr_and_get_attr('displayText', 'Mandatory Text', 'uri') }}"}
        )

        get_workmanagement = rail.XMLAdaptorOperator(
            task_id="get_workmanagement",
            source='{{ result("parse_xml") }}',
            target='artifact',
            adaptor=[
                'WorkManagement',
                {
                    'ConfidentialFlag': 'ConfidentialFlag/text()',
                    'EngagementPartyRole': [
                        'EngagementPartyRole',
                        {
                            'PartyId': 'PartyId/text()',
                            'PrimaryPartyName': 'PrimaryPartyName/text()',
                            'PartyAlternateIdentifierType': 'PartyAlternateIdentifierType/text()',
                            'PartyAlternateIdentifierValue': 'PartyAlternateIdentifierValue/text()',
                            'EngagementPartyRoleType': 'EngagementPartyRoleType/text()',
                            'EngagementPartyRoleTypeId': 'EngagementPartyRoleTypeId/text()'
                        }
                    ],
                    'ChargeCode': [
                        'ChargeCode',
                        {
                            'ChargeCode': 'ChargeCode/text()',
                            'ChargeCodeName': 'ChargeCodeName/text()',
                            'ChargeCodeDescription': 'ChargeCodeDescription/text()',
                            'ChargeCodeType': 'ChargeCodeType/text()',
                            'ChargeCodeTypeId': 'ChargeCodeTypeId/text()',
                            'ChargeCodeStartDate': 'ChargeCodeStartDate/text()',
                            'ChargeCodeEndDate': 'ChargeCodeEndDate/text()',
                            'CurrentStatus': {
                                'OpenForTime': 'CurrentStatus/OpenForTime/text()'
                            },
                            'MandatoryTextFlag': 'MandatoryTextFlag/text()',
                            'PartyRole': [
                                'PartyRole',
                                {
                                    'PartyId': 'PartyId/text()',
                                    'PartyRoleType': 'PartyRoleType/text()',
                                    'PartyRoleTypeId': 'PartyRoleTypeId/text()'
                                }
                            ],
                            'InternalPersonRole': [
                                'InternalPersonRole',
                                {
                                    'InternalPersonRoleType': 'InternalPersonRoleType/text()',
                                    'InternalPersonRoleTypeId': 'InternalPersonRoleTypeId/text()',
                                    'InternalWorkRelationship': {
                                        'InternalPerson': {
                                            'PartyId': 'InternalWorkRelationship/InternalPerson/PartyId/text()'
                                        },
                                        'PwCLegalEntity': {
                                            'PartyId': 'InternalWorkRelationship/PwCLegalEntity/PartyId/text()'
                                        }
                                    }
                                }
                            ],
                            'CostCentre': {
                                'CostCentreCode': 'CostCentre/CostCentreCode/text()'
                            },
                            'WorkItem': [
                                'WorkItem',
                                {
                                    'WorkItemType': 'WorkItemType/text()',
                                    'WorkItemTypeId': 'WorkItemTypeId/text()'
                                }
                            ],
                            'EngagementLine': [
                                'EngagementLine',
                                {
                                    'EngagementLineDescription': 'EngagementLineDescription/text()',
                                    'EngagementLineType': 'EngagementLineType/text()',
                                    'EngagementLineTypeId': 'EngagementLineTypeId/text()'
                                }
                            ]
                        }
                    ]
                }
            ]
        )

        for_each_workmanagement = rail.ForEachOperator(
            task_id='for_each_workmanagement',
            items="{{ result('get_workmanagement') }}",
            start_task='get_client_data',
            end_task='for_each_workmanagement_end'
        )

        get_client_data = rail.PythonOperator(
            task_id='get_client_data',
            python_callable=python_callable_method.get_client_name_code_partyid
        )

        should_process_client = rail.IfOperator(
            task_id="should_process_client",
            test=lambda: bool(rail.result('get_client_data')['client_name']) and
            bool(rail.result('get_client_data')['client_code']),
            yes_task='get_client_list_from_party_alternate_identifier',
            no_task='should_process_projects'
        )

        get_client_list_from_party_alternate_identifier = rail.RepliconServiceOperator(
            task_id='get_client_list_from_party_alternate_identifier',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_payload,
            response_filter=response_filter.map_client_list
        )

        is_client_present_in_replicon = rail.IfOperator(
            task_id="is_client_present_in_replicon",
            test="{{ result('get_client_list_from_party_alternate_identifier') | \
                find_first_by_attr_and_get_attr('code', result('get_client_data').client_code, 'uri', '') | \
                    is_truthy }}",
            yes_task='is_client_name_equals_primary_party_name',
            no_task='create_client_in_replicon'
        )

        is_client_name_equals_primary_party_name = rail.IfOperator(
            task_id="is_client_name_equals_primary_party_name",
            test=lambda: rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'name', ''),
            yes_task='get_client_details',
            no_task='update_client_name_in_replicon'
        )

        create_client_in_replicon = rail.RepliconServiceOperator(
            task_id='create_client_in_replicon',
            endpoint='/services/ClientService1.svc/CreateClientOrApplyModifications',
            data=request_payload.get_create_client_payload
        )

        update_client_name_in_replicon = rail.RepliconServiceOperator(
            task_id="update_client_name_in_replicon",
            endpoint='/services/ClientService1.svc/UpdateName',
            data=lambda: {
                'clientUri': rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                'name': rail.result('get_client_data')['client_name']
            }
        )

        get_client_details = rail.RepliconServiceOperator(
            task_id = "get_client_details",
            endpoint="/services/ClientService1.svc/BulkGetClientDetails",
            data=lambda : {
                "clientUris" : [
                    rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri')
                ]
            }
        )

        def test_can_update_the_client_party_id():
            current_party_id = rail.find_first_by_attr_and_get_attr(rail.result("get_client_details")[0]['customFields'], 'customField.displayText', 'Client Party Id', 'text')
            if not current_party_id:
                if rail.result('get_client_data')['client_party_id']:
                    return True
            return False

        can_update_the_client_party_id = rail.IfOperator(
            task_id = "can_update_the_client_party_id",
            test=test_can_update_the_client_party_id,
            yes_task="update_client_party_id",
            no_task="can_log_update"
        )

        update_client_party_id = rail.RepliconServiceOperator(
            task_id = "update_client_party_id",
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda: {
                "objectUri": rail.find_first_by_attr_and_get_attr(rail.result(
                    'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'uri'),
                "customFieldUri": rail.find_first_by_attr_and_get_attr(
                        rail.result('get_all_client_custom_fields'), 'displayText', 'Client Party Id', 'uri'),
                "value": rail.result('get_client_data')['client_party_id']
            }
        )

        def can_log_update_test():
            return (not (rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'name', ''))) or test_can_update_the_client_party_id()

        can_log_update = rail.IfOperator(
            task_id = "can_log_update",
            test=can_log_update_test,
            yes_task="log_create_update_client",
            no_task="should_process_projects"
        )


        def get_details_action(caller):
            if caller == "action":
                return "Add" if bool(rail.result("create_client_in_replicon")) else "Update"
            if bool(rail.result("create_client_in_replicon")):
                return "Client created"
            if not (rail.result('get_client_data')['client_name'] == rail.find_first_by_attr_and_get_attr(rail.result(
                'get_client_list_from_party_alternate_identifier'), 'code', rail.result('get_client_data')['client_code'], 'name', '')):
                if test_can_update_the_client_party_id():
                    return "Client name update. Client Party Id Added"
                return "Client name updated"

        log_create_update_client = rail.WriteLogOperator(
            task_id="log_create_update_client",
            message="{{ result('get_client_data').client_name }} is processed",
            severity='Success',
            properties=lambda : {
                'SenderID': "Oracle | Client",
                'Project Name|Project Code': 'nil',
                'Client Name|Client Code': rail.render_template("{{ result('get_client_data').client_name }} | {{ result('get_client_data').client_code }}"),
                'Task Name|Task Code': 'nil',
                'status': 'Success',
                'details': get_details_action("details"),
                'UnitLoggedDateTime': "{{ current_time() }}",
                'Action': get_details_action("action")
            }
        )

        should_process_projects = rail.IfOperator(
            task_id='should_process_projects',
            test="{{ result('for_each_workmanagement') | \
                attr_or_default('ChargeCode') | length > 0 }}",
            yes_task='process_projects',
            no_task='for_each_workmanagement_end'
        )

        process_projects = rail.TriggerDagRunForEachItemOperator(
            task_id='process_projects',
            items=lambda: rail.result('for_each_workmanagement')['ChargeCode'],
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_project_flat_file_based_{config.instance}_v3',
            conf=request_payload.get_process_project_conf
        )

        get_process_project_dagruns = rail.SetVariableOperator(
            task_id='get_process_project_dagruns',
            name='process_project_dagruns',
            value=lambda: rail.result('process_projects'),
            append=True
        )

        for_each_workmanagement_end = rail.EmptyOperator(
            task_id='for_each_workmanagement_end'
        )

        wait_for_process_projects = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_projects',
            dag_runs='{{ result("process_projects") }}',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        gather_logs = rail.GatherResultsFromDagRunsOperator(
            task_id='gather_logs',
            dag_runs="{{ result('get_process_project_dagruns').value }}",
            dagrun_task_id='create_log',
            flatten=True,
        )

        process_log_generation = rail.TriggerDagRunOperator(
            task_id='process_log_generation',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            trigger_dag_id=f'pwc_project_import_child_log_flat_file_based_{config.instance}_v3',
            conf=lambda: {
                'master_log': get_master_log_artifact_name(rail.get_current_context()),
                'child_log': rail.result('gather_logs'),
                'sender': rail.result('get_sender_identifier')['Sender'],
                'identifier': rail.result('get_sender_identifier')['Identifier']
            }
        )

        should_fail_dag = rail.IfOperator(
            task_id='should_fail_dag',
            trigger_rule='all_done',
            test="{{ get_failed_upstream_task_ids() | length > 0 }}",
            yes_task='fail_dag',
            no_task='process_logtosumo'
        )

        fail_dag = rail.FailOperator(
            task_id='fail_dag',
            message="{{ get_error_message() }}"
        )

        process_logtosumo = rail.EmptyOperator(
            task_id='process_logtosumo'
        )

        check_if_new_file_found = rail.IfOperator(
            task_id='check_if_new_file_found',
            test="{{ get_task_state('new_file_sensor') == 'success' }}",
            yes_task='log_dagrun_to_sumo'
        )

        log_dagrun_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_dagrun_to_sumo',
            sumo_conn_id=config.dagrun_log_conn_id,
            trigger_rule='all_done',
            extra_info={
                'Filename': "{{ result('new_file_sensor') | file_base }}"
            }
        )


        get_file_manually >> rail.Label("Yes") >> dummy_get_file_manually_yes_task >> download_file_name
        get_file_manually >> rail.Label("No") >> new_file_sensor

        new_file_sensor >> validate_if_file_is_process

        validate_if_file_is_process >> can_process_file >> rail.Label("Yes") >> is_xml >> rail.Label(
            'Yes') >> is_xml_yes_dummy >> download_file_name >> download_file

        download_file >> rail.Label(
            'Always') >> was_new_file_found

        if config.should_archive:
            was_new_file_found >> rail.Label(
                'Yes') >> archive_file
        else:
            was_new_file_found >> rail.Label(
                'Yes') >> finish

        was_new_file_found >> rail.Label(
            'No') >> delete_this_dagrun

        download_file >> parse_xml >> has_data

        has_data >> rail.Label(
            'Yes') >> get_sender_identifier >> [get_all_permission_sets, get_all_legal_entities, get_all_locations,
                                                get_all_client_custom_fields, get_all_project_custom_fields,
                                                get_all_project_object_extension_field_details] >> \
            get_object_extension_tag_definition_details >> \
            get_confidential_project_custom_field_dropdown_options >> \
            get_mandatory_text_custom_field_dropdown_options >> get_workmanagement >> \
            for_each_workmanagement

        for_each_workmanagement >> get_client_data >> should_process_client

        should_process_client >> rail.Label(
            "Yes") >> get_client_list_from_party_alternate_identifier >> is_client_present_in_replicon
        should_process_client >> rail.Label(
            "No") >> should_process_projects

        is_client_present_in_replicon >> rail.Label(
            "Yes") >> is_client_name_equals_primary_party_name

        is_client_name_equals_primary_party_name >> rail.Label(
            "Yes") >> get_client_details
        is_client_name_equals_primary_party_name >> rail.Label(
            "No") >> update_client_name_in_replicon >> get_client_details >> can_update_the_client_party_id >> rail.Label("No") >> can_log_update
        can_log_update >> rail.Label("Yes") >> log_create_update_client
        can_log_update >> rail.Label("No") >> should_process_projects
        can_update_the_client_party_id >> rail.Label("Yes") >> update_client_party_id >> log_create_update_client

        is_client_present_in_replicon >> rail.Label(
            "No") >> create_client_in_replicon >> log_create_update_client

        log_create_update_client >> should_process_projects

        should_process_projects >> rail.Label(
            "Yes") >> process_projects >> get_process_project_dagruns >> for_each_workmanagement_end

        should_process_projects >> rail.Label(
            "No") >> for_each_workmanagement_end

        for_each_workmanagement >> for_each_workmanagement_end

        for_each_workmanagement_end >> wait_for_process_projects >> gather_logs

        gather_logs >> process_log_generation >> should_fail_dag

        has_data >> rail.Label(
            'No') >> should_fail_dag

        is_xml >> rail.Label(
            'No') >> should_fail_dag

        should_fail_dag >> rail.Label(
            'Yes') >> fail_dag

        should_fail_dag >> rail.Label(
            'No') >> process_logtosumo >> check_if_new_file_found

        check_if_new_file_found >> rail.Label(
            'Yes') >> log_dagrun_to_sumo

    return dag


rail.for_each_instance(create_main_airflow_dag)
