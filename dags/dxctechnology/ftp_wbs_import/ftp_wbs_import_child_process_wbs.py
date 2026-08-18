import rail
from dxctechnology.ftp_wbs_import.utils import request_payload
from dxctechnology.ftp_wbs_import.utils import response_filter
from dxctechnology.ftp_wbs_import.utils import python_callable_method
from dxctechnology.ftp_wbs_import.task.validate_person_responsible import validate_persons_responsible
from dxctechnology.ftp_wbs_import.task.ensure_user_has_permissions import ensure_user_has_permissions


# pylint: disable=too-many-statements
def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'dxctechnology_ftp_wbs_import_child_process_wbs_{config.instance}',
        description='DXC_FTP_WBS_Automation Child V1.0',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_child_dag_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        has_mandatory_fields = rail.IfOperator(
            task_id='has_mandatory_fields',
            test=request_payload.get_all_mandatory_check,
            yes_task="create_exception_log",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_madatory_fields_not_present',
            message='\
                {%- if dag_run.conf.WBS | is_falsy -%} \
                    Project Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Projectcode | is_falsy -%} \
                    Project code is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectstart | is_falsy -%} \
                    Project Start Date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectend | is_falsy -%} \
                    Project End Date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Status | is_falsy -%} \
                    Project Status is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Projectmanager | is_falsy -%} \
                    Project Manager ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Projectgroup | is_falsy -%} \
                    Company code is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.Projecttype | is_falsy -%} \
                    Project Type is not present in payload \
                {%- endif -%}\
                {%- if dag_run.conf.Parentwbs | is_falsy -%} \
                    Project Number is not present in payload \
                {%- endif -%}\
                {%- if dag_run.conf.Companycodelog | is_falsy -%} \
                    The company code is not present/disabled in Replicon,\
                {%- elif dag_run.conf.Companycode | is_falsy -%} \
                    The parent company code is not FTP \
                {%- endif -%}',
            severity='Exception',
            properties=request_payload.get_properties_exception
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log',
        )

        validate_persons_group_entry, (validate_persons_group_exit1,
                                       validate_persons_group_exit2) = validate_persons_responsible()

        ensure_projectleader_permissions_group_entry, ensure_projectleader_permissions_group_exit = ensure_user_has_permissions(
            'projectmanager')
        ensure_comanager_permissions_group_entry, ensure_comanager_permissions_group_exit = ensure_user_has_permissions(
            'coprojectmanager')

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_project_payload,
            response_filter=lambda resp: resp.json()['d'][0]['projectDetails'] if resp.json()[
                'd'][0]['projectDetails'] else None,
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') | is_truthy }}",
            yes_task="get_project_uri",
            no_task="create_project",
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint='/services/ProjectService1.svc/PutProject5',
            data=request_payload.get_create_payload,
        )

        unassign_all_users = rail.RepliconServiceOperator(
            task_id='unassign_all_users',
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment',
            data=lambda: {
                "projectUri": rail.result('create_project')['uri'],
                "resourceUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:unassign"
            }
        )

        get_project_uri = rail.PythonOperator(
            task_id='get_project_uri',
            python_callable=lambda: rail.result('load_project')['uri'] if rail.result(
                'load_project') else rail.result('create_project')['uri'],
        )

        has_parentwbs = rail.IfOperator(
            task_id="has_parentwbs",
            test="{{dag_run.conf.Parentwbs | is_truthy }}",
            no_task="update_oef_value",
            yes_task="has_billingindicator"
        )

        update_oef_value = rail.RepliconServiceOperator(
            task_id='update_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_payload
        )

        has_billingindicator = rail.IfOperator(
            task_id="has_billingindicator",
            test="{{dag_run.conf.Billingindicator | is_truthy }}",
            no_task="update_billingindicator_oef_value",
            yes_task="has_businessarea"
        )

        update_billingindicator_oef_value = rail.RepliconServiceOperator(
            task_id='update_billingindicator_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_billingindicator_oef_payload
        )

        has_businessarea = rail.IfOperator(
            task_id="has_businessarea",
            test="{{dag_run.conf.Businessarea | is_truthy }}",
            no_task="update_businessarea_oef_value",
            yes_task="has_masterwbs_uri"
        )

        update_businessarea_oef_value = rail.RepliconServiceOperator(
            task_id='update_businessarea_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_businessarea_oef_payload
        )

        has_masterwbs_uri = rail.IfOperator(
            task_id="has_masterwbs_uri",
            test="{{dag_run.conf.Masterwbsuri | is_truthy }}",
            no_task="update_masterwbsuri_oef_value",
            yes_task="is_client_present"
        )

        update_masterwbsuri_oef_value = rail.RepliconServiceOperator(
            task_id='update_masterwbsuri_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_masterwbsuri_oef_payload
        )

        is_client_present = rail.IfOperator(
            task_id="is_client_present",
            test="{{dag_run.conf.Clientname | is_truthy }}",
            no_task="remove_client",
            yes_task="is_update_scenario"
        )

        is_update_scenario = rail.IfOperator(
            task_id="is_update_scenario",
            test=python_callable_method.update_scenario_check,
            no_task="apply_project_modifications",
            yes_task="get_client_uri"
        )

        get_client_uri = rail.RepliconServiceOperator(
            task_id='get_client_uri',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_uri_payload,
            response_filter=response_filter.map_project_client
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id='update_project_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=request_payload.get_update_client_payload,
        )

        remove_client = rail.RepliconServiceOperator(
            task_id='remove_client',
            endpoint='/services/ProjectService1.svc/ApplyNewClient2',
            data=request_payload.get_remove_client_payload,
        )

        apply_project_modifications = rail.RepliconServiceOperator(
            task_id='apply_project_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_project_modifications,
        )

        should_apply_comanager = rail.IfOperator(
            task_id="should_apply_comanager",
            test="{{ result('user_details').comanageruri | is_truthy and result('user_details').useruri != result('user_details').comanageruri \
                    and result('user_details').comanageremployeegroup!= 'Contractor' and \
                     result('determine_necessary_coprojectmanager_updates').should_apply | is_truthy}}",
            yes_task="apply_comanager",
            no_task="update_project_division",
        )

        apply_comanager = rail.RepliconServiceOperator(
            task_id='apply_comanager',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data={
                "projectUri": "{{ result('get_project_uri') }}",
                "sharedUris": ["{{ result('determine_necessary_coprojectmanager_updates').user_uri }}"]
            }
        )

        update_project_division = rail.RepliconServiceOperator(
            task_id='update_project_division',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data=request_payload.get_update_division_payload
        )

        update_data_access_scopes = rail.RepliconServiceOperator(
            task_id='update_data_access_scopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data=request_payload.get_access_scopes_payload
        )

        put_key_value = rail.RepliconServiceOperator(
            task_id='put_key_value',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data=request_payload.get_put_keyvalue_payload
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            severity='{{ "Success" if result("create_exception_log") | load_all_records | length == 0 else "Exception" }}',
            message='\
                {%- if result("create_exception_log") | load_all_records | length == 0 -%} \
                    {{ "Project created successfully" if result("create_project") | is_truthy else "Project updated sucessfully" }} \
                {%- else -%} \
                    {{ "Project created partially, " if result("create_project") | is_truthy else "Project updated partially, " -}} \
                    {{ result("create_exception_log") | load_all_records | map_to_attr("message") | join(", ") }} \
                {%- endif -%}',
            properties={
                'projectname': '{{ dag_run.conf.WBS }}',
                'projectcode': '{{ dag_run.conf.Projectcode }}',
                'status': '{{ "Success" if result("create_exception_log") | load_all_records | length == 0 else "Exception" }}',
            },
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{ dag_run.conf.WBS }}',
                'projectcode': '{{ dag_run.conf.Projectcode }}',
                'status': 'Error',
            },
        )

        has_mandatory_fields >> rail.Label(
            'Yes') >> create_exception_log >> validate_persons_group_entry
        has_mandatory_fields >> rail.Label(
            "No") >> log_madatory_fields_not_present >> catch_and_log_errors
        validate_persons_group_exit1 >> [
            ensure_projectleader_permissions_group_entry, ensure_comanager_permissions_group_entry]
        [ensure_projectleader_permissions_group_exit,
            ensure_comanager_permissions_group_exit] >> load_project >> does_project_exist
        update_project_division >> update_data_access_scopes >> put_key_value >> log_completion >> catch_and_log_errors
        validate_persons_group_exit2 >> catch_and_log_errors
        does_project_exist >> rail.Label(
            "Yes") >> get_project_uri >> has_parentwbs
        does_project_exist >> rail.Label(
            "No") >> create_project >> unassign_all_users >> get_project_uri
        has_parentwbs >> rail.Label("Yes") >> has_billingindicator >> rail.Label(
            "Yes") >> has_businessarea
        has_parentwbs >> rail.Label("No") >> update_oef_value
        update_oef_value >> has_billingindicator >> rail.Label(
            "No") >> update_billingindicator_oef_value >> has_businessarea
        has_businessarea >> rail.Label("Yes") >> has_masterwbs_uri
        has_businessarea >> rail.Label(
            "No") >> update_businessarea_oef_value >> has_masterwbs_uri
        has_masterwbs_uri >> rail.Label("Yes") >> is_client_present
        has_masterwbs_uri >> rail.Label(
            "No") >> update_masterwbsuri_oef_value >> is_client_present
        is_client_present >> rail.Label("Yes") >> is_update_scenario
        is_update_scenario >> rail.Label(
            "Yes") >> get_client_uri >> update_project_client >> apply_project_modifications
        is_update_scenario >> rail.Label("No") >> apply_project_modifications
        is_client_present >> rail.Label(
            "No") >> remove_client >> apply_project_modifications
        apply_project_modifications >> should_apply_comanager >> rail.Label(
            'Yes') >> apply_comanager >> update_project_division
        should_apply_comanager >> rail.Label('No') >> update_project_division

    return dag


rail.for_each_instance(create_child_dag_wbs)
