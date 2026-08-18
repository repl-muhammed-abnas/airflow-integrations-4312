import rail
from dxctechnology.ftp_sales_order_import import request_payload
from dxctechnology.ftp_sales_order_import import response_filter
from dxctechnology.ftp_sales_order_import.validate_person_responsible import validate_persons_responsible
from dxctechnology.ftp_sales_order_import.ensure_user_has_permissions import ensure_user_has_permissions

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/dxctechnology/ftp_sales_order_import/config.py

def create_child_dag_wbs(config):
    dag_id_postfix = f'_{config.instance}' if config.instance else ''
    with rail.create_airflow_dag(
        dag_id = f'dxctechnology_ftp_sales_order_import_child_process_salesorder{dag_id_postfix}',
        description = 'DXC_FTP_SalesOrder_Automation Child V1.1',
        company_key = config.company_key,
        replicon_conn_id = config.replicon_conn_id,
        max_active_runs = config.max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id = "view_dagrun_config")

        has_mandatory_fields = rail.IfOperator(
            task_id ='has_mandatory_fields',
            test = request_payload.get_all_mandatory_check,
            yes_task="create_exception_log",
            no_task="log_madatory_fields_not_present"
        )

        log_madatory_fields_not_present =rail.WriteLogOperator(
            task_id = 'log_madatory_fields_not_present',
            message = '\
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
                {%- if dag_run.conf.Companycodename | is_falsy -%} \
                    Company code is not present in payload \
                {%- elif dag_run.conf.Companycode | is_falsy -%} \
                    The company code is not present/disabled in Replicon \
                {%- endif -%}',
            severity='Exception',
            properties = request_payload.get_properties_exception
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log',
        )

        validate_persons_group_entry, (validate_persons_group_exit1,validate_persons_group_exit2) = validate_persons_responsible()

        ensure_projectleader_permissions_group_entry, ensure_projectleader_permissions_group_exit = ensure_user_has_permissions('projectmanager')
        ensure_comanager_permissions_group_entry, ensure_comanager_permissions_group_exit = ensure_user_has_permissions('coprojectmanager')

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data= request_payload.get_project_payload,
            response_filter=lambda resp: resp.json()['d'][0]['projectDetails'] if resp.json()['d'][0]['projectDetails'] else None,
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
            data = request_payload.get_create_payload,
        )

        unassign_all_users = rail.RepliconServiceOperator(
            task_id='unassign_all_users',
            endpoint='/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment',
            data=lambda : {
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
            test= "{{dag_run.conf.Parentwbs | is_truthy }}",
            no_task="update_oef_value",
            yes_task= "search_program"
        )

        update_oef_value = rail.RepliconServiceOperator(
            task_id='update_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_payload
        )

        search_program = rail.RepliconServiceOperator(
            task_id='search_program',
            endpoint='/services/ProgramListService1.svc/GetData',
            data=request_payload.search_programs('{{dag_run.conf.Programname}}'),
            response_filter=lambda response: response_filter.program_filter(response, request_payload.get_dag_run_conf()['Programname'])
        )

        apply_project_modifications = rail.RepliconServiceOperator(
            task_id = 'apply_project_modifications',
            endpoint ='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data = request_payload.get_project_modifications,
        )

        update_project_team_member_assignment = rail.RepliconServiceOperator(
            task_id ='update_project_team_member_assignment',
            endpoint = '/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment',
            data = request_payload.get_project_teammember_payload,
        )

        should_apply_comanager = rail.IfOperator(
            task_id="should_apply_comanager",
            test = "{{ result('user_details').comanageruri | is_truthy and result('user_details').useruri != result('user_details').comanageruri \
                    and result('user_details').comanageremployeegroup!= 'Contractor' and \
                     result('determine_necessary_coprojectmanager_updates').should_apply | is_truthy}}",
            yes_task="apply_comanager",
            no_task="update_project_division",
        )

        apply_comanager = rail.RepliconServiceOperator(
            task_id='apply_comanager',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data= {
                "projectUri": "{{ result('get_project_uri') }}",
                "sharedUris": ["{{ result('determine_necessary_coprojectmanager_updates').user_uri }}"]
            }
        )

        update_project_division = rail.RepliconServiceOperator(
            task_id='update_project_division',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data = request_payload.get_update_division_payload
        )

        update_data_access_scopes = rail.RepliconServiceOperator(
            task_id='update_data_access_scopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data = request_payload.get_access_scopes_payload
        )

        put_key_value = rail.RepliconServiceOperator(
            task_id='put_key_value',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data = request_payload.get_put_keyvalue_payload
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
            severity = 'Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{ dag_run.conf.WBS }}',
                'projectcode': '{{ dag_run.conf.Projectcode }}',
                'status': 'Error',
            },
        )

        has_mandatory_fields >> rail.Label('Yes') >> create_exception_log >> validate_persons_group_entry
        has_mandatory_fields >> rail.Label("No") >> log_madatory_fields_not_present >> catch_and_log_errors
        validate_persons_group_exit1 >> [ensure_projectleader_permissions_group_entry,ensure_comanager_permissions_group_entry]
        [ensure_projectleader_permissions_group_exit, ensure_comanager_permissions_group_exit] >> load_project >> does_project_exist
        apply_project_modifications >> update_project_team_member_assignment
        update_project_team_member_assignment >> should_apply_comanager >> rail.Label('Yes') >> apply_comanager >> update_project_division
        should_apply_comanager >> rail.Label('No') >> update_project_division
        update_project_division >> update_data_access_scopes >> put_key_value >> log_completion >> catch_and_log_errors
        validate_persons_group_exit2 >> catch_and_log_errors
        has_parentwbs >> rail.Label("Yes") >> search_program
        does_project_exist >> rail.Label("Yes") >> get_project_uri >> has_parentwbs >> rail.Label("No") >> update_oef_value
        update_oef_value >> search_program >> apply_project_modifications
        does_project_exist >> rail.Label("No") >> create_project >> unassign_all_users >> get_project_uri

    return dag

rail.for_each_instance(create_child_dag_wbs)
