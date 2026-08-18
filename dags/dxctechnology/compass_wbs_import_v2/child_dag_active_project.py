# pylint: disable=too-many-statements
import uuid
from datetime import datetime,timedelta
from airflow.models import Variable
import rail
from dxctechnology.compass_wbs_import_v2.utils import request_payload
from dxctechnology.compass_wbs_import_v2.utils import response_filter
from dxctechnology.compass_wbs_import_v2.tasks.ensure_user_has_permissions import ensure_user_has_permissions
from dxctechnology.compass_wbs_import_v2.tasks.validate_persons_responsible import validate_persons_responsible

def create_child_active_airflow_dag(config):

    with rail.create_airflow_dag(
        dag_id=config.active_project_dagid,
        description='DXC_COMPASS_WBS_Automation Child - Active Projects Processing',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_active_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='has_company_code'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_company_code',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        has_company_code = rail.IfOperator(
            task_id="has_company_code",
            test="{{ dag_run.conf.companycode | length > 0 }}",
            yes_task="create_exception_log",
            no_task="log_no_company_code",
        )

        create_exception_log = rail.CreateLogOperator(
            task_id='create_exception_log',
        )

        validate_persons_group_entry, validate_persons_group_exit = validate_persons_responsible()

        log_no_company_code = rail.WriteLogOperator(
            task_id='log_no_company_code',
            message='\
                {%- if dag_run.conf.payloadcompanycode | length > 0 -%} \
                    Company code "{{ dag_run.conf.payloadcompanycode }}" is not present/disabled in Replicon \
                {%- else -%} \
                    Company code is not present in payload for {{ dag_run.conf.wbs }} \
                {%- endif -%}',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'projectcode': '{{ dag_run.conf.description }}',
                'status': 'Exception',
            }
        )

        ensure_projectleader_permissions_group = ensure_user_has_permissions(
            'projectleader', 'Person responsible 1', 'project leader')
        ensure_comanager_permissions_group= ensure_user_has_permissions(
            'comanager', 'Person responsible 2', 'project co-manager')

        load_project = rail.RepliconServiceOperator(
            task_id='load_project',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data={"projects": [{"name": "{{ dag_run.conf['wbs'] }}"}]},
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                        {"projectDetails": None}])[0]['projectDetails'],
        )

        does_project_exist = rail.IfOperator(
            task_id="does_project_exist",
            test="{{ result('load_project') is not none }}",
            yes_task="get_project_uri",
            no_task="create_project",
        )

        create_project = rail.RepliconServiceOperator(
            task_id='create_project',
            endpoint='/services/ProjectService1.svc/PutProject5',
            data={
                "project": {
                    "target": {"name": "{{ dag_run.conf['wbs'] }}"},
                    "projectInfo": {
                        "name": "{{ dag_run.conf['wbs'] }}",
                        "projectStatusLabel": {"name": "{{ dag_run.conf['status'] }}"},
                        "percentCompleted": 0,
                        "isTimeEntryAllowed": True,
                        "isProjectLeaderApprovalRequired": True,
                        "billingTypeUri": "urn:replicon:billing-type:time-and-material",
                        "timeAndMaterials": {
                            "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable",
                        },
                    }
                }
            }
        )

        unassign_all_users = rail.RepliconServiceOperator(
            task_id='unassign_all_users',
            endpoint='/services/ProjectService1.svc/GetAllUserTeamMemberUri',
            data=lambda :{
                "projectUri": rail.result('create_project')['uri'],
                "resourceUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":department:1",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:unassign"
            }
        )

        set_auto_assign_task = rail.RepliconServiceOperator(
            task_id='set_auto_assign_task',
            endpoint='/services/ProjectService1.svc/PutKeyValueForProject',
            data={
                "projectUri": "{{ result('create_project').uri }}",
                "keyValue": {
                    "keyUri": "urn:replicon:project-key-value-key:project-team-member-assignment-type",
                    "value": {"uri": "urn:replicon:project-team-member-assignment-type:automatically-assign-task"}
                }
            },
        )

        get_project_uri = rail.PythonOperator(
            task_id='get_project_uri',
            python_callable=lambda: rail.result('load_project')['uri'] if rail.result(
                'load_project') else rail.result('create_project')['uri'],
        )

        recievd_wbs_notin_inprogress=rail.IfOperator(
            task_id='recievd_wbs_notin_inprogress',
            test="{{ dag_run.conf.status != 'In Progress' and result('load_project').uri is not none }}",
            yes_task="is_in_progress_in_wts",
            no_task="validate_date_format"
        )

        is_in_progress_in_wts = rail.IfOperator(
            task_id='is_in_progress_in_wts',
            test="{{ result('load_project').status.name != 'Completed'}}",
            yes_task="set_project_complete_in_wts",
            no_task="log_project_already_completed",
        )

        set_project_complete_in_wts = rail.RepliconServiceOperator(
            task_id='set_project_complete_in_wts',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data={
                'target': {'uri': '{{ result("get_project_uri") }}'},
                'modifications': {
                    'statusToApply': {'name': 'Completed'},
                },
                'unitOfWorkId': str(uuid.uuid4()),
            }
        )

        log_project_updated_as_completed = rail.WriteLogOperator(
            task_id='log_project_updated_as_completed',
            message='Project updated as completed',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'projectcode': '{{ dag_run.conf.description }}',
                'status': 'Success',
            }
        )

        log_project_already_completed = rail.WriteLogOperator(
            task_id='log_project_already_completed',
            message='Project is already marked as completed',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'projectcode': '{{ dag_run.conf.description }}',
                'status': 'Skipped',
            }
        )

        def get_validate_date_format_message():
            date_format ="%Y%m%d" # date format in 20060401
            context = rail.get_current_context()
            conf = context['dag_run'].conf
            def parse_date(date_str):
                try:
                    return datetime.strptime(date_str, date_format)
                except: # pylint: disable=bare-except
                    return None
            message = []
            if conf['startdate'] and not parse_date(conf['startdate']):
                message.append('start date is invalid')
            if conf['enddate'] and not parse_date(conf['enddate']):
                message.append('end date is invalid')
            return ",".join(message)

        validate_date_format =rail.PythonOperator(
            task_id="validate_date_format",
            python_callable=get_validate_date_format_message,
        )

        has_validate_date_format =rail.IfOperator(
            task_id="has_validate_date_format",
            test= lambda : not bool(rail.result('validate_date_format')),
            yes_task="is_child_wbs",
            no_task="fail_invalid_date_format",
        )

        fail_invalid_date_format = rail.FailOperator(
            task_id = "fail_invalid_date_format",
            message= "{{ result('validate_date_format') }}"
        )

        is_child_wbs = rail.IfOperator(
            task_id="is_child_wbs",
            test= request_payload.is_child_wbs,
            yes_task="get_parent_wbs_info",
            no_task="apply_project_update_modifications",
        )

        get_parent_wbs_info = rail.RepliconServiceOperator(
            task_id='get_parent_wbs_info',
            endpoint='/services/ProjectService1.svc/BulkGetProjectDetails3',
            data=request_payload.get_parent_wbs_info,
            response_filter=lambda resp: (resp.json()['d'][0:1] or [
                                        {"projectDetails": None}])[0]['projectDetails'],
        )

        apply_project_update_modifications = rail.RepliconServiceOperator(
            task_id='apply_project_update_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_project_update_modifications,
        )

        is_client_present = rail.IfOperator(
            task_id = "is_client_present",
            test = "{{ dag_run.conf.client | is_truthy }}",
            yes_task = "get_client_info",
            no_task = 'should_apply_comanager',
        )

        get_client_info = rail.RepliconServiceOperator(
            task_id='get_client_info',
            endpoint='/services/ClientListService1.svc/GetData',
            data=request_payload.get_client_list_search_param,
            response_filter=response_filter.map_client_uri
        )
        is_client_available_in_replicon = rail.IfOperator(
            task_id = "is_client_available_in_replicon",
            test = "{{ result('get_client_info') | is_truthy }}",
            yes_task = "update_project_client",
            no_task = 'should_apply_comanager',
        )

        update_project_client = rail.RepliconServiceOperator(
            task_id = 'update_project_client',
            endpoint = '/services/ProjectService1.svc/ApplyNewClient2',
            data = request_payload.get_update_client_param
        )

        should_apply_comanager = rail.IfOperator(
            task_id="should_apply_comanager",
            test="{{ result('determine_necessary_comanager_updates').should_apply and \
                result('determine_necessary_comanager_updates').user_uri != result('determine_necessary_projectleader_updates').user_uri }}",
            yes_task="apply_comanager",
            no_task="update_project_division",
        )

        apply_comanager = rail.RepliconServiceOperator(
            task_id='apply_comanager',
            endpoint='/services/ProjectService1.svc/PutExplicitSharingAssignments',
            data={
                "projectUri": "{{ result('get_project_uri') }}",
                "sharedUris": ["{{ result('determine_necessary_comanager_updates').user_uri }}"]
            }
        )

        update_project_division = rail.RepliconServiceOperator(
            task_id='update_project_division',
            endpoint='/services/ProjectService1.svc/UpdateDivision2',
            data={
                "projectUri": "{{ result('get_project_uri') }}",
                "division": {"uri": "{{ dag_run.conf['companycode'] }}"}
            }
        )

        update_data_access_scopes = rail.RepliconServiceOperator(
            task_id='update_data_access_scopes',
            endpoint='/services/ProjectService1.svc/PutEligibleProjectTeamMemberDataAccessScopesForProject',
            data={
                "projectUri": "{{ result('get_project_uri') }}",
                "teamMemberDataAccessScopes": [
                    {
                        "locations": [],
                        "divisions": [{"uri": "{{ dag_run.conf['projectteamassignment'] if dag_run.conf['wbsofferinggroupvalue'] != 'Velocity Only' \
                            else dag_run.conf['psaprojectteamassignmenturi'] }}"}],
                        "costCenters": [],
                        "serviceCenters": [],
                        "departmentGroups": [{"uri": "{{ dag_run.conf['organizationunituri'] }}"}],
                        "employeeTypeGroups": []
                    }
                ]
            }
        )

        get_data_of_child_wbs = rail.RepliconServiceOperator(
            task_id='get_data_of_child_wbs',
            endpoint='/services/ProjectListService1.svc/GetData',
            data=request_payload.get_child_projects,
            response_filter=response_filter.get_filtered_child_projects
        )

        has_childs_wbs = rail.IfOperator(
            task_id="has_childs_wbs",
            test=lambda: bool(rail.result('get_data_of_child_wbs')),
            yes_task="process_child_projects",
            no_task="log_completion",
        )

        process_child_projects = rail.TriggerDagRunForEachItemOperator(
            task_id = 'process_child_projects',
            retries=0,
            items="{{ result('get_data_of_child_wbs') | to_json }}",
            trigger_dag_id=config.process_child_project_dagid,
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            conf=lambda item,dag_run:{
                'childwbs': item['textValue'],
                'childwbsuri': item['uri'],
                'timetrackingattributetaguri': dag_run.conf['timetrackingattribute'],
                'timetrackingattributedefinitionuri': dag_run.conf['timetrackingattributeuri'],
                'parentwbs': dag_run.conf['wbs'],
                'wbsofferinggroupvalue': dag_run.conf['wbsofferinggroupvalue'],
                'wbsofferinggroupdefinitionuri' : dag_run.conf['wbsofferinggroupuri'],
                'wbsofferinggrouptaguri' : dag_run.conf['wbsofferinggroup'],
                'psa_x_flaguri': dag_run.conf['psa_x_flaguri'],
                'psaflagdefinitionuri': dag_run.conf['psaflagdefinitionuri']
            }
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            # pylint: disable=line-too-long
            message='\
                {%- if result("create_exception_log") | load_all_records | length == 0 -%} \
                    {{ "Project created successfully" if result("create_project") | is_truthy else "Project updated sucessfully" }} \
                {%- else -%} \
                    {{ "Project created partially, " if result("create_project") | is_truthy else "Project updated partially, " -}} \
                    {{ result("create_exception_log") | load_all_records | map_to_attr("message") | join(", ") }} \
                {%- endif -%}',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'projectcode': '{{ dag_run.conf.description }}',
                'status': '{{ "Success" if result("create_exception_log") | load_all_records | length == 0 else "Exception" }}',
            },
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'projectname': '{{ dag_run.conf.wbs }}',
                'projectcode': '{{ dag_run.conf.description }}',
                'status': 'Error',
            },
        )

        # pylint: disable=line-too-long
        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> has_company_code
        has_company_code >> rail.Label(
            "Yes") >> create_exception_log >> validate_persons_group_entry
        has_company_code >> rail.Label(
            "No") >> log_no_company_code >> finish >> catch_and_log_errors
        validate_persons_group_exit >> ensure_projectleader_permissions_group >>\
            ensure_comanager_permissions_group >> load_project >> does_project_exist >> rail.Label(
                "Yes") >> get_project_uri >> recievd_wbs_notin_inprogress >> rail.Label("Yes") >> is_in_progress_in_wts >> rail.Label(
                "Yes") >> set_project_complete_in_wts >> log_project_updated_as_completed >> validate_date_format
        recievd_wbs_notin_inprogress >> rail.Label(
            "No") >> validate_date_format
        is_in_progress_in_wts >> rail.Label(
            "No") >> log_project_already_completed >> validate_date_format
        validate_date_format >> has_validate_date_format >> rail.Label(
            "Yes") >> is_child_wbs

        is_child_wbs >> rail.Label('Yes') >> get_parent_wbs_info >> apply_project_update_modifications
        is_child_wbs >> rail.Label('No') >> apply_project_update_modifications

        validate_date_format >> has_validate_date_format >> rail.Label(
            "No") >> fail_invalid_date_format >> finish >> catch_and_log_errors
        apply_project_update_modifications >> is_client_present >> rail.Label('Yes') >> get_client_info >> is_client_available_in_replicon >> rail.Label('Yes') >> update_project_client >>\
            should_apply_comanager
        is_client_present >> rail.Label('No') >> should_apply_comanager
        is_client_available_in_replicon >> rail.Label('No') >> should_apply_comanager
        should_apply_comanager >> rail.Label(
            'Yes') >> apply_comanager >> update_project_division
        should_apply_comanager >> rail.Label('No') >> update_project_division
        update_project_division >> update_data_access_scopes >> get_data_of_child_wbs >> has_childs_wbs
        has_childs_wbs >> rail.Label('No') >> log_completion
        has_childs_wbs >> rail.Label('Yes') >> process_child_projects >> log_completion
        log_completion >> finish >> catch_and_log_errors
        does_project_exist >> rail.Label(
            "No") >> create_project >> unassign_all_users >> set_auto_assign_task >> get_project_uri

    return dag

rail.for_each_instance(create_child_active_airflow_dag)
