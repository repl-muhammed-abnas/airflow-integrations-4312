from datetime import timedelta
import rail
from matlensilver.client_project_task_sync import request_payload
from matlensilver.client_project_task_sync import python_callable_method
from matlensilver.client_project_task_sync import response_filter

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/matlensilver/client_project_task_sync/config.py


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlensilver_client_project_task_sync_process_projects_{config.instance}',
        description='Matlen_Silver_Client_Project_Task_Sync_Process_Projects',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_projects,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        project_id = "{{dag_run.conf.projectid}}"

        create_project_logs = rail.CreateLogOperator(
            task_id='create_project_logs',
        )

        has_mandatory_fields_for_projects = rail.IfOperator(
            task_id="has_mandatory_fields_for_projects",
            test=request_payload.get_all_mandatory_fields_check_projects,
            yes_task="query_task_data",
            no_task="query_task_data_for_logging"
        )

        query_task_data_for_logging = rail.QueryCollectionOperator(
            task_id='query_task_data_for_logging',
            query=f'''SELECT DISTINCT assignmentid, assignmenttitle, projectid, projectname
                FROM inputdatacollection WHERE projectid ='{project_id}' ''',
        )

        log_mandatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_fields_not_present',
            items="{{ result('query_task_data_for_logging') }}",
            message='\
                {%- if dag_run.conf.projectname | is_falsy -%} \
                    Project Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectid | is_falsy -%} \
                    Project ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectstatus | is_falsy -%} \
                    Project Status is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectstartdate | is_falsy -%} \
                    Project Start Date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.projectstartdate == "Invalid" -%} \
                    Project Start Date is not in valid format, \
                {%- endif -%}\
                {%- if dag_run.conf.projectenddate == "Invalid" -%} \
                    Project End Date is not in valid format, \
                {%- endif -%}\
                {%- if dag_run.conf.projecttype | is_falsy -%} \
                    Project Type is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.client_error_log | is_truthy -%} \
                    {{dag_run.conf.client_error_log}} \
                {%- endif -%}\
                {%- if dag_run.conf.clientname | is_falsy -%} \
                    Client Name is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.clientid | is_falsy -%} \
                    Client Code is not present in payload, \
                {%- endif -%}',
            severity=lambda dag_run: 'Exception' if not dag_run.conf['client_error_log'] else 'Error',
            properties=lambda item, dag_run: {
                'assignmentid': item['assignmentid'],
                'assignmenttitle': item['assignmenttitle'],
                'clientid': request_payload.get_dag_run_conf()['clientid'],
                'clientname': request_payload.get_dag_run_conf()['clientname'],
                'projectid': item['projectid'],
                'projectname': item['projectname'],
                'status': 'Exception' if not dag_run.conf['client_error_log'] else 'Error',
            }
        )

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

        is_project_fixed_bid = rail.IfOperator(
            task_id="is_project_fixed_bid",
            test=lambda dag_run: dag_run.conf['projecttype'] == 'Fixed Price',
            yes_task="update_project_fixed_bid_rate",
            no_task="get_project_uri",
        )

        update_project_fixed_bid_rate = rail.RepliconServiceOperator(
            task_id='update_project_fixed_bid_rate',
            endpoint='services/FixedBidProjectService1.svc/UpdateProjectFixedBidRate',
            data=request_payload.get_update_project_fixed_bid_rate_payload,
        )

        get_project_uri = rail.PythonOperator(
            task_id='get_project_uri',
            python_callable=lambda: rail.result('load_project')['uri'] if rail.result(
                'load_project') else rail.result('create_project')['uri'],
        )

        apply_project_modifications = rail.RepliconServiceOperator(
            task_id='apply_project_modifications',
            endpoint='/services/ProjectService1.svc/CreateProjectOrApplyModifications',
            data=request_payload.get_project_modifications,
        )

        query_task_data = rail.QueryCollectionOperator(
            task_id='query_task_data',
            query=f'''SELECT DISTINCT assignmentid,assignmenttitle,assignmentstartdate,assignmentenddate,
                assignmentstatus,personid,solomonid,clientcontactassignmentlevel,assignmentbillingclient,
                assignmentcontactemail, assignmentcontact,billingclientstreet,billingclientcity,
                billingclientstate,billingclientzip,projectid,projectname,clientmanager,projectclientcontact
                FROM inputdatacollection WHERE projectid ='{project_id}' ''',
        )

        get_task_oefs = rail.RepliconServiceOperator(
            task_id="get_task_oefs",
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={"bindingContextUri": "urn:replicon:object-type:task"},
            data_handler=lambda oefs: {
                'clientcontacturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Client Contact', 'uri'),
                'clientcontactassignmentleveluri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Client Contact Assignment Level', 'uri'),
                'empiduri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Employee ID', 'uri'),
                'solomoniduri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Solomon ID', 'uri'),
                'assignmentbillingclienturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Assignment Billing Client', 'uri'),
                'billingclientstreeturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Billing Client Street', 'uri'),
                'billingclientcityuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Billing Client City', 'uri'),
                'billingclientstateuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Billing Client State', 'uri'),
                'billingclientzipuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Billing Client Zip', 'uri'),
                'assignmentcontacturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Assignment Contact', 'uri'),
                'assignmentcontactemailuri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Assignment Contact Email', 'uri'),
                'clientmanageruri': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Client Manager', 'uri'),
                'projectclientcontacturi': rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Project Client Contact Assignment Level', 'uri')

            },
        )

        get_project_team_member_details = rail.RepliconServiceOperator(
            task_id="get_project_team_member_details",
            endpoint="/services/ProjectService1.svc/GetAllProjectTeamMemberDetails",
            data=request_payload.get_project_team_member_payload,
            response_filter=response_filter.get_filtered_resources
        )

        get_all_project_tasks_from_replicon = rail.RepliconServiceOperator(
            task_id="get_all_project_tasks_from_replicon",
            endpoint="/services/TaskService1.svc/GetDescendantTaskDetails",
            data=request_payload.get_project_tasks_payload,
            response_filter=response_filter.get_filtered_tasks
        )

        process_tasks = rail.TriggerDagRunForEachItemOperator(
            task_id='process_tasks',
            retries=0,
            items="{{ result('query_task_data') }}",
            execution_timeout=timedelta(days=14),
            trigger_dag_id=f'matlensilver_client_project_task_sync_process_tasks_{config.instance}',
            conf=lambda item: {
                'assignmentid': item['assignmentid'],
                'assignmenttitle': item['assignmenttitle'],
                'assignmentstartdate': request_payload.get_datetime_object(item['assignmentstartdate']),
                'assignmentenddate': request_payload.get_datetime_object(item['assignmentenddate']),
                'assignmentstatus': item['assignmentstatus'],
                'personid': item['personid'],
                'solomonid': item['solomonid'],
                'clientcontactassignmentlevel': item['clientcontactassignmentlevel'],
                'assignmentbillingclient': item['assignmentbillingclient'],
                'assignmentcontact': item['assignmentcontact'],
                'assignmentcontactemail': item['assignmentcontactemail'],
                'billingclientstreet': item['billingclientstreet'],
                'billingclientcity': item['billingclientcity'],
                'billingclientstate': item['billingclientstate'],
                'billingclientzip': item['billingclientzip'],
                'projectid': item['projectid'],
                'projectname': item['projectname'],
                'projectclientcontact': item['projectclientcontact'],
                'clientmanager': item['clientmanager'],
                'clientid': request_payload.get_dag_run_conf()['clientid'],
                'clientname': request_payload.get_dag_run_conf()['clientname'],
                'projecturi': rail.result('get_project_uri'),
                'clientcontacturi': rail.result('get_task_oefs')['clientcontacturi'],
                'clientcontactassignmentleveluri': rail.result('get_task_oefs')['clientcontactassignmentleveluri'],
                'personiduri': rail.result('get_task_oefs')['empiduri'],
                'solomoniduri': rail.result('get_task_oefs')['solomoniduri'],
                'projectclientcontacturi': rail.result('get_task_oefs')['projectclientcontacturi'],
                'assignmentbillingclienturi': rail.result('get_task_oefs')['assignmentbillingclienturi'],
                'assignmentcontacturi': rail.result('get_task_oefs')['assignmentcontacturi'],
                'assignmentcontactemailuri': rail.result('get_task_oefs')['assignmentcontactemailuri'],
                'billingclientstreeturi': rail.result('get_task_oefs')['billingclientstreeturi'],
                'billingclientcityuri': rail.result('get_task_oefs')['billingclientcityuri'],
                'billingclientstateuri': rail.result('get_task_oefs')['billingclientstateuri'],
                'billingclientzipuri': rail.result('get_task_oefs')['billingclientzipuri'],
                'clientmanageruri': rail.result('get_task_oefs')['clientmanageruri'],
                'task_uri': rail.find_first_by_attr_and_get_attr(rail.result('get_all_project_tasks_from_replicon'), 'taskcode', item['assignmentid'], 'uri')
                if rail.result('get_all_project_tasks_from_replicon') != [] else False,
                'resources': rail.result('get_project_team_member_details'),
                'clientlogsuccess': request_payload.get_dag_run_conf()['client_success_log']
                if request_payload.get_dag_run_conf()['client_success_log'] else None,
                'clientlogerror': request_payload.get_dag_run_conf()['client_error_log']
                if request_payload.get_dag_run_conf()['client_error_log'] else None,
                'clientlogseverity': (request_payload.get_dag_run_conf()['client_success_log'])['severity']
                if request_payload.get_dag_run_conf()['client_success_log'] else None,
                'projectlog': 'Project Updated Successfully' if bool(rail.result('load_project')) else 'Project Added Successfully',
                'filename': request_payload.get_dag_run_conf()['filename']

            }
        )

        wait_for_process_tasks = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_tasks',
            dag_runs='{{ result("process_tasks") }}',
            execution_timeout=timedelta(days=14)
        )

        log_project_status = rail.WriteLogOperator(
            task_id='log_project_status',
            log='{{ result("create_project_logs") }}',
            message='\
                {%- if result("load_project") | is_falsy -%} \
                    Project Added Successfully \
                {%- else -%} \
                    Project Updated Successfully \
                {%- endif -%}',
            severity=lambda: 'Project_Updated' if bool(
                rail.result('load_project')) else 'Project_Added',
        )

        get_project_success_status = rail.PythonOperator(
            task_id='get_project_success_status',
            python_callable=python_callable_method.get_project_success_status,
        )

        get_task_success_status = rail.GatherResultsFromDagRunsOperator(
            task_id='get_task_success_status',
            dag_runs="{{ result('process_tasks') }}",
            dagrun_task_id='get_task_success',
            flatten=True,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            items="{{ result('query_task_data') }}",
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties=lambda item: {
                'assignmentid': item['assignmentid'],
                'assignmenttitle': item['assignmenttitle'],
                'clientid': request_payload.get_dag_run_conf()['clientid'],
                'clientname': request_payload.get_dag_run_conf()['clientname'],
                'projectid': item['projectid'],
                'projectname': item['projectname'],
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_project_logs >> has_mandatory_fields_for_projects >> rail.Label(
            'No') >> query_task_data_for_logging >> log_mandatory_fields_not_present >> catch_and_log_errors
        has_mandatory_fields_for_projects >> rail.Label(
            'Yes') >> query_task_data >> load_project >> does_project_exist >> rail.Label('No') >> create_project
        create_project >> is_project_fixed_bid >> rail.Label(
            'Yes') >> update_project_fixed_bid_rate >> get_project_uri
        create_project >> is_project_fixed_bid >> rail.Label(
            'No') >> get_project_uri
        does_project_exist >> rail.Label(
            'Yes') >> get_project_uri >> apply_project_modifications >> get_task_oefs
        get_task_oefs >> get_project_team_member_details >> get_all_project_tasks_from_replicon
        get_all_project_tasks_from_replicon >> process_tasks >> wait_for_process_tasks >> log_project_status
        log_project_status >> get_project_success_status >> get_task_success_status >> catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
