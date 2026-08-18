import rail
from matlensilver.client_project_task_sync import request_payload
from matlensilver.client_project_task_sync import response_filter
from matlensilver.client_project_task_sync import python_callable_method

# config
# https://github.com/replicon/airflow-integrations/blob/main/dags/matlensilver/client_project_task_sync/config.py


def create_child_dag_wbs(config):
    with rail.create_airflow_dag(
        dag_id=f'matlensilver_client_project_task_sync_process_tasks_{config.instance}',
        description='Matlen_Silver_Client_Project_Task_Sync_Process_Tasks',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_tasks,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        create_task_logs = rail.CreateLogOperator(
            task_id='create_task_logs',
        )

        has_mandatory_fields_for_tasks = rail.IfOperator(
            task_id="has_mandatory_fields_for_tasks",
            test=request_payload.get_all_mandatory_fields_check_tasks,
            yes_task="is_task_already_available",
            no_task="log_mandatory_fields_not_present"
        )

        log_mandatory_fields_not_present = rail.WriteLogOperator(
            task_id='log_mandatory_fields_not_present',
            message='\
                {%- if dag_run.conf.assignmentid | is_falsy -%} \
                    Assignment ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmenttitle | is_falsy -%} \
                    Assignment Title is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmentstartdate | is_falsy -%} \
                    Assignment Start Date is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmentstartdate == "Invalid" -%} \
                    Assignment Start Date is not valid, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmentenddate == "Invalid" -%} \
                    Assignment End Date is not valid, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmentstatus | is_falsy -%} \
                    Assignment Status is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.personid | is_falsy -%} \
                    Person ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.solomonid | is_falsy -%} \
                    Solomon ID is not present in payload, \
                {%- endif -%}\
                {%- if dag_run.conf.assignmentstatus !="Active" or dag_run.conf.assignmentstatus != "Closed" -%} \
                    Assignment Status not present in replicon, \
                {%- endif -%}\
                {%- if dag_run.conf.clientcontactassignmentlevel | is_falsy -%} \
                    Client Contact Assignment Level is not present in payload \
                {%- endif -%}',
            severity='Exception',
            properties={
                'assignmentid': '{{dag_run.conf.assignmentid}}',
                'assignmenttitle': '{{dag_run.conf.assignmenttitle}}',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '{{dag_run.conf.projectid}}',
                'projectname': '{{dag_run.conf.projectname}}',
                'status': "Exception",
            },
        )

        is_task_already_available = rail.IfOperator(
            task_id="is_task_already_available",
            test=lambda dag_run: bool(dag_run.conf['task_uri']),
            yes_task="get_task_uri",
            no_task='create_task'
        )

        create_task = rail.RepliconServiceOperator(
            task_id="create_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_create_task_payload
        )

        get_task_uri = rail.PythonOperator(
            task_id='get_task_uri',
            python_callable=lambda dag_run: dag_run.conf['task_uri'] if dag_run.conf['task_uri'] else rail.result('create_task')[
                'uri'],
        )

        get_oef_values = rail.RepliconServiceOperator(
            task_id="get_oef_values",
            endpoint="/services/ObjectExtensionService1.svc/GetObjectExtensionFieldValues",
            data=request_payload.get_oef_values_payload,
            response_filter=response_filter.get_filtered_oef_values
        )

        apply_task_modifications = rail.RepliconServiceOperator(
            task_id="apply_task_modifications",
            endpoint="/services/TaskService1.svc/CreateTaskOrApplyModifications",
            data=request_payload.get_task_modifications_payload
        )

        has_any_users_to_assign = rail.IfOperator(
            task_id="has_any_users_to_assign",
            test=lambda dag_run: bool(dag_run.conf['personid']) and
            (not rail.find_first_by_attr_and_get_attr(rail.result(
                'get_oef_values'), 'displayText', 'Employee ID')),
            yes_task="get_resource_details",
            no_task="log_completion"
        )

        get_resource_details = rail.RepliconServiceOperator(
            task_id='get_resource_details',
            endpoint='services/UserListService1.svc/GetData',
            data=request_payload.get_resource_payload,
            response_filter=lambda response: response_filter.get_filtered_resource_data(
                response, request_payload.get_dag_run_conf()['personid'])
        )

        is_resource_in_replicon = rail.IfOperator(
            task_id="is_resource_in_replicon",
            test=lambda: rail.result('get_resource_details') != [],
            yes_task="is_resource_already_present_in_project",
            no_task="update_person_id_oef_value"
        )

        is_resource_already_present_in_project = rail.IfOperator(
            task_id="is_resource_already_present_in_project",
            test=lambda dag_run: bool(rail.find_first_by_attr_and_get_attr(
                dag_run.conf['resources'], 'uri', rail.result('get_resource_details')[0]['uri'])),
            yes_task="add_resource_to_task",
            no_task="add_resource_to_project"
        )

        add_resource_to_project = rail.RepliconServiceOperator(
            task_id='add_resource_to_project',
            endpoint='services/ProjectService1.svc/UpdateProjectTeamMemberAssignment',
            data=request_payload.get_add_resource_payload
        )

        add_resource_to_task = rail.RepliconServiceOperator(
            task_id="add_resource_to_task",
            endpoint="/services/TaskService1.svc/UpdateResourceAssignment",
            data=lambda dag_run: {
                "taskUri": rail.result('get_task_uri'),
                "resourceUri": rail.result('get_resource_details')[0]['uri'],
                "isAssigned": "true"
            }
        )

        update_person_id_oef_value = rail.RepliconServiceOperator(
            task_id='update_person_id_oef_value',
            endpoint='/services/ObjectExtensionService1.svc/UpdateObjectExtensionFieldValue',
            data=request_payload.get_update_oef_payload
        )

        log_resource_not_in_replicon = rail.WriteLogOperator(
            task_id='log_resource_not_in_replicon',
            log='{{result("create_task_logs")}}',
            message='Person ID not available/disabled in Replicon,Not added to WBS',
            severity='Exception',
            properties={
                'assignmentid': '{{dag_run.conf.assignmentid}}',
                'assignmenttitle': '{{dag_run.conf.assignmenttitle}}',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '{{dag_run.conf.projectid}}',
                'projectname': '{{dag_run.conf.projectname}}',
                'status': "Exception",
            },
        )

        log_completion = rail.WriteLogOperator(
            task_id='log_completion',
            severity=lambda dag_run: 'Success' if dag_run.conf['clientlogseverity'] == 'Client_Success' and python_callable_method.get_log_task_state(
            ) == 'skipped' else 'Exception' if dag_run.conf['clientlogseverity'] == 'Client_Exception' or python_callable_method.get_log_task_state(
            ) != 'skipped' else 'Error',
            message=lambda dag_run:
            ('Client:'+(dag_run.conf['clientlogsuccess']['message'] if dag_run.conf['clientlogsuccess'] else dag_run.conf['clientlogerror']['message'])+", "
             + 'Project:'+dag_run.conf['projectlog']+", "+'Assignment:'+('Assignment Updated Successfully'
                if python_callable_method.get_log_task_state() == 'skipped' else python_callable_method.get_log_task_state()))
            if dag_run.conf['task_uri'] else
            'Client:' + (dag_run.conf['clientlogsuccess']['message'] if dag_run.conf['clientlogsuccess']
                         else dag_run.conf['clientlogerror']['message'])+", "
            + 'Project:'+dag_run.conf['projectlog'] + ", "+'Assignment:'+('Assignment Added Successfully' if python_callable_method.get_log_task_state()
                                                                          == 'skipped' else python_callable_method.get_log_task_state()),
            properties=lambda dag_run: {
                'assignmentid': dag_run.conf['assignmentid'],
                'assignmenttitle': dag_run.conf['assignmenttitle'],
                'clientid': dag_run.conf['clientid'],
                'clientname': dag_run.conf['clientname'],
                'projectid': dag_run.conf['projectid'],
                'projectname': dag_run.conf['projectname'],
                'status': 'Success' if dag_run.conf['clientlogseverity'] == 'Client_Success' and python_callable_method.get_log_task_state() == 'skipped'
                else 'Exception' if dag_run.conf['clientlogseverity'] == 'Client_Exception' or python_callable_method.get_log_task_state() != 'skipped'
                else 'Error',
            },
        )

        log_task_status = rail.WriteLogOperator(
            task_id='log_task_status',
            log='{{result("create_task_logs") }}',
            message='\
                        {%- if dag_run.conf.task_uri | is_falsy -%} \
                            Assignment Added Successfully \
                        {%- else -%} \
                            Assignment Updated Successfully \
                        {%- endif -%}',
            severity=lambda dag_run: 'Task_Updated' if bool(
                dag_run.conf['task_uri']) else 'Task_Added',
            properties={
                'assignmentid': '{{dag_run.conf.assignmentid}}',
                'assignmenttitle': '{{dag_run.conf.assignmenttitle}}',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '{{dag_run.conf.projectid}}',
                'projectname': '{{dag_run.conf.projectname}}',
                'status': 'Success',
            },
        )

        get_task_success = rail.PythonOperator(
            task_id='get_task_success',
            python_callable=python_callable_method.get_task_success,
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            severity='Error',
            # pylint: disable=line-too-long
            message='{{ get_error_message() }}',
            properties={
                'assignmentid': '{{dag_run.conf.assignmentid}}',
                'assignmenttitle': '{{dag_run.conf.assignmenttitle}}',
                'clientid': '{{dag_run.conf.clientid}}',
                'clientname': '{{dag_run.conf.clientname}}',
                'projectid': '{{dag_run.conf.projectid}}',
                'projectname': '{{dag_run.conf.projectname}}',
                'status': 'Error',
            },
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        create_task_logs >> has_mandatory_fields_for_tasks >> rail.Label(
            'No') >> log_mandatory_fields_not_present >> catch_and_log_errors
        has_mandatory_fields_for_tasks >> rail.Label(
            'Yes') >> is_task_already_available
        is_task_already_available >> rail.Label(
            'No') >> create_task >> get_task_uri
        is_task_already_available >> rail.Label(
            'Yes') >> get_task_uri >> get_oef_values >> apply_task_modifications
        apply_task_modifications >> has_any_users_to_assign >> rail.Label(
            'Yes') >> get_resource_details >> is_resource_in_replicon
        is_resource_in_replicon >> rail.Label(
            'Yes') >> is_resource_already_present_in_project
        is_resource_in_replicon >> rail.Label(
            'No') >> update_person_id_oef_value >> log_resource_not_in_replicon >> log_completion
        has_any_users_to_assign >> rail.Label(
            'No') >> log_completion >> log_task_status
        is_resource_already_present_in_project >> rail.Label(
            'No') >> add_resource_to_project >> add_resource_to_task >> log_completion >> log_task_status >> get_task_success >> catch_and_log_errors
        is_resource_already_present_in_project >> rail.Label(
            'Yes') >> add_resource_to_task
        catch_and_log_errors >> log_to_sumo

    return dag


rail.for_each_instance(create_child_dag_wbs)
