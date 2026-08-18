from datetime import timedelta
import rail
from transparentbpo.project_and_task_sync.utils import request_payload, python_callable
from transparentbpo.project_and_task_sync.mapper.default_level_2_tasks_mapper import level_2_tasks_mapper
null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.master_dag_id,
        description=f'Transparentbpo_project_and_task_sync_master dag',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_master,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        if_clientname_is_present = rail.IfOperator(
            task_id="if_clientname_is_present",
            test=lambda dag_run: bool(dag_run.conf.get('clientName')),
            yes_task='get_project_details',
            no_task='log_data_ignored'
        )
        
        log_data_ignored = rail.WriteLogOperator(
            task_id='log_data_ignored',
            log='{{ dag_run.conf.project_log }}',
            message="Project data ignored as no clientname value received",
            severity='Ignored',
            properties=lambda dag_run:{
                'projectname': dag_run.conf['clientName'],
                'tasklevel1': dag_run.conf['customDirectIndirect'],
                'tasklevel2': dag_run.conf['projectName'],
                'status': 'Ignored',
                'details': 'project could not be created as no clientname value received',
                'timelog': dag_run.conf['timelog'],
                'employeenumber': dag_run.conf['employeeNumber'],
            }
        )

        get_project_details = rail.RepliconServiceOperator(
            task_id='get_project_details',
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data=request_payload.get_project_details_payload
        )

        if_project_details_not_present = rail.IfOperator(
            task_id='if_project_details_not_present',
            test=lambda : not(rail.result('get_project_details')),
            yes_task='create_project_copy_batch',
            no_task='project_uri_to_pass'
        )

        create_project_copy_batch = rail.RepliconServiceOperator(
            task_id='create_project_copy_batch',
            endpoint='/services/ProjectService1.svc/CreateProjectCopyBatch2',
            data=lambda dag_run: request_payload.create_project_copy_batch_payload(dag_run, config.run_date_format)
        )

        project_copy_batch_group_entry, project_copy_batch_group_exit = rail.batch_execution(
            'execute_project_copy_batch', create_project_copy_batch.task_id)

        get_project_copy_batch_results = rail.RepliconServiceOperator(
            task_id='get_project_copy_batch_results',
            endpoint='/services/ProjectService1.svc/GetProjectCopyBatchResults',
            data=lambda: {
                "projectCopyBatchUri": rail.result('create_project_copy_batch')
            }
        )

        update_project_leader = rail.RepliconServiceOperator(
            task_id="update_project_leader",
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data=lambda: {
                "projectUri": rail.result('get_project_copy_batch_results')["project"]["uri"]
            }
        )
        
        project_uri_to_pass = rail.PythonOperator(
            task_id = "project_uri_to_pass",
            python_callable=lambda : rail.result('get_project_details')[0]['uri'] if bool(
                rail.result('get_project_details')) else rail.result('get_project_copy_batch_results')["project"]["uri"]
        )
        

        if_uri_and_customfield_direct_or_indirect_is_present = rail.IfOperator(
            task_id='if_uri_and_customfield_direct_or_indirect_is_present',
            test=lambda dag_run : bool(dag_run.conf["customDirectIndirect"]) and bool(
                rail.result("project_uri_to_pass")),
            yes_task='get_project_task_details',
            no_task='add_success_entries'
        )

        get_project_task_details = rail.RepliconServiceOperator(
            task_id='get_project_task_details',
            endpoint="/services/ProjectService1.svc/BulkGetTaskDetails",
            data=lambda: {
                "pageIndex": "1",
                    "pageSize": "10000",
                    "projectUris": [ rail.result("project_uri_to_pass") ]
            }
        )
        
        get_matching_task_uri = rail.PythonOperator(
            task_id='get_matching_task_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(
                    rail.result('get_project_task_details'), 'displayText', dag_run.conf['customDirectIndirect'], 'uri', '')
        )
        
        if_matching_task_found = rail.IfOperator(
            task_id='if_matching_task_found',
            test=lambda: bool(rail.result('get_matching_task_uri')),
            yes_task='task_level_1_uri',
            no_task='add_task_level_1'
        )

        task_level_1_uri = rail.PythonOperator(
            task_id='task_level_1_uri',
            python_callable=lambda: rail.result('get_matching_task_uri')
        )

        if_projectname_present_1 = rail.IfOperator(
            task_id='if_projectname_present_1',
            test="{{ dag_run.conf.projectName | is_truthy }}",
            yes_task='task_level_2_uri',
            no_task='add_success_entries'
        )
        
        task_level_2_uri = rail.PythonOperator(
            task_id='task_level_2_uri',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'get_project_task_details'), 'displayText', f'''{dag_run.conf['customDirectIndirect']} / {dag_run.conf['projectName']}''', 'uri', '')
        )

        if_task_level_2_present = rail.IfOperator(
            task_id='if_task_level_2_present',
            test=lambda: bool(rail.result('task_level_2_uri')),
            yes_task='if_useruri_present_1',
            no_task='add_task_level_2'
        )

        if_useruri_present_1 = rail.IfOperator(
            task_id='if_useruri_present_1',
            test="{{ dag_run.conf.useruri | is_truthy }}",
            yes_task='bulk_get_resource_assignments',
            no_task='add_success_entries'
        )

        bulk_get_resource_assignments = rail.RepliconServiceOperator(
            task_id='bulk_get_resource_assignments',
            endpoint='/services/TaskService1.svc/BulkGetResourceAssignments',
            data=lambda dag_run: request_payload.bulk_get_resource_assignments(dag_run, config.run_date_format),
            data_handler=lambda response: list(map(lambda x: {
                'user_uri': x['resource']['user']['uri'] if x['resource']['user'] else '',
                'user_loginname': x['resource']['user']['loginName'] if x['resource']['user'] else ''
            }, response[0]['assignments'])) if (response and response[0]) else []
        )

        if_user_not_assigned_at_task_level_2 = rail.IfOperator(
            task_id='if_user_not_assigned_at_task_level_2',
            test=lambda dag_run: not(bool(rail.find_first_by_attr_and_get_attr(
                rail.result("bulk_get_resource_assignments"), 'user_uri', dag_run.conf['useruri'], ''))),
            yes_task='update_resource_assignment_task_level_2',
            no_task='add_success_entries'
        )

        update_resource_assignment_task_level_2 = rail.RepliconServiceOperator(
            task_id='update_resource_assignment_task_level_2',
            endpoint='/services/TaskService1.svc/UpdateResourceAssignment',
            data=lambda dag_run: {
                "taskUri": rail.result("task_level_2_uri"),
                "resourceUri": dag_run.conf['useruri'],
                "isAssigned": "true"
            }
        )

        add_task_level_2 = rail.RepliconServiceOperator(
            task_id='add_task_level_2',
            endpoint='/services/ProjectService1.svc/AddTask',
            data=lambda dag_run: request_payload.add_task_level_2_payload(
                dag_run, rail.result("task_level_1_uri"), config.run_date_format)
        )

        if_useruri_present_2 = rail.IfOperator(
            task_id='if_useruri_present_2',
            test="{{ dag_run.conf.useruri | is_truthy }}",
            yes_task='update_resource_assignment_2',
            no_task='add_success_entries'
        )

        update_resource_assignment_2 = rail.RepliconServiceOperator(
            task_id='update_resource_assignment_2',
            endpoint='/services/TaskService1.svc/UpdateResourceAssignment',
            data=lambda dag_run: {
                "taskUri": rail.result("add_task_level_2")['uri'],
                "resourceUri": dag_run.conf['useruri'],
                "isAssigned": "true"
            }
        )

        add_task_level_1 = rail.RepliconServiceOperator(
            task_id='add_task_level_1',
            endpoint='/services/ProjectService1.svc/AddTask',
            data=lambda dag_run: request_payload.add_task_level_1_payload(dag_run, config.run_date_format)
        )

        if_projectname_present_2 = rail.IfOperator(
            task_id='if_projectname_present_2',
            test="{{ dag_run.conf.projectName | is_truthy }}",
            yes_task='add_task_level_2_2',
            no_task='transparentbpo_default_level_2_tasks'
        )

        add_task_level_2_2 = rail.RepliconServiceOperator(
            task_id='add_task_level_2_2',
            endpoint='/services/ProjectService1.svc/AddTask',
            data=lambda dag_run: request_payload.add_task_level_2_payload(
                dag_run, rail.result("add_task_level_1")['uri'], config.run_date_format)
        )

        if_useruri_present_3 = rail.IfOperator(
            task_id='if_useruri_present_3',
            test="{{ dag_run.conf.useruri | is_truthy }}",
            yes_task='update_resource_assignment_3',
            no_task='add_success_entries'
        )

        update_resource_assignment_3 = rail.RepliconServiceOperator(
            task_id='update_resource_assignment_3',
            endpoint='/services/TaskService1.svc/UpdateResourceAssignment',
            data=lambda dag_run: {
                "taskUri": rail.result("add_task_level_2_2")['uri'],
                "resourceUri": dag_run.conf['useruri'],
                "isAssigned": "true"
            }
        )

        transparentbpo_default_level_2_tasks = rail.PythonOperator(
            task_id='transparentbpo_default_level_2_tasks',
            python_callable=lambda dag_run: python_callable.serialize_level_2_tasks(
                level_2_tasks_mapper, dag_run)
        )

        add_default_task_level_2 = rail.RepliconServiceCallForEachItemOperator(
            task_id='add_default_task_level_2',
            items="{{ result('transparentbpo_default_level_2_tasks') }}",
            endpoint='/services/ProjectService1.svc/AddTask',
            data=lambda dag_run, item: request_payload.add_default_task_level_2_payload(item, dag_run, config.run_date_format)
        )

        add_success_entries = rail.WriteLogOperator(
            task_id='add_success_entries',
            log='{{ dag_run.conf.project_log }}',
            message="Project Updated",
            severity='Success',
            properties=lambda dag_run:{
                'projectname': dag_run.conf['clientName'],
                'tasklevel1': dag_run.conf['customDirectIndirect'],
                'tasklevel2': dag_run.conf['projectName'],
                'status': 'Success',
                'details': 'Project Updated',
                'timelog': dag_run.conf['timelog'],
                'employeenumber': dag_run.conf['employeeNumber'],
            }
        )

        one_failed = rail.EmptyOperator(
            task_id='one_failed',
            trigger_rule='one_failed'
        )

        if_error_condition_met = rail.IfOperator(
            task_id='if_error_condition_met',
            test=python_callable.get_error_message,
            yes_task='catch_and_log_error'
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log='{{ dag_run.conf.project_log }}',
            message="Failed",
            severity='Error',
            trigger_rule='one_failed',
            properties=lambda dag_run:{
                'projectname': dag_run.conf['clientName'],
                'tasklevel1': dag_run.conf['customDirectIndirect'],
                'tasklevel2': dag_run.conf['projectName'],
                'status': 'Error',
                'details': rail.render_template("{{get_error_message()}}"),
                'timelog': dag_run.conf['timelog'],
                'employeenumber': dag_run.conf['employeeNumber'],
            }
        )
        
        if_clientname_is_present >> rail.Label("No") >> log_data_ignored
        if_clientname_is_present >> rail.Label("Yes") >> get_project_details
        
        get_project_details >> if_project_details_not_present 
        
        if_project_details_not_present >> rail.Label("Yes") >> create_project_copy_batch >> project_copy_batch_group_entry \
            >> project_copy_batch_group_exit >> get_project_copy_batch_results >> update_project_leader >> project_uri_to_pass

        if_project_details_not_present >> rail.Label(
            "No") >> project_uri_to_pass
        
        project_uri_to_pass >> if_uri_and_customfield_direct_or_indirect_is_present
        
        if_uri_and_customfield_direct_or_indirect_is_present >> rail.Label(
            "No") >> add_success_entries
        
        if_uri_and_customfield_direct_or_indirect_is_present >> rail.Label("Yes") >> \
            get_project_task_details >> get_matching_task_uri >> if_matching_task_found 
            
        if_matching_task_found >> rail.Label("Yes") >> task_level_1_uri >> if_projectname_present_1 
        
        if_projectname_present_1 >> rail.Label("Yes") >> task_level_2_uri >> if_task_level_2_present
        
        if_task_level_2_present >> rail.Label("Yes") >> if_useruri_present_1
        
        if_useruri_present_1 >> rail.Label("Yes") >> \
            bulk_get_resource_assignments >> if_user_not_assigned_at_task_level_2 >> rail.Label(
                "Yes") >> update_resource_assignment_task_level_2 >> add_success_entries

        

        if_task_level_2_present >> rail.Label("No") >> add_task_level_2 >> if_useruri_present_2 >> rail.Label(
            "Yes") >> update_resource_assignment_2 >> add_success_entries

        if_matching_task_found >> rail.Label("No") >> add_task_level_1 >> if_projectname_present_2 
        
        if_projectname_present_2 >> rail.Label("Yes") >> add_task_level_2_2 >> if_useruri_present_3 
            
        if_useruri_present_3 >> rail.Label("Yes") >> update_resource_assignment_3 >> add_success_entries
        
            
        if_error_condition_met >> rail.Label("Yes") >> catch_and_log_error

        if_projectname_present_2 >> rail.Label(
            "No") >> transparentbpo_default_level_2_tasks
        
        transparentbpo_default_level_2_tasks >>\
            add_default_task_level_2 >> add_success_entries >> one_failed >> if_error_condition_met 

        if_useruri_present_3 >> rail.Label(
            "No") >> add_success_entries

        if_projectname_present_1 >> rail.Label("No") >> add_success_entries

        if_useruri_present_1 >> rail.Label("No") >> add_success_entries

        if_user_not_assigned_at_task_level_2 >> rail.Label(
            "No") >> add_success_entries

        if_useruri_present_2 >> rail.Label("No") >> add_success_entries

    return dag


rail.for_each_instance(create_child_dag)
