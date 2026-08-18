from datetime import timedelta
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.process_tasks_per_project_dag_id,
        description=f'Audaxgroup process tasks per project {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=1,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)


        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='parse_csv_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='parse_csv_2',
            end_task='catch_and_log_errors',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        parse_csv_2 = rail.PythonOperator(
            task_id='parse_csv_2',
            python_callable=lambda dag_run: rail.load_all_records(
                dag_run.conf['projecttaskdata']),
        )

        get_all_custom_fields_4=rail.RepliconServiceOperator(
            task_id='get_all_custom_fields_4',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:task"
                }
        )

        log_task_business_unit_uri_5=rail.PythonOperator(
            task_id='log_task_business_unit_uri_5',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_4'),'displayText', "Task Business Unit",'uri', '')
        )

        get_all_custom_field_drop_down_options_6=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_6',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
                "customFieldUri": "{{ result('log_task_business_unit_uri_5') }}"
                }
        )

        log_deal_opportunity_i_d_task_uri_7=rail.PythonOperator(
            task_id='log_deal_opportunity_i_d_task_uri_7',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_4'),'displayText', "Deal Opportunity ID (Task)",'uri', '')
        )


        log_task_department_uri_8=rail.PythonOperator(
            task_id='log_task_department_uri_8',
            python_callable= lambda: rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_fields_4'),'displayText', "Task Department", 'uri', '')
        )


        get_all_custom_field_drop_down_options_9=rail.RepliconServiceOperator(
            task_id='get_all_custom_field_drop_down_options_9',
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions",
            data={
            "customFieldUri": "{{ result('log_task_department_uri_8') }}"
            }
        )
        declare_list = rail.SetVariableOperator(
            task_id='declare_list',
            append=False,
            name='dagrunlist',
            value=[]
        )

        foreach_parse_csv_2_13=rail.ForEachOperator(
            task_id='foreach_parse_csv_2_13',
            items=lambda: rail.result('parse_csv_2'),
            start_task = 'if_foreach_parse_csv_2_13_column_1_present_14',
            end_task = 'foreach_parse_csv_2_13_end'
        )

        if_foreach_parse_csv_2_13_column_1_present_14=rail.IfOperator(
            task_id='if_foreach_parse_csv_2_13_column_1_present_14',
            test='''{{ result('foreach_parse_csv_2_13').get('tasklevel1') | is_truthy  and result('foreach_parse_csv_2_13').get('tasklevel2') | is_falsy  and result('foreach_parse_csv_2_13').get('tasklevel3') | is_falsy }}''',
            yes_task="trigger_dag_run_add_update_tasksasync_15",
            no_task="if_foreach_parse_csv_2_13_column_1_present_16",
        )

        trigger_dag_run_add_update_tasksasync_15=rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_add_update_tasksasync_15',
            trigger_dag_id=config.add_update_tasks_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "projecturi": dag_run.conf['projecturi'],
                "taskcode": rail.result('foreach_parse_csv_2_13').get('taskcode').strip()[:50] if rail.result('foreach_parse_csv_2_13').get('taskcode') else '',
                "timeentrystartdate": rail.result('foreach_parse_csv_2_13').get('timeentrystartdate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentrystartdate') else '',
                "timeentryenddate":  rail.result('foreach_parse_csv_2_13').get('timeentryenddate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentryenddate') else '',
                "assignedusers": rail.result('foreach_parse_csv_2_13').get('assignedusers').strip() if rail.result('foreach_parse_csv_2_13').get('assignedusers') else '',
                "assigneddepartments":  rail.result('foreach_parse_csv_2_13').get('assigneddepartments').strip() if rail.result('foreach_parse_csv_2_13').get('assigneddepartments') else '',
                "istimeentryallowed": rail.result('foreach_parse_csv_2_13').get('istimeentryallowed').strip() if rail.result('foreach_parse_csv_2_13').get('istimeentryallowed') else '',
                "taskdescription":  rail.result('foreach_parse_csv_2_13').get('taskdescription').strip() if rail.result('foreach_parse_csv_2_13').get('taskdescription') else '',
                "taskinfo1":  rail.result('foreach_parse_csv_2_13').get('taskinfo1').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "taskinfo2":  rail.result('foreach_parse_csv_2_13').get('taskinfo2').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo2') else '',
                "taskinfo3":  rail.result('foreach_parse_csv_2_13').get('taskinfo3').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tenanturl": rail.get_tenant_slug(),
                "parentjobid": dag_run.conf['parent_ecid'],
                "TaskBusinessUnitURI": rail.result('log_task_business_unit_uri_5'),
                "taskinfo1URI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_6'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo1'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "DealOpportunityIDTaskURI": rail.result('log_deal_opportunity_i_d_task_uri_7'),
                "TaskDepartmentURI": rail.result('log_task_department_uri_8'),
                "taskinfo3URI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_9'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo3'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tasklevel1":rail.result('foreach_parse_csv_2_13').get('tasklevel1').strip() if  rail.result('foreach_parse_csv_2_13').get('tasklevel1') else '',
                "tasklevel2": "",
                "tasklevel3": "",
                "audax_users_and_departments_lookup_table": dag_run.conf['audax_users_and_departments_lookup_table'],
                "audax_project_task_import_logs": dag_run.conf['audax_project_task_import_logs']
            }
        )

        insert_to_list_15 = rail.SetVariableOperator(
            task_id='insert_to_list_15',
            append=True,
            name='{{ result("declare_list").name }}',
            value='{{result("trigger_dag_run_add_update_tasksasync_15")}}'
        )

        if_foreach_parse_csv_2_13_column_1_present_16=rail.IfOperator(
            task_id='if_foreach_parse_csv_2_13_column_1_present_16',
            test='''{{ result('foreach_parse_csv_2_13').get('tasklevel1') | is_truthy  and result('foreach_parse_csv_2_13').get('tasklevel2') | is_truthy  and result('foreach_parse_csv_2_13').get('tasklevel3') | is_falsy }}''',
            yes_task="trigger_dag_run_add_update_tasksasync_17",
            no_task="if_foreach_parse_csv_2_13_column_1_present_18",
        )

        trigger_dag_run_add_update_tasksasync_17=rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_add_update_tasksasync_17',
            trigger_dag_id=config.add_update_tasks_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "projectname": dag_run.conf['projectname'],
                "projecturi": dag_run.conf['projecturi'],
                "taskcode": rail.result('foreach_parse_csv_2_13').get('taskcode').strip()[:50] if rail.result('foreach_parse_csv_2_13').get('taskcode') else '',
                "timeentrystartdate": rail.result('foreach_parse_csv_2_13').get('timeentrystartdate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentrystartdate') else '',
                "timeentryenddate":  rail.result('foreach_parse_csv_2_13').get('timeentryenddate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentryenddate') else '',
                "assignedusers": rail.result('foreach_parse_csv_2_13').get('assignedusers').strip() if rail.result('foreach_parse_csv_2_13').get('assignedusers') else '',
                "assigneddepartments":  rail.result('foreach_parse_csv_2_13').get('assigneddepartments').strip() if rail.result('foreach_parse_csv_2_13').get('assigneddepartments') else '',
                "istimeentryallowed": rail.result('foreach_parse_csv_2_13').get('istimeentryallowed').strip() if rail.result('foreach_parse_csv_2_13').get('istimeentryallowed') else '',
                "taskdescription":  rail.result('foreach_parse_csv_2_13').get('taskdescription').strip() if rail.result('foreach_parse_csv_2_13').get('taskdescription') else '',
                "taskinfo1":  rail.result('foreach_parse_csv_2_13').get('taskinfo1').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "taskinfo2":  rail.result('foreach_parse_csv_2_13').get('taskinfo2').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo2') else '',
                "taskinfo3":  rail.result('foreach_parse_csv_2_13').get('taskinfo3').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tenanturl": rail.get_tenant_slug(),
                "parentjobid": dag_run.conf['parent_ecid'],
                "TaskBusinessUnitURI": rail.result('log_task_business_unit_uri_5'),
                "taskinfo1URI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_6'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo1'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "DealOpportunityIDTaskURI": rail.result('log_deal_opportunity_i_d_task_uri_7'),
                "TaskDepartmentURI": rail.result('log_task_department_uri_8'),
                "taskinfo3URI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_9'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo3'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tasklevel1": rail.result('foreach_parse_csv_2_13').get('tasklevel1').strip() if  rail.result('foreach_parse_csv_2_13').get('tasklevel1') else '',
                "tasklevel2": rail.result('foreach_parse_csv_2_13').get('tasklevel2').strip() if rail.result('foreach_parse_csv_2_13').get('tasklevel2') else '',
                "tasklevel3": "",
                "audax_users_and_departments_lookup_table": dag_run.conf['audax_users_and_departments_lookup_table'],
                "audax_project_task_import_logs": dag_run.conf['audax_project_task_import_logs']
            }
        )

        insert_to_list_17 = rail.SetVariableOperator(
            task_id='insert_to_list_17',
            append=True,
            name='{{ result("declare_list").name }}',
            value='{{result("trigger_dag_run_add_update_tasksasync_17")}}'
        )

        if_foreach_parse_csv_2_13_column_1_present_18=rail.IfOperator(
            task_id='if_foreach_parse_csv_2_13_column_1_present_18',
            test='''{{ result('foreach_parse_csv_2_13').get('tasklevel1') | is_truthy  and result('foreach_parse_csv_2_13').get('tasklevel2') | is_truthy  and result('foreach_parse_csv_2_13').get('tasklevel3') | is_truthy }}''',
            yes_task="trigger_dag_run_add_update_tasksasync_19",
            no_task="if_foreach_parse_csv_2_13_column_1_blank_20",
        )

        trigger_dag_run_add_update_tasksasync_19=rail.TriggerDagRunOperator(
            task_id='trigger_dag_run_add_update_tasksasync_19',
            trigger_dag_id=config.add_update_tasks_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run:{
                "projectname": dag_run.conf['projectname'],
                "projecturi": dag_run.conf['projecturi'],
                "taskcode": rail.result('foreach_parse_csv_2_13').get('taskcode').strip()[:50] if rail.result('foreach_parse_csv_2_13').get('taskcode') else '',
                "timeentrystartdate": rail.result('foreach_parse_csv_2_13').get('timeentrystartdate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentrystartdate') else '',
                "timeentryenddate":  rail.result('foreach_parse_csv_2_13').get('timeentryenddate').strip() if rail.result('foreach_parse_csv_2_13').get('timeentryenddate') else '',
                "assignedusers": rail.result('foreach_parse_csv_2_13').get('assignedusers').strip() if rail.result('foreach_parse_csv_2_13').get('assignedusers') else '',
                "assigneddepartments":  rail.result('foreach_parse_csv_2_13').get('assigneddepartments').strip() if rail.result('foreach_parse_csv_2_13').get('assigneddepartments') else '',
                "istimeentryallowed": rail.result('foreach_parse_csv_2_13').get('istimeentryallowed').strip() if rail.result('foreach_parse_csv_2_13').get('istimeentryallowed') else '',
                "taskdescription":  rail.result('foreach_parse_csv_2_13').get('taskdescription').strip() if rail.result('foreach_parse_csv_2_13').get('taskdescription') else '',
                "taskinfo1":  rail.result('foreach_parse_csv_2_13').get('taskinfo1').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "taskinfo2":  rail.result('foreach_parse_csv_2_13').get('taskinfo2').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo2') else '',
                "taskinfo3":  rail.result('foreach_parse_csv_2_13').get('taskinfo3').strip() if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tenanturl": rail.get_tenant_slug(),
                "parentjobid": dag_run.conf['parent_ecid'],
                "TaskBusinessUnitURI": rail.result('log_task_business_unit_uri_5'),
                "taskinfo1URI": rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_6'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo1'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo1') else '',
                "DealOpportunityIDTaskURI": rail.result('log_deal_opportunity_i_d_task_uri_7'),
                "TaskDepartmentURI": rail.result('log_task_department_uri_8'),
                "taskinfo3URI":  rail.find_first_by_attr_and_get_attr( rail.result('get_all_custom_field_drop_down_options_9'),'displayText', rail.result('foreach_parse_csv_2_13').get('taskinfo3'),'uri', '')  if rail.result('foreach_parse_csv_2_13').get('taskinfo3') else '',
                "tasklevel1": rail.result('foreach_parse_csv_2_13').get('tasklevel1').strip() if  rail.result('foreach_parse_csv_2_13').get('tasklevel1') else '',
                "tasklevel2": rail.result('foreach_parse_csv_2_13').get('tasklevel2').strip() if rail.result('foreach_parse_csv_2_13').get('tasklevel2') else '',
                "tasklevel3": rail.result('foreach_parse_csv_2_13').get('tasklevel3').strip() if rail.result('foreach_parse_csv_2_13').get('tasklevel3') else '',
                "audax_users_and_departments_lookup_table": dag_run.conf['audax_users_and_departments_lookup_table'],
                "audax_project_task_import_logs": dag_run.conf['audax_project_task_import_logs']
            }
        )

        insert_to_list_19 = rail.SetVariableOperator(
            task_id='insert_to_list_19',
            append=True,
            name='{{ result("declare_list").name }}',
            value='{{result("trigger_dag_run_add_update_tasksasync_19")}}'
        )

        if_foreach_parse_csv_2_13_column_1_blank_20=rail.IfOperator(
            task_id='if_foreach_parse_csv_2_13_column_1_blank_20',
            test='''{{ result('foreach_parse_csv_2_13').get('tasklevel1') | is_falsy  and result('foreach_parse_csv_2_13').get('tasklevel2') | is_falsy  and result('foreach_parse_csv_2_13').get('tasklevel3') | is_falsy }}''',
            yes_task="audaxgroup_project_task_import_logs_add_entry_21",
            no_task="foreach_parse_csv_2_13_end",
        )

        audaxgroup_project_task_import_logs_add_entry_21=rail.WriteLogOperator(
            task_id='audaxgroup_project_task_import_logs_add_entry_21',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            message="na",
            severity="Skipped",
            properties={
                "jobid": "{{dag_run.conf.parent_ecid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "||",
                "taskcode": "",
                "status": "Skipped",
                "details": "No Tasks were provided for Task import",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )


        foreach_parse_csv_2_13_end=rail.EmptyOperator(
            task_id='foreach_parse_csv_2_13_end',
        )

        wait_for_completion_dag_add_update_tasksasync = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_dag_add_update_tasksasync',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs="{{ dag_run_var(result('declare_list').name) | to_json }}"
        )

        finish =  rail.EmptyOperator(
            task_id='finish'
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{ dag_run.conf.audax_project_task_import_logs }}",
            trigger_rule='one_failed',
            severity='Error',
            message='{{ get_error_message() }}',
            properties={
                "jobid": "{{dag_run.conf.parent_ecid}}",
                "projectname": "{{ dag_run.conf.projectname }}",
                "taskname": "||",
                "taskcode": "",
                "status": "Error",
                "details": "Error processing tasks for Project '{{ dag_run.conf.projectname }}' - {{ get_error_message() }}",
                "childjobid": "{{ dag_run_ecid() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_errors
        can_run_batch_task >> rail.Label('No') >> parse_csv_2 >> get_all_custom_fields_4 >> log_task_business_unit_uri_5 >> get_all_custom_field_drop_down_options_6 \
        >> log_deal_opportunity_i_d_task_uri_7 >> log_task_department_uri_8 >> get_all_custom_field_drop_down_options_9 >> declare_list >> foreach_parse_csv_2_13 \
        >> if_foreach_parse_csv_2_13_column_1_present_14
        if_foreach_parse_csv_2_13_column_1_present_14 >> rail.Label('Yes') >> trigger_dag_run_add_update_tasksasync_15 >> insert_to_list_15 >> foreach_parse_csv_2_13_end
        if_foreach_parse_csv_2_13_column_1_present_14 >> rail.Label('No') >> if_foreach_parse_csv_2_13_column_1_present_16
        if_foreach_parse_csv_2_13_column_1_present_16 >> rail.Label('Yes') >> trigger_dag_run_add_update_tasksasync_17 >> insert_to_list_17 >> foreach_parse_csv_2_13_end
        if_foreach_parse_csv_2_13_column_1_present_16 >> rail.Label('No') >> if_foreach_parse_csv_2_13_column_1_present_18
        if_foreach_parse_csv_2_13_column_1_present_18 >> rail.Label('Yes') >> trigger_dag_run_add_update_tasksasync_19 >> insert_to_list_19 >> foreach_parse_csv_2_13_end
        if_foreach_parse_csv_2_13_column_1_present_18 >> rail.Label('No') >> if_foreach_parse_csv_2_13_column_1_blank_20
        foreach_parse_csv_2_13 >> foreach_parse_csv_2_13_end
        if_foreach_parse_csv_2_13_column_1_blank_20 >> rail.Label('Yes') >> audaxgroup_project_task_import_logs_add_entry_21 >> foreach_parse_csv_2_13_end
        if_foreach_parse_csv_2_13_column_1_blank_20 >> rail.Label('No') >> foreach_parse_csv_2_13_end
        foreach_parse_csv_2_13_end >> wait_for_completion_dag_add_update_tasksasync >> finish >> catch_and_log_errors >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
