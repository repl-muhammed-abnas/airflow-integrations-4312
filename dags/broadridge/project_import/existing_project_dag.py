
from datetime import timedelta, datetime
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_existing_project_child_{config.instance}',
        description=f'Broadridge_existing_project_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='load_csv_create_list_35_35_from_csv_3_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='load_csv_create_list_35_35_from_csv_3_3',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        load_csv_create_list_35_35_from_csv_3_3 = rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_35_35_from_csv_3_3",
            document="{{dag_run.conf.inputfile}}"
        )

        create_collection_create_list_35_35_from_csv_3_3 = rail.CreateCollectionOperator(
            task_id='create_collection_create_list_35_35_from_csv_3_3',
            source="{{ result('load_csv_create_list_35_35_from_csv_3_3') }}",
            name="inputdata",
            columns={
                'Project Name': 'projectname',
                'Project Code': 'projectcode',
                'Start Date': 'startdate',
                'End Date': 'enddate',
                'Project Manager': 'projectmanager',
                'Client Code': 'clientcode',
                'Task Name': 'taskname',
                'Task Team Assignment': 'taskteam',
                'Task Start Date': 'taskstartdate',
                'Task End Date': 'taskenddate',
                'TaskOutlinelevel': 'taskoutlinelevel',
                'TaskOutlineNumber': 'taskoutlinenumber',
                'Metis_ProjectUID': 'metisprojectuid',
                'Metis_TaskUID': 'metistaskuid'
            }
        )

        query_list_4_4_4 = rail.QueryCollectionOperator(
            task_id='query_list_4_4_4',
            query="""SELECT  inputdata.projectname, inputdata.projectcode, inputdata.startdate, inputdata.enddate, inputdata.projectmanager, inputdata.clientcode, inputdata.taskname, inputdata.taskteam, inputdata.taskstartdate, inputdata.taskenddate, inputdata.taskoutlinelevel, inputdata.taskoutlinenumber, inputdata.metisprojectuid, inputdata.metistaskuid FROM  inputdata WHERE metisprojectuid='{{ dag_run.conf.metisprojectuid }}'""",
        )

        load_query_list_data = rail.PythonOperator(
            task_id='load_query_list_data',
            python_callable=lambda: rail.load_all_records(
                rail.result('query_list_4_4_4'))
        )

        get_project_details_5_5_5 = rail.RepliconServiceOperator(
            task_id='get_project_details_5_5_5',
            endpoint="/services/ProjectService1.svc/GetProjectDetails",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi']
            }
        )

        log_projectmanagernamemodifiedforcomparison_6 = rail.PythonOperator(
            task_id='log_projectmanagernamemodifiedforcomparison_6',
            python_callable=lambda: rail.result('load_query_list_data')[0]['projectmanager'].replace(
                ";", ",") if rail.result('load_query_list_data') else null
        )

        if_projectmanagernamemodifiedforcomparison_not_equals_to_projectmanager_8 = rail.IfOperator(
            task_id='if_projectmanagernamemodifiedforcomparison_not_equals_to_projectmanager_8',
            test=lambda dag_run: rail.result(
                'log_projectmanagernamemodifiedforcomparison_6') != dag_run.conf['projectmanager'],
            yes_task="list_project_leaders_9_9_9",
            no_task="log_startdatemodified_13",
        )

        list_project_leaders_9_9_9 = rail.RepliconServiceOperator(
            task_id='list_project_leaders_9_9_9',
            endpoint="/services/ProjectService1.svc/GetEligibleProjectLeaders",
        )

        log_new_projectmanageruri_10 = rail.PythonOperator(
            task_id='log_new_projectmanageruri_10',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(
                rail.result('list_project_leaders_9_9_9'), 'displayText', rail.result('log_projectmanagernamemodifiedforcomparison_6'), 'uri', null) if rail.result('list_project_leaders_9_9_9') else null
        )

        if_log_new_projectmanageruri_present_11 = rail.IfOperator(
            task_id='if_log_new_projectmanageruri_present_11',
            test='''{{ result('log_new_projectmanageruri_10') | is_truthy }}''',
            yes_task="update_project_leader_12",
            no_task="log_startdatemodified_13",
        )

        update_project_leader_12 = rail.RepliconServiceOperator(
            task_id='update_project_leader_12',
            endpoint="/services/ProjectService1.svc/UpdateProjectLeader",
            data={
                "projectUri": "{{ dag_run.conf.projecturi }}",
                "userUri": "{{ result('log_new_projectmanageruri_10') }}"
            }
        )

        log_startdatemodified_13 = rail.PythonOperator(
            task_id='log_startdatemodified_13',
            python_callable=lambda:  rail.result('load_query_list_data')[
                0]['startdate'].replace("-", "/")
        )

        log_enddate_17 = rail.PythonOperator(
            task_id='log_enddate_17',
            python_callable=lambda:  rail.result('load_query_list_data')[
                0]['enddate'].replace("-", "/")
        )

        updatedaterange_21 = rail.RepliconServiceOperator(
            task_id='updatedaterange_21',
            endpoint="/services/ProjectService1.svc/UpdateTimeEntryDateRange",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi'],
                "dateRange": {
                    "startDate": {
                        "year": datetime.strptime(rail.result('log_startdatemodified_13'), "%m/%d/%Y").year,
                        "month": datetime.strptime(rail.result('log_startdatemodified_13'), "%m/%d/%Y").month,
                        "day": datetime.strptime(rail.result('log_startdatemodified_13'), "%m/%d/%Y").day
                    },
                    "endDate": {
                        "year": datetime.strptime(rail.result('log_enddate_17'), "%m/%d/%Y").year,
                        "month": datetime.strptime(rail.result('log_enddate_17'), "%m/%d/%Y").month,
                        "day": datetime.strptime(rail.result('log_enddate_17'), "%m/%d/%Y").day
                    },
                    "relativeDateRangeUri": null,
                    "relativeDateRangeAsOfDate": null
                }
            }

        )

        updatename_22 = rail.RepliconServiceOperator(
            task_id='updatename_22',
            endpoint="/services/ProjectService1.svc/UpdateName",
            data=lambda dag_run: {
                "projectUri": dag_run.conf['projecturi'],
                "name": rail.result('load_query_list_data')[0]['projectname']
            }
        )

        update_project_metis_u_i_d_23 = rail.RepliconServiceOperator(
            task_id='update_project_metis_u_i_d_23',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['projecturi'],
                "customFieldUri": dag_run.conf['project_metis_UID_customfield'],
                "value": rail.result('load_query_list_data')[0]['metisprojectuid']
            }
        )

        update_project_codecustomfield_24 = rail.RepliconServiceOperator(
            task_id='update_project_codecustomfield_24',
            endpoint="/services/CustomFieldService1.svc/UpdateTextValue",
            data=lambda dag_run: {
                "objectUri": dag_run.conf['projecturi'],
                "customFieldUri": dag_run.conf['project_code_uri'],
                "value": rail.result('load_query_list_data')[0]['projectcode']
            }
        )

        process_task_child1 = rail.TriggerDagRunForEachItemOperator(
            task_id='process_task_child1',
            retries=0,
            items="{{result('query_list_4_4_4')}}",
            trigger_dag_id=f'broadridge_project_import_task_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run, item: {
                "task_items": item,
                "action": "update",
                "projectname": dag_run.conf['projectname'],
                "projecturi": dag_run.conf['projecturi'],
                "projectid": dag_run.conf['projectid'],
                "jobid":  dag_run.conf['jobid'],
                "lookup_table": dag_run.conf['lookup_table']
            }
        )

        wait_for_process_task_child1 = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_task_child1',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_task_child1") }}'
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            log="{{ dag_run.conf.lookup_table }}",
            message="na",
            severity="Failed",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "projectname": dag_run.conf['projectname'],
                "taskname": '',
                "status": "Failed",
                "failure/reason": rail.render_template("{{get_error_message()}}"),
                "jobid":  dag_run.conf['jobid'],
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label(
            'No') >> load_csv_create_list_35_35_from_csv_3_3
        load_csv_create_list_35_35_from_csv_3_3 >> create_collection_create_list_35_35_from_csv_3_3 >> query_list_4_4_4
        query_list_4_4_4 >> load_query_list_data >> get_project_details_5_5_5
        get_project_details_5_5_5 >> log_projectmanagernamemodifiedforcomparison_6
        log_projectmanagernamemodifiedforcomparison_6 >> if_projectmanagernamemodifiedforcomparison_not_equals_to_projectmanager_8
        if_projectmanagernamemodifiedforcomparison_not_equals_to_projectmanager_8 >> rail.Label(
            'Yes') >> list_project_leaders_9_9_9 >> log_new_projectmanageruri_10 >> if_log_new_projectmanageruri_present_11
        if_log_new_projectmanageruri_present_11 >> rail.Label(
            'Yes') >> update_project_leader_12 >> log_startdatemodified_13
        if_log_new_projectmanageruri_present_11 >> rail.Label(
            'No') >> log_startdatemodified_13
        if_projectmanagernamemodifiedforcomparison_not_equals_to_projectmanager_8 >> rail.Label(
            'No') >> log_startdatemodified_13 >> log_enddate_17 >> updatedaterange_21 >> updatename_22
        updatename_22 >> update_project_metis_u_i_d_23 >> update_project_codecustomfield_24
        update_project_codecustomfield_24 >> process_task_child1 >> wait_for_process_task_child1
        wait_for_process_task_child1 >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
