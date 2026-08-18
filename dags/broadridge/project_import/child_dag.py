from datetime import timedelta
from airflow.models import Variable
import rail


null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'broadridge_project_main_version2_0_child_{config.instance}',
        description=f'Broadridge_project_main_version2_0_child {config.instance}',
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
            no_task='search_projects_basedon_code'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='search_projects_basedon_code',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        search_projects_basedon_code = rail.RepliconServiceOperator(
            task_id='search_projects_basedon_code',
            endpoint="/services/ProjectListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                     "urn:replicon:project-list-column:project",
                     "urn:replicon:project-list-column:code",
                     "urn:replicon:project-list-column:project-leader"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:code"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{dag_run.conf.batch_items.metisprojectuid}}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        foreach_in_query_do = rail.ForEachOperator(
            task_id='foreach_in_query_do',
            items=lambda: rail.result('search_projects_basedon_code')['rows'] if rail.result(
                'search_projects_basedon_code') else [],
            start_task='accumulate_list_items',
            end_task='foreach_in_query_do_end'
        )

        accumulate_list_items = rail.SetVariableOperator(
            task_id='accumulate_list_items',
            name='projectdata',
            append=True,
            value=lambda: {
                "name": rail.find_first_by_attr_and_get_attr(rail.result('foreach_in_query_do')['cells'], 'objectType', 'urn:replicon:object-type:project', 'textValue', ''),
                "uri": rail.find_first_by_attr_and_get_attr(rail.result('foreach_in_query_do')['cells'], 'objectType', 'urn:replicon:object-type:project', 'uri', ''),
                "projectmanager": rail.find_first_by_attr_and_get_attr(rail.result('foreach_in_query_do')['cells'], 'objectType', 'urn:replicon:object-type:project-leader', 'textValue', ''),
                "code": rail.find_first_by_attr_and_get_attr(rail.result('foreach_in_query_do')['cells'], 'dataType', 'urn:replicon:list-type:string', 'textValue', '')
            }
        )

        foreach_in_query_do_end = rail.EmptyOperator(
            task_id='foreach_in_query_do_end',
        )

        log_projecturi = rail.PythonOperator(
            task_id='log_projecturi',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('accumulate_list_items')[
                'value'], 'code', dag_run.conf['batch_items']['metisprojectuid'], 'uri', '') if rail.result('accumulate_list_items') and rail.result('accumulate_list_items')['value'] and rail.result('accumulate_list_items')['value'][0] and rail.result('accumulate_list_items')['value'][0]['name'] else null
        )

        log_projectmanager = rail.PythonOperator(
            task_id='log_projectmanager',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('accumulate_list_items')[
                'value'], 'code', dag_run.conf['batch_items']['metisprojectuid'], 'projectmanager', '') if rail.result('accumulate_list_items') and rail.result('accumulate_list_items')['value'] and rail.result('accumulate_list_items')['value'][0] and rail.result('accumulate_list_items')['value'][0]['name'] else null
        )

        if_log_projecturi_present = rail.IfOperator(
            task_id='if_log_projecturi_present',
            test='''{{ result('log_projecturi') | is_truthy }}''',
            yes_task="process_existing_project_child",
            no_task="if_log_projecturi_not_present",
        )

        process_existing_project_child = rail.TriggerDagRunOperator(
            task_id='process_existing_project_child',
            trigger_dag_id=f'broadridge_existing_project_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "projectname": dag_run.conf['batch_items']['projectname'],
                "inputfile": dag_run.conf['input_file'],
                "projectid": rail.result('log_projecturi').split(":")[-1].strip(),
                "projecturi": rail.result('log_projecturi'),
                "projectmanager": rail.result('log_projectmanager'),
                "jobid": dag_run.conf['jobid'],
                "metisprojectuid": dag_run.conf['batch_items']['metisprojectuid'],
                "project_code_uri": dag_run.conf['customfield'],
                "project_metis_UID_customfield": dag_run.conf['metis_uid'],
                "lookup_table": dag_run.conf['lookup_table']
            }
        )

        wait_for_process_existing_project_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_existing_project_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_existing_project_child") }}'
        )

        if_log_projecturi_not_present = rail.IfOperator(
            task_id='if_log_projecturi_not_present',
            test='''{{ result('log_projecturi') | is_falsy }}''',
            yes_task="process_new_project_child",
            no_task="log_to_sumo",
        )

        process_new_project_child = rail.TriggerDagRunOperator(
            task_id='process_new_project_child',
            trigger_dag_id=f'broadridge_new_project_child_{config.instance}',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf=lambda dag_run: {
                "inputfile": dag_run.conf['input_file'],
                "jobid": dag_run.conf['jobid'],
                "projectname": dag_run.conf['batch_items']['projectname'],
                "project_code_custom_field_uri":  dag_run.conf['customfield'],
                "metis_projectuid_custom_field_uri": dag_run.conf['metis_uid'],
                "lookup_table": dag_run.conf['lookup_table'],
            }
        )

        wait_for_process_new_project_child = rail.WaitForDagRunsSensor(
            task_id='wait_for_process_new_project_child',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("process_new_project_child") }}'
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> search_projects_basedon_code
        search_projects_basedon_code >> foreach_in_query_do >> accumulate_list_items >> foreach_in_query_do_end
        foreach_in_query_do_end >> log_projecturi >> log_projectmanager >> if_log_projecturi_present
        if_log_projecturi_present >> rail.Label(
            'Yes') >> process_existing_project_child >> wait_for_process_existing_project_child
        wait_for_process_existing_project_child >> if_log_projecturi_not_present
        if_log_projecturi_present >> rail.Label(
            'No') >> if_log_projecturi_not_present
        if_log_projecturi_not_present >> rail.Label(
            'Yes') >> process_new_project_child >> wait_for_process_new_project_child
        if_log_projecturi_not_present >> rail.Label(
            'No') >> log_to_sumo
        wait_for_process_new_project_child >> log_to_sumo
        foreach_in_query_do >> foreach_in_query_do_end

        return dag


rail.for_each_instance(create_dag)
