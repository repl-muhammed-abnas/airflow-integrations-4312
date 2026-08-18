import ast
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'omnicommedia_createsubtask_child_v1_0_{config.instance}',
        description=f'Omnicommedia_Createsubtask_child  V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_task_details_4'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_task_details_4',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_task_details_4 = rail.RepliconServiceOperator(
            task_id='get_task_details_4',
            endpoint="/services/TaskService1.svc/GetTaskDetails",
            data={
                "taskUri": "{{ dag_run.conf.taskuri }}"
            }
        )

        bulk_get_projects2_5 = rail.RepliconServiceOperator(
            task_id='bulk_get_projects2_5',
            endpoint="/services/ImportService1.svc/BulkGetProjects2",
            data={
                "projects": [
                    {
                        "uri": "{{ result('get_task_details_4').project.uri }}",
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    }
                ]
            }

        )

        if_program_name_equals_to_phd_6 = rail.IfOperator(
            task_id='if_program_name_equals_to_phd_6',
            test=lambda: rail.result('bulk_get_projects2_5')['results'][0]['project']['program'] and
            rail.result('bulk_get_projects2_5')[
                'results'][0]['project']['program']['name'] == 'PHD',
            yes_task="foreach_d_7",
            no_task="log_to_sumo",
        )

        foreach_d_7 = rail.ForEachOperator(
            task_id='foreach_d_7',
            items="{{ result('bulk_get_projects2_5').results | to_json }}",
            start_task='invoke_custom_ruby_code_8',
            end_task='foreach_d_7_end'
        )

        invoke_custom_ruby_code_8 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_8',
            python_callable=lambda: {
                "startDate": rail.result('get_task_details_4')['timeEntryDateRange']['startDate'] or rail.result('foreach_d_7')['project']['timeEntryDateRange']['startDate'],
                "endDate": rail.result('get_task_details_4')['timeEntryDateRange']['endDate'] or rail.result('foreach_d_7')['project']['timeEntryDateRange']['endDate']
            }
        )

        foreach_d_7_end = rail.EmptyOperator(
            task_id='foreach_d_7_end',
        )

        def get_default_tasks():
            project_tasks_mapper = ast.literal_eval(Variable.get(
                config.omnicommedia_task_mapper, default_var=[]))
            return list(filter(lambda x: x['include'] == 'yes', project_tasks_mapper))

        omnicommedia_defaultsubtask_search_entries_9 = rail.PythonOperator(
            task_id='omnicommedia_defaultsubtask_search_entries_9',
            python_callable=get_default_tasks
        )

        foreach_omnicommedia_defaultsubtask_search_entries_9_10 = rail.ForEachOperator(
            task_id='foreach_omnicommedia_defaultsubtask_search_entries_9_10',
            items="{{ result('omnicommedia_defaultsubtask_search_entries_9') | to_json }}",
            start_task='put_task_11',
            end_task='foreach_omnicommedia_defaultsubtask_search_entries_9_10_end'
        )

        put_task_11 = rail.RepliconServiceOperator(
            task_id='put_task_11',
            endpoint="/services/ProjectService1.svc/PutTask",
            data=lambda: {
                "project": {
                    "uri": rail.result('get_task_details_4')['project']['uri'],
                    "name": null,
                    "code": null,
                    "parameterCorrelationId": null
                },
                "task": {
                    "target": {
                        "uri": null,
                        "name": rail.result('foreach_omnicommedia_defaultsubtask_search_entries_9_10')['taskname'],
                        "parent": {
                            "uri": rail.result('get_task_details_4')['uri'],
                            "name": null,
                            "parent": null,
                            "parameterCorrelationId": null
                        },
                        "parameterCorrelationId": null
                    },
                    "name": rail.result('foreach_omnicommedia_defaultsubtask_search_entries_9_10')['taskname'],
                    "code": null,
                    "description": null,
                    "timeEntryDateRange": {
                        "startDate": rail.result('invoke_custom_ruby_code_8')['startDate'] if rail.result('invoke_custom_ruby_code_8')['startDate'] else null,
                        "endDate": rail.result('invoke_custom_ruby_code_8')['endDate'] if rail.result('invoke_custom_ruby_code_8')['endDate'] else null,
                        "relativeDateRangeUri": null,
                        "relativeDateRangeAsOfDate": null
                    },
                    "percentCompleted": "0",
                    "isTimeEntryAllowed": "true",
                    "estimatedHours": null,
                    "isClosed": "false",
                    "customFieldValues": [],
                    "estimatedCost": null,
                    "costTypeUri": null,
                    "timeAndExpenseEntryTypeUri": "urn:replicon:time-and-expense-entry-type:billable-and-non-billable",
                    "assignedResources": [{'uri': team['resource']['uri']} for team in rail.result('bulk_get_projects2_5')['results'][0]['team']],
                    "keyValues": [],
                    "historicalKeyValues": [],
                    "extensionFieldValues": []
                }
            }
        )

        foreach_omnicommedia_defaultsubtask_search_entries_9_10_end = rail.EmptyOperator(
            task_id='foreach_omnicommedia_defaultsubtask_search_entries_9_10_end',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> get_task_details_4 >> bulk_get_projects2_5 >> if_program_name_equals_to_phd_6
        if_program_name_equals_to_phd_6 >> rail.Label(
            'Yes') >> foreach_d_7 >> invoke_custom_ruby_code_8 >> foreach_d_7_end
        foreach_d_7 >> foreach_d_7_end >> omnicommedia_defaultsubtask_search_entries_9 \
            >> foreach_omnicommedia_defaultsubtask_search_entries_9_10 >> put_task_11 >> foreach_omnicommedia_defaultsubtask_search_entries_9_10_end
        foreach_omnicommedia_defaultsubtask_search_entries_9_10 >> foreach_omnicommedia_defaultsubtask_search_entries_9_10_end >> log_to_sumo
        if_program_name_equals_to_phd_6 >> rail.Label(
            'No') >> log_to_sumo

        return dag


rail.for_each_instance(create_dag)
