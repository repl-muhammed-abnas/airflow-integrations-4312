from datetime import timedelta
from pendulum import datetime
import rail

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'cbrefcg_project_team_assignment_master_{config.instance}',
        description=f'cbrefcg_Project_Team_assignment_scheduled_Master - V1.0 {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2022, 1, 1),
        replicon_conn_id=config.replicon_conn_id,
        schedule_interval=config.schedule_interval,
        max_active_runs= config.master_dag_max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config",extra_config=config)

        get_webhook_log = rail.CreateLogOperator(
            task_id="get_webhook_log",
            tenant_wide_name="cbre_webhook_project_data",
            existing_log_mode="truncate",
        )

        has_any_data = rail.IfOperator(
            task_id="has_any_data",
            test=lambda: bool(rail.load_all_records(rail.result('get_webhook_log', 'truncated_data'))),
            yes_task='write_csv_project_data',
            no_task='delete_this_dagrun'
        )

        delete_this_dagrun= rail.DeleteCurrentDagRunOperator(
            task_id='delete_this_dagrun',
        )

        write_csv_project_data= rail.WriteCSVFileOperator(
            task_id='write_csv_project_data',
            source="{{ result('get_webhook_log', 'truncated_data') }}",
            header=['x_correlation_id',
                    'projecturi',
                    'projectname',
                    'eventdatetime',
                    'eventdate',
                    'eventtype'],
            row=['{{ item.ecid }}', '{{ item.properties.projecturi }}', '{{ item.properties.Projectname }}', '{{ item.properties.eventdatetime }}',
                 '{{ item.properties.eventdate }}', '{{ item.properties.eventtype }}'],
        )

        load_csv_create_list_from_csv=rail.LoadCSVFileOperator(
            task_id="load_csv_create_list_from_csv",
            document="{{ result('write_csv_project_data') }}",
        )

        create_webhook_event_collection = rail.CreateCollectionOperator(
            task_id='create_webhook_event_collection',
            source = "{{ result('load_csv_create_list_from_csv') }}",
            name = "webhookevents",
        )

        query_webhook_event_collection= rail.QueryCollectionOperator(
            task_id='query_webhook_event_collection',
            query="""SELECT * FROM webhookevents WHERE webhookevents.projecturi IN
                    (SELECT DISTINCT webhookevents.projecturi FROM  webhookevents) GROUP BY (webhookevents.projecturi)""",
        )

        has_query_webhook_event_collection= rail.IfOperator(
            task_id='has_query_webhook_event_collection',
            test="{{ result('query_webhook_event_collection', 'length') > 0 }}",
            yes_task="process_project_team_billing_rate",
            no_task="log_to_sumo",
        )

        process_project_team_billing_rate= rail.TriggerDagRunForEachItemOperator(
            task_id='process_project_team_billing_rate',
            retries=0,
            items="{{ result('query_webhook_event_collection') }}",
            trigger_dag_id=config.child_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            conf={
                "projecturi": "{{ item.projecturi }}",
                "projectname": "{{ item.projectname }}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        get_webhook_log >> has_any_data >> rail.Label(
            'No')  >> delete_this_dagrun

        has_any_data >> rail.Label(
            'Yes') >> write_csv_project_data >> load_csv_create_list_from_csv >> create_webhook_event_collection >> \
                    query_webhook_event_collection >> has_query_webhook_event_collection

        has_query_webhook_event_collection >> rail.Label(
            'Yes') >> process_project_team_billing_rate >> log_to_sumo

        has_query_webhook_event_collection >> rail.Label(
            'No') >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
