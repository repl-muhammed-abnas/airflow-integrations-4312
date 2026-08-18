from datetime import timedelta
from wipro.adhoc_efforts_submit_v1.utils.custom_methods import get_it_proj_efforts, get_it_training_efforts, get_non_proj_efforts, map_time_data_per_day
import rail
from airflow.models import Variable
null = None
dag_created = []


def create_airflow_child_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"{config.process_perday_child}_{cnt}_{config.instance}_v1",
            description=f"efforts submit to wipro child {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_child_runs,
        ) as dag:
            rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

            create_collection_time_export_per_period = rail.CreateCollectionOperator(
                task_id="create_collection_time_export_per_period",
                source= '{{dag_run.conf.time_export_per_period  | load_all_records |to_json}}',
                name="time_export_per_period"
            )

            query_each_entry_date = rail.QueryCollectionOperator(
                task_id="query_each_entry_date",
                query="""SELECT * FROM time_export_per_period p WHERE p.entry_date='{{dag_run.conf.entry_date}}' """,
                name="each_entry_date"
            )

            unique_work_location_ksa = rail.QueryCollectionOperator(
                task_id="unique_work_location_ksa",
                query="""SELECT DISTINCT work_location_ksa FROM each_entry_date WHERE CAST(hours_current AS DECIMAL) > 0"""
            )

            unique_work_location = rail.QueryCollectionOperator(
                task_id="unique_work_location",
                query="""SELECT DISTINCT work_location FROM each_entry_date WHERE CAST(hours_current AS DECIMAL) > 0"""
            )

            create_collection_per_day = rail.CreateCollectionOperator(
                task_id="create_collection_per_day",
                source='{{result("query_each_entry_date")}}',
                name="collection_per_day"
            )

            query_it_projects = rail.QueryCollectionOperator(
                task_id="query_unique_it_projects",
                query="""SELECT * FROM collection_per_day WHERE project_name IS NOT NULL AND project_name != '' AND project_export_type='IT_PROJ_DETAILS' """,
                name="it_projects"
            )

            if_it_projects = rail.IfOperator(
                task_id="if_it_projects",
                test='{{result("query_unique_it_projects", "length") > 0}}',
                yes_task="query_distinct_it_projects",
                no_task="combine_perday_data"
            )

            query_distinct_it_projects = rail.QueryCollectionOperator(
                task_id="query_distinct_it_projects",
                query="""SELECT DISTINCT project_name,entry_date FROM it_projects"""
            )

            it_project_list = rail.SetVariableOperator(
                task_id="it_project_list",
                name="it_projects_per_day",
                value=[],
                append=True
            )

            process_each_it_project = rail.ForEachOperator(
                task_id="process_each_it_project",
                items='{{result("query_distinct_it_projects")}}',
                start_task="start_it_projects",
                end_task="end_it_projects"
            )

            start_it_projects = rail.EmptyOperator(
                task_id="start_it_projects"
            )

            query_per_project = rail.QueryCollectionOperator(
                task_id="query_per_project",
                query="""SELECT * FROM it_projects WHERE project_name='{{result("process_each_it_project").project_name}}' """
            )

            map_it_project = rail.SetVariableOperator(
                task_id="map_it_project",
                name='{{ result("it_project_list").name }}',
                append=True,
                value=get_it_proj_efforts
            )

            end_it_projects = rail.EmptyOperator(
                task_id="end_it_projects"
            )

            query_it_training_projects = rail.QueryCollectionOperator(
                task_id="query_it_training_projects",
                query="""SELECT * FROM collection_per_day WHERE project_name IS NOT NULL AND project_name != '' AND project_export_type='IT_TRAINING' """
            )

            if_it_training_projects = rail.IfOperator(
                task_id="if_it_training_projects",
                test='{{result("query_it_training_projects","length") > 0}}',
                yes_task="map_it_training_projects",
                no_task="combine_perday_data"
            )

            map_it_training_projects = rail.PythonOperator(
                task_id="map_it_training_projects",
                python_callable=get_it_training_efforts
            )

            query_it_non_proj = rail.QueryCollectionOperator(
                task_id="query_it_non_proj",
                query="""SELECT * FROM collection_per_day WHERE project_export_type='IT_NONPROJ' """
            )

            if_it_non_proj = rail.IfOperator(
                task_id="if_it_non_proj",
                test='{{result("query_it_non_proj", "length") > 0}}',
                yes_task="map_it_non_proj",
                no_task="combine_perday_data"
            )

            map_it_non_proj = rail.PythonOperator(
                task_id="map_it_non_proj",
                python_callable=get_non_proj_efforts
            )

            combine_perday_data = rail.PythonOperator(
                task_id="combine_perday_data",
                python_callable=map_time_data_per_day
            )

            if_data_perday = rail.IfOperator(
                task_id="if_data_perday",
                test='{{result("combine_perday_data") | is_truthy}}',
                yes_task="submit_data_to_wipro",
                no_task="catch_and_log_errors"
            )

            submit_data_to_wipro = rail.TriggerDagRunOperator(
                task_id="submit_data_to_wipro",
                trigger_dag_id=f"{config.submit_data_child}_{cnt}_{config.instance}_v1",
                conf=lambda: {
                            "data": rail.result("combine_perday_data"),
                            "time_export_name": rail.render_template('{{dag_run.conf.time_export_name}}')
                        },
                wait_for_completion=True,
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            catch_and_log_errors = rail.EmptyOperator(
                task_id="catch_and_log_errors",
                trigger_rule="one_failed"
            )

            create_collection_time_export_per_period >> query_each_entry_date >> unique_work_location_ksa >> unique_work_location >>\
            create_collection_per_day >> query_it_projects >> if_it_projects >> rail.Label("Yes") >> query_distinct_it_projects >>\
                it_project_list >>\
                process_each_it_project >> end_it_projects
            process_each_it_project >> start_it_projects >> query_per_project >>\
                map_it_project >> end_it_projects >> combine_perday_data
            if_it_projects >> rail.Label("No") >> combine_perday_data
            create_collection_per_day >> query_it_training_projects >>\
                if_it_training_projects >> rail.Label(
                    "Yes") >> map_it_training_projects >> combine_perday_data
            if_it_training_projects >> rail.Label("No") >> combine_perday_data
            create_collection_per_day >> query_it_non_proj >>\
                if_it_non_proj >> rail.Label(
                    "Yes") >> map_it_non_proj >> combine_perday_data
            if_it_non_proj >> rail.Label("No") >> combine_perday_data >>\
                if_data_perday >> rail.Label("Yes") >>\
                submit_data_to_wipro >> catch_and_log_errors
            if_data_perday >> rail.Label("No") >> catch_and_log_errors

        dag_created.append(dag)


rail.for_each_instance(create_airflow_child_dag)
