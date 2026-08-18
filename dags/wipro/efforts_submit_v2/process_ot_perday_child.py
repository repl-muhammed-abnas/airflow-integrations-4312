from datetime import timedelta
# from wipro.efforts_submit_v2.utils.custom_methods import get_it_proj_efforts, get_it_training_efforts, get_non_proj_efforts, map_time_data_per_day
from wipro.efforts_submit_v2.utils import custom_methods
import rail
null = None
dag_created = []


def create_airflow_child_dag(config):
    for country in config.time_export_for_country:
        cnt=str(country).replace(" ","_")
        with rail.create_airflow_dag(
            dag_id=f"{config.process_ot_perday_child}_{cnt}_{config.instance}_v2",
            description=f"efforts submit to wipro child {country} {config.instance}",
            company_key=config.company_key,
            replicon_conn_id=config.replicon_conn_id,
            max_active_runs=config.max_active_child_runs,
        ) as dag:
            rail.ViewDagRunConfOperator(task_id="view_dag_run_config")

            create_collection_time_export_per_period = rail.CreateCollectionOperator(
                task_id="create_collection_time_export_per_period",
                source= '{{dag_run.conf.time_export_per_period  | load_all_records |to_json}}',
                name="ot_time_export_per_period"
            )

            query_each_entry_date = rail.QueryCollectionOperator(
                task_id="query_each_entry_date",
                query="""SELECT * FROM ot_time_export_per_period p WHERE p.entry_date='{{dag_run.conf.entry_date}}' """
            )

            create_collection_per_day = rail.CreateCollectionOperator(
                task_id="create_collection_per_day",
                source='{{result("query_each_entry_date")}}',
                name="ot_collection_per_day"
            )

            get_query_conditions = rail.PythonOperator(
                task_id="get_query_conditions",
                python_callable=lambda dag_run: {
                    "oncall": custom_methods.get_oef_oncall_query(dag_run.conf['cntry'], config.query_mapper_for_contry),
                    "callout": custom_methods.get_oef_callout_query(dag_run.conf['cntry'], config.query_mapper_for_contry),
                    "overtime": custom_methods.get_oef_overtime_query(dag_run.conf['cntry'], config.query_mapper_for_contry),
                    "columns": custom_methods.get_oef_distinct_columns_query(dag_run.conf['cntry'], config.query_mapper_for_contry),
                }
            )

            if_oef_oncall_column_present = rail.IfOperator(
                task_id="if_oef_oncall_column_present",
                test=lambda: bool(rail.result('get_query_conditions')['oncall']),
                yes_task="query_oef_oncall",
                no_task="if_oef_callout_column_present"
            )

            query_oef_oncall = rail.QueryCollectionOperator(
                task_id="query_oef_oncall",
                query= """SELECT * FROM ot_collection_per_day WHERE {{result('get_query_conditions').oncall}}""",
                name="ot_oef_oncall"
            )

            if_oef_callout_column_present = rail.IfOperator(
                task_id="if_oef_callout_column_present",
                test=lambda: bool(rail.result('get_query_conditions')['callout']),
                yes_task="query_oef_callout",
                no_task="if_oef_overtime_column_present"
            )

            query_oef_callout = rail.QueryCollectionOperator(
                task_id="query_oef_callout",
                query= """SELECT * FROM ot_collection_per_day WHERE {{result('get_query_conditions').callout}}""",
                name="ot_oef_callout"
            )

            if_oef_overtime_column_present = rail.IfOperator(
                task_id="if_oef_overtime_column_present",
                test=lambda: bool(rail.result('get_query_conditions')['overtime']),
                yes_task="query_oef_overtime",
                no_task="get_distinct_querys_options"
            )

            query_oef_overtime = rail.QueryCollectionOperator(
                task_id="query_oef_overtime",
                query= """SELECT * FROM ot_collection_per_day WHERE {{result('get_query_conditions').overtime}}""",
                name="ot_oef_overtime"
            )

            get_distinct_querys_options = rail.PythonOperator(
                task_id="get_distinct_querys_options",
                python_callable=lambda dag_run: {
                    "oncall": custom_methods.get_distint_oef_query(dag_run.conf['cntry'], 'ot_oef_oncall', config.query_mapper_for_contry),
                    "callout": custom_methods.get_distint_oef_query(dag_run.conf['cntry'], 'ot_oef_callout', config.query_mapper_for_contry),
                    "overtime": custom_methods.get_distint_oef_query(dag_run.conf['cntry'], 'ot_oef_overtime', config.query_mapper_for_contry),
                }
            )

            if_oef_oncall = rail.IfOperator(
                task_id="if_oef_oncall",
                test='{{result("get_query_conditions").oncall | is_truthy and result("query_oef_oncall", "length") > 0}}',
                yes_task="map_oncall_projects",
                no_task="if_oef_callout"
            )

            # query_distinct_oef_oncall = rail.QueryCollectionOperator(
            #     task_id="query_distinct_oef_oncall",
            #     query="""SELECT * FROM ot_oef_oncall WHERE {{result('get_query_conditions').oncall}}"""
            # )

            map_oncall_projects = rail.PythonOperator(
                task_id="map_oncall_projects",
                python_callable=lambda: custom_methods.get_oncall_projects(config.instance)
            )

            if_oef_callout = rail.IfOperator(
                task_id="if_oef_callout",
                test='{{result("get_query_conditions").callout | is_truthy and result("query_oef_callout", "length") > 0}}',
                yes_task="map_callout_projects",
                no_task="if_oef_overtime"
            )

            # query_distinct_oef_callout = rail.QueryCollectionOperator(
            #     task_id="query_distinct_oef_callout",
            #     query="""SELECT * FROM ot_oef_callout WHERE {{result('get_query_conditions').callout}}"""
            # )

            map_callout_projects = rail.PythonOperator(
                task_id="map_callout_projects",
                python_callable=lambda: custom_methods.get_callout_projects(config.instance)
            )

            if_oef_overtime = rail.IfOperator(
                task_id="if_oef_overtime",
                test='{{result("get_query_conditions").overtime | is_truthy and result("query_oef_overtime", "length") > 0}}',
                yes_task="map_overtime_projects",
                no_task="combine_perday_data"
            )

            # query_distinct_oef_overtime = rail.QueryCollectionOperator(
            #     task_id="query_distinct_oef_overtime",
            #     query="""SELECT * FROM ot_oef_overtime WHERE {{result('get_query_conditions').overtime}}"""
            # )

            map_overtime_projects = rail.PythonOperator(
                task_id="map_overtime_projects",
                python_callable=lambda dag_run: custom_methods.get_overtime_projects(dag_run, config.instance)
            )

            combine_perday_data = rail.PythonOperator(
                task_id="combine_perday_data",
                python_callable=custom_methods.map_ot_oncall_callout_time_data_per_day
            )

            submit_data_to_wipro = rail.TriggerDagRunOperator(
                task_id="submit_data_to_wipro",
                trigger_dag_id=f"{config.submit_ot_data_child}_{cnt}_{config.instance}_v2",
                conf=lambda: {
                            "data": rail.result("combine_perday_data"),
                            "time_export_name": rail.render_template('{{dag_run.conf.time_export_name}}')
                        },
                execution_timeout=timedelta(days=config.execution_timeout_days)
            )

            wait_submit_data_to_wipro = rail.WaitForDagRunsSensor(
                task_id="wait_submit_data_to_wipro",
                dag_runs="{{result('submit_data_to_wipro')}}",
                execution_timeout=timedelta(
                    days=config.execution_timeout_days)
            )

            catch_and_log_errors = rail.EmptyOperator(
                task_id="catch_and_log_errors",
                trigger_rule="one_failed"
            )

            create_collection_time_export_per_period >> query_each_entry_date >>create_collection_per_day >>\
            get_query_conditions >> if_oef_oncall_column_present >> rail.Label("Yes") >> query_oef_oncall >> if_oef_callout_column_present
            if_oef_oncall_column_present >> rail.Label("No") >> if_oef_callout_column_present
            if_oef_callout_column_present >> rail.Label("Yes") >> query_oef_callout >> if_oef_overtime_column_present
            if_oef_callout_column_present >> rail.Label("No") >> if_oef_overtime_column_present
            if_oef_overtime_column_present >> rail.Label("Yes") >> query_oef_overtime >> get_distinct_querys_options
            if_oef_overtime_column_present >> rail.Label("No") >> get_distinct_querys_options
            get_distinct_querys_options >> if_oef_oncall >> rail.Label("Yes") >> map_oncall_projects >> if_oef_callout
            if_oef_oncall >> rail.Label("No") >> if_oef_callout
            if_oef_callout >> rail.Label("Yes") >> map_callout_projects >> if_oef_overtime
            if_oef_callout >> rail.Label("No") >> if_oef_overtime
            if_oef_overtime >> rail.Label("Yes") >> map_overtime_projects >> combine_perday_data
            if_oef_overtime >> rail.Label("No") >> combine_perday_data
            combine_perday_data >> submit_data_to_wipro >> wait_submit_data_to_wipro >> catch_and_log_errors

        dag_created.append(dag)


rail.for_each_instance(create_airflow_child_dag)
