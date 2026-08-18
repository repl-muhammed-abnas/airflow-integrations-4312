from americanintegrated.project_import import request_payload
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.process_task_dag_id,
        description="americanintegrated project client",
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        company_key=config.company_key,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:
        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_prevailing_wages_yes = rail.IfOperator(
            task_id="if_prevailing_wages_yes",
            test='{{dag_run.conf["prevailingwages"] | lower == "yes" }}',
            yes_task="query_collection_prevailing_wage",
            no_task="start_basic_task"
        )

        query_collection_prevailing_wage = rail.QueryCollectionOperator(
            task_id="query_collection_prevailing_wage",
            query="""SELECT * FROM reference_file_content_prevailing_wage"""
        )

        parse_prevailing_wage_data = rail.PythonOperator(
            task_id="parse_prevailing_wage_data",
            python_callable=lambda dag_run: list(map(lambda i:
                                                     {
                                                         "paygroup": i["taskcode"],
                                                         "name": i["taskname"],
                                                         "payroll": i["payrollclassification"],
                                                         "rate1": i["rate1"],
                                                         "rate2": i["rate2"],
                                                         "rate3": i["rate3"]
                                                     }, rail.load_all_records(dag_run.conf["prevailing_wage_artifact"])))
        )

        create_task_with_paygroups = rail.ForEachOperator(
            task_id="create_task_with_paygroups",
            items='{{result("parse_prevailing_wage_data")|to_json}}',
            start_task="create_prevailing_wage",
            end_task="prevailing_wage_task_end"

        )

        create_prevailing_wage = rail.RepliconServiceOperator(
            task_id="create_prevailing_wage",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_prevailing_wage_task_request
        )

        prevailing_wage_task_end = rail.EmptyOperator(
            task_id="prevailing_wage_task_end")

        start_basic_task = rail.EmptyOperator(task_id="start_basic_task")
        query_collection_basic_task = rail.QueryCollectionOperator(
            task_id="query_collection_basic_task",
            query="""SELECT * FROM reference_file_content_basic_task"""
        )

        parse_basic_task_data = rail.PythonOperator(
            task_id="parse_basic_task_data",
            python_callable=lambda dag_run: list(map(lambda i:
                                                     {
                                                         "costcode": i["taskcode"],
                                                         "costcodename": i["taskname"],

                                                     }, rail.load_all_records(dag_run.conf["basic_task_artifact"])))
        )

        create_task_with_basic_task = rail.ForEachOperator(
            task_id="create_task_with_basic_task",
            items='{{result("parse_basic_task_data")|to_json}}',
            start_task="create_basic_task",
            end_task="basic_task_end"

        )

        create_basic_task = rail.RepliconServiceOperator(
            task_id="create_basic_task",
            endpoint="/services/ProjectService1.svc/PutTask",
            data=request_payload.get_basic_task_request
        )

        basic_task_end = rail.EmptyOperator(task_id="basic_task_end")

        write_task_creation_error_log = rail.WriteLogOperator(
            task_id="write_task_creation_error_log",
            message="Project  task creation error",
            log='{{dag_run.conf.lookuptable}}',
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Project Name": dag_run.conf["projectname"],
                "Project Code": dag_run.conf["projectcode"],
                "Status": "Error",
                "Reason": "Project  task creation error " + rail.render_template('{{get_error_message()}}'),
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        if_prevailing_wages_yes >> rail.Label("Yes") >> query_collection_prevailing_wage >>\
            parse_prevailing_wage_data >>\
            create_task_with_paygroups >> prevailing_wage_task_end
        create_task_with_paygroups >> create_prevailing_wage >> prevailing_wage_task_end >>\
        start_basic_task
        if_prevailing_wages_yes >> rail.Label("No") >> start_basic_task
        start_basic_task >> query_collection_basic_task >> parse_basic_task_data >>\
            create_task_with_basic_task >> basic_task_end
        create_task_with_basic_task >> create_basic_task >> basic_task_end >>\
        write_task_creation_error_log
        return dag


rail.for_each_instance(create_airflow_child_dag)
