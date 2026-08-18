from americanintegrated.project_import import request_payload
import rail
null = None


def create_airflow_child_dag(config):
    with rail.create_airflow_dag(
        dag_id= config.process_client_dag_id,
        description="americanintegrated create client",
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
        company_key=config.company_key,
        default_args={
            "sftp_conn_id": config.sftp_conn_id
        }
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")
        get_client_data_by_client_code = rail.RepliconServiceOperator(
            task_id="get_client_data_by_client_code",
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.client_data_request,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(list(map(lambda i: {
                    "clientname": i["cells"][0]["textValue"],
                    "clienturi": i["cells"][0]["uri"],
                    "clientcode": i["cells"][1]["textValue"]
            }, response["rows"])),
                "clientcode",
                dag_run.conf["clientcode"],
                "clienturi") if response["rows"] else null
        )

        if_client_uri_by_code_exits = rail.IfOperator(
            task_id="if_client_uri_by_code_exits",
            test='{{result("get_client_data_by_client_code")|is_truthy}}',
            yes_task="write_client_exists_log",
            no_task="get_client_data_by_client_name"
        )

        write_client_exists_log = rail.WriteLogOperator(
            task_id="write_client_exists_log",
            message="Client Ignored",
            severity="Ignored",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Client Name": dag_run.conf["clientname"],
                "Client Code": dag_run.conf["clientcode"],
                "Status": "Ignored",
                "Reason": "Client already present in Replicon",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        get_client_data_by_client_name = rail.RepliconServiceOperator(
            task_id="get_client_data_by_client_name",
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_client_by_name_request,
            data_handler=lambda response, dag_run: rail.find_first_by_attr_and_get_attr(list(map(lambda i: {
                "clientname": i["cells"][0]["textValue"],
                "clienturi": i["cells"][0]["uri"],
            }, response["rows"])),
                "clientname",
                dag_run.conf["clientname"],
                "clienturi") if response["rows"] else null
        )

        if_client_uri_by_name_exists = rail.IfOperator(
            task_id="if_client_uri_by_name_exists",
            test='{{result("get_client_data_by_client_name")|is_truthy}}',
            yes_task="write_client_ignored_log",
            no_task="create_new_client"
        )

        create_new_client = rail.RepliconServiceOperator(
            task_id="create_new_client",
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=request_payload.create_client_request
        )

        write_client_ignored_log = rail.WriteLogOperator(
            task_id="write_client_ignored_log",
            message="Client Ignored",
            severity="Ignored",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Client Name": dag_run.conf["clientname"],
                "Client Code": dag_run.conf["clientcode"],
                "Status": "Ignored",
                "Reason": "Client already present in Replicon",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        write_client_success_log = rail.WriteLogOperator(
            task_id="write_client_success_log",
            message="Client created",
            severity="Success",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Client Name": dag_run.conf["clientname"],
                "Client Code": dag_run.conf["clientcode"],
                "Status": "Success",
                "Reason": "Client created",
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        log_client_details = rail.PythonOperator(
            task_id = 'log_client_details',
            python_callable= lambda dag_run: {
                'client_uri': rail.result("get_client_data_by_client_code") if rail.result(
                    "get_client_data_by_client_code") else rail.result("get_client_data_by_client_name") if rail.result(
                        "get_client_data_by_client_name") else rail.result("create_new_client")['uri'],
                'client_code': dag_run.conf['clientcode']
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            message='{{get_error_message()}}',
            severity="Error",
            trigger_rule="one_failed",
            properties=lambda dag_run: {
                "Job ID": dag_run.conf["parent_ecid"],
                "Client Name": dag_run.conf["clientname"],
                "Client Code": dag_run.conf["clientcode"],
                "Status": "Error",
                "Reason": rail.render_template('{{get_error_message()}}'),
                "Child Job ID": rail.render_template('{{ecid()}}'),
            }
        )

        get_client_data_by_client_code >>\
            if_client_uri_by_code_exits >> rail.Label(
                "Yes") >> write_client_exists_log >> log_client_details
        if_client_uri_by_code_exits >> rail.Label("No") >> get_client_data_by_client_name >>\
            if_client_uri_by_name_exists >> rail.Label(
                "Yes") >> write_client_ignored_log >> log_client_details
        if_client_uri_by_name_exists >> rail.Label("No") >>\
            create_new_client >> write_client_success_log >> log_client_details >> catch_and_log_errors

        return dag


rail.for_each_instance(create_airflow_child_dag)
