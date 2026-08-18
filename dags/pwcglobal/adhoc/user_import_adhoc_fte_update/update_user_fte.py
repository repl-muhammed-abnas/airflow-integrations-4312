import json
import rail
def create_airflow_master(config):
    with rail.create_airflow_dag(
        dag_id=config.user_fte_update_child,
        description="pwcglobal adhoc fte initial update",
        company_key=config.company_key,
        max_active_runs=config.max_active_runs_child,
        replicon_conn_id=config.replicon_conn_id
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dag_run_config"
        )

        if_fte_percent_present = rail.IfOperator(
            task_id="if_fte_percent_present",
            test=lambda dag_run:bool(dag_run.conf["FTE"] and dag_run.conf["user_start_date"]),
            yes_task="update_ftepercent_customfield",
            no_task="end_fte_update"
        )

        update_ftepercent_customfield = rail.RepliconServiceOperator(
            task_id='update_ftepercent_customfield',
            endpoint='/services/CustomFieldService1.svc/UpdateNumericValue',
            data={
                "objectUri": '{{dag_run.conf.user_uri}}',
                "customFieldUri": '{{dag_run.conf.customFieldUri}}',
                "value":'{{dag_run.conf.FTE}}'
            }
        )

        put_key_value_to_ftevalue_space = rail.RepliconServiceOperator(
            task_id="put_key_value_to_ftevalue_space",
            endpoint="/services/GenericKeyValueStoreService1.svc/PutKeyValue",
            data=lambda dag_run: {
                    "keyNamespace": config.keynamespace,
                    "keyValue": {
                        "key": dag_run.conf["user_uri"],
                        "jsonValue": json.dumps(
                            [{
                                "value": dag_run.conf["FTE"],
                                "effectivedate":dag_run.conf["user_start_date"]
                            }]
                        )
                    }
            }
        )

        end_fte_update = rail.EmptyOperator(
            task_id="end_fte_update"
        )

        write_fte_update_success_log = rail.WriteLogOperator(
            task_id="write_fte_update_success_log",
            log='{{dag_run.conf.lookuptable}}',
            message="FTE adhoc run success",
            properties={
                "user":'{{dag_run.conf.user_login_name}}',
                "FTE":'{{dag_run.conf.FTE}}',
                "user_uri":'{{dag_run.conf.user_uri}}',
                "FTE_percent_effective_date":'{{dag_run.conf.FTE_percent_effective_date}}',
                "details":"FTE update success",
                "jobid":'{{dag_run.conf.ecid}}'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id="catch_and_log_errors",
            log='{{dag_run.conf.lookuptable}}',
            message="FTE adhoc run failed",
            trigger_rule="one_failed",
            properties={
                "user":'{{dag_run.conf.user_login_name}}',
                "FTE":'{{dag_run.conf.FTE}}',
                "user_uri":'{{dag_run.conf.user_uri}}',
                "FTE_percent_effective_date":'{{dag_run.conf.FTE_percent_effective_date}}',
                "details":"{{get_error_message()}}",
                "jobid":'{{dag_run.conf.ecid}}'
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id="log_to_sumo",
            sumo_conn_id="sumologic-dagrunlogger",
            trigger_rule="all_done"
        )

        if_fte_percent_present >> rail.Label("Yes") >>\
        update_ftepercent_customfield >> put_key_value_to_ftevalue_space >> end_fte_update
        if_fte_percent_present >> rail.Label("No") >> end_fte_update>>\
        write_fte_update_success_log >>\
        catch_and_log_errors>>\
        log_to_sumo

        return dag

rail.for_each_instance(create_airflow_master)
