import rail
from pwcglobal.user_import_australia.tasks.assign_supervisor_task import create_assign_supervisor_task
from pwcglobal.user_import_australia import custom_methods


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f"pwcglobal_user_import_australia_user_import_process_supervisor_child_{config.instance}",
        description=f"PwCGlobal User Import Australia - User import update user {config.instance}",
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        default_args={
            'sftp_conn_id': config.sftp_conn_id
        },
        max_active_runs=config.max_active_runs_supervisor
    )as dag:

        rail.ViewDagRunConfOperator(task_id="view_dag_config")

        is_manager_id_present = rail.IfOperator(
            task_id="is_manager_id_present",
            test="{{dag_run.conf.manager_id | is_truthy}}",
            yes_task="get_user_details"
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id="get_user_details",
            endpoint="/services/ImportService1.svc/BulkGetUsers3",
            data={
                "users": [
                    {
                        "uri": None,
                        "loginName": '{{dag_run.conf.guid}}',
                        "parameterCorrelationId": None
                    }
                ],
                "dataLoadOptionUri": "urn:replicon:data-load-option:fail-if-insufficient-data-access-permission"
            }
        )

        is_user_exists = rail.IfOperator(
            task_id="is_user_exists",
            test=lambda: bool(rail.result("get_user_details")),
            yes_task="is_supervisor_already_assigned"
        )

        def get_update_record_properties(dag_run):
            log_message = []
            success_list = ["Supervisor updated", "Initial supervisor added"]
            custom_methods.get_manager_logs(dag_run, log_message)
            if log_message and (log_message[0] in dag_run.conf['details']):
                log_message = []
            return {
                "guid": dag_run.conf['guid'],
                "status": "Exception" if (log_message and log_message[0] not in success_list) else dag_run.conf['status'],
                "action": dag_run.conf['action'],
                "details": "".join(log_message),
                "manager_id": dag_run.conf['manager_id']
            }

        update_record = rail.WriteLogOperator(
            task_id="update_record",
            log="{{dag_run.conf.log}}",
            severity=lambda dag_run: dag_run.conf['severity'],
            message="processed",
            properties=get_update_record_properties
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            log="{{dag_run.conf.log}}",
            trigger_rule='one_failed',
            severity='Error',
            message="{{dag_run.conf.details}}" + ";" +
            '{{ get_error_message() }}',
            properties={
                "guid": "{{dag_run.conf.guid}}",
                "action": "{{dag_run.conf.action}}",
                "status": "Error",
                "details": "{{dag_run.conf.details}}" + ";"
                    + '{{get_error_message()}}',
                "manager_id": "{{dag_run.conf.manager_id}}"
            },
        )
        user_uri = "{{result('get_user_details')[0].userDetails.uri}}"
        is_supervisor_already_assigned, manager_end = create_assign_supervisor_task(
            user_uri, caller="supervisor")

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )
        is_manager_id_present >> rail.Label("Yes") >> get_user_details >> is_user_exists >> rail.Label(
            "Yes") >> is_supervisor_already_assigned
        manager_end >> update_record >> rail.Label(
            "On error") >> catch_and_log_errors >> log_to_sumo
    return dag


rail.for_each_instance(create_child_dag)
