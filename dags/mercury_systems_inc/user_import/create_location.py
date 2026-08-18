import rail

from mercury_systems_inc.user_import.utils.request_payload import get_add_location_payload

null = None


def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_new_location_add_dagid,
        description='MercurySystermsInc User Import Process New Location Add',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_groups,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        if_parent_uri_not_exists = rail.IfOperator(
            task_id="if_parent_uri_not_exists",
            test="{{ dag_run.conf.parent_uri | is_falsy or dag_run.conf.parent_uri == 'None' }}",
            yes_task="log_parent_uri_does_not_exist",
            no_task="create_new_location"
        )

        create_new_location = rail.RepliconServiceOperator(
            task_id="create_new_location",
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data=get_add_location_payload
        )

        log_parent_uri_does_not_exist = rail.WriteLogOperator(
            task_id="log_parent_uri_does_not_exist",
            log='{{dag_run.conf.groups_log_table}}',
            message="Parent Location Not Found",
            severity='Exception',
            properties=lambda dag_run: {
                'group': 'location',
                "name": dag_run.conf['location_name'],
                "parent_fullpath": dag_run.conf['parent_fullpath_code'],
                "parent_uri": dag_run.conf['parent_uri'],
                "action": "Validation",
                "status": "Exception",
                "detail": "Parent Location does not exist in Replicon",
                "uri_if_created": null
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            trigger_rule='one_failed',
            log='{{dag_run.conf.groups_log_table}}',
            message="na",
            severity='Error',
            properties=lambda dag_run: {
                'group': 'location',
                "name": dag_run.conf['location_name'],
                "parent_fullpath": dag_run.conf['parent_fullpath_code'],
                "parent_uri": dag_run.conf['parent_uri'],
                "action": "Add",
                "status": "Error",
                "detail": rail.render_template("{{get_error_message()}}"),
                "uri_if_created": rail.result('create_new_location')['uri'] if rail.result('create_new_location') else null,
            }
        )

        if_parent_uri_not_exists >> rail.Label(
            "No") >> create_new_location

        if_parent_uri_not_exists >> rail.Label(
            "Yes") >> log_parent_uri_does_not_exist >> catch_and_log_error

        create_new_location >> catch_and_log_error

    return dag


rail.for_each_instance(create_child_dag)
