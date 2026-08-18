from datetime import timedelta
from airflow.models import Variable
import rail
from lendingclub.user_import.utils import request_payload
from lendingclub.user_import.utils.python_callable import get_uri_data_on_code, get_uri_data_on_name

def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=f'lendingclub_user_import_location_add_update_child_{config.instance}',
        description=f'lendingclub_user_import_location_add_update_child_{config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.location_add_update_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='is_location_code_and_name_absent'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='is_location_code_and_name_absent',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        is_location_code_and_name_absent = rail.IfOperator(
            task_id='is_location_code_and_name_absent',
            test="{{ dag_run.conf.locationcode | is_falsy and dag_run.conf.locationname | is_falsy }}",
            yes_task="catch_and_log_error",
            no_task="is_location_code_present",
        )

        is_location_code_present = rail.IfOperator(
            task_id='is_location_code_present',
            test="{{ dag_run.conf.locationcode | is_truthy }}",
            yes_task="get_locationdata_based_on_code",
            no_task="if_location_code_absent_and_name_present",
        )

        get_locationdata_based_on_code = rail.RepliconServiceOperator(
            task_id='get_locationdata_based_on_code',
            endpoint="/services/LocationListService1.svc/GetData",
            data = request_payload.get_locationdata_on_code_payload,
            data_handler=lambda response, dag_run: get_uri_data_on_code(
                response, 'location_code', dag_run.conf['locationcode'],'location_uri','location_name')
        )

        if_location_name_present = rail.IfOperator(
            task_id='if_location_name_present',
            test="{{ dag_run.conf.locationname | is_truthy }}",
            yes_task="if_existing_location_uri_present",
            no_task="catch_and_log_error",
        )

        if_existing_location_uri_present = rail.IfOperator(
            task_id='if_existing_location_uri_present',
            test="{{ result('get_locationdata_based_on_code').location_uri | is_truthy }}",
            yes_task="if_locationname_mismatch",
            no_task="get_locationdata_based_on_name",
        )

        if_locationname_mismatch = rail.IfOperator(
            task_id='if_locationname_mismatch',
            test="{{ result('get_locationdata_based_on_code').location_name.lower() != dag_run.conf.locationname.lower() }}",
            yes_task="update_locationname",
            no_task="catch_and_log_error",
        )

        update_locationname = rail.RepliconServiceOperator(
            task_id='update_locationname',
            endpoint="/services/LocationService1.svc/UpdateName",
            data = {
                "locationUri":"{{ result('get_locationdata_based_on_code').location_uri }}",
                "name": "{{ dag_run.conf.locationname }}"
            }
        )

        if_location_code_absent_and_name_present = rail.IfOperator(
            task_id='if_location_code_absent_and_name_present',
            test="{{ dag_run.conf.locationcode | is_falsy and dag_run.conf.locationname | is_truthy }}",
            yes_task="get_locationdata_based_on_name",
            no_task="catch_and_log_error",
        )

        get_locationdata_based_on_name = rail.RepliconServiceOperator(
            task_id='get_locationdata_based_on_name',
            endpoint="/services/LocationListService1.svc/GetData",
            data = request_payload.get_locationdata_on_name_payload,
            data_handler=lambda response, dag_run: get_uri_data_on_name(
                response, dag_run.conf['locationname'],'location_uri','location_name')
        )

        if_location_uri_absent = rail.IfOperator(
            task_id='if_location_uri_absent',
            test="{{ result('get_locationdata_based_on_name').location_uri | is_falsy }}",
            yes_task="create_new_draft_location",
            no_task="catch_and_log_error",
        )

        create_new_draft_location = rail.RepliconServiceOperator(
            task_id='create_new_draft_location',
            endpoint="/services/LocationService1.svc/CreateNewDraft",
            data = {
                "parentLocationUri": None
            }
        )

        update_location_name = rail.RepliconServiceOperator(
            task_id='update_location_name',
            endpoint="/services/LocationService1.svc/UpdateName",
            data = {
                "locationUri":"{{ result('create_new_draft_location') }}",
                "name": "{{ dag_run.conf.locationname }}"
            }
        )

        if_location_code_absent = rail.IfOperator(
            task_id='if_location_code_absent',
            test="{{ dag_run.conf.locationcode | is_falsy }}",
            yes_task="publish_location",
            no_task="update_location_code",
        )

        update_location_code = rail.RepliconServiceOperator(
            task_id='update_location_code',
            endpoint="/services/LocationService1.svc/UpdateCode",
            data = {
                "locationUri":"{{ result('create_new_draft_location') }}",
                "code": "{{ dag_run.conf.locationcode }}"
            }
        )

        publish_location = rail.RepliconServiceOperator(
            task_id='publish_location',
            endpoint="/services/LocationService1.svc/PublishDraft",
            data = {
                "draftUri":"{{ result('create_new_draft_location') }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log = '{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "UserID": "",
                "Action": "Location Add/Update",
                "Status": "Error",
                "Details": "{{ get_error_message() }}"
            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> is_location_code_and_name_absent

        is_location_code_and_name_absent >> rail.Label('Yes') >> catch_and_log_error
        is_location_code_and_name_absent >> rail.Label('No') >> is_location_code_present

        is_location_code_present >> rail.Label('Yes') >> get_locationdata_based_on_code >> if_location_name_present

        if_location_name_present >> rail.Label('Yes') >> if_existing_location_uri_present

        if_existing_location_uri_present >> rail.Label('Yes') >> if_locationname_mismatch

        if_locationname_mismatch >> rail.Label('Yes') >> update_locationname >> catch_and_log_error
        if_locationname_mismatch >> rail.Label('No') >> catch_and_log_error

        if_existing_location_uri_present >> rail.Label('No') >> get_locationdata_based_on_name

        if_location_name_present >> rail.Label('No') >> catch_and_log_error

        is_location_code_present >> rail.Label('No') >> if_location_code_absent_and_name_present

        if_location_code_absent_and_name_present >> rail.Label('Yes') >> get_locationdata_based_on_name
        if_location_code_absent_and_name_present >> rail.Label('No') >> catch_and_log_error

        get_locationdata_based_on_name >> if_location_uri_absent

        if_location_uri_absent >> rail.Label('Yes') >> create_new_draft_location
        if_location_uri_absent >> rail.Label('No') >> catch_and_log_error

        create_new_draft_location >> update_location_name >> if_location_code_absent

        if_location_code_absent >> rail.Label('Yes') >> publish_location >> catch_and_log_error
        if_location_code_absent >> rail.Label('No') >> update_location_code >> publish_location >> catch_and_log_error

        catch_and_log_error >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
