from datetime import timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v4.utils.request_payload import get_customfield_dropdown_option


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_process_customfield_for_dropdown_dag_id,
        description=f'strayeruniversity_usersync_process_customfield_for_dropdown_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.process_customfield_dd_child_dag_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_all_customfield_dropdowns'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='get_all_customfield_dropdowns',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_customfield_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_customfield_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ dag_run.conf.udf_uri }}"
            }
        )

        get_udf_value_uri = rail.PythonOperator(
            task_id="get_udf_value_uri",
            python_callable=lambda dag_run: {
                'udf_value_uri': rail.find_first_by_attr_and_get_attr(
                    rail.result('get_all_customfield_dropdowns'), 'displayText', dag_run.conf['udf_value'], 'uri', '')
            }
        )

        if_udf_value_uri_present = rail.IfOperator(
            task_id='if_udf_value_uri_present',
            test='''{{ result('get_udf_value_uri').udf_value_uri | is_truthy }}''',
            yes_task="update_udf_type_customfield",
            no_task="put_dropdown_options_udf",
        )

        update_udf_type_customfield = rail.RepliconServiceOperator(
            task_id='update_udf_type_customfield',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_udf_value_uri').udf_value_uri }}"
            }
        )

        put_dropdown_options_udf = rail.RepliconServiceOperator(
            task_id='put_dropdown_options_udf',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda dag_run: {
                'customFieldUri': dag_run.conf['udf_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option(dag_run)
            }
        )

        get_all_customfield_dropdowns_repeat = rail.RepliconServiceOperator(
            task_id='get_all_customfield_dropdowns_repeat',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ dag_run.conf.udf_uri }}"
            },
            data_handler=lambda response, dag_run: {
                'udf_value_uri': rail.find_first_by_attr_and_get_attr(
                    response, 'displayText', dag_run.conf['udf_value'], 'uri', '')
            }
        )

        update_udf_type_customfield_afteradd = rail.RepliconServiceOperator(
            task_id='update_udf_type_customfield_afteradd',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.udf_uri }}",
                "customFieldDropDownOptionUri": "{{ result('get_all_customfield_dropdowns_repeat') }}"
            }
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id="catch_and_log_error",
            log='{{ dag_run.conf.logger}}',
            severity="Error",
            trigger_rule="one_failed",
            message='{{ get_error_message() }}',
            properties={
                "username": "{{ dag_run.conf.username }}" + "|" + "{{ dag_run.conf.emplid }}",
                "action": "Customfield check",
                "status": "Error",
                "details": "{{ dag_run_ecid() }}" + "-" + "{{ get_error_message() }}"
            }
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> get_all_customfield_dropdowns

        get_all_customfield_dropdowns >> get_udf_value_uri >> if_udf_value_uri_present

        if_udf_value_uri_present >> rail.Label(
            'Yes') >> update_udf_type_customfield >> catch_and_log_error
        if_udf_value_uri_present >> rail.Label('No') >> put_dropdown_options_udf >> get_all_customfield_dropdowns_repeat >> \
            update_udf_type_customfield_afteradd >> catch_and_log_error

    return dag


rail.for_each_instance(create_dag)
