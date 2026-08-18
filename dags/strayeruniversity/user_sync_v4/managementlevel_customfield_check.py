from datetime import timedelta
from airflow.models import Variable
import rail
from strayeruniversity.user_sync_v4.utils.request_payload import get_customfield_dropdown_option_uris


def create_dag(config):
    # pylint: disable=too-many-statements, line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_managementlevel_customfield_check_dag_id,
        description=f'strayeruniversity_usersync_managementlevel_customfield_check_child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.management_level_child_dag_active_runs,
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
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        get_all_customfield_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_customfield_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ dag_run.conf.managementlevel_customfield_uri }}"
            }
        )

        create_collection_from_managementlevel_inputfile_csv = rail.CreateCollectionOperator(
            task_id='create_collection_from_managementlevel_inputfile_csv',
            source="{{ dag_run.conf.managementlevel }}",
            name="sourceuserdata_managementlist",
            columns={
                "managementlevel": "managementlevel"
            }
        )

        current_managementlevel_values = rail.CreateCollectionOperator(
            task_id='current_managementlevel_values',
            source=lambda: rail.result('get_all_customfield_dropdowns'),
            name='managementlevelvalues'
        )

        new_managementlevel_values = rail.QueryCollectionOperator(
            task_id='new_managementlevel_values',
            query="""SELECT * FROM sourceuserdata_managementlist WHERE
                    lower(managementlevel) NOT IN (SELECT DISTINCT LOWER(displayText) FROM managementlevelvalues)""",
            name='newmanagementlevelvalues'
        )

        is_managementlevel_new_values_present = rail.IfOperator(
            task_id='is_managementlevel_new_values_present',
            test="{{ result('new_managementlevel_values', 'length') > 0 }}",
            yes_task='put_dropdown_options_managementlevel',
            no_task='finish'
        )

        put_dropdown_options_managementlevel = rail.RepliconServiceOperator(
            task_id='put_dropdown_options_managementlevel',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': "{{ dag_run.conf.managementlevel_customfield_uri }}",
                'customFieldDropDownOptionUris': get_customfield_dropdown_option_uris()
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label('No') >> get_all_customfield_dropdowns

        get_all_customfield_dropdowns >> create_collection_from_managementlevel_inputfile_csv >> \
            current_managementlevel_values >> new_managementlevel_values >> is_managementlevel_new_values_present

        is_managementlevel_new_values_present >> rail.Label(
            'Yes') >> put_dropdown_options_managementlevel >> finish
        is_managementlevel_new_values_present >> rail.Label(
            'No') >> finish

    return dag


rail.for_each_instance(create_dag)
