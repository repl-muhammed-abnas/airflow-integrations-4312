from datetime import timedelta
from airflow.models import Variable
import rail
from adtalem.user_import.utils.request_payload import get_customfield_dropdown_option_uris


# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/adtalem/user_import/config.py


def create_jobcode_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_userimport_update_jobcodedropdowns_{config.instance}',
        description=f'Adtalem Update Jobcode dropdowns {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id='view_dagrun_config')

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_jobcode_usercustomfield'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_jobcode_usercustomfield',
            end_task='catch_error',
        )

        get_jobcode_usercustomfield = rail.RepliconServiceOperator(
            task_id='get_jobcode_usercustomfield',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={
                'objectUri': 'urn:replicon:object-type:user'
            },
            data_handler=lambda response: rail.find_first_by_attr_and_get_attr(
                response, 'displayText', 'Job Code', 'uri', '')
        )

        get_jobcode_dropdowns = rail.RepliconServiceOperator(
            task_id='get_jobcode_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_jobcode_usercustomfield') }}"
            }
        )

        current_jobcode_values = rail.CreateCollectionOperator(
            task_id='current_jobcode_values',
            source=lambda: rail.result('get_jobcode_dropdowns'),
            name='jobcodevalues'
        )

        new_jobcode_values = rail.QueryCollectionOperator(
            task_id='new_jobcode_values',
            query="""SELECT DISTINCT jobcode FROM rawdatacollection WHERE
                    NULLIF(jobcode, '') IS NOT NULL AND
                    lower(jobcode) NOT IN (SELECT DISTINCT LOWER(displayText) FROM jobcodevalues)""",
            name='newjobcodevalues'
        )

        is_jobcode_new_values = rail.IfOperator(
            task_id='is_jobcode_new_values',
            test="{{ result('new_jobcode_values', 'length') > 0 }}",
            yes_task='put_dropdown_options_jobcode',
            no_task='catch_error'
        )

        put_dropdown_options_jobcode = rail.RepliconServiceOperator(
            task_id='put_dropdown_options_jobcode',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': rail.result('get_jobcode_usercustomfield'),
                'customFieldDropDownOptionUris': get_customfield_dropdown_option_uris()
            }
        )

        catch_error = rail.FailOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}'
        )

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error

        can_run_batch_task >> rail.Label(
            'No') >> get_jobcode_usercustomfield >> get_jobcode_dropdowns >> \
            current_jobcode_values >> new_jobcode_values >> is_jobcode_new_values

        is_jobcode_new_values >> rail.Label(
            'Yes') >> put_dropdown_options_jobcode >> catch_error

        is_jobcode_new_values >> rail.Label(
            'No') >> catch_error

        return dag


rail.for_each_instance(create_jobcode_child_dag)
