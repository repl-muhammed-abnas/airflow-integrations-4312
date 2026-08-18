from datetime import timedelta
import rail
from airflow.models import Variable
from technicolorg3.user_import.task.process_udf import process_udfs_task_group

# config :
# https://github.com/replicon/airflow-integrations/blob/main/dags/technicolorg3/user_import/config.py


def create_udfs_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'technicolorg3_user_import_child_udfs_{config.instance}',
        description=f'Technicolor Drop Down UDF Custom field check {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_udfs_max_active_runs,
        max_active_tasks=config.dag_max_active_tasks
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='get_required_user_customfields'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
            start_task='get_required_user_customfields',
            end_task='catch_error',
        )

        get_required_user_customfields = rail.RepliconServiceOperator(
            task_id='get_required_user_customfields',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFields',
            data={'objectUri': 'urn:replicon:object-type:user'},
            data_handler=lambda response: {
                'referencejobcode_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Reference Job code', 'uri'),
                'jobtitle_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Reference Job Title', 'uri'),
                'department_uri': rail.find_first_by_attr_and_get_attr(response, 'displayText', 'Department', 'uri')
            }
        )

        get_referencejobcode_dropdown = rail.RepliconServiceOperator(
            task_id='get_referencejobcode_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_required_user_customfields').referencejobcode_uri }}"}
        )

        get_jobtitle_dropdown = rail.RepliconServiceOperator(
            task_id='get_jobtitle_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_required_user_customfields').jobtitle_uri }}"}
        )

        get_department_dropdown = rail.RepliconServiceOperator(
            task_id='get_department_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_required_user_customfields').department_uri }}"}
        )

        catch_error = rail.FailOperator(
            task_id='catch_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}'
        )

        (current_referencejobcode_values, is_referencejobcode_new_values,
         put_dropdown_options_referencejobcode) = process_udfs_task_group('referencejobcode', 'jobtitle')

        (current_jobtitle_values, is_jobtitle_new_values,
         put_dropdown_options_jobtitle) = process_udfs_task_group('jobtitle', 'department')

        (current_department_values, is_department_new_values,
         put_dropdown_options_department) = process_udfs_task_group('department')

        can_run_batch_task >> rail.Label(
            'Yes') >> batch_task >> catch_error

        can_run_batch_task >> rail.Label(
            'No') >> get_required_user_customfields

        get_required_user_customfields >> get_referencejobcode_dropdown >> get_jobtitle_dropdown >> \
            get_department_dropdown >> current_referencejobcode_values

        put_dropdown_options_referencejobcode >> current_jobtitle_values

        is_referencejobcode_new_values >> rail.Label(
            'No') >> current_jobtitle_values

        put_dropdown_options_jobtitle >> current_department_values

        is_jobtitle_new_values >> rail.Label(
            'No') >> current_department_values

        put_dropdown_options_department >> catch_error

        is_department_new_values >> rail.Label(
            'No') >> catch_error

        return dag


rail.for_each_instance(create_udfs_child_dag)
