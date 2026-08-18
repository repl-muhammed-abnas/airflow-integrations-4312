from datetime import timedelta
import rail
from airflow.models import Variable

from deltek_northstar.user_sync_polaris_philippines.utils.request_payload import get_customfield_dropdown_option
from deltek_northstar.user_sync_polaris_philippines.utils.python_callable import get_option_list_to_add

null = None

# pylint: disable=too-many-statements
def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.process_dropdowns,
        description='Deltek Costpoint User Import - Process Dropdowns',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_process_dropdowns,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config", extra_config=config)

        get_user_udf_uris = rail.RepliconServiceOperator(
            task_id="get_user_udf_uris",
            endpoint="/services/CustomFieldService1.svc/GetAllCustomFields",
            data={
                "objectUri": "urn:replicon:object-type:user"
            },
            data_handler=lambda udfs: {
                'personal_action_code_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Personnel Action Code', 'uri'),
                'job_title_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Detail Job Title', 'uri'),
                'line_of_business_uri': rail.find_first_by_attr_and_get_attr(udfs, 'displayText', 'Line of Business', 'uri')
            },
        )

        query_unique_personal_action_code = rail.QueryCollectionOperator(
            task_id='query_unique_personal_action_code',
            query="""SELECT DISTINCT personal_action_code FROM valid_records""",
            name='unique_personal_action_code'
        )

        get_all_personal_action_code_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_personal_action_code_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_udf_uris').personal_action_code_uri }}"
            }
        )

        query_unique_detail_job_title = rail.QueryCollectionOperator(
            task_id='query_unique_detail_job_title',
            query="""SELECT DISTINCT title_desc FROM valid_records""",
            name='unique_detail_job_title'
        )

        get_all_detail_job_title_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_detail_job_title_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_udf_uris').job_title_uri }}"
            }
        )

        query_unique_line_of_business = rail.QueryCollectionOperator(
            task_id='query_unique_line_of_business',
            query="""SELECT DISTINCT hr_organization FROM valid_records""",
            name='unique_line_of_business'
        )

        get_all_line_of_business_dropdowns = rail.RepliconServiceOperator(
            task_id='get_all_line_of_business_dropdowns',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data={
                'customFieldUri': "{{ result('get_user_udf_uris').line_of_business_uri }}"
            }
        )

        get_udf_values_to_add = rail.PythonOperator(
            task_id="get_udf_values_to_add",
            python_callable=get_option_list_to_add
            
        )

        is_personal_action_code_to_add = rail.IfOperator(
            task_id='is_personal_action_code_to_add',
            test='''{{ result('get_udf_values_to_add').personal_action_code_to_add | is_truthy }}''',
            yes_task="put_personal_action_code",
            no_task="is_detail_job_title_to_add",
        )

        put_personal_action_code = rail.RepliconServiceOperator(
            task_id='put_personal_action_code',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': rail.result('get_user_udf_uris')['personal_action_code_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option('get_all_personal_action_code_dropdowns', 'personal_action_code_to_add')
            }
        )

        is_detail_job_title_to_add = rail.IfOperator(
            task_id='is_detail_job_title_to_add',
            test='''{{ result('get_udf_values_to_add').detail_job_title_to_add | is_truthy }}''',
            yes_task="put_detail_job_title_to_add",
            no_task="is_line_of_business_to_add",
        )

        put_detail_job_title_to_add = rail.RepliconServiceOperator(
            task_id='put_detail_job_title_to_add',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': rail.result('get_user_udf_uris')['job_title_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option('get_all_detail_job_title_dropdowns', 'detail_job_title_to_add')
            }
        )

        is_line_of_business_to_add = rail.IfOperator(
            task_id='is_line_of_business_to_add',
            test='''{{ result('get_udf_values_to_add').line_of_business_to_add | is_truthy }}''',
            yes_task="put_line_of_business_to_add",
            no_task="finish",
        )

        put_line_of_business_to_add = rail.RepliconServiceOperator(
            task_id='put_line_of_business_to_add',
            endpoint='/services/CustomFieldService1.svc/PutDropDownOptions',
            data=lambda: {
                'customFieldUri': rail.result('get_user_udf_uris')['line_of_business_uri'],
                'customFieldDropDownOptionUris': get_customfield_dropdown_option('get_all_line_of_business_dropdowns', 'line_of_business_to_add')
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        get_user_udf_uris >> query_unique_personal_action_code >> get_all_personal_action_code_dropdowns >> get_udf_values_to_add

        get_user_udf_uris >> query_unique_detail_job_title >> get_all_detail_job_title_dropdowns >> get_udf_values_to_add

        get_user_udf_uris >> query_unique_line_of_business >> get_all_line_of_business_dropdowns >> get_udf_values_to_add

        get_udf_values_to_add >> is_personal_action_code_to_add >> rail.Label(
            'Yes') >> put_personal_action_code >> is_detail_job_title_to_add
        is_personal_action_code_to_add >> rail.Label(
            'No') >> is_detail_job_title_to_add
        
        is_detail_job_title_to_add >> rail.Label(
            'Yes') >> put_detail_job_title_to_add >> is_line_of_business_to_add
        is_detail_job_title_to_add >> rail.Label(
            'No') >> is_line_of_business_to_add
        
        is_line_of_business_to_add >> rail.Label(
            'Yes') >> put_line_of_business_to_add >> finish
        is_line_of_business_to_add >> rail.Label(
            'No') >> finish

    return dag

rail.for_each_instance(create_child_dag)
