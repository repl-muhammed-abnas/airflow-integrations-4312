
from datetime import timedelta
import uuid
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'intercontinentalexchange_groups_check_location_v2_{config.instance}',
        description=f'IntercontinentalExchange_Groups_Check - Location v2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='create_list_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='create_list_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        create_list_3 = rail.CreateCollectionOperator(
            task_id='create_list_3',
            source="{{ dag_run.conf.group | to_json }}",
            name="groupvaluesfromfeedfile",
        )

        if_request_grouptype_equals_to_location_4 = rail.IfOperator(
            task_id='if_request_grouptype_equals_to_location_4',
            test='''{{ dag_run.conf.grouptype == 'Location' }}''',
            yes_task="get_data_location_list_service1_5",
            no_task="catch_16",
        )

        def get_filtered_groups_data(response):
            data = response.json()['d']['rows']
            groups_info = list(map(lambda item: {
                "code": item['cells'][0].get('textValue'),
                "textvalue": item['cells'][1]['textValue'],
                "uri": item['cells'][1].get('uri'),
            }, data))
            return groups_info if groups_info else []

        get_data_location_list_service1_5 = rail.RepliconServiceOperator(
            task_id='get_data_location_list_service1_5',
            endpoint="/services/LocationListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:location-list-column:code",
                    "urn:replicon:location-list-column:location"
                ],
                "sort": [],
                "filterExpression": null
            },
            response_filter=get_filtered_groups_data
        )

        create_list_7 = rail.CreateCollectionOperator(
            task_id='create_list_7',
            source="{{ result('get_data_location_list_service1_5') | to_json }}",
            name="parentgroupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_8 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_8',
            query="""SELECT DISTINCT  groupvaluesfromfeedfile.Parent FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.Parent NOT IN ( SELECT DISTINCT  parentgroupvaluesinreplicon.textValue FROM  parentgroupvaluesinreplicon)""",
        )

        foreach_query_list_checkfornewdropdownvalues_8_9 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_8_9',
            items="{{ result('query_list_checkfornewdropdownvalues_8') }}",
            start_task='create_location_or_apply_modification_10',
            end_task='foreach_query_list_checkfornewdropdownvalues_8_9_end'
        )

        create_location_or_apply_modification_10 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_10',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": null,
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_8_9').parent }}",
                    "codeToApply": {
                        "value": null
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_8_9').parent }}" + str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_8_9_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_8_9_end',
        )

        create_list_11 = rail.CreateCollectionOperator(
            task_id='create_list_11',
            source="{{ result('get_data_location_list_service1_5') | to_json }}",
            name="groupvaluesinreplicon",
        )

        query_list_checkfornewdropdownvalues_12 = rail.QueryCollectionOperator(
            task_id='query_list_checkfornewdropdownvalues_12',
            query="""SELECT * FROM  groupvaluesfromfeedfile WHERE  groupvaluesfromfeedfile.code NOT IN (SELECT DISTINCT  groupvaluesinreplicon.code FROM  groupvaluesinreplicon WHERE ( groupvaluesinreplicon.code!= "" AND  groupvaluesinreplicon.code IS NOT NULL))""",
        )

        if_query_list_checkfornewdropdownvalues_12_rows_greater_than_0_13 = rail.IfOperator(
            task_id='if_query_list_checkfornewdropdownvalues_12_rows_greater_than_0_13',
            test='''{{ result('query_list_checkfornewdropdownvalues_12', key='length') > 0 }}''',
            yes_task="foreach_query_list_checkfornewdropdownvalues_12_14",
            no_task="finish",
        )

        foreach_query_list_checkfornewdropdownvalues_12_14 = rail.ForEachOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_12_14',
            items="{{ result('query_list_checkfornewdropdownvalues_12') }}",
            start_task='create_location_or_apply_modification_15',
            end_task='foreach_query_list_checkfornewdropdownvalues_12_14_end'
        )

        create_location_or_apply_modification_15 = rail.RepliconServiceOperator(
            task_id='create_location_or_apply_modification_15',
            endpoint="/services/LocationService1.svc/CreateLocationOrApplyModification",
            data={
                "location": {
                    "name": null,
                    "uri": null,
                    "parent": {
                        "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_12_14').parent }}",
                        "uri": null,
                        "parent": null,
                        "parameterCorrelationId": null
                    },
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "name": "{{ result('foreach_query_list_checkfornewdropdownvalues_12_14').display_text }}",
                    "codeToApply": {
                        "value": "{{ result('foreach_query_list_checkfornewdropdownvalues_12_14').code }}"
                    },
                    "descriptionToApply": null,
                    "isEnabled": "true"
                },
                "unitOfWorkId": "{{ result('foreach_query_list_checkfornewdropdownvalues_12_14').parent }}" + str(uuid.uuid4())
            }
        )

        foreach_query_list_checkfornewdropdownvalues_12_14_end = rail.EmptyOperator(
            task_id='foreach_query_list_checkfornewdropdownvalues_12_14_end',
        )

        catch_16 = rail.EmptyOperator(
            task_id='catch_16',
            trigger_rule='one_failed',
        )

        stop_18 = rail.EmptyOperator(
            task_id='stop_18',

        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> create_list_3 >> if_request_grouptype_equals_to_location_4
        if_request_grouptype_equals_to_location_4 >> rail.Label(
            'Yes') >> get_data_location_list_service1_5 >> create_list_7 >> query_list_checkfornewdropdownvalues_8 >> \
            foreach_query_list_checkfornewdropdownvalues_8_9 >> create_location_or_apply_modification_10 >> \
            foreach_query_list_checkfornewdropdownvalues_8_9_end
        foreach_query_list_checkfornewdropdownvalues_8_9 >> foreach_query_list_checkfornewdropdownvalues_8_9_end >> \
            create_list_11 >> query_list_checkfornewdropdownvalues_12 >> if_query_list_checkfornewdropdownvalues_12_rows_greater_than_0_13
        if_query_list_checkfornewdropdownvalues_12_rows_greater_than_0_13 >> rail.Label(
            'Yes') >> foreach_query_list_checkfornewdropdownvalues_12_14 >> create_location_or_apply_modification_15 >> \
            foreach_query_list_checkfornewdropdownvalues_12_14_end
        foreach_query_list_checkfornewdropdownvalues_12_14 >> foreach_query_list_checkfornewdropdownvalues_12_14_end >> finish
        if_query_list_checkfornewdropdownvalues_12_rows_greater_than_0_13 >> rail.Label(
            'No') >> finish
        if_request_grouptype_equals_to_location_4 >> rail.Label(
            'No') >> catch_16 >> stop_18

    return dag


rail.for_each_instance(create_dag)
