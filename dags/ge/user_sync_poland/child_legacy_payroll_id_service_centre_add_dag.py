from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylnot: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_legacy_payroll_id_servicecenter_add_dag_id,
        description=f'GE POLAND User Import Add Legacy payroll ID Service Center Child',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config"
        )

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='query_inputfilerawdata_for_records'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='query_inputfilerawdata_for_records',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        query_inputfilerawdata_for_records = rail.QueryCollectionOperator(
            task_id='query_inputfilerawdata_for_records',
            name='valid_servicecenter_records_from_input',
            query="""SELECT * FROM inputfilerawdata WHERE NULLIF(HRMSSOID, '') IS NOT NULL AND NULLIF(LegacyPayrollID, '') IS NOT NULL"""
        )

        query_servicecenter_fullpath_from_valid_input_servicecenter_records = rail.QueryCollectionOperator(
            task_id='query_servicecenter_fullpath_from_valid_input_servicecenter_records',
            name='servicecentre_details_inputfile',
            query="""SELECT HRMSSOID, LegacyPayrollID, LegacyPayrollID || '/' || HRMSSOID AS fullpath_input FROM valid_servicecenter_records_from_input """
        )

        get_all_service_centers_7 = rail.RepliconServiceOperator(
            task_id='get_all_service_centers_7',
            endpoint="/services/ServiceCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000000",
                "columnUris": [
                    "urn:replicon:service-center-list-column:service-center",
                    "urn:replicon:service-center-list-column:effectively-enabled",
                    "urn:replicon:service-center-list-column:full-path"
                ],
                "sort": [],
                "filterExpression": null
            },
            data_handler=lambda response: list(map(lambda x: {
                'servicecentrename': x['cells'][0]['textValue'],
                'uri': x['cells'][0]['uri'],
                'fullpath': '/'.join(list(map(lambda c: c['textValue'], x['cells'][2]['cellCollection']))),
                'length': len(x['cells'][2]['cellCollection']),
                'status': x['cells'][1]['textValue']
            }, response['rows'])) if response['rows'] else []
        )

        create_collection_replicon_servicecenters = rail.CreateCollectionOperator(
            task_id='create_collection_replicon_servicecenters',
            source="{{ result('get_all_service_centers_7') | to_json }}",
            name="replicon_servicecenters",
        )

        query_get_servicecenters_not_present_in_replicon_18 = rail.QueryCollectionOperator(
            task_id='query_get_servicecenters_not_present_in_replicon_18',
            name='service_centers_to_create',
            query="""SELECT * FROM servicecentre_details_inputfile WHERE
                servicecentre_details_inputfile.fullpath_input NOT IN (SELECT DISTINCT fullpath FROM replicon_servicecenters) """
        )

        query_get_input_servicecenters_disabled_in_replicon_19 = rail.QueryCollectionOperator(
            task_id='query_get_input_servicecenters_disabled_in_replicon_19',
            name='service_centers_to_enable',
            query="""SELECT * FROM replicon_servicecenters WHERE replicon_servicecenters.status = 'False' AND 
                replicon_servicecenters.fullpath IN (SELECT DISTINCT fullpath_input FROM servicecentre_details_inputfile) """
        )

        trigger_dag_run_service_centre_add_child_22 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_service_centre_add_child_22',
            retries=0,
            items="{{ result('query_get_servicecenters_not_present_in_replicon_18') }}",
            trigger_dag_id=config.sub_child_legacy_payroll_id_servicecenter_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "servicecenter": item['HRMSSOID'],
                "legacypayrollid": item['LegacyPayrollID'],
                "parenturi": rail.find_first_by_attr_and_get_attr(rail.result('get_all_service_centers_7'), 'fullpath', item['LegacyPayrollID'], 'uri'),
                "type": "add",
                "servicecentreuri": ""
            }
        )

        wait_for_completion_trigger_dag_run_service_centre_add_child_22 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_service_centre_add_child_22',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_service_centre_add_child_22") }}'
        )

        trigger_dag_run_service_centre_add_enable_24 = rail.TriggerDagRunForEachItemOperator(
            task_id='trigger_dag_run_service_centre_add_enable_24',
            retries=0,
            items="{{ result('query_get_input_servicecenters_disabled_in_replicon_19') }}",
            trigger_dag_id=config.sub_child_legacy_payroll_id_servicecenter_add_dag_id,
            execution_timeout=timedelta(days=config.execution_timeout_days),
            accumulate_result=True,
            conf=lambda item: {
                "servicecenter": item['name'],
                "legacypayrollid": item['fullpath'].split('/')[0],
                "parenturi": '',
                "type": 'enable',
                "servicecentreuri": item['uri']
            }
        )

        wait_for_completion_trigger_dag_run_service_centre_add_enable_24 = rail.WaitForDagRunsSensor(
            task_id='wait_for_completion_trigger_dag_run_service_centre_add_enable_24',
            execution_timeout=timedelta(days=config.execution_timeout_days),
            dag_runs='{{ result("trigger_dag_run_service_centre_add_enable_24") }}'
        )

        finish = rail.EmptyOperator(
            task_id='finish'
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> query_inputfilerawdata_for_records

        query_inputfilerawdata_for_records >> query_servicecenter_fullpath_from_valid_input_servicecenter_records >> get_all_service_centers_7 \
            >> create_collection_replicon_servicecenters >> query_get_servicecenters_not_present_in_replicon_18 \
            >> query_get_input_servicecenters_disabled_in_replicon_19 >> trigger_dag_run_service_centre_add_child_22

        trigger_dag_run_service_centre_add_child_22 >> wait_for_completion_trigger_dag_run_service_centre_add_child_22 \
            >> trigger_dag_run_service_centre_add_enable_24 >> wait_for_completion_trigger_dag_run_service_centre_add_enable_24 >> finish

    return dag


rail.for_each_instance(create_dag)
