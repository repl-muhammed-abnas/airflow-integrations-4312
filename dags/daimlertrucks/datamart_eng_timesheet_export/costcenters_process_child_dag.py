
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'daimlertrucks_datamart_eng_timesheet_export_costcenter_process_child_{config.instance}',
        description=f'DTNA_Get the List of active costcenters to process {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='getallcostcenters_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='getallcostcenters_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        getallcostcenters_2 = rail.RepliconServiceOperator(
            task_id='getallcostcenters_2',
            endpoint="/services/CostCenterListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:cost-center-list-column:cost-center",
                    "urn:replicon:cost-center-list-column:full-path",
                    "urn:replicon:cost-center-list-column:effectively-enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:cost-center-list-filter:effectively-enabled"
                    },
                    "operatorUri": "urn:replicon:filter-operator:equal",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": "true",
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": null,
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        invoke_custom_ruby_code_3 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_3',
            python_callable=lambda: list(map(lambda x: {
                "name": x['cells'][0]['textValue'],
                "fullpath": "|".join(list(filter(lambda x: x, map(lambda y: y['textValue'], x['cells'][1]['cellCollection'])))),
                "uri": x['cells'][0]['uri'],
                "id": x['cells'][0]['uri'].split(":")[-1],
                "status": x['cells'][2]['textValue']
            }, rail.result('getallcostcenters_2')['rows']))
        )

        create_list_4 = rail.CreateCollectionOperator(
            task_id='create_list_4',
            source="{{ result('invoke_custom_ruby_code_3') | to_json }}",
            name="costcenterdata",
        )

        get_report_details_5 = rail.RepliconReportDetailsOperator(
            task_id='get_report_details_5',
            report_name="{{ dag_run.conf.report_name }}"
        )

        if_request_domain_equals_to_dtnaeng_6 = rail.IfOperator(
            task_id='if_request_domain_equals_to_dtnaeng_6',
            test='''{{ dag_run.conf.domain == 'DTNA ENG' }}''',
            yes_task="query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_7",
            no_task="if_request_domain_equals_to_all_12",
        )

        query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_7 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_7',
            query="""SELECT * FROM  costcenterdata WHERE ( costcenterdata.fullpath LIKE '%DTNA ENG' OR  costcenterdata.fullpath LIKE '%DTNA ENG%') AND  costcenterdata.status='True'""",
        )

        invoke_custom_ruby_code_8 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_8',
            python_callable=lambda: list(map(lambda x: {
                "value": x['id'],
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_5')['filterConfiguration']['enabledFilters'], 'displayText', "CostCenterFilter", 'uri')
            }, rail.load_all_records(rail.result('query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_7'))))
        )

        if_request_domain_equals_to_all_12 = rail.IfOperator(
            task_id='if_request_domain_equals_to_all_12',
            test='''{{ dag_run.conf.domain == 'ALL' }}''',
            yes_task="query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_a_n_d_d_t_n_a_i_t_13",
            no_task="if_request_domain_equals_to_dtnait_18",
        )

        query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_a_n_d_d_t_n_a_i_t_13 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_a_n_d_d_t_n_a_i_t_13',
            query="""SELECT * FROM  costcenterdata WHERE ( costcenterdata.fullpath LIKE '%DTNA ENG' OR  costcenterdata.fullpath LIKE '%DTNA ENG%' OR  costcenterdata.fullpath LIKE '%DTNA IT' OR  costcenterdata.fullpath LIKE '%DTNA IT%') AND  costcenterdata.status='True' AND  costcenterdata.name !='4164-5040' AND  costcenterdata.name !='4164-5094' AND  costcenterdata.name !='4164-5221' AND  costcenterdata.name !='4164-6022' AND  costcenterdata.name !='4164-6047' AND  costcenterdata.name !='4164-6210' AND  costcenterdata.name !='4605-5000' AND  costcenterdata.name !='4590-6190'""",
        )

        invoke_custom_ruby_code_14 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_14',
            python_callable=lambda: list(map(lambda x: {
                "value": x['id'],
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_5')['filterConfiguration']['enabledFilters'], 'displayText', "CostCenterFilter", 'uri')
            }, rail.load_all_records(rail.result('query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_a_n_d_d_t_n_a_i_t_13'))))
        )

        if_request_domain_equals_to_dtnait_18 = rail.IfOperator(
            task_id='if_request_domain_equals_to_dtnait_18',
            test='''{{ dag_run.conf.domain == 'DTNA IT' }}''',
            yes_task="query_list_getallcostcentersassociatedwith_d_t_n_a_i_t_19",
            no_task="finish",
        )

        query_list_getallcostcentersassociatedwith_d_t_n_a_i_t_19 = rail.QueryCollectionOperator(
            task_id='query_list_getallcostcentersassociatedwith_d_t_n_a_i_t_19',
            query="""SELECT * FROM  costcenterdata WHERE ( costcenterdata.fullpath LIKE '%DTNA IT' OR  costcenterdata.fullpath LIKE '%DTNA IT%') AND  costcenterdata.status='True' AND  costcenterdata.name !='4164-5040' AND  costcenterdata.name !='4164-5094' AND  costcenterdata.name !='4164-5221' AND  costcenterdata.name !='4164-6022' AND  costcenterdata.name !='4164-6047' AND  costcenterdata.name !='4164-6210' AND  costcenterdata.name !='4605-5000' AND  costcenterdata.name !='4590-6190'""",
        )

        invoke_custom_ruby_code_20 = rail.PythonOperator(
            task_id='invoke_custom_ruby_code_20',
            python_callable=lambda: list(map(lambda x: {
                "value": x['id'],
                "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_5')['filterConfiguration']['enabledFilters'], 'displayText', "CostCenterFilter", 'uri')
            }, rail.load_all_records(rail.result('query_list_getallcostcentersassociatedwith_d_t_n_a_i_t_19'))))
        )

        log_final_list = rail.PythonOperator(
            task_id='log_final_list',
            python_callable=lambda:  rail.result('invoke_custom_ruby_code_8') or rail.result(
                'invoke_custom_ruby_code_14') or rail.result('invoke_custom_ruby_code_20')
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> getallcostcenters_2
        getallcostcenters_2 >> invoke_custom_ruby_code_3 >> create_list_4 >> get_report_details_5 >> if_request_domain_equals_to_dtnaeng_6
        if_request_domain_equals_to_dtnaeng_6 >> rail.Label(
            'Yes') >> query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_7 >> invoke_custom_ruby_code_8 >> log_final_list >> finish
        if_request_domain_equals_to_dtnaeng_6 >> rail.Label(
            'No') >> if_request_domain_equals_to_all_12
        if_request_domain_equals_to_all_12 >> rail.Label(
            'Yes') >> query_list_getallcostcentersassociatedwith_d_t_n_a_e_n_g_a_n_d_d_t_n_a_i_t_13 >> invoke_custom_ruby_code_14 >> log_final_list >> finish
        if_request_domain_equals_to_all_12 >> rail.Label(
            'No') >> if_request_domain_equals_to_dtnait_18
        if_request_domain_equals_to_dtnait_18 >> rail.Label(
            'Yes') >> query_list_getallcostcentersassociatedwith_d_t_n_a_i_t_19 >> invoke_custom_ruby_code_20 >> log_final_list >> finish
        if_request_domain_equals_to_dtnait_18 >> rail.Label(
            'No') >> finish
        finish >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
