import json
import rail
from dxctechnology.report_batch_processing.utils import request_payload, custom_method, response_filter
from airflow.models import Variable
from datetime import timedelta

null=None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'dxc_report_processing_{config.instance}',
        description=f'DXC Report Processing {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=rail.WebhookConf(
            response_data_task_id='response_data_from_report_processing',
            bearer_token_var=config.bearer_token_var),
        max_active_runs=config.max_active_runs
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_response_variable'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_response_variable',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days)
        )

        declare_response_variable=rail.SetVariableOperator(
            task_id='declare_response_variable',
            append=False,
            name='response_data',
            value=None
        )

        if_request_requestor_blank_3=rail.IfOperator(
            task_id='if_request_requestor_blank_3',
            test= custom_method.mandatory_data_checks,
            yes_task="send_reply_4",
            no_task="if_requestor_downcase_not_equals_to_c1_6",
        )

        send_reply_4=rail.SetVariableOperator(
            task_id='send_reply_4',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value={
                "event": {
                    "eventid":"{{ dag_run_ecid() }}",
                    "status":"Error",
                    "message":"Requestor/Type/Start Date/End Date/Time Type is not present"
                    }
                }
        )

        if_requestor_downcase_not_equals_to_c1_6=rail.IfOperator(
            task_id='if_requestor_downcase_not_equals_to_c1_6',
            test= custom_method.valid_requestor_check,
            yes_task="send_reply_7",
            no_task="report_filter_list_9",
        )

        send_reply_7=rail.SetVariableOperator(
            task_id='send_reply_7',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda dag_run: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Error",
                    "message":"Requester is not allowed for " + str(dag_run.conf['webhook']['data']['requestor'])
                }
            }
        )

        report_filter_list_9=rail.SetVariableOperator(
            task_id='report_filter_list_9',
            append=False,
            name='reportfilter',
            value=None
        )

        if_first_value_present_11=rail.IfOperator(
            task_id='if_first_value_present_11',
            test= custom_method.valid_wbs_check,
            yes_task="bulk_get_projects_12",
            no_task="if_first_value_present_13",
        )

        bulk_get_projects_12=rail.RepliconServiceOperator(
            task_id='bulk_get_projects_12',
            endpoint="/services/ProjectService1.svc/BulkGetProjects2",
            data= lambda dag_run: {
                "projects": list(map(lambda item: { "name": item['value'] }, dag_run.conf['webhook']['data']['WBS']))
            }
        )

        if_first_value_present_13=rail.IfOperator(
            task_id='if_first_value_present_13',
            test=custom_method.valid_user_check,
            yes_task="user_list_14",
            no_task="if_first_value_present_21",
        )

        user_list_14=rail.SetVariableOperator(
            task_id='user_list_14',
            append=False,
            name='user',
            value=[]
        )

        foreach_request_15=rail.ForEachOperator(
            task_id='foreach_request_15',
            items="{{ dag_run.conf.webhook.data.users | to_json }}",
            start_task = 'get_datafor_userbasedonemployeeid_16',
            end_task = 'foreach_request_15_end'
        )

        get_datafor_userbasedonemployeeid_16=rail.RepliconServiceOperator(
            task_id='get_datafor_userbasedonemployeeid_16',
            endpoint="/services/UserListService1.svc/GetData",
            data=request_payload.get_user_list_payload
        )

        invoke_custom_ruby_code_17=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_17',
            python_callable= lambda: { "userlistoutput": list(filter(
                lambda item: item['validate'] == 'Yes',
                list(map(lambda item:{"name": item['cells'][0]['textValue'],
                    "employeeid": item['cells'][1]['textValue'],
                    "uri": item['cells'][0]['uri'],
                    "validate": "Yes" if item['cells'][1]['textValue'] ==  rail.result('foreach_request_15')['value'] else "No"
                    },
                    rail.result('get_datafor_userbasedonemployeeid_16')['rows']))
                    ))
                }
        )

        if_first_uri_present_18=rail.IfOperator(
            task_id='if_first_uri_present_18',
            test='''{{ result('invoke_custom_ruby_code_17').userlistoutput | is_truthy and \
                result('invoke_custom_ruby_code_17').userlistoutput[0].uri | is_truthy}}''',
            yes_task="insert_to_list_19",
            no_task="foreach_request_15_end",
        )

        insert_to_list_19=rail.SetVariableOperator(
            task_id='insert_to_list_19',
            append=True,
            name='{{ result("user_list_14").name }}',
            value=lambda:{
                "name": rail.result('invoke_custom_ruby_code_17')['userlistoutput'][0]['name'],
                "uri": rail.result('invoke_custom_ruby_code_17')['userlistoutput'][0]['uri'],
                "id": rail.result('invoke_custom_ruby_code_17')['userlistoutput'][0]['uri'].split(":")[-1],
                "employeeid": rail.result('invoke_custom_ruby_code_17')['userlistoutput'][0]['employeeid']
            }
        )

        foreach_request_15_end=rail.EmptyOperator(
            task_id='foreach_request_15_end',
        )

        get_user_list_value = rail.GetVariableOperator(
            task_id='get_user_list_value',
            name='{{ result("user_list_14").name }}'
        )

        invoke_custom_ruby_code_20=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_20',
            python_callable= lambda: { "userlist": list(map(lambda item:{
                "employeeid": item['employeeid'],
                "uri": item['uri'],
                "id": item['id'],
                "available": "Yes"
                },
                rail.result('get_user_list_value')['value'] ))
            }
        )

        if_first_value_present_21=rail.IfOperator(
            task_id='if_first_value_present_21',
            test= custom_method.valid_company_code_check,
            yes_task="get_all_divisions_22",
            no_task="if_first_value_present_23",
        )

        get_all_divisions_22=rail.RepliconServiceOperator(
            task_id='get_all_divisions_22',
            endpoint="/services/DivisionService1.svc/GetAllDivisions",
        )

        if_first_value_present_23=rail.IfOperator(
            task_id='if_first_value_present_23',
            test= custom_method.valid_clientid_check,
            yes_task="client_list_24",
            no_task="if_first_value_present_30",
        )

        client_list_24=rail.SetVariableOperator(
            task_id='client_list_24',
            append=False,
            name='client',
            value=[]
        )

        foreach_request_25=rail.ForEachOperator(
            task_id='foreach_request_25',
            items="{{ dag_run.conf.webhook.data.clientid | to_json }}",
            start_task = 'get_datafor_client_26',
            end_task = 'foreach_request_25_end'
        )

        get_datafor_client_26=rail.RepliconServiceOperator(
            task_id='get_datafor_client_26',
            endpoint="/services/ClientListService1.svc/GetData",
            data=request_payload.get_client_list_payload
        )

        invoke_custom_ruby_code_27=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_27',
            python_callable= lambda: { "clientlistoutput": list(filter(
                lambda item: item['validate'] == 'Yes',
                list(map(lambda item:{
                    "name": item['cells'][1]['textValue'],
                    "uri": item['cells'][0]['uri'],
                    "validate": "Yes" if item['cells'][1]['textValue'] ==  rail.result('foreach_request_25')['value'] else "No"
                    },
                    rail.result('get_datafor_client_26')['rows']))
                    ))
                }
        )

        if_first_uri_present_28=rail.IfOperator(
            task_id='if_first_uri_present_28',
            test='''{{ result('invoke_custom_ruby_code_27').clientlistoutput | is_truthy and \
                result('invoke_custom_ruby_code_27').clientlistoutput[0].uri | is_truthy }}''',
            yes_task="insert_to_list_29",
            no_task="foreach_request_25_end",
        )

        insert_to_list_29=rail.SetVariableOperator(
            task_id='insert_to_list_29',
            append=True,
            name='{{ result("client_list_24").name }}',
            value=lambda:{
                "name": rail.result('invoke_custom_ruby_code_27')['clientlistoutput'][0]['name'],
                "uri": rail.result('invoke_custom_ruby_code_27')['clientlistoutput'][0]['uri'],
                "id": rail.result('invoke_custom_ruby_code_27')['clientlistoutput'][0]['uri'].split(":")[-1],
            }
        )

        foreach_request_25_end=rail.EmptyOperator(
            task_id='foreach_request_25_end',
        )

        if_first_value_present_30=rail.IfOperator(
            task_id='if_first_value_present_30',
            test= custom_method.valid_program_check,
            yes_task="program_list_31",
            no_task="if_request_soldtoparty_present_36",
        )

        program_list_31=rail.SetVariableOperator(
            task_id='program_list_31',
            append=False,
            name='program',
            value=[]
        )

        foreach_request_32=rail.ForEachOperator(
            task_id='foreach_request_32',
            items="{{ dag_run.conf.webhook.data.program | to_json }}",
            start_task = 'search_program_in_replicon_33',
            end_task = 'foreach_request_32_end'
        )

        search_program_in_replicon_33 = rail.RepliconServiceOperator(
            task_id='search_program_in_replicon_33',
            endpoint='/services/ProgramListService1.svc/GetData',
            data=request_payload.get_program_list_search_param,
            response_filter=lambda response: response_filter.program_filter(response, rail.result('foreach_request_32')['value'])
        )

        if_first_uri_present_34=rail.IfOperator(
            task_id='if_first_uri_present_34',
            test='''{{ result('search_program_in_replicon_33') | is_truthy and \
                 result('search_program_in_replicon_33')[0].uri | is_truthy }}''',
            yes_task="insert_to_list_35",
            no_task="foreach_request_32_end",
        )

        insert_to_list_35=rail.SetVariableOperator(
            task_id='insert_to_list_35',
            append=True,
            name='{{ result("program_list_31").name }}',
            value=lambda: {
                "name": rail.result('search_program_in_replicon_33')[0]['name'],
                "uri": rail.result('search_program_in_replicon_33')[0]['uri'],
                "id": str(rail.result('search_program_in_replicon_33')[0]['uri']).split(":")[-1]
            }
        )

        foreach_request_32_end=rail.EmptyOperator(
            task_id='foreach_request_32_end',
        )

        if_request_soldtoparty_present_36=rail.IfOperator(
            task_id='if_request_soldtoparty_present_36',
            test= custom_method.valid_soldtoparty_check,
            yes_task="get_all_object_extension_field_details_37",
            no_task="if_requestor_downcase_equals_to_c1_40",
        )

        get_all_object_extension_field_details_37=rail.RepliconServiceOperator(
            task_id='get_all_object_extension_field_details_37',
            endpoint="/services/ObjectExtensionService1.svc/GetAllObjectExtensionFieldDetails",
            data={
                "bindingContextUri": "urn:replicon:object-type:project"
            },
            data_handler=lambda oefs: rail.find_first_by_attr_and_get_attr(oefs, 'name', 'Sold to Party', 'uri')

        )

        log_o_e_f_filter_38=rail.PythonOperator(
            task_id='log_o_e_f_filter_38',
            python_callable= lambda: "OEFilter_" + str(rail.smartjoin_by_delim(str(rail.result('get_all_object_extension_field_details_37')).split(":")[-1].split("-"), ""))
        )

        invoke_custom_ruby_code_39=rail.PythonOperator(
            task_id='invoke_custom_ruby_code_39',
            python_callable= lambda dag_run: { "output_list": list(map(lambda item:{
                    "oefvalue": item,
                    },
                    str(dag_run.conf['webhook']['data']['soldtoparty']).split(",")))
                }
        )

        if_requestor_downcase_equals_to_c1_40=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_c1_40',
            test='''{{ dag_run.conf.webhook.data.requestor.lower() =='c1' and dag_run.conf.webhook.data.timetype.lower() =='time-off' }}''',
            yes_task="get_report_details_41",
            no_task="if_requestor_downcase_equals_to_c1_49",
        )

        get_report_details_41=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_41',
            report_name='C1 API Time Off Report',
        )

        insert_to_list_batch_date_filter_42=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_42',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_41')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_43=rail.IfOperator(
            task_id='if_first_value_present_user_filter_43',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_44",
            no_task="get_report_filter_value_from_variable_44",
        )

        insert_to_list_batch_44=rail.SetVariableOperator(
            task_id='insert_to_list_batch_44',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_41')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_44 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_44',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_44 = rail.PythonOperator(
            task_id='get_report_filters_44',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_44').value | to_json }}"]
        )

        generate_reports_batch_45 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_45',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_41')['uri'],
                        "filterValues": rail.result('get_report_filters_44'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_46 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_46',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_45') }}"
            },
        )

        send_reply_47=rail.SetVariableOperator(
            task_id='send_reply_47',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_45')
                }
            }
        )

        if_requestor_downcase_equals_to_c1_49=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_c1_49',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='c1' and dag_run.conf.webhook.data.timetype.lower()=='time' }}''',
            yes_task="get_report_details_50",
            no_task="if_requestor_downcase_equals_to_compass_68",
        )

        get_report_details_50=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_50',
            report_name='C1 APi Report',
        )

        insert_to_list_batch_date_filter_51=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_51',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )

        )

        if_first_value_present_user_filter_52=rail.IfOperator(
            task_id='if_first_value_present_user_filter_52',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_53",
            no_task="if_first_value_present_project_w_b_s_filter_54",
        )

        insert_to_list_batch_53=rail.SetVariableOperator(
            task_id='insert_to_list_batch_53',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_54=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_54',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_55",
            no_task="if_first_value_present_company_code_filter_56",
        )

        insert_to_list_batch_55=rail.SetVariableOperator(
            task_id='insert_to_list_batch_55',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )

        if_first_value_present_company_code_filter_56=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_56',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_57",
            no_task="if_first_value_present_client_filter_58",
        )

        insert_to_list_batch_57=rail.SetVariableOperator(
            task_id='insert_to_list_batch_57',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_58=rail.IfOperator(
            task_id='if_first_value_present_client_filter_58',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_59",
            no_task="if_first_value_present_program_filter_60",
        )

        insert_to_list_batch_59=rail.SetVariableOperator(
            task_id='insert_to_list_batch_59',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )


        if_first_value_present_program_filter_60=rail.IfOperator(
            task_id='if_first_value_present_program_filter_60',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_61",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_62",
        )

        insert_to_list_batch_61=rail.SetVariableOperator(
            task_id='insert_to_list_batch_61',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_62=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_62',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_63",
            no_task="get_report_filter_value_from_variable_64",
        )

        insert_to_list_batch_63=rail.SetVariableOperator(
            task_id='insert_to_list_batch_63',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_50')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_64 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_64',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_64 = rail.PythonOperator(
            task_id='get_report_filters_64',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_64').value | to_json }}"]
        )

        generate_reports_batch_64 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_64',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_50')['uri'],
                        "filterValues": rail.result('get_report_filters_64'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_65 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_65',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_64') }}"
            },
        )

        send_reply_66=rail.SetVariableOperator(
            task_id='send_reply_66',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_64')
                }
            }
        )

        if_requestor_downcase_equals_to_compass_68=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compass_68',
            test='''{{ dag_run.conf.webhook.data.requestor.lower() =='compass' and dag_run.conf.webhook.data.timetype.lower() =='time-off' }}''',
            yes_task="get_report_details_69",
            no_task="if_requestor_downcase_equals_to_compassnt3_77",
        )

        get_report_details_69=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_69',
            report_name='Compass API Time Off Report',
        )

        insert_to_list_batch_date_filter_70=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_70',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_69')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_71=rail.IfOperator(
            task_id='if_first_value_present_user_filter_71',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_72",
            no_task="get_report_filter_value_from_variable_73",
        )

        insert_to_list_batch_72=rail.SetVariableOperator(
            task_id='insert_to_list_batch_72',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_69')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_73 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_73',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_73 = rail.PythonOperator(
            task_id='get_report_filters_73',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_73').value | to_json }}"]
        )

        generate_reports_batch_73 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_73',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_69')['uri'],
                        "filterValues": rail.result('get_report_filters_73'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_74 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_74',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_73') }}"
            },
        )

        send_reply_75=rail.SetVariableOperator(
            task_id='send_reply_75',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_73')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt3_77=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt3_77',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt3' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_78",
            no_task="if_requestor_downcase_equals_to_compasspj1_86",
        )

        get_report_details_78=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_78',
            report_name='Compass PJ1 API Time Off Report',
        )

        insert_to_list_batch_date_filter_79=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_79',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_78')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_80=rail.IfOperator(
            task_id='if_first_value_present_user_filter_80',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_81",
            no_task="get_report_filter_value_from_variable_82",
        )

        insert_to_list_batch_81=rail.SetVariableOperator(
            task_id='insert_to_list_batch_81',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_78')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_82 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_82',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_82 = rail.PythonOperator(
            task_id='get_report_filters_82',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_82').value | to_json }}"]
        )

        generate_reports_batch_82 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_82',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_78')['uri'],
                        "filterValues": rail.result('get_report_filters_82'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_83 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_83',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_82') }}"
            },
        )

        send_reply_84=rail.SetVariableOperator(
            task_id='send_reply_84',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_82')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspj1_86=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspj1_86',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pj1' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_87",
            no_task="if_requestor_downcase_equals_to_compassnt1_95",
        )

        get_report_details_87=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_87',
            report_name='Compass PJ1 API Time Off Report',
        )

        insert_to_list_batch_date_filter_88=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_88',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_87')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_89=rail.IfOperator(
            task_id='if_first_value_present_user_filter_89',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_90",
            no_task="get_report_filter_value_from_variable_91",
        )

        insert_to_list_batch_90=rail.SetVariableOperator(
            task_id='insert_to_list_batch_90',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_87')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_91 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_91',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_91 = rail.PythonOperator(
            task_id='get_report_filters_91',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_91').value | to_json }}"]
        )

        generate_reports_batch_91 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_91',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_87')['uri'],
                        "filterValues": rail.result('get_report_filters_91'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_92 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_92',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_91') }}"
            },
        )

        send_reply_93=rail.SetVariableOperator(
            task_id='send_reply_93',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_91')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt1_95=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt1_95',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt1' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_96",
            no_task="if_requestor_downcase_equals_to_compasspn1_104",
        )

        get_report_details_96=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_96',
            report_name='Compass PN1 API Time Off Report',
        )

        insert_to_list_batch_date_filter_97=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_97',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_96')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_98=rail.IfOperator(
            task_id='if_first_value_present_user_filter_98',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_99",
            no_task="get_report_filter_value_from_variable_100",
        )

        insert_to_list_batch_99=rail.SetVariableOperator(
            task_id='insert_to_list_batch_99',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_96')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_100 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_100',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_100 = rail.PythonOperator(
            task_id='get_report_filters_100',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_100').value | to_json }}"]
        )

        generate_reports_batch_100 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_100',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_96')['uri'],
                        "filterValues": rail.result('get_report_filters_100'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_101 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_101',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_100') }}"
            },
        )

        send_reply_102=rail.SetVariableOperator(
            task_id='send_reply_102',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_100')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspn1_104=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspn1_104',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pn1' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_105",
            no_task="if_requestor_downcase_equals_to_compassnt2_113",
        )


        get_report_details_105=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_105',
            report_name='Compass PN1 API Time Off Report',
        )

        insert_to_list_batch_date_filter_106=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_106',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_105')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )


        if_first_value_present_user_filter_107=rail.IfOperator(
            task_id='if_first_value_present_user_filter_107',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_108",
            no_task="get_report_filter_value_from_variable_109",
        )

        insert_to_list_batch_108=rail.SetVariableOperator(
            task_id='insert_to_list_batch_108',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_105')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_109 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_109',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_109 = rail.PythonOperator(
            task_id='get_report_filters_109',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_109').value | to_json }}"]
        )

        generate_reports_batch_109 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_109',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_105')['uri'],
                        "filterValues": rail.result('get_report_filters_109'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_110 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_110',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_109') }}"
            },
        )

        send_reply_111=rail.SetVariableOperator(
            task_id='send_reply_111',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_109')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt2_113=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt2_113',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt2' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_114",
            no_task="if_requestor_downcase_equals_to_compassp01_122",
        )


        get_report_details_114=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_114',
            report_name='Compass P01 API Time Off Report',
        )

        insert_to_list_batch_date_filter_115=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_115',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_114')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )


        if_first_value_present_user_filter_116=rail.IfOperator(
            task_id='if_first_value_present_user_filter_116',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_117",
            no_task="get_report_filter_value_from_variable_118",
        )

        insert_to_list_batch_117=rail.SetVariableOperator(
            task_id='insert_to_list_batch_117',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_114')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_118 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_118',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_118 = rail.PythonOperator(
            task_id='get_report_filters_118',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_118').value | to_json }}"]
        )

        generate_reports_batch_118 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_118',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_114')['uri'],
                        "filterValues": rail.result('get_report_filters_118'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_119 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_119',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_118') }}"
            },
        )

        send_reply_120=rail.SetVariableOperator(
            task_id='send_reply_120',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_118')
                }
            }
        )

        if_requestor_downcase_equals_to_compassp01_122=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassp01_122',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-p01' and dag_run.conf.webhook.data.timetype.lower()=='time-off' }}''',
            yes_task="get_report_details_123",
            no_task="if_requestor_downcase_equals_to_compass_131",
        )


        get_report_details_123=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_123',
            report_name='Compass P01 API Time Off Report',
        )

        insert_to_list_batch_date_filter_124=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_124',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_123')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'DateRangeFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_125=rail.IfOperator(
            task_id='if_first_value_present_user_filter_125',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_126",
            no_task="get_report_filter_value_from_variable_127",
        )

        insert_to_list_batch_126=rail.SetVariableOperator(
            task_id='insert_to_list_batch_126',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_123')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        get_report_filter_value_from_variable_127 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_127',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_127 = rail.PythonOperator(
            task_id='get_report_filters_127',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_127').value | to_json }}"]
        )

        generate_reports_batch_127 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_127',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_123')['uri'],
                        "filterValues": rail.result('get_report_filters_127'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_128 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_128',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_127') }}"
            },
        )

        send_reply_129=rail.SetVariableOperator(
            task_id='send_reply_129',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_127')
                }
            }
        )

        if_requestor_downcase_equals_to_compass_131=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compass_131',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass' and \
                        dag_run.conf.webhook.data.timetype.lower()=='time' and \
                            dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_132",
            no_task="if_requestor_downcase_equals_to_compassp01_150",
        )


        get_report_details_132=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_132',
            report_name='Compass Api Report',
        )

        insert_to_list_batch_date_filter_133=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_133',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_134=rail.IfOperator(
            task_id='if_first_value_present_user_filter_134',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_135",
            no_task="if_first_value_present_project_w_b_s_filter_136",
        )

        insert_to_list_batch_135=rail.SetVariableOperator(
            task_id='insert_to_list_batch_135',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_136=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_136',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_137",
            no_task="if_first_value_present_company_code_filter_138",
        )

        insert_to_list_batch_137=rail.SetVariableOperator(
            task_id='insert_to_list_batch_137',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_138=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_138',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_139",
            no_task="if_first_value_present_client_filter_140",
        )

        insert_to_list_batch_139=rail.SetVariableOperator(
            task_id='insert_to_list_batch_139',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )


        if_first_value_present_client_filter_140=rail.IfOperator(
            task_id='if_first_value_present_client_filter_140',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_141",
            no_task="if_first_value_present_program_filter_142",
        )

        insert_to_list_batch_141=rail.SetVariableOperator(
            task_id='insert_to_list_batch_141',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_142=rail.IfOperator(
            task_id='if_first_value_present_program_filter_142',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_143",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_144",
        )

        insert_to_list_batch_143=rail.SetVariableOperator(
            task_id='insert_to_list_batch_143',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )


        if_request_soldtoparty_present_soldto_party_o_e_f_filter_144=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_144',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_145",
            no_task="get_report_filter_value_from_variable_146",
        )

        insert_to_list_batch_145=rail.SetVariableOperator(
            task_id='insert_to_list_batch_145',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_132')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_146 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_146',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_146 = rail.PythonOperator(
            task_id='get_report_filters_146',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_146').value | to_json }}"]
        )

        generate_reports_batch_146 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_146',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_132')['uri'],
                        "filterValues": rail.result('get_report_filters_146'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_147 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_147',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_146') }}"
            },
        )

        send_reply_148=rail.SetVariableOperator(
            task_id='send_reply_148',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_146')
                }
            }
        )

        if_requestor_downcase_equals_to_compassp01_150=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassp01_150',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-p01' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_151",
            no_task="if_requestor_downcase_equals_to_compassnt2_169",
        )


        get_report_details_151=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_151',
            report_name='COMPASS P01 API Report - Full',
        )

        insert_to_list_batch_date_filter_152=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_152',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_153=rail.IfOperator(
            task_id='if_first_value_present_user_filter_153',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_154",
            no_task="if_first_value_present_project_w_b_s_filter_155",
        )

        insert_to_list_batch_154=rail.SetVariableOperator(
            task_id='insert_to_list_batch_154',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_155=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_155',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_156",
            no_task="if_first_value_present_company_code_filter_157",
        )

        insert_to_list_batch_156=rail.SetVariableOperator(
            task_id='insert_to_list_batch_156',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_157=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_157',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_158",
            no_task="if_first_value_present_client_filter_159",
        )

        insert_to_list_batch_158=rail.SetVariableOperator(
            task_id='insert_to_list_batch_158',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )


        if_first_value_present_client_filter_159=rail.IfOperator(
            task_id='if_first_value_present_client_filter_159',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_160",
            no_task="if_first_value_present_program_filter_161",
        )

        insert_to_list_batch_160=rail.SetVariableOperator(
            task_id='insert_to_list_batch_160',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_161=rail.IfOperator(
            task_id='if_first_value_present_program_filter_161',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_162",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_163",
        )

        insert_to_list_batch_162=rail.SetVariableOperator(
            task_id='insert_to_list_batch_162',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )


        if_request_soldtoparty_present_soldto_party_o_e_f_filter_163=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_163',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_164",
            no_task="get_report_filter_value_from_variable_165",
        )

        insert_to_list_batch_164=rail.SetVariableOperator(
            task_id='insert_to_list_batch_164',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_151')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_165 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_165',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_165 = rail.PythonOperator(
            task_id='get_report_filters_165',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_165').value | to_json }}"]
        )

        generate_reports_batch_165 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_165',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_151')['uri'],
                        "filterValues": rail.result('get_report_filters_165'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_166 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_166',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_165') }}"
            },
        )

        send_reply_167=rail.SetVariableOperator(
            task_id='send_reply_167',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_165')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt2_169=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt2_169',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt2' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_170",
            no_task="if_requestor_downcase_equals_to_compassnt3_188",
        )

        get_report_details_170=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_170',
            report_name='COMPASS P01 API Report - Full',
        )

        insert_to_list_batch_date_filter_171=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_171',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_172=rail.IfOperator(
            task_id='if_first_value_present_user_filter_172',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_173",
            no_task="if_first_value_present_project_w_b_s_filter_174",
        )

        insert_to_list_batch_173=rail.SetVariableOperator(
            task_id='insert_to_list_batch_173',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_174=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_174',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_175",
            no_task="if_first_value_present_company_code_filter_176",
        )

        insert_to_list_batch_175=rail.SetVariableOperator(
            task_id='insert_to_list_batch_175',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_176=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_176',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_177",
            no_task="if_first_value_present_client_filter_178",
        )

        insert_to_list_batch_177=rail.SetVariableOperator(
            task_id='insert_to_list_batch_177',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )


        if_first_value_present_client_filter_178=rail.IfOperator(
            task_id='if_first_value_present_client_filter_178',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_179",
            no_task="if_first_value_present_program_filter_180",
        )

        insert_to_list_batch_179=rail.SetVariableOperator(
            task_id='insert_to_list_batch_179',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_180=rail.IfOperator(
            task_id='if_first_value_present_program_filter_180',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_181",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_182",
        )

        insert_to_list_batch_181=rail.SetVariableOperator(
            task_id='insert_to_list_batch_181',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )


        if_request_soldtoparty_present_soldto_party_o_e_f_filter_182=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_182',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_183",
            no_task="get_report_filter_value_from_variable_184",
        )

        insert_to_list_batch_183=rail.SetVariableOperator(
            task_id='insert_to_list_batch_183',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_170')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_184 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_184',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_184 = rail.PythonOperator(
            task_id='get_report_filters_184',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_184').value | to_json }}"]
        )

        generate_reports_batch_184 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_184',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_170')['uri'],
                        "filterValues": rail.result('get_report_filters_184'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_185 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_185',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_184') }}"
            },
        )

        send_reply_186=rail.SetVariableOperator(
            task_id='send_reply_186',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_184')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt3_188=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt3_188',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt3' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_189",
            no_task="if_requestor_downcase_equals_to_compasspj1_207",
        )


        get_report_details_189=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_189',
            report_name='COMPASS PJ1 API Report - Full',
        )

        insert_to_list_batch_date_filter_190=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_190',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_191=rail.IfOperator(
            task_id='if_first_value_present_user_filter_191',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_192",
            no_task="if_first_value_present_project_w_b_s_filter_193",
        )

        insert_to_list_batch_192=rail.SetVariableOperator(
            task_id='insert_to_list_batch_192',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_193=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_193',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_194",
            no_task="if_first_value_present_company_code_filter_195",
        )

        insert_to_list_batch_194=rail.SetVariableOperator(
            task_id='insert_to_list_batch_194',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_195=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_195',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_196",
            no_task="if_first_value_present_client_filter_197",
        )

        insert_to_list_batch_196=rail.SetVariableOperator(
            task_id='insert_to_list_batch_196',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )


        if_first_value_present_client_filter_197=rail.IfOperator(
            task_id='if_first_value_present_client_filter_197',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_198",
            no_task="if_first_value_present_program_filter_199",
        )

        insert_to_list_batch_198=rail.SetVariableOperator(
            task_id='insert_to_list_batch_198',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_199=rail.IfOperator(
            task_id='if_first_value_present_program_filter_199',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_200",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_201",
        )

        insert_to_list_batch_200=rail.SetVariableOperator(
            task_id='insert_to_list_batch_200',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )


        if_request_soldtoparty_present_soldto_party_o_e_f_filter_201=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_201',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_202",
            no_task="get_report_filter_value_from_variable_203",
        )

        insert_to_list_batch_202=rail.SetVariableOperator(
            task_id='insert_to_list_batch_202',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_189')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_203 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_203',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_203 = rail.PythonOperator(
            task_id='get_report_filters_203',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_203').value | to_json }}"]
        )

        generate_reports_batch_203 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_203',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_189')['uri'],
                        "filterValues": rail.result('get_report_filters_203'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_204 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_204',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_203') }}"
            },
        )

        send_reply_205=rail.SetVariableOperator(
            task_id='send_reply_205',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_203')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspj1_207=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspj1_207',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pj1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_208",
            no_task="if_requestor_downcase_equals_to_compassnt1_226",
        )


        get_report_details_208=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_208',
            report_name='COMPASS PJ1 API Report - Full',
        )

        insert_to_list_batch_date_filter_209=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_209',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_210=rail.IfOperator(
            task_id='if_first_value_present_user_filter_210',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_211",
            no_task="if_first_value_present_project_w_b_s_filter_212",
        )

        insert_to_list_batch_211=rail.SetVariableOperator(
            task_id='insert_to_list_batch_211',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_212=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_212',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_213",
            no_task="if_first_value_present_company_code_filter_214",
        )

        insert_to_list_batch_213=rail.SetVariableOperator(
            task_id='insert_to_list_batch_213',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_214=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_214',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_215",
            no_task="if_first_value_present_client_filter_216",
        )

        insert_to_list_batch_215=rail.SetVariableOperator(
            task_id='insert_to_list_batch_215',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_216=rail.IfOperator(
            task_id='if_first_value_present_client_filter_216',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_217",
            no_task="if_first_value_present_program_filter_218",
        )

        insert_to_list_batch_217=rail.SetVariableOperator(
            task_id='insert_to_list_batch_217',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_218=rail.IfOperator(
            task_id='if_first_value_present_program_filter_218',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_219",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_220",
        )

        insert_to_list_batch_219=rail.SetVariableOperator(
            task_id='insert_to_list_batch_219',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_220=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_220',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_221",
            no_task="get_report_filter_value_from_variable_222",
        )

        insert_to_list_batch_221=rail.SetVariableOperator(
            task_id='insert_to_list_batch_221',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_208')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_222 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_222',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_222 = rail.PythonOperator(
            task_id='get_report_filters_222',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_222').value | to_json }}"]
        )

        generate_reports_batch_222 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_222',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_208')['uri'],
                        "filterValues": rail.result('get_report_filters_222'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_223 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_223',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_222') }}"
            },
        )

        send_reply_224=rail.SetVariableOperator(
            task_id='send_reply_224',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_222')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt1_226=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt1_226',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_227",
            no_task="if_requestor_downcase_equals_to_compasspn1_245",
        )


        get_report_details_227=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_227',
            report_name='COMPASS PN1 API Report - Full',
        )

        insert_to_list_batch_date_filter_228=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_228',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_229=rail.IfOperator(
            task_id='if_first_value_present_user_filter_229',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_230",
            no_task="if_first_value_present_project_w_b_s_filter_231",
        )

        insert_to_list_batch_230=rail.SetVariableOperator(
            task_id='insert_to_list_batch_230',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_231=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_231',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_232",
            no_task="if_first_value_present_company_code_filter_233",
        )

        insert_to_list_batch_232=rail.SetVariableOperator(
            task_id='insert_to_list_batch_232',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_233=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_233',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_234",
            no_task="if_first_value_present_client_filter_235",
        )

        insert_to_list_batch_234=rail.SetVariableOperator(
            task_id='insert_to_list_batch_234',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_235=rail.IfOperator(
            task_id='if_first_value_present_client_filter_235',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_236",
            no_task="if_first_value_present_program_filter_237",
        )

        insert_to_list_batch_236=rail.SetVariableOperator(
            task_id='insert_to_list_batch_236',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_237=rail.IfOperator(
            task_id='if_first_value_present_program_filter_237',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_238",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_239",
        )

        insert_to_list_batch_238=rail.SetVariableOperator(
            task_id='insert_to_list_batch_238',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_239=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_239',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_240",
            no_task="get_report_filter_value_from_variable_241",
        )

        insert_to_list_batch_240=rail.SetVariableOperator(
            task_id='insert_to_list_batch_240',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_227')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_241 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_241',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_241 = rail.PythonOperator(
            task_id='get_report_filters_241',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_241').value | to_json }}"]
        )

        generate_reports_batch_241 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_241',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_227')['uri'],
                        "filterValues": rail.result('get_report_filters_241'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_242 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_242',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_241') }}"
            },
        )

        send_reply_243=rail.SetVariableOperator(
            task_id='send_reply_243',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_241')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspn1_245=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspn1_245',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pn1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='full' }}''',
            yes_task="get_report_details_246",
            no_task="if_requestor_downcase_equals_to_compass_264",
        )


        get_report_details_246=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_246',
            report_name='COMPASS PN1 API Report - Full',
        )

        insert_to_list_batch_date_filter_247=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_247',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_248=rail.IfOperator(
            task_id='if_first_value_present_user_filter_248',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_249",
            no_task="if_first_value_present_project_w_b_s_filter_250",
        )

        insert_to_list_batch_249=rail.SetVariableOperator(
            task_id='insert_to_list_batch_249',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_250=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_250',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_251",
            no_task="if_first_value_present_company_code_filter_252",
        )

        insert_to_list_batch_251=rail.SetVariableOperator(
            task_id='insert_to_list_batch_251',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_252=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_252',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_253",
            no_task="if_first_value_present_client_filter_254",
        )

        insert_to_list_batch_253=rail.SetVariableOperator(
            task_id='insert_to_list_batch_253',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_254=rail.IfOperator(
            task_id='if_first_value_present_client_filter_254',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_255",
            no_task="if_first_value_present_program_filter_256",
        )

        insert_to_list_batch_255=rail.SetVariableOperator(
            task_id='insert_to_list_batch_255',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_256=rail.IfOperator(
            task_id='if_first_value_present_program_filter_256',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_257",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_258",
        )

        insert_to_list_batch_257=rail.SetVariableOperator(
            task_id='insert_to_list_batch_257',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_258=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_258',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_259",
            no_task="get_report_filter_value_from_variable_260",
        )

        insert_to_list_batch_259=rail.SetVariableOperator(
            task_id='insert_to_list_batch_259',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_246')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_260 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_260',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_260 = rail.PythonOperator(
            task_id='get_report_filters_260',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_260').value | to_json }}"]
        )

        generate_reports_batch_260 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_260',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_246')['uri'],
                        "filterValues": rail.result('get_report_filters_260'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_261 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_261',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_260') }}"
            },
        )

        send_reply_262=rail.SetVariableOperator(
            task_id='send_reply_262',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_260')
                }
            }
        )

        if_requestor_downcase_equals_to_compass_264=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compass_264',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_265",
            no_task="if_requestor_downcase_equals_to_compassnt2_283",
        )


        get_report_details_265=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_265',
            report_name='Compass API Report- Limited',
        )

        insert_to_list_batch_date_filter_266=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_266',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_267=rail.IfOperator(
            task_id='if_first_value_present_user_filter_267',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_268",
            no_task="if_first_value_present_project_w_b_s_filter_269",
        )

        insert_to_list_batch_268=rail.SetVariableOperator(
            task_id='insert_to_list_batch_268',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_269=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_269',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_270",
            no_task="if_first_value_present_company_code_filter_271",
        )

        insert_to_list_batch_270=rail.SetVariableOperator(
            task_id='insert_to_list_batch_270',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_271=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_271',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_272",
            no_task="if_first_value_present_client_filter_273",
        )

        insert_to_list_batch_272=rail.SetVariableOperator(
            task_id='insert_to_list_batch_272',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_273=rail.IfOperator(
            task_id='if_first_value_present_client_filter_273',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_274",
            no_task="if_first_value_present_program_filter_275",
        )

        insert_to_list_batch_274=rail.SetVariableOperator(
            task_id='insert_to_list_batch_274',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_275=rail.IfOperator(
            task_id='if_first_value_present_program_filter_275',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_276",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_277",
        )

        insert_to_list_batch_276=rail.SetVariableOperator(
            task_id='insert_to_list_batch_276',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_277=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_277',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_278",
            no_task="get_report_filter_value_from_variable_279",
        )

        insert_to_list_batch_278=rail.SetVariableOperator(
            task_id='insert_to_list_batch_278',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_265')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_279 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_279',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_279 = rail.PythonOperator(
            task_id='get_report_filters_279',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_279').value | to_json }}"]
        )

        generate_reports_batch_279 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_279',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_265')['uri'],
                        "filterValues": rail.result('get_report_filters_279'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_280 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_280',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_279') }}"
            },
        )

        send_reply_281=rail.SetVariableOperator(
            task_id='send_reply_281',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_279')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt2_283=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt2_283',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt2' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_284",
            no_task="if_requestor_downcase_equals_to_compassp01_302",
        )


        get_report_details_284=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_284',
            report_name='COMPASS P01 API Report - Limited',
        )

        insert_to_list_batch_date_filter_285=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_285',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_286=rail.IfOperator(
            task_id='if_first_value_present_user_filter_286',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_287",
            no_task="if_first_value_present_project_w_b_s_filter_288",
        )

        insert_to_list_batch_287=rail.SetVariableOperator(
            task_id='insert_to_list_batch_287',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_288=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_288',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_289",
            no_task="if_first_value_present_company_code_filter_290",
        )

        insert_to_list_batch_289=rail.SetVariableOperator(
            task_id='insert_to_list_batch_289',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_290=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_290',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_291",
            no_task="if_first_value_present_client_filter_292",
        )

        insert_to_list_batch_291=rail.SetVariableOperator(
            task_id='insert_to_list_batch_291',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_292=rail.IfOperator(
            task_id='if_first_value_present_client_filter_292',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_293",
            no_task="if_first_value_present_program_filter_294",
        )

        insert_to_list_batch_293=rail.SetVariableOperator(
            task_id='insert_to_list_batch_293',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_294=rail.IfOperator(
            task_id='if_first_value_present_program_filter_294',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_295",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_296",
        )

        insert_to_list_batch_295=rail.SetVariableOperator(
            task_id='insert_to_list_batch_295',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_296=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_296',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_297",
            no_task="get_report_filter_value_from_variable_298",
        )

        insert_to_list_batch_297=rail.SetVariableOperator(
            task_id='insert_to_list_batch_297',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_284')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_298 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_298',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_298 = rail.PythonOperator(
            task_id='get_report_filters_298',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_298').value | to_json }}"]
        )

        generate_reports_batch_298 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_298',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_284')['uri'],
                        "filterValues": rail.result('get_report_filters_298'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_299 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_299',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_298') }}"
            },
        )

        send_reply_300=rail.SetVariableOperator(
            task_id='send_reply_300',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_298')
                }
            }
        )

        if_requestor_downcase_equals_to_compassp01_302=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassp01_302',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-p01' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_303",
            no_task="if_requestor_downcase_equals_to_compassnt3_321",
        )


        get_report_details_303=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_303',
            report_name='COMPASS P01 API Report - Limited',
        )

        insert_to_list_batch_date_filter_304=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_304',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_305=rail.IfOperator(
            task_id='if_first_value_present_user_filter_305',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_306",
            no_task="if_first_value_present_project_w_b_s_filter_307",
        )

        insert_to_list_batch_306=rail.SetVariableOperator(
            task_id='insert_to_list_batch_306',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_307=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_307',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_308",
            no_task="if_first_value_present_company_code_filter_309",
        )

        insert_to_list_batch_308=rail.SetVariableOperator(
            task_id='insert_to_list_batch_308',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_309=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_309',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_310",
            no_task="if_first_value_present_client_filter_311",
        )

        insert_to_list_batch_310=rail.SetVariableOperator(
            task_id='insert_to_list_batch_310',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_311=rail.IfOperator(
            task_id='if_first_value_present_client_filter_311',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_312",
            no_task="if_first_value_present_program_filter_313",
        )

        insert_to_list_batch_312=rail.SetVariableOperator(
            task_id='insert_to_list_batch_312',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_313=rail.IfOperator(
            task_id='if_first_value_present_program_filter_313',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_314",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_315",
        )

        insert_to_list_batch_314=rail.SetVariableOperator(
            task_id='insert_to_list_batch_314',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_315=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_315',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_316",
            no_task="get_report_filter_value_from_variable_317",
        )

        insert_to_list_batch_316=rail.SetVariableOperator(
            task_id='insert_to_list_batch_316',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_303')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_317 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_317',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_317 = rail.PythonOperator(
            task_id='get_report_filters_317',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_317').value | to_json }}"]
        )

        generate_reports_batch_317 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_317',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_303')['uri'],
                        "filterValues": rail.result('get_report_filters_317'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_318 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_318',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_317') }}"
            },
        )

        send_reply_319=rail.SetVariableOperator(
            task_id='send_reply_319',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_317')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt3_321=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt3_321',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt3' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_322",
            no_task="if_requestor_downcase_equals_to_compasspj1_340",
        )


        get_report_details_322=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_322',
            report_name='COMPASS PJ1 API Report - Limited',
        )

        insert_to_list_batch_date_filter_323=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_323',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_324=rail.IfOperator(
            task_id='if_first_value_present_user_filter_324',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_325",
            no_task="if_first_value_present_project_w_b_s_filter_326",
        )

        insert_to_list_batch_325=rail.SetVariableOperator(
            task_id='insert_to_list_batch_325',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_326=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_326',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_327",
            no_task="if_first_value_present_company_code_filter_328",
        )

        insert_to_list_batch_327=rail.SetVariableOperator(
            task_id='insert_to_list_batch_327',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_328=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_328',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_329",
            no_task="if_first_value_present_client_filter_330",
        )

        insert_to_list_batch_329=rail.SetVariableOperator(
            task_id='insert_to_list_batch_329',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_330=rail.IfOperator(
            task_id='if_first_value_present_client_filter_330',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_331",
            no_task="if_first_value_present_program_filter_332",
        )

        insert_to_list_batch_331=rail.SetVariableOperator(
            task_id='insert_to_list_batch_331',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_332=rail.IfOperator(
            task_id='if_first_value_present_program_filter_332',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_333",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_334",
        )

        insert_to_list_batch_333=rail.SetVariableOperator(
            task_id='insert_to_list_batch_333',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_334=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_334',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_335",
            no_task="get_report_filter_value_from_variable_336",
        )

        insert_to_list_batch_335=rail.SetVariableOperator(
            task_id='insert_to_list_batch_335',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_322')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_336 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_336',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_336 = rail.PythonOperator(
            task_id='get_report_filters_336',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_336').value | to_json }}"]
        )

        generate_reports_batch_336 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_336',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_322')['uri'],
                        "filterValues": rail.result('get_report_filters_336'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_337 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_337',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_336') }}"
            },
        )

        send_reply_338=rail.SetVariableOperator(
            task_id='send_reply_338',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_336')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspj1_340=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspj1_340',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pj1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_341",
            no_task="if_requestor_downcase_equals_to_compassnt1_359",
        )


        get_report_details_341=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_341',
            report_name='COMPASS PJ1 API Report - Limited',
        )

        insert_to_list_batch_date_filter_342=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_342',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_343=rail.IfOperator(
            task_id='if_first_value_present_user_filter_343',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_344",
            no_task="if_first_value_present_project_w_b_s_filter_345",
        )

        insert_to_list_batch_344=rail.SetVariableOperator(
            task_id='insert_to_list_batch_344',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_345=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_345',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_346",
            no_task="if_first_value_present_company_code_filter_347",
        )

        insert_to_list_batch_346=rail.SetVariableOperator(
            task_id='insert_to_list_batch_346',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_347=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_347',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_348",
            no_task="if_first_value_present_client_filter_349",
        )

        insert_to_list_batch_348=rail.SetVariableOperator(
            task_id='insert_to_list_batch_348',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_349=rail.IfOperator(
            task_id='if_first_value_present_client_filter_349',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_350",
            no_task="if_first_value_present_program_filter_351",
        )

        insert_to_list_batch_350=rail.SetVariableOperator(
            task_id='insert_to_list_batch_350',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_351=rail.IfOperator(
            task_id='if_first_value_present_program_filter_351',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_352",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_353",
        )

        insert_to_list_batch_352=rail.SetVariableOperator(
            task_id='insert_to_list_batch_352',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_353=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_353',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_354",
            no_task="get_report_filter_value_from_variable_355",
        )

        insert_to_list_batch_354=rail.SetVariableOperator(
            task_id='insert_to_list_batch_354',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_341')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_355 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_355',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_355 = rail.PythonOperator(
            task_id='get_report_filters_355',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_355').value | to_json }}"]
        )

        generate_reports_batch_355 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_355',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_341')['uri'],
                        "filterValues": rail.result('get_report_filters_355'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_356 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_356',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_355') }}"
            },
        )

        send_reply_357=rail.SetVariableOperator(
            task_id='send_reply_357',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_355')
                }
            }
        )

        if_requestor_downcase_equals_to_compassnt1_359=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compassnt1_359',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-nt1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_360",
            no_task="if_requestor_downcase_equals_to_compasspn1_378",
        )


        get_report_details_360=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_360',
            report_name='COMPASS PN1 API Report - Limited',
        )

        insert_to_list_batch_date_filter_361=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_361',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_362=rail.IfOperator(
            task_id='if_first_value_present_user_filter_362',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_363",
            no_task="if_first_value_present_project_w_b_s_filter_364",
        )

        insert_to_list_batch_363=rail.SetVariableOperator(
            task_id='insert_to_list_batch_363',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_364=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_364',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_365",
            no_task="if_first_value_present_company_code_filter_366",
        )

        insert_to_list_batch_365=rail.SetVariableOperator(
            task_id='insert_to_list_batch_365',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_366=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_366',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_367",
            no_task="if_first_value_present_client_filter_368",
        )

        insert_to_list_batch_367=rail.SetVariableOperator(
            task_id='insert_to_list_batch_367',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_368=rail.IfOperator(
            task_id='if_first_value_present_client_filter_368',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_369",
            no_task="if_first_value_present_program_filter_370",
        )

        insert_to_list_batch_369=rail.SetVariableOperator(
            task_id='insert_to_list_batch_369',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_370=rail.IfOperator(
            task_id='if_first_value_present_program_filter_370',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_371",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_372",
        )

        insert_to_list_batch_371=rail.SetVariableOperator(
            task_id='insert_to_list_batch_371',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_372=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_372',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_373",
            no_task="get_report_filter_value_from_variable_374",
        )

        insert_to_list_batch_373=rail.SetVariableOperator(
            task_id='insert_to_list_batch_373',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_360')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_374 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_374',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_374 = rail.PythonOperator(
            task_id='get_report_filters_374',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_374').value | to_json }}"]
        )

        generate_reports_batch_374 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_374',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_360')['uri'],
                        "filterValues": rail.result('get_report_filters_374'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_375 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_375',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_374') }}"
            },
        )

        send_reply_376=rail.SetVariableOperator(
            task_id='send_reply_376',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_374')
                }
            }
        )

        if_requestor_downcase_equals_to_compasspn1_378=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_compasspn1_378',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='compass-pn1' and \
                dag_run.conf.webhook.data.timetype.lower()=='time' and \
                    dag_run.conf.webhook.data.reportType.lower()=='limited' }}''',
            yes_task="get_report_details_379",
            no_task="if_requestor_downcase_equals_to_ftp_396",
        )


        get_report_details_379=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_379',
            report_name='COMPASS PN1 API Report - Limited',
        )

        insert_to_list_batch_date_filter_380=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_380',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )
        )

        if_first_value_present_user_filter_381=rail.IfOperator(
            task_id='if_first_value_present_user_filter_381',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_382",
            no_task="if_first_value_present_project_w_b_s_filter_383",
        )

        insert_to_list_batch_382=rail.SetVariableOperator(
            task_id='insert_to_list_batch_382',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_383=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_383',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_384",
            no_task="if_first_value_present_company_code_filter_385",
        )

        insert_to_list_batch_384=rail.SetVariableOperator(
            task_id='insert_to_list_batch_384',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )


        if_first_value_present_company_code_filter_385=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_385',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_386",
            no_task="if_first_value_present_client_filter_387",
        )

        insert_to_list_batch_386=rail.SetVariableOperator(
            task_id='insert_to_list_batch_386',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_387=rail.IfOperator(
            task_id='if_first_value_present_client_filter_387',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_388",
            no_task="if_first_value_present_program_filter_389",
        )

        insert_to_list_batch_388=rail.SetVariableOperator(
            task_id='insert_to_list_batch_388',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )

        if_first_value_present_program_filter_389=rail.IfOperator(
            task_id='if_first_value_present_program_filter_389',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_390",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_391",
        )

        insert_to_list_batch_390=rail.SetVariableOperator(
            task_id='insert_to_list_batch_390',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_391=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_391',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_392",
            no_task="get_report_filter_value_from_variable_393",
        )

        insert_to_list_batch_392=rail.SetVariableOperator(
            task_id='insert_to_list_batch_392',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_379')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_393 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_393',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_393 = rail.PythonOperator(
            task_id='get_report_filters_393',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_393').value | to_json }}"]
        )

        generate_reports_batch_393 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_393',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_379')['uri'],
                        "filterValues": rail.result('get_report_filters_393'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_394 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_394',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_393') }}"
            },
        )

        send_reply_395=rail.SetVariableOperator(
            task_id='send_reply_395',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_393')
                }
            }
        )

        if_requestor_downcase_equals_to_ftp_396=rail.IfOperator(
            task_id='if_requestor_downcase_equals_to_ftp_396',
            test='''{{ dag_run.conf.webhook.data.requestor.lower()=='ftp' }}''',
            yes_task="get_report_details_397",
            no_task="catch_416",
        )

        get_report_details_397=rail.RepliconReportDetailsOperator(
            task_id='get_report_details_397',
            report_name='FTP API Report',
        )

        insert_to_list_batch_date_filter_398=rail.SetVariableOperator(
            task_id='insert_to_list_batch_date_filter_398',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: custom_method.get_report_filter(rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'EntryDateFilter',
                                                                        'uri'),
                                                                        dag_run.conf['webhook']['data']['dateRange']["startDate"],
                                                                        dag_run.conf['webhook']['data']['dateRange']["endDate"]
                                                                        )

        )

        if_first_value_present_user_filter_399=rail.IfOperator(
            task_id='if_first_value_present_user_filter_399',
            test= custom_method.valid_user_check,
            yes_task="insert_to_list_batch_400",
            no_task="if_first_value_present_project_w_b_s_filter_401",
        )

        insert_to_list_batch_400=rail.SetVariableOperator(
            task_id='insert_to_list_batch_400',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'UserFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('invoke_custom_ruby_code_20')['userlist']))
        )

        if_first_value_present_project_w_b_s_filter_401=rail.IfOperator(
            task_id='if_first_value_present_project_w_b_s_filter_401',
            test= custom_method.valid_wbs_check,
            yes_task="insert_to_list_batch_402",
            no_task="if_first_value_present_company_code_filter_403",
        )

        insert_to_list_batch_402=rail.SetVariableOperator(
            task_id='insert_to_list_batch_402',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProjectFilter',
                                                                        'uri'),
                    "value": str(item['uri']).split(":")[-1]
                    },
                    rail.result('bulk_get_projects_12')))
        )

        if_first_value_present_company_code_filter_403=rail.IfOperator(
            task_id='if_first_value_present_company_code_filter_403',
            test= custom_method.valid_company_code_check,
            yes_task="insert_to_list_batch_404",
            no_task="if_first_value_present_client_filter_405",
        )

        insert_to_list_batch_404=rail.SetVariableOperator(
            task_id='insert_to_list_batch_404',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda dag_run: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'CurrentDivisionFilter',
                                                                        'uri'),
                    "value": str(rail.find_first_by_attr_and_get_attr(rail.result('get_all_divisions_22'), 'displayText',
                                                                        item['value'],
                                                                        'uri')).split(':')[-1]
                    },
                    dag_run.conf['webhook']['data']['companyCode']))
        )

        if_first_value_present_client_filter_405=rail.IfOperator(
            task_id='if_first_value_present_client_filter_405',
            test= custom_method.valid_clientid_check,
            yes_task="insert_to_list_batch_406",
            no_task="if_first_value_present_program_filter_407",
        )

        insert_to_list_batch_406=rail.SetVariableOperator(
            task_id='insert_to_list_batch_406',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ClientFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('client_list_24')['value']))
        )


        if_first_value_present_program_filter_407=rail.IfOperator(
            task_id='if_first_value_present_program_filter_407',
            test= custom_method.valid_program_check,
            yes_task="insert_to_list_batch_408",
            no_task="if_request_soldtoparty_present_soldto_party_o_e_f_filter_409",
        )

        insert_to_list_batch_408=rail.SetVariableOperator(
            task_id='insert_to_list_batch_408',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        'ProgramFilter',
                                                                        'uri'),
                    "value": item['id']
                    },
                    rail.result('program_list_31')['value']))
        )

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_409=rail.IfOperator(
            task_id='if_request_soldtoparty_present_soldto_party_o_e_f_filter_409',
            test= custom_method.valid_soldtoparty_check,
            yes_task="insert_to_list_batch_410",
            no_task="get_report_filter_value_from_variable_411",
        )

        insert_to_list_batch_410=rail.SetVariableOperator(
            task_id='insert_to_list_batch_410',
            append=True,
            name='{{ result("report_filter_list_9").name }}',
            value=lambda: list(map(lambda item:{
                    "reportFilterUri": rail.find_first_by_attr_and_get_attr(rail.result('get_report_details_397')['filterConfiguration']['enabledFilters'],
                                                                        'displayText',
                                                                        rail.result('log_o_e_f_filter_38'),
                                                                        'uri'),
                    "value": item['oefvalue']
                    },
                    rail.result('invoke_custom_ruby_code_39')['output_list']))
        )

        get_report_filter_value_from_variable_411 = rail.GetVariableOperator(
            task_id='get_report_filter_value_from_variable_411',
            name='{{ result("report_filter_list_9").name }}'
        )

        get_report_filters_412 = rail.PythonOperator(
            task_id='get_report_filters_412',
            python_callable=custom_method.get_filter_fields,
            op_args=["{{ result('get_report_filter_value_from_variable_411').value | to_json }}"]
        )

        generate_reports_batch_413 = rail.RepliconServiceOperator(
            task_id='generate_reports_batch_413',
            endpoint="/services/ReportService1.svc/CreateReportGenerationBatch",
            data=lambda: {"reportParameters": [
                    {
                        "reportUri": rail.result('get_report_details_397')['uri'],
                        "filterValues": rail.result('get_report_filters_412'),
                        "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
            ]}
        )

        execute_batch_report_414 = rail.RepliconServiceOperator(
            task_id='execute_batch_report_414',
            endpoint='/services/BatchManagementService1.svc/ExecuteInBackground',
            data={
                "batchUri": "{{ result('generate_reports_batch_413') }}"
            },
        )

        send_reply_415=rail.SetVariableOperator(
            task_id='send_reply_415',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Success",
                    "message":rail.result('generate_reports_batch_413')
                }
            }
        )

        catch_416=rail.EmptyOperator(
            task_id='catch_416',
            trigger_rule='one_failed',
        )

        send_reply_417=rail.SetVariableOperator(
            task_id='send_reply_417',
            append=False,
            name='{{ result("declare_response_variable").name }}',
            value=lambda: {
                "event": {
                    "eventid":rail.render_template("{{dag_run_ecid()}}"),
                    "status":"Error",
                    "message":rail.render_template("{{get_error_message()}}")
                }
            }
        )

        get_response_data = rail.GetVariableOperator(
            task_id='get_response_data',
            name='{{ result("declare_response_variable").name }}'
        )

        response_data_from_report_processing=rail.PythonOperator(
            task_id='response_data_from_report_processing',
            python_callable=lambda: json.dumps(rail.result('get_response_data')['value'],
                 ensure_ascii=False)
        )

        finish=rail.EmptyOperator(
            task_id='finish',
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish

        can_run_batch_task >> rail.Label('No') >> declare_response_variable

        declare_response_variable >> if_request_requestor_blank_3

        if_request_requestor_blank_3 >> rail.Label('Yes')  >> send_reply_4 >> get_response_data
        if_request_requestor_blank_3 >> rail.Label('No') >> if_requestor_downcase_not_equals_to_c1_6

        if_requestor_downcase_not_equals_to_c1_6 >> rail.Label('Yes')  >> send_reply_7 >> get_response_data
        if_requestor_downcase_not_equals_to_c1_6 >> rail.Label('No') >> report_filter_list_9 >> if_first_value_present_11

        if_first_value_present_11 >> rail.Label('Yes')  >> bulk_get_projects_12 >> if_first_value_present_13
        if_first_value_present_11 >> rail.Label('No') >> if_first_value_present_13

        if_first_value_present_13 >> rail.Label('Yes')  >> user_list_14 >> foreach_request_15 >> get_datafor_userbasedonemployeeid_16 >> invoke_custom_ruby_code_17 >> if_first_uri_present_18
        if_first_uri_present_18 >> rail.Label('Yes')  >> insert_to_list_19 >> foreach_request_15_end
        if_first_uri_present_18 >> rail.Label('No') >> foreach_request_15_end

        foreach_request_15 >> foreach_request_15_end >> get_user_list_value >> invoke_custom_ruby_code_20 >> if_first_value_present_21

        if_first_value_present_13 >> rail.Label('No') >> if_first_value_present_21

        if_first_value_present_21 >> rail.Label('Yes')  >> get_all_divisions_22 >> if_first_value_present_23
        if_first_value_present_21 >> rail.Label('No') >> if_first_value_present_23

        if_first_value_present_23 >> rail.Label('Yes')  >> client_list_24 >> foreach_request_25 >> get_datafor_client_26 >> invoke_custom_ruby_code_27 >> if_first_uri_present_28

        if_first_uri_present_28 >> rail.Label('Yes')  >> insert_to_list_29 >> foreach_request_25_end
        if_first_uri_present_28 >> rail.Label('No') >> foreach_request_25_end

        foreach_request_25 >> foreach_request_25_end >> if_first_value_present_30

        if_first_value_present_23 >> rail.Label('No') >> if_first_value_present_30

        if_first_value_present_30 >> rail.Label('Yes')  >> program_list_31 >> foreach_request_32 >> search_program_in_replicon_33 >> if_first_uri_present_34

        if_first_uri_present_34 >> rail.Label('Yes')  >> insert_to_list_35 >> foreach_request_32_end
        if_first_uri_present_34 >> rail.Label('No') >> foreach_request_32_end

        foreach_request_32 >> foreach_request_32_end >> if_request_soldtoparty_present_36

        if_first_value_present_30 >> rail.Label('No') >> if_request_soldtoparty_present_36

        if_request_soldtoparty_present_36 >> rail.Label('Yes')  >> get_all_object_extension_field_details_37 >> log_o_e_f_filter_38 >> invoke_custom_ruby_code_39 >> if_requestor_downcase_equals_to_c1_40
        if_request_soldtoparty_present_36 >> rail.Label('No') >> if_requestor_downcase_equals_to_c1_40

        if_requestor_downcase_equals_to_c1_40 >> rail.Label('Yes')  >> get_report_details_41 >> insert_to_list_batch_date_filter_42 >> if_first_value_present_user_filter_43

        if_first_value_present_user_filter_43 >> rail.Label('Yes')  >> insert_to_list_batch_44 >> get_report_filter_value_from_variable_44
        if_first_value_present_user_filter_43 >> rail.Label('No') >> get_report_filter_value_from_variable_44

        get_report_filter_value_from_variable_44 >> get_report_filters_44 >> generate_reports_batch_45

        generate_reports_batch_45 >> execute_batch_report_46 >> send_reply_47 >> get_response_data

        if_requestor_downcase_equals_to_c1_40 >> rail.Label('No') >> if_requestor_downcase_equals_to_c1_49

        if_requestor_downcase_equals_to_c1_49 >> rail.Label('Yes')  >> get_report_details_50 >> insert_to_list_batch_date_filter_51 >> if_first_value_present_user_filter_52

        if_first_value_present_user_filter_52 >> rail.Label('Yes')  >> insert_to_list_batch_53 >> if_first_value_present_project_w_b_s_filter_54
        if_first_value_present_user_filter_52 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_54

        if_first_value_present_project_w_b_s_filter_54 >> rail.Label('Yes')  >> insert_to_list_batch_55 >> if_first_value_present_company_code_filter_56
        if_first_value_present_project_w_b_s_filter_54 >> rail.Label('No') >> if_first_value_present_company_code_filter_56

        if_first_value_present_company_code_filter_56 >> rail.Label('Yes')  >> insert_to_list_batch_57 >> if_first_value_present_client_filter_58
        if_first_value_present_company_code_filter_56 >> rail.Label('No') >> if_first_value_present_client_filter_58

        if_first_value_present_client_filter_58 >> rail.Label('Yes')  >> insert_to_list_batch_59 >> if_first_value_present_program_filter_60
        if_first_value_present_client_filter_58 >> rail.Label('No') >> if_first_value_present_program_filter_60

        if_first_value_present_program_filter_60 >> rail.Label('Yes')  >> insert_to_list_batch_61 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_62
        if_first_value_present_program_filter_60 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_62

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_62 >> rail.Label('Yes')  >> insert_to_list_batch_63 >> get_report_filter_value_from_variable_64
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_62 >> rail.Label('No') >> get_report_filter_value_from_variable_64
        get_report_filter_value_from_variable_64  >> get_report_filters_64 >> generate_reports_batch_64

        generate_reports_batch_64 >> execute_batch_report_65 >> send_reply_66 >> get_response_data

        if_requestor_downcase_equals_to_c1_49 >> rail.Label('No') >> if_requestor_downcase_equals_to_compass_68

        if_requestor_downcase_equals_to_compass_68 >> rail.Label('Yes')  >> get_report_details_69 >> insert_to_list_batch_date_filter_70 >> if_first_value_present_user_filter_71

        if_first_value_present_user_filter_71 >> rail.Label('Yes')  >> insert_to_list_batch_72 >> get_report_filter_value_from_variable_73
        if_first_value_present_user_filter_71 >> rail.Label('No') >> get_report_filter_value_from_variable_73
        get_report_filter_value_from_variable_73  >> get_report_filters_73 >> generate_reports_batch_73

        generate_reports_batch_73 >> execute_batch_report_74 >> send_reply_75 >> get_response_data

        if_requestor_downcase_equals_to_compass_68 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt3_77

        if_requestor_downcase_equals_to_compassnt3_77 >> rail.Label('Yes')  >> get_report_details_78 >> insert_to_list_batch_date_filter_79 >> if_first_value_present_user_filter_80
        if_first_value_present_user_filter_80 >> rail.Label('Yes')  >> insert_to_list_batch_81 >> get_report_filter_value_from_variable_82
        if_first_value_present_user_filter_80 >> rail.Label('No') >> get_report_filter_value_from_variable_82

        get_report_filter_value_from_variable_82 >> get_report_filters_82 >> generate_reports_batch_82

        generate_reports_batch_82 >> execute_batch_report_83 >> send_reply_84 >> get_response_data

        if_requestor_downcase_equals_to_compassnt3_77 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspj1_86

        if_requestor_downcase_equals_to_compasspj1_86 >> rail.Label('Yes')  >> get_report_details_87 >> insert_to_list_batch_date_filter_88 >> if_first_value_present_user_filter_89
        if_first_value_present_user_filter_89 >> rail.Label('Yes')  >> insert_to_list_batch_90 >> get_report_filter_value_from_variable_91
        if_first_value_present_user_filter_89 >> rail.Label('No') >> get_report_filter_value_from_variable_91

        get_report_filter_value_from_variable_91 >> get_report_filters_91 >> generate_reports_batch_91

        generate_reports_batch_91 >> execute_batch_report_92 >> send_reply_93 >> get_response_data

        if_requestor_downcase_equals_to_compasspj1_86 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt1_95
        if_requestor_downcase_equals_to_compassnt1_95 >> rail.Label('Yes')  >> get_report_details_96 >> insert_to_list_batch_date_filter_97 >> if_first_value_present_user_filter_98

        if_first_value_present_user_filter_98 >> rail.Label('Yes')  >> insert_to_list_batch_99 >> get_report_filter_value_from_variable_100
        if_first_value_present_user_filter_98 >> rail.Label('No') >> get_report_filter_value_from_variable_100

        get_report_filter_value_from_variable_100 >> get_report_filters_100 >> generate_reports_batch_100

        generate_reports_batch_100 >> execute_batch_report_101 >> send_reply_102 >> get_response_data

        if_requestor_downcase_equals_to_compassnt1_95 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspn1_104

        if_requestor_downcase_equals_to_compasspn1_104 >> rail.Label('Yes')  >> get_report_details_105 >> insert_to_list_batch_date_filter_106 >> if_first_value_present_user_filter_107

        if_first_value_present_user_filter_107 >> rail.Label('Yes')  >> insert_to_list_batch_108 >> get_report_filter_value_from_variable_109
        if_first_value_present_user_filter_107 >> rail.Label('No') >> get_report_filter_value_from_variable_109

        get_report_filter_value_from_variable_109 >> get_report_filters_109 >> generate_reports_batch_109

        generate_reports_batch_109 >> execute_batch_report_110 >> send_reply_111 >> get_response_data

        if_requestor_downcase_equals_to_compasspn1_104 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt2_113

        if_requestor_downcase_equals_to_compassnt2_113 >> rail.Label('Yes')  >> get_report_details_114 >> insert_to_list_batch_date_filter_115 >> if_first_value_present_user_filter_116

        if_first_value_present_user_filter_116 >> rail.Label('Yes')  >> insert_to_list_batch_117 >> get_report_filter_value_from_variable_118
        if_first_value_present_user_filter_116 >> rail.Label('No') >> get_report_filter_value_from_variable_118

        get_report_filter_value_from_variable_118 >> get_report_filters_118 >> generate_reports_batch_118

        generate_reports_batch_118 >> execute_batch_report_119 >> send_reply_120 >> get_response_data

        if_requestor_downcase_equals_to_compassnt2_113 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassp01_122
        if_requestor_downcase_equals_to_compassp01_122 >> rail.Label('Yes')  >> get_report_details_123 >> insert_to_list_batch_date_filter_124 >> if_first_value_present_user_filter_125

        if_first_value_present_user_filter_125 >> rail.Label('Yes')  >> insert_to_list_batch_126 >> get_report_filter_value_from_variable_127
        if_first_value_present_user_filter_125 >> rail.Label('No') >> get_report_filter_value_from_variable_127

        get_report_filter_value_from_variable_127 >> get_report_filters_127 >> generate_reports_batch_127

        generate_reports_batch_127 >> execute_batch_report_128 >> send_reply_129 >> get_response_data

        if_requestor_downcase_equals_to_compassp01_122 >> rail.Label('No') >> if_requestor_downcase_equals_to_compass_131

        if_requestor_downcase_equals_to_compass_131 >> rail.Label('Yes')  >> get_report_details_132 >> insert_to_list_batch_date_filter_133 >> if_first_value_present_user_filter_134

        if_first_value_present_user_filter_134 >> rail.Label('Yes')  >> insert_to_list_batch_135 >> if_first_value_present_project_w_b_s_filter_136
        if_first_value_present_user_filter_134 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_136

        if_first_value_present_project_w_b_s_filter_136 >> rail.Label('Yes')  >> insert_to_list_batch_137 >> if_first_value_present_company_code_filter_138
        if_first_value_present_project_w_b_s_filter_136 >> rail.Label('No') >> if_first_value_present_company_code_filter_138

        if_first_value_present_company_code_filter_138 >> rail.Label('Yes')  >> insert_to_list_batch_139 >> if_first_value_present_client_filter_140
        if_first_value_present_company_code_filter_138 >> rail.Label('No') >> if_first_value_present_client_filter_140

        if_first_value_present_client_filter_140 >> rail.Label('Yes')  >> insert_to_list_batch_141 >> if_first_value_present_program_filter_142
        if_first_value_present_client_filter_140 >> rail.Label('No') >> if_first_value_present_program_filter_142

        if_first_value_present_program_filter_142 >> rail.Label('Yes')  >> insert_to_list_batch_143 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_144
        if_first_value_present_program_filter_142 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_144

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_144 >> rail.Label('Yes')  >> insert_to_list_batch_145 >> get_report_filter_value_from_variable_146
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_144 >> rail.Label('No') >> get_report_filter_value_from_variable_146

        get_report_filter_value_from_variable_146 >> get_report_filters_146 >> generate_reports_batch_146

        generate_reports_batch_146 >> execute_batch_report_147 >> send_reply_148 >> get_response_data

        if_requestor_downcase_equals_to_compass_131 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassp01_150

        if_requestor_downcase_equals_to_compassp01_150 >> rail.Label('Yes')  >> get_report_details_151 >> insert_to_list_batch_date_filter_152 >> if_first_value_present_user_filter_153

        if_first_value_present_user_filter_153 >> rail.Label('Yes')  >> insert_to_list_batch_154 >> if_first_value_present_project_w_b_s_filter_155
        if_first_value_present_user_filter_153 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_155

        if_first_value_present_project_w_b_s_filter_155 >> rail.Label('Yes')  >> insert_to_list_batch_156 >> if_first_value_present_company_code_filter_157
        if_first_value_present_project_w_b_s_filter_155 >> rail.Label('No') >> if_first_value_present_company_code_filter_157

        if_first_value_present_company_code_filter_157 >> rail.Label('Yes')  >> insert_to_list_batch_158 >> if_first_value_present_client_filter_159
        if_first_value_present_company_code_filter_157 >> rail.Label('No') >> if_first_value_present_client_filter_159

        if_first_value_present_client_filter_159 >> rail.Label('Yes')  >> insert_to_list_batch_160 >> if_first_value_present_program_filter_161
        if_first_value_present_client_filter_159 >> rail.Label('No') >> if_first_value_present_program_filter_161

        if_first_value_present_program_filter_161 >> rail.Label('Yes')  >> insert_to_list_batch_162 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_163
        if_first_value_present_program_filter_161 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_163

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_163 >> rail.Label('Yes')  >> insert_to_list_batch_164 >> get_report_filter_value_from_variable_165
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_163 >> rail.Label('No') >> get_report_filter_value_from_variable_165

        get_report_filter_value_from_variable_165 >> get_report_filters_165 >> generate_reports_batch_165

        generate_reports_batch_165 >> execute_batch_report_166 >> send_reply_167 >> get_response_data

        if_requestor_downcase_equals_to_compassp01_150 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt2_169

        if_requestor_downcase_equals_to_compassnt2_169 >> rail.Label('Yes')  >> get_report_details_170 >> insert_to_list_batch_date_filter_171 >> if_first_value_present_user_filter_172

        if_first_value_present_user_filter_172 >> rail.Label('Yes')  >> insert_to_list_batch_173 >> if_first_value_present_project_w_b_s_filter_174
        if_first_value_present_user_filter_172 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_174

        if_first_value_present_project_w_b_s_filter_174 >> rail.Label('Yes')  >> insert_to_list_batch_175 >> if_first_value_present_company_code_filter_176
        if_first_value_present_project_w_b_s_filter_174 >> rail.Label('No') >> if_first_value_present_company_code_filter_176

        if_first_value_present_company_code_filter_176 >> rail.Label('Yes')  >> insert_to_list_batch_177 >> if_first_value_present_client_filter_178
        if_first_value_present_company_code_filter_176 >> rail.Label('No') >> if_first_value_present_client_filter_178

        if_first_value_present_client_filter_178 >> rail.Label('Yes')  >> insert_to_list_batch_179 >> if_first_value_present_program_filter_180
        if_first_value_present_client_filter_178 >> rail.Label('No') >> if_first_value_present_program_filter_180

        if_first_value_present_program_filter_180 >> rail.Label('Yes')  >> insert_to_list_batch_181 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_182
        if_first_value_present_program_filter_180 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_182

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_182 >> rail.Label('Yes')  >> insert_to_list_batch_183 >> get_report_filter_value_from_variable_184
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_182 >> rail.Label('No') >> get_report_filter_value_from_variable_184

        get_report_filter_value_from_variable_184 >> get_report_filters_184 >> generate_reports_batch_184

        generate_reports_batch_184 >> execute_batch_report_185 >> send_reply_186 >> get_response_data

        if_requestor_downcase_equals_to_compassnt2_169 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt3_188

        if_requestor_downcase_equals_to_compassnt3_188 >> rail.Label('Yes')  >> get_report_details_189 >> insert_to_list_batch_date_filter_190 >> if_first_value_present_user_filter_191

        if_first_value_present_user_filter_191 >> rail.Label('Yes')  >> insert_to_list_batch_192 >> if_first_value_present_project_w_b_s_filter_193
        if_first_value_present_user_filter_191 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_193

        if_first_value_present_project_w_b_s_filter_193 >> rail.Label('Yes')  >> insert_to_list_batch_194 >> if_first_value_present_company_code_filter_195
        if_first_value_present_project_w_b_s_filter_193 >> rail.Label('No') >> if_first_value_present_company_code_filter_195

        if_first_value_present_company_code_filter_195 >> rail.Label('Yes')  >> insert_to_list_batch_196 >> if_first_value_present_client_filter_197
        if_first_value_present_company_code_filter_195 >> rail.Label('No') >> if_first_value_present_client_filter_197

        if_first_value_present_client_filter_197 >> rail.Label('Yes')  >> insert_to_list_batch_198 >> if_first_value_present_program_filter_199
        if_first_value_present_client_filter_197 >> rail.Label('No') >> if_first_value_present_program_filter_199

        if_first_value_present_program_filter_199 >> rail.Label('Yes')  >> insert_to_list_batch_200 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_201
        if_first_value_present_program_filter_199 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_201

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_201 >> rail.Label('Yes')  >> insert_to_list_batch_202 >> get_report_filter_value_from_variable_203
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_201 >> rail.Label('No') >> get_report_filter_value_from_variable_203

        get_report_filter_value_from_variable_203 >> get_report_filters_203 >> generate_reports_batch_203

        generate_reports_batch_203 >> execute_batch_report_204 >> send_reply_205 >> get_response_data

        if_requestor_downcase_equals_to_compassnt3_188 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspj1_207

        if_requestor_downcase_equals_to_compasspj1_207 >> rail.Label('Yes')  >> get_report_details_208 >> insert_to_list_batch_date_filter_209 >> if_first_value_present_user_filter_210

        if_first_value_present_user_filter_210 >> rail.Label('Yes')  >> insert_to_list_batch_211 >> if_first_value_present_project_w_b_s_filter_212
        if_first_value_present_user_filter_210 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_212

        if_first_value_present_project_w_b_s_filter_212 >> rail.Label('Yes')  >> insert_to_list_batch_213 >> if_first_value_present_company_code_filter_214
        if_first_value_present_project_w_b_s_filter_212 >> rail.Label('No') >> if_first_value_present_company_code_filter_214

        if_first_value_present_company_code_filter_214 >> rail.Label('Yes')  >> insert_to_list_batch_215 >> if_first_value_present_client_filter_216
        if_first_value_present_company_code_filter_214 >> rail.Label('No') >> if_first_value_present_client_filter_216

        if_first_value_present_client_filter_216 >> rail.Label('Yes')  >> insert_to_list_batch_217 >> if_first_value_present_program_filter_218
        if_first_value_present_client_filter_216 >> rail.Label('No') >> if_first_value_present_program_filter_218

        if_first_value_present_program_filter_218 >> rail.Label('Yes')  >> insert_to_list_batch_219 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_220
        if_first_value_present_program_filter_218 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_220

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_220 >> rail.Label('Yes')  >> insert_to_list_batch_221 >> get_report_filter_value_from_variable_222
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_220 >> rail.Label('No') >> get_report_filter_value_from_variable_222

        get_report_filter_value_from_variable_222 >> get_report_filters_222 >> generate_reports_batch_222

        generate_reports_batch_222 >> execute_batch_report_223 >> send_reply_224 >> get_response_data

        if_requestor_downcase_equals_to_compasspj1_207 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt1_226

        if_requestor_downcase_equals_to_compassnt1_226 >> rail.Label('Yes')  >> get_report_details_227 >> insert_to_list_batch_date_filter_228 >> if_first_value_present_user_filter_229

        if_first_value_present_user_filter_229 >> rail.Label('Yes')  >> insert_to_list_batch_230 >> if_first_value_present_project_w_b_s_filter_231
        if_first_value_present_user_filter_229 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_231

        if_first_value_present_project_w_b_s_filter_231 >> rail.Label('Yes')  >> insert_to_list_batch_232 >> if_first_value_present_company_code_filter_233
        if_first_value_present_project_w_b_s_filter_231 >> rail.Label('No') >> if_first_value_present_company_code_filter_233

        if_first_value_present_company_code_filter_233 >> rail.Label('Yes')  >> insert_to_list_batch_234 >> if_first_value_present_client_filter_235
        if_first_value_present_company_code_filter_233 >> rail.Label('No') >> if_first_value_present_client_filter_235

        if_first_value_present_client_filter_235 >> rail.Label('Yes')  >> insert_to_list_batch_236 >> if_first_value_present_program_filter_237
        if_first_value_present_client_filter_235 >> rail.Label('No') >> if_first_value_present_program_filter_237

        if_first_value_present_program_filter_237 >> rail.Label('Yes')  >> insert_to_list_batch_238 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_239
        if_first_value_present_program_filter_237 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_239

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_239 >> rail.Label('Yes')  >> insert_to_list_batch_240 >> get_report_filter_value_from_variable_241
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_239 >> rail.Label('No') >> get_report_filter_value_from_variable_241

        get_report_filter_value_from_variable_241  >> get_report_filters_241 >> generate_reports_batch_241

        generate_reports_batch_241 >> execute_batch_report_242 >> send_reply_243 >> get_response_data

        if_requestor_downcase_equals_to_compassnt1_226 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspn1_245

        if_requestor_downcase_equals_to_compasspn1_245 >> rail.Label('Yes')  >> get_report_details_246 >> insert_to_list_batch_date_filter_247 >> if_first_value_present_user_filter_248

        if_first_value_present_user_filter_248 >> rail.Label('Yes')  >> insert_to_list_batch_249 >> if_first_value_present_project_w_b_s_filter_250
        if_first_value_present_user_filter_248 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_250

        if_first_value_present_project_w_b_s_filter_250 >> rail.Label('Yes')  >> insert_to_list_batch_251 >> if_first_value_present_company_code_filter_252
        if_first_value_present_project_w_b_s_filter_250 >> rail.Label('No') >> if_first_value_present_company_code_filter_252

        if_first_value_present_company_code_filter_252 >> rail.Label('Yes')  >> insert_to_list_batch_253 >> if_first_value_present_client_filter_254
        if_first_value_present_company_code_filter_252 >> rail.Label('No') >> if_first_value_present_client_filter_254

        if_first_value_present_client_filter_254 >> rail.Label('Yes')  >> insert_to_list_batch_255 >> if_first_value_present_program_filter_256
        if_first_value_present_client_filter_254 >> rail.Label('No') >> if_first_value_present_program_filter_256

        if_first_value_present_program_filter_256 >> rail.Label('Yes')  >> insert_to_list_batch_257 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_258
        if_first_value_present_program_filter_256 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_258

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_258 >> rail.Label('Yes')  >> insert_to_list_batch_259 >> get_report_filter_value_from_variable_260
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_258 >> rail.Label('No') >> get_report_filter_value_from_variable_260

        get_report_filter_value_from_variable_260 >> get_report_filters_260 >> generate_reports_batch_260

        generate_reports_batch_260 >> execute_batch_report_261 >> send_reply_262 >> get_response_data

        if_requestor_downcase_equals_to_compasspn1_245 >> rail.Label('No') >> if_requestor_downcase_equals_to_compass_264

        if_requestor_downcase_equals_to_compass_264 >> rail.Label('Yes')  >> get_report_details_265 >> insert_to_list_batch_date_filter_266 >> if_first_value_present_user_filter_267

        if_first_value_present_user_filter_267 >> rail.Label('Yes')  >> insert_to_list_batch_268 >> if_first_value_present_project_w_b_s_filter_269
        if_first_value_present_user_filter_267 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_269

        if_first_value_present_project_w_b_s_filter_269 >> rail.Label('Yes')  >> insert_to_list_batch_270 >> if_first_value_present_company_code_filter_271
        if_first_value_present_project_w_b_s_filter_269 >> rail.Label('No') >> if_first_value_present_company_code_filter_271

        if_first_value_present_company_code_filter_271 >> rail.Label('Yes')  >> insert_to_list_batch_272 >> if_first_value_present_client_filter_273
        if_first_value_present_company_code_filter_271 >> rail.Label('No') >> if_first_value_present_client_filter_273

        if_first_value_present_client_filter_273 >> rail.Label('Yes')  >> insert_to_list_batch_274 >> if_first_value_present_program_filter_275
        if_first_value_present_client_filter_273 >> rail.Label('No') >> if_first_value_present_program_filter_275

        if_first_value_present_program_filter_275 >> rail.Label('Yes')  >> insert_to_list_batch_276 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_277
        if_first_value_present_program_filter_275 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_277

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_277 >> rail.Label('Yes')  >> insert_to_list_batch_278 >> get_report_filter_value_from_variable_279
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_277 >> rail.Label('No') >> get_report_filter_value_from_variable_279
        get_report_filter_value_from_variable_279 >> get_report_filters_279 >> generate_reports_batch_279

        generate_reports_batch_279 >> execute_batch_report_280 >> send_reply_281 >> get_response_data

        if_requestor_downcase_equals_to_compass_264 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt2_283

        if_requestor_downcase_equals_to_compassnt2_283 >> rail.Label('Yes')  >> get_report_details_284 >> insert_to_list_batch_date_filter_285 >> if_first_value_present_user_filter_286

        if_first_value_present_user_filter_286 >> rail.Label('Yes')  >> insert_to_list_batch_287 >> if_first_value_present_project_w_b_s_filter_288
        if_first_value_present_user_filter_286 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_288

        if_first_value_present_project_w_b_s_filter_288 >> rail.Label('Yes')  >> insert_to_list_batch_289 >> if_first_value_present_company_code_filter_290
        if_first_value_present_project_w_b_s_filter_288 >> rail.Label('No') >> if_first_value_present_company_code_filter_290

        if_first_value_present_company_code_filter_290 >> rail.Label('Yes')  >> insert_to_list_batch_291 >> if_first_value_present_client_filter_292
        if_first_value_present_company_code_filter_290 >> rail.Label('No') >> if_first_value_present_client_filter_292

        if_first_value_present_client_filter_292 >> rail.Label('Yes')  >> insert_to_list_batch_293 >> if_first_value_present_program_filter_294
        if_first_value_present_client_filter_292 >> rail.Label('No') >> if_first_value_present_program_filter_294

        if_first_value_present_program_filter_294 >> rail.Label('Yes')  >> insert_to_list_batch_295 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_296
        if_first_value_present_program_filter_294 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_296

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_296 >> rail.Label('Yes')  >> insert_to_list_batch_297 >> get_report_filter_value_from_variable_298
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_296 >> rail.Label('No') >> get_report_filter_value_from_variable_298

        get_report_filter_value_from_variable_298 >> get_report_filters_298 >> generate_reports_batch_298

        generate_reports_batch_298 >> execute_batch_report_299 >> send_reply_300 >> get_response_data

        if_requestor_downcase_equals_to_compassnt2_283 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassp01_302

        if_requestor_downcase_equals_to_compassp01_302 >> rail.Label('Yes')  >> get_report_details_303 >> insert_to_list_batch_date_filter_304 >> if_first_value_present_user_filter_305

        if_first_value_present_user_filter_305 >> rail.Label('Yes')  >> insert_to_list_batch_306 >> if_first_value_present_project_w_b_s_filter_307
        if_first_value_present_user_filter_305 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_307

        if_first_value_present_project_w_b_s_filter_307 >> rail.Label('Yes')  >> insert_to_list_batch_308 >> if_first_value_present_company_code_filter_309
        if_first_value_present_project_w_b_s_filter_307 >> rail.Label('No') >> if_first_value_present_company_code_filter_309

        if_first_value_present_company_code_filter_309 >> rail.Label('Yes')  >> insert_to_list_batch_310 >> if_first_value_present_client_filter_311
        if_first_value_present_company_code_filter_309 >> rail.Label('No') >> if_first_value_present_client_filter_311

        if_first_value_present_client_filter_311 >> rail.Label('Yes')  >> insert_to_list_batch_312 >> if_first_value_present_program_filter_313
        if_first_value_present_client_filter_311 >> rail.Label('No') >> if_first_value_present_program_filter_313

        if_first_value_present_program_filter_313 >> rail.Label('Yes')  >> insert_to_list_batch_314 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_315
        if_first_value_present_program_filter_313 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_315

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_315 >> rail.Label('Yes')  >> insert_to_list_batch_316 >> get_report_filter_value_from_variable_317
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_315 >> rail.Label('No') >> get_report_filter_value_from_variable_317

        get_report_filter_value_from_variable_317  >> get_report_filters_317 >> generate_reports_batch_317

        generate_reports_batch_317 >> execute_batch_report_318 >> send_reply_319 >> get_response_data

        if_requestor_downcase_equals_to_compassp01_302 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt3_321

        if_requestor_downcase_equals_to_compassnt3_321 >> rail.Label('Yes')  >> get_report_details_322 >> insert_to_list_batch_date_filter_323 >> if_first_value_present_user_filter_324

        if_first_value_present_user_filter_324 >> rail.Label('Yes')  >> insert_to_list_batch_325 >> if_first_value_present_project_w_b_s_filter_326
        if_first_value_present_user_filter_324 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_326

        if_first_value_present_project_w_b_s_filter_326 >> rail.Label('Yes')  >> insert_to_list_batch_327 >> if_first_value_present_company_code_filter_328
        if_first_value_present_project_w_b_s_filter_326 >> rail.Label('No') >> if_first_value_present_company_code_filter_328

        if_first_value_present_company_code_filter_328 >> rail.Label('Yes')  >> insert_to_list_batch_329 >> if_first_value_present_client_filter_330
        if_first_value_present_company_code_filter_328 >> rail.Label('No') >> if_first_value_present_client_filter_330

        if_first_value_present_client_filter_330 >> rail.Label('Yes')  >> insert_to_list_batch_331 >> if_first_value_present_program_filter_332
        if_first_value_present_client_filter_330 >> rail.Label('No') >> if_first_value_present_program_filter_332

        if_first_value_present_program_filter_332 >> rail.Label('Yes')  >> insert_to_list_batch_333 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_334
        if_first_value_present_program_filter_332 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_334

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_334 >> rail.Label('Yes')  >> insert_to_list_batch_335 >> get_report_filter_value_from_variable_336
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_334 >> rail.Label('No') >> get_report_filter_value_from_variable_336

        get_report_filter_value_from_variable_336  >> get_report_filters_336 >> generate_reports_batch_336

        generate_reports_batch_336 >> execute_batch_report_337 >> send_reply_338 >> get_response_data

        if_requestor_downcase_equals_to_compassnt3_321 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspj1_340

        if_requestor_downcase_equals_to_compasspj1_340 >> rail.Label('Yes')  >> get_report_details_341 >> insert_to_list_batch_date_filter_342 >> if_first_value_present_user_filter_343

        if_first_value_present_user_filter_343 >> rail.Label('Yes')  >> insert_to_list_batch_344 >> if_first_value_present_project_w_b_s_filter_345
        if_first_value_present_user_filter_343 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_345

        if_first_value_present_project_w_b_s_filter_345 >> rail.Label('Yes')  >> insert_to_list_batch_346 >> if_first_value_present_company_code_filter_347
        if_first_value_present_project_w_b_s_filter_345 >> rail.Label('No') >> if_first_value_present_company_code_filter_347

        if_first_value_present_company_code_filter_347 >> rail.Label('Yes')  >> insert_to_list_batch_348 >> if_first_value_present_client_filter_349
        if_first_value_present_company_code_filter_347 >> rail.Label('No') >> if_first_value_present_client_filter_349

        if_first_value_present_client_filter_349 >> rail.Label('Yes')  >> insert_to_list_batch_350 >> if_first_value_present_program_filter_351
        if_first_value_present_client_filter_349 >> rail.Label('No') >> if_first_value_present_program_filter_351

        if_first_value_present_program_filter_351 >> rail.Label('Yes')  >> insert_to_list_batch_352 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_353
        if_first_value_present_program_filter_351 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_353

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_353 >> rail.Label('Yes')  >> insert_to_list_batch_354 >> get_report_filter_value_from_variable_355
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_353 >> rail.Label('No') >> get_report_filter_value_from_variable_355

        get_report_filter_value_from_variable_355  >> get_report_filters_355 >> generate_reports_batch_355

        generate_reports_batch_355 >> execute_batch_report_356 >> send_reply_357 >> get_response_data

        if_requestor_downcase_equals_to_compasspj1_340 >> rail.Label('No') >> if_requestor_downcase_equals_to_compassnt1_359

        if_requestor_downcase_equals_to_compassnt1_359 >> rail.Label('Yes')  >> get_report_details_360 >> insert_to_list_batch_date_filter_361 >> if_first_value_present_user_filter_362

        if_first_value_present_user_filter_362 >> rail.Label('Yes')  >> insert_to_list_batch_363 >> if_first_value_present_project_w_b_s_filter_364
        if_first_value_present_user_filter_362 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_364

        if_first_value_present_project_w_b_s_filter_364 >> rail.Label('Yes')  >> insert_to_list_batch_365 >> if_first_value_present_company_code_filter_366
        if_first_value_present_project_w_b_s_filter_364 >> rail.Label('No') >> if_first_value_present_company_code_filter_366

        if_first_value_present_company_code_filter_366 >> rail.Label('Yes')  >> insert_to_list_batch_367 >> if_first_value_present_client_filter_368
        if_first_value_present_company_code_filter_366 >> rail.Label('No') >> if_first_value_present_client_filter_368

        if_first_value_present_client_filter_368 >> rail.Label('Yes')  >> insert_to_list_batch_369 >> if_first_value_present_program_filter_370
        if_first_value_present_client_filter_368 >> rail.Label('No') >> if_first_value_present_program_filter_370

        if_first_value_present_program_filter_370 >> rail.Label('Yes')  >> insert_to_list_batch_371 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_372
        if_first_value_present_program_filter_370 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_372

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_372 >> rail.Label('Yes')  >> insert_to_list_batch_373 >> get_report_filter_value_from_variable_374
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_372 >> rail.Label('No') >> get_report_filter_value_from_variable_374

        get_report_filter_value_from_variable_374  >> get_report_filters_374 >> generate_reports_batch_374

        generate_reports_batch_374 >> execute_batch_report_375 >> send_reply_376 >> get_response_data

        if_requestor_downcase_equals_to_compassnt1_359 >> rail.Label('No') >> if_requestor_downcase_equals_to_compasspn1_378

        if_requestor_downcase_equals_to_compasspn1_378 >> rail.Label('Yes')  >> get_report_details_379 >> insert_to_list_batch_date_filter_380 >> if_first_value_present_user_filter_381

        if_first_value_present_user_filter_381 >> rail.Label('Yes')  >> insert_to_list_batch_382 >> if_first_value_present_project_w_b_s_filter_383
        if_first_value_present_user_filter_381 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_383

        if_first_value_present_project_w_b_s_filter_383 >> rail.Label('Yes')  >> insert_to_list_batch_384 >> if_first_value_present_company_code_filter_385
        if_first_value_present_project_w_b_s_filter_383 >> rail.Label('No') >> if_first_value_present_company_code_filter_385

        if_first_value_present_company_code_filter_385 >> rail.Label('Yes')  >> insert_to_list_batch_386 >> if_first_value_present_client_filter_387
        if_first_value_present_company_code_filter_385 >> rail.Label('No') >> if_first_value_present_client_filter_387

        if_first_value_present_client_filter_387 >> rail.Label('Yes')  >> insert_to_list_batch_388 >> if_first_value_present_program_filter_389
        if_first_value_present_client_filter_387 >> rail.Label('No') >> if_first_value_present_program_filter_389

        if_first_value_present_program_filter_389 >> rail.Label('Yes')  >> insert_to_list_batch_390 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_391
        if_first_value_present_program_filter_389 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_391

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_391 >> rail.Label('Yes')  >> insert_to_list_batch_392 >> get_report_filter_value_from_variable_393
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_391 >> rail.Label('No') >> get_report_filter_value_from_variable_393

        get_report_filter_value_from_variable_393  >> get_report_filters_393 >> generate_reports_batch_393

        generate_reports_batch_393 >> execute_batch_report_394 >> send_reply_395 >> get_response_data

        if_requestor_downcase_equals_to_compasspn1_378 >> rail.Label('No') >> if_requestor_downcase_equals_to_ftp_396

        if_requestor_downcase_equals_to_ftp_396 >> rail.Label('Yes')  >> get_report_details_397 >> insert_to_list_batch_date_filter_398 >> if_first_value_present_user_filter_399

        if_first_value_present_user_filter_399 >> rail.Label('Yes')  >> insert_to_list_batch_400 >> if_first_value_present_project_w_b_s_filter_401
        if_first_value_present_user_filter_399 >> rail.Label('No') >> if_first_value_present_project_w_b_s_filter_401

        if_first_value_present_project_w_b_s_filter_401 >> rail.Label('Yes')  >> insert_to_list_batch_402 >> if_first_value_present_company_code_filter_403
        if_first_value_present_project_w_b_s_filter_401 >> rail.Label('No') >> if_first_value_present_company_code_filter_403

        if_first_value_present_company_code_filter_403 >> rail.Label('Yes')  >> insert_to_list_batch_404 >> if_first_value_present_client_filter_405
        if_first_value_present_company_code_filter_403 >> rail.Label('No') >> if_first_value_present_client_filter_405

        if_first_value_present_client_filter_405 >> rail.Label('Yes')  >> insert_to_list_batch_406 >> if_first_value_present_program_filter_407
        if_first_value_present_client_filter_405 >> rail.Label('No') >> if_first_value_present_program_filter_407

        if_first_value_present_program_filter_407 >> rail.Label('Yes')  >> insert_to_list_batch_408 >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_409
        if_first_value_present_program_filter_407 >> rail.Label('No') >> if_request_soldtoparty_present_soldto_party_o_e_f_filter_409

        if_request_soldtoparty_present_soldto_party_o_e_f_filter_409 >> rail.Label('Yes')  >> insert_to_list_batch_410 >> get_report_filter_value_from_variable_411
        if_request_soldtoparty_present_soldto_party_o_e_f_filter_409 >> rail.Label('No') >> get_report_filter_value_from_variable_411

        get_report_filter_value_from_variable_411  >> get_report_filters_412 >> generate_reports_batch_413

        generate_reports_batch_413 >> execute_batch_report_414 >> send_reply_415 >> get_response_data

        if_requestor_downcase_equals_to_ftp_396 >> rail.Label('No') >> catch_416 >> send_reply_417 >> get_response_data

        get_response_data >> response_data_from_report_processing >> finish

        finish >> log_to_sumo

    return dag

rail.for_each_instance(create_dag)
