
from datetime import timedelta, datetime
import uuid
from airflow.models import Variable
from sectranorthamericainc.client_project_import.utils.response_filter import get_clientlist, get_projectlist
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description=f'Sectranorthamerica_client_project_import_child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='declare_variable_2'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='declare_variable_2',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        declare_variable_2 = rail.SetVariableOperator(
            task_id='declare_variable_2',
            append=False,
            name='status',
            value='Not processed'
        )

        log_clientcountrytouse_3 = rail.PythonOperator(
            task_id='log_clientcountrytouse_3',
            python_callable=lambda dag_run: dag_run.conf['webhook']['data']['payload'][
                'ClientCountry'] if dag_run.conf['webhook']['data']['payload']['ClientCountry'] else null
        )

        log_billingcountrytouse_4 = rail.PythonOperator(
            task_id='log_billingcountrytouse_4',
            python_callable=lambda dag_run: dag_run.conf['webhook']['data']['payload'][
                'BillingCountry'] if dag_run.conf['webhook']['data']['payload']['BillingCountry'] else null
        )

        if_request_clientmanager_present_7 = rail.IfOperator(
            task_id='if_request_clientmanager_present_7',
            test='''{{ dag_run.conf.webhook.data.payload.ClientManager | is_truthy }}''',
            yes_task="search_users_8",
            no_task="send_mail_11"
        )

        search_users_8 = rail.RepliconServiceOperator(
            task_id='search_users_8',
            endpoint='/services/UserListService1.svc/GetData',
            data=lambda dag_run: {
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:user-list-column:user-name",
                    "urn:replicon:user-list-column:email-address",
                    "urn:replicon:user-list-column:login-name",
                    "urn:replicon:user-list-column:enabled"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:user-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": dag_run.conf['webhook']['data']['payload']['ClientManager'].lower(),
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            }
        )

        send_mail_11 = rail.EmailOperator(
            task_id='send_mail_11',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject="{{get_company_key()}} | | Client/Project Sync from MS_Dynamics - Completed on {{current_time()}}",
            html_content='templates/emails/complete_mail.html'
        )

        foreach_search_users_8_16 = rail.ForEachOperator(
            task_id='foreach_search_users_8_16',
            items="{{ result('search_users_8').rows | to_json}}",
            start_task='accumulate_list_items_17',
            end_task='foreach_search_users_8_16_end'
        )

        accumulate_list_items_17 = rail.SetVariableOperator(
            task_id='accumulate_list_items_17',
            name='User details',
            append=True,
            value=lambda: {
                "loginname": rail.result('foreach_search_users_8_16')['cells'][2]['textValue'].lower(),
                "uri": rail.result('foreach_search_users_8_16')['cells'][2]['uri'],
                "status": rail.result('foreach_search_users_8_16')['cells'][3]['boolValue'],
                "email": rail.result('foreach_search_users_8_16')['cells'][1]['textValue'].lower() if rail.result('foreach_search_users_8_16') and rail.result('foreach_search_users_8_16')['cells'] and rail.result('foreach_search_users_8_16')['cells'][1] and rail.result('foreach_search_users_8_16')['cells'][1]['textValue'] else null,
            }
        )

        foreach_search_users_8_16_end = rail.EmptyOperator(
            task_id='foreach_search_users_8_16_end',
        )

        log_loginname_18 = rail.PythonOperator(
            task_id='log_loginname_18',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('accumulate_list_items_17')['value'], 'email', dag_run.conf['webhook']['data']['payload']['ClientManager'].lower(
            ), 'loginname', "") if rail.result('accumulate_list_items_17') and rail.result('accumulate_list_items_17')['value'] and rail.result('accumulate_list_items_17')['value'][0] and rail.result('accumulate_list_items_17')['value'][0]['loginname'] else null
        )

        log_clientmanageruritoassign_19 = rail.PythonOperator(
            task_id='log_clientmanageruritoassign_19',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('accumulate_list_items_17')['value'], 'email', dag_run.conf['webhook']['data']['payload']['ClientManager'].lower(
            ), 'uri', "") if rail.result('accumulate_list_items_17') and rail.result('accumulate_list_items_17')['value'] and rail.result('accumulate_list_items_17')['value'][0] and rail.result('accumulate_list_items_17')['value'][0]['loginname'] else null
        )

        log_loginenabled_20 = rail.PythonOperator(
            task_id='log_loginenabled_20',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result(
                'accumulate_list_items_17')['value'], 'email', dag_run.conf['webhook']['data']['payload']['ClientManager'].lower(), 'status', "")if rail.result('accumulate_list_items_17') and rail.result('accumulate_list_items_17')['value'] and rail.result('accumulate_list_items_17')['value'][0] and rail.result('accumulate_list_items_17')['value'][0]['loginname'] else null
        )

        if_log_clientmanageruritoassign_19_blank_21 = rail.IfOperator(
            task_id='if_log_clientmanageruritoassign_19_blank_21',
            test=lambda: not (rail.result('log_clientmanageruritoassign_19')) or str(
                rail.result('log_loginenabled_20')).lower() != 'true',
            yes_task="log_l_o_g_userdisablednotfound_22",
            no_task="if_log_clientmanageruritoassign_19_present_24",
        )

        log_l_o_g_userdisablednotfound_22 = rail.PythonOperator(
            task_id='log_l_o_g_userdisablednotfound_22',
            python_callable=lambda dag_run:  "<p>Client manager could not be assigned since user with email '" +
            dag_run.conf['webhook']['data']['payload']['ClientManager'] +
            "' was not found/disabled in Replicon.</p>"
        )

        if_log_clientmanageruritoassign_19_present_24 = rail.IfOperator(
            task_id='if_log_clientmanageruritoassign_19_present_24',
            test=lambda: rail.result('log_clientmanageruritoassign_19') and str(
                rail.result('log_loginenabled_20')).lower() == 'true',
            yes_task="getpermissionsetassignedtouser_25",
            no_task="log_required_client_manager_uri_35",
        )

        getpermissionsetassignedtouser_25 = rail.RepliconServiceOperator(
            task_id='getpermissionsetassignedtouser_25',
            endpoint="/services/PermissionSetService1.svc/GetAssignedPermissionSetsForUser2",
            data={
                "userUri": "{{ result('log_clientmanageruritoassign_19') }}"
            }
        )

        foreach_response_26 = rail.ForEachOperator(
            task_id='foreach_response_26',
            items="{{ result('getpermissionsetassignedtouser_25') | to_json }}",
            start_task='if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27',
            end_task='foreach_response_26_end'
        )

        if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27 = rail.IfOperator(
            task_id='if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27',
            test='''{{ result('foreach_response_26').policyUri == 'urn:replicon:policy:project-management' }}''',
            yes_task="log_permissionset_uri_28",
            no_task="foreach_response_26_end",
        )

        log_permissionset_uri_28 = rail.PythonOperator(
            task_id='log_permissionset_uri_28',
            python_callable=lambda:  rail.result('foreach_response_26')[
                'permissionSet']['uri']
        )

        foreach_response_26_end = rail.EmptyOperator(
            task_id='foreach_response_26_end',
        )

        log_requiredprojectmanagerpermissionuri_29 = rail.PythonOperator(
            task_id='log_requiredprojectmanagerpermissionuri_29',
            python_callable=lambda:  rail.result('log_permissionset_uri_28') if rail.result(
                'log_permissionset_uri_28') else null
        )

        if_log_requiredprojectmanagerpermissionuri_29_present_30 = rail.IfOperator(
            task_id='if_log_requiredprojectmanagerpermissionuri_29_present_30',
            test='''{{ result('log_requiredprojectmanagerpermissionuri_29') | is_truthy }}''',
            yes_task="_adhoc_http_action_31",
            no_task="if_log_checkfor_clientmanageraccessinpermssion_32_blank_33",
        )

        _adhoc_http_action_31 = rail.RepliconServiceOperator(
            task_id='_adhoc_http_action_31',
            endpoint="/services/PermissionSetService1.svc/GetPermissionSetDetails",
            data={
                "permissionSetUri": "{{ result('log_requiredprojectmanagerpermissionuri_29') }}"
            }
        )

        log_checkfor_clientmanageraccessinpermssion_32 = rail.PythonOperator(
            task_id='log_checkfor_clientmanageraccessinpermssion_32',
            python_callable=lambda: rail.result('log_requiredprojectmanagerpermissionuri_29') if "client-manager" in rail.smartjoin_by_delim(
                rail.result('_adhoc_http_action_31')['configuration']['enabledUserAccessRoleUris'], "||") else null

        )

        if_log_checkfor_clientmanageraccessinpermssion_32_blank_33 = rail.IfOperator(
            task_id='if_log_checkfor_clientmanageraccessinpermssion_32_blank_33',
            test='''{{ result('log_checkfor_clientmanageraccessinpermssion_32') | is_falsy }}''',
            yes_task="log_l_o_g_clientmanagerpermissionnotassigned_34",
            no_task="log_required_client_manager_uri_35",
        )

        log_l_o_g_clientmanagerpermissionnotassigned_34 = rail.PythonOperator(
            task_id='log_l_o_g_clientmanagerpermissionnotassigned_34',
            python_callable=lambda dag_run:  "<p>Client manager '" +
            dag_run.conf['webhook']['data']['payload']['ClientManager'] +
            "' does not have the required 'Client manager' permission assigned in Replicon.</p>"
        )

        log_required_client_manager_uri_35 = rail.PythonOperator(
            task_id='log_required_client_manager_uri_35',
            python_callable=lambda: (rail.result('log_clientmanageruritoassign_19') if rail.result
                                     ('log_checkfor_clientmanageraccessinpermssion_32') else null) if rail.result('log_clientmanageruritoassign_19') else null
        )

        getalldepartments_36 = rail.RepliconServiceOperator(
            task_id='getalldepartments_36',
            endpoint="/services/DepartmentService1.svc/GetEnabledDepartments",
            data={}
        )

        get_enabled_company_billing_rates_37 = rail.RepliconServiceOperator(
            task_id='get_enabled_company_billing_rates_37',
            endpoint="/services/BillingRateService1.svc/GetEnabledCompanyBillingRates",
        )

        log_executivedepartment_uri_38 = rail.PythonOperator(
            task_id='log_executivedepartment_uri_38',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getalldepartments_36'), 'displayText', 'Executive', 'uri', '') if rail.result('getalldepartments_36') and rail.result('getalldepartments_36')[0] and rail.result('getalldepartments_36')[0]['uri'] else null
        )

        log_salessupportdepartment_uri_39 = rail.PythonOperator(
            task_id='log_salessupportdepartment_uri_39',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getalldepartments_36'), 'displayText', 'Sales Support', 'uri', '') if rail.result('getalldepartments_36') and rail.result('getalldepartments_36')[0] and rail.result('getalldepartments_36')[0]['uri'] else null
        )

        log_finance_admin_u_ri_40 = rail.PythonOperator(
            task_id='log_finance_admin_u_ri_40',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result(
                'getalldepartments_36'), 'displayText', 'Finance & Administration', 'uri', '') if rail.result('getalldepartments_36') and rail.result('getalldepartments_36')[0] and rail.result('getalldepartments_36')[0]['uri'] else null
        )

        if_request_code_blank_41 = rail.IfOperator(
            task_id='if_request_code_blank_41',
            test='''{{ dag_run.conf.webhook.data.payload.Code | is_falsy }}''',
            yes_task="log_onerror_42",
            no_task="if_request_code_present_46",
        )

        log_onerror_42 = rail.PythonOperator(
            task_id='log_onerror_42',
            python_callable=lambda dag_run:
            "<strong>This is an automated mail, please don't reply.</strong><br />\
            <br />Hello, <br />\
            <br />The Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "'  and project '" + 'Sales' + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' create/update is completed and there is an unexpected error occured.</br>\
            <br/>Error: Client code is mandatory<br />\
            <br/>For any queries, please contact our support team at https://support.deltek.com <br />\
            <br />Regards, <br />\
            Deltek Inc."

        )

        if_request_code_present_46 = rail.IfOperator(
            task_id='if_request_code_present_46',
            test='''{{ dag_run.conf.webhook.data.payload.Code | is_truthy }}''',
            yes_task="searchclientbycode_47",
            no_task="log_final_e_m_a_i_l_b_o_d_y_111",
        )

        searchclientbycode_47 = rail.RepliconServiceOperator(
            task_id='searchclientbycode_47',
            endpoint="/services/ClientListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "10000",
                "columnUris": [
                    "urn:replicon:client-list-column:client",
                    "urn:replicon:client-list-column:code"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:client-list-filter:code"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{dag_run.conf.webhook.data.payload.Code}}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null,
                            "dateTimeUtcRange": null,
                            "numberRange": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_clientlist,
        )

        log_clienturitosearch_49 = rail.PythonOperator(
            task_id='log_clienturitosearch_49',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('searchclientbycode_47')[
                'clientlist'], 'Code', dag_run.conf['webhook']['data']['payload']['Code'], 'URI', '') if rail.result('searchclientbycode_47')['clientlist'] and rail.result('searchclientbycode_47')['clientlist'][0] and rail.result('searchclientbycode_47')['clientlist'][0]['URI'] else null
        )

        log_clientnametosearch_50 = rail.PythonOperator(
            task_id='log_clientnametosearch_50',
            python_callable=lambda dag_run: rail.find_first_by_attr_and_get_attr(rail.result('searchclientbycode_47')[
                'clientlist'], 'Code', dag_run.conf['webhook']['data']['payload']['Code'], 'Name', '') if rail.result('searchclientbycode_47')['clientlist'] and rail.result('searchclientbycode_47')['clientlist'][0] and rail.result('searchclientbycode_47')['clientlist'][0]['URI'] else null
        )

        if_log_clienturitosearch_49_blank_clientnotpresentclientcreation_52 = rail.IfOperator(
            task_id='if_log_clienturitosearch_49_blank_clientnotpresentclientcreation_52',
            test='''{{ result('log_clienturitosearch_49') | is_falsy }}''',
            yes_task="if_request_statusreason_equals_to_inactive_53",
            no_task="if_log_clienturitosearch_49_present_77",
        )

        if_request_statusreason_equals_to_inactive_53 = rail.IfOperator(
            task_id='if_request_statusreason_equals_to_inactive_53',
            test='''{{ dag_run.conf.webhook.data.payload.StatusReason == 'Inactive'  or dag_run.conf.webhook.data.payload.StatusReason == 'Former' }}''',
            yes_task="create_client_54",
            no_task="create_client_57",
        )

        create_client_54 = rail.RepliconServiceOperator(
            task_id='create_client_54',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['ClientName'],
                    },
                    "codeToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['Code'],
                    },
                    "descriptionToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['Description'],
                    },
                    "statusToApply": False,
                    "clientContactToApply": null,
                    "clientAddressToApply": {
                        "address": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientStreet'],
                        },
                        "city": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientCity'],
                        },
                        "stateProvince": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientStateProvince'],
                        },
                        "country": {
                            "value": {
                                "uri": null,
                                "name": dag_run.conf['webhook']['data']['payload']['BillingCountry']
                            }
                        },
                        "zipPostalCode": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientZipPostal'],
                        },
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": null,
                        "website": null
                    },
                    "billingAddressToApply": {
                        "address": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingAddress'],
                        },
                        "city": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingCity'],
                        },
                        "stateProvince": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingStateProvince'],
                        },
                        "country": {
                            "value": {
                                "uri": null,
                                "name": dag_run.conf['webhook']['data']['payload']['BillingCountry']
                            }
                        },
                        "zipPostalCode": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingZipPostal'],
                        },
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": null,
                        "website": null
                    },
                    "billingRatesToApply": null,
                    "clientManagerToApply": null,
                    "clientSharingToApply": null,
                    "expenseCodesToApply": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:394a7428-e31b-4081-acb8-24e51d7ad73b",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_relationship_type_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['RelationshipType'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:d20a4090-8955-4b80-8df5-e8a0895d5a0d",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_status_reason_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['StatusReason'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:f4a3ef4b-da5c-4d5b-96fb-3f9d987ae034",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_level_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['ServiceLevel'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                    ],
                    "taxProfileToApply": null
                },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        update_client_manager = rail.RepliconServiceOperator(
            task_id='update_client_manager',
            endpoint="services/ClientService1.svc/UpdateClientManager",
            data=lambda:
                {
                    "clientUri": rail.result('create_client_54')['uri'],
                    "clientManagerUri": rail.result('log_required_client_manager_uri_35') if rail.result('log_required_client_manager_uri_35') else null,
                }
        )

        create_project_55 = rail.RepliconServiceOperator(
            task_id='create_project_55',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": "Sales"+" - "+str(dag_run.conf['webhook']['data']['payload']['ClientName']),
                    },
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.now().strftime("%Y"),
                            "month": datetime.now().strftime("%m"),
                            "day": datetime.now().strftime("%d"),
                        }
                    },
                    "endDateToApply": null,
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:non-billable"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('create_client_54')['uri'],
                                    "name": null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:e809a7ea-771c-4522-a855-0a93991bacc7",
                        "name": null
                    },
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": {
                        "program": {
                            "uri": null,
                            "name": "Sales"
                        }
                    },
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": "USD$"
                            }
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:98c3403f-c56b-4e50-8028-cd6678952f32",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_spectrastatus_dropdown'), 'displayText', 'In Progress', 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                    ],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        create_client_57 = rail.RepliconServiceOperator(
            task_id='create_client_57',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['ClientName'],
                    },
                    "codeToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['Code'],
                    },
                    "descriptionToApply": {
                        "value": dag_run.conf['webhook']['data']['payload']['Description'],
                    },
                    "statusToApply": True,
                    "clientContactToApply": null,
                    "clientAddressToApply": {
                        "address": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientStreet'],
                        },
                        "city": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientCity'],
                        },
                        "stateProvince": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientStateProvince'],
                        },
                        "country": {
                            "value": {
                                "uri": null,
                                "name": dag_run.conf['webhook']['data']['payload']['BillingCountry']
                            }
                        },
                        "zipPostalCode": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientZipPostal'],
                        },
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": null,
                        "website": null
                    },
                    "billingAddressToApply": {
                        "address": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingAddress'],
                        },
                        "city": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingCity'],
                        },
                        "stateProvince": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingStateProvince'],
                        },
                        "country": {
                            "value": {
                                "uri": null,
                                "name": dag_run.conf['webhook']['data']['payload']['BillingCountry']
                            }
                        },
                        "zipPostalCode": {
                            "value": dag_run.conf['webhook']['data']['payload']['BillingZipPostal'],
                        },
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": null,
                        "website": null
                    },
                    "billingRatesToApply": null,
                    "clientManagerToApply": null,
                    "clientSharingToApply": null,
                    "expenseCodesToApply": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:394a7428-e31b-4081-acb8-24e51d7ad73b",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_relationship_type_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['RelationshipType'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:d20a4090-8955-4b80-8df5-e8a0895d5a0d",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_status_reason_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['StatusReason'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:f4a3ef4b-da5c-4d5b-96fb-3f9d987ae034",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_level_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['ServiceLevel'], 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                    ],
                    "taxProfileToApply": null
                },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        update_client_manager1 = rail.RepliconServiceOperator(
            task_id='update_client_manager1',
            endpoint="services/ClientService1.svc/UpdateClientManager",
            data=lambda:
                {
                    "clientUri": rail.result('create_client_57')['uri'],
                    "clientManagerUri": rail.result('log_required_client_manager_uri_35') if rail.result('log_required_client_manager_uri_35') else null,
                }
        )

        create_project_58 = rail.RepliconServiceOperator(
            task_id='create_project_58',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda dag_run: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": "Sales"+" - "+str(dag_run.conf['webhook']['data']['payload']['ClientName']),
                    },
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.now().strftime("%Y"),
                            "month": datetime.now().strftime("%m"),
                            "day": datetime.now().strftime("%d"),
                        }
                    },
                    "endDateToApply": null,
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:non-billable"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('create_client_57')['uri'],
                                    "name": null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:e809a7ea-771c-4522-a855-0a93991bacc7",
                        "name": null
                    },
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": {
                        "program": {
                            "uri": null,
                            "name": "Sales"
                        }
                    },
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": "USD$"
                            }
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:98c3403f-c56b-4e50-8028-cd6678952f32",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_spectrastatus_dropdown'), 'displayText', 'In Progress', 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                    ],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        log_cilenturicreated_59 = rail.PythonOperator(
            task_id='log_cilenturicreated_59',
            python_callable=lambda: rail.result('create_client_54')['uri'] if rail.result(
                'create_client_54') and rail.result('create_client_54')['uri'] else rail.result('create_client_57')['uri']
        )

        foreach_response_60 = rail.ForEachOperator(
            task_id='foreach_response_60',
            items="{{ result('get_enabled_company_billing_rates_37') | to_json }}",
            start_task='update_billing_rate_is_allowed_by_default_on_new_projects_61',
            end_task='foreach_response_60_end'
        )

        update_billing_rate_is_allowed_by_default_on_new_projects_61 = rail.RepliconServiceOperator(
            task_id='update_billing_rate_is_allowed_by_default_on_new_projects_61',
            endpoint="/services/ClientService1.svc/UpdateBillingRateIsAllowedByDefaultOnNewProjects",
            data={
                "clientUri": "{{ result('log_cilenturicreated_59') }}",
                "billingRateUri": "{{ result('foreach_response_60').uri }}",
                "isAllowedByDefaultOnNewProjects": "true"
            }
        )

        foreach_response_60_end = rail.EmptyOperator(
            task_id='foreach_response_60_end',
        )

        log_project_uri_62 = rail.PythonOperator(
            task_id='log_project_uri_62',
            python_callable=lambda: rail.result('create_project_58')['uri'] if rail.result('create_project_58') and rail.result('create_project_58')['uri'] else rail.result('create_project_55')[
                'uri']
        )

        removeexistingprojectresources_63 = rail.RepliconServiceOperator(
            task_id='removeexistingprojectresources_63',
            endpoint="/services/ProjectService1.svc/PutProjectTeamMemberAssignments",
            data={
                "projectUri": "{{ result('log_project_uri_62') }}",
                "resourceUris": []
            },
        )

        if_log_required_client_manager_uri_35_present_64 = rail.IfOperator(
            task_id='if_log_required_client_manager_uri_35_present_64',
            test='''{{ result('log_required_client_manager_uri_35') | is_truthy }}''',
            yes_task="assignprojectresources_65",
            no_task="if_log_required_client_manager_uri_35_blank_66",
        )

        assignprojectresources_65 = rail.RepliconServiceOperator(
            task_id='assignprojectresources_65',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
                "projectUri": "{{ result('log_project_uri_62') }}",
                "resourceUri": [
                    "{{ result('log_executivedepartment_uri_38') }}",
                    "{{ result('log_salessupportdepartment_uri_39') }}",
                    "{{ result('log_finance_admin_u_ri_40') }}",
                    "{{ result('log_required_client_manager_uri_35') }}"
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            },
        )

        if_log_required_client_manager_uri_35_blank_66 = rail.IfOperator(
            task_id='if_log_required_client_manager_uri_35_blank_66',
            test='''{{ result('log_required_client_manager_uri_35') | is_falsy }}''',
            yes_task="assignprojectresources_67",
            no_task="log_email_bodyforclientprojectcreated_68",
        )

        assignprojectresources_67 = rail.RepliconServiceOperator(
            task_id='assignprojectresources_67',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
                "projectUri": "{{ result('log_project_uri_62') }}",
                "resourceUri": [
                    "{{ result('log_executivedepartment_uri_38') }}",
                    "{{ result('log_salessupportdepartment_uri_39') }}",
                    "{{ result('log_finance_admin_u_ri_40') }}"
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            },
        )

        log_email_bodyforclientprojectcreated_68 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreated_68',
            python_callable=lambda dag_run:
            "<p><strong>This is an Automated email, Please do not reply!</strong></p> \
            <p>Dear Customer,</p>\
            <p>Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "'  and project '" + 'Sales-' + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' have been succesfully created in Replicon.\
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_69 = rail.IfOperator(
            task_id='if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_69',
            test='''{{ result('log_l_o_g_clientmanagerpermissionnotassigned_34') | is_truthy }}''',
            yes_task="log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70",
            no_task="if_log_l_o_g_userdisablednotfound_22_present_71",
        )

        log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70',
            python_callable=lambda dag_run:
            "<p><strong>This is an Automated email, Please do not reply!</strong></p>\
            <p>Dear Customer,</p>\
            <p>Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' and the project '" + 'Sales-' + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' have been succesfully created in Replicon.\
            <p> "+rail.result('log_l_o_g_clientmanagerpermissionnotassigned_34') + "</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        if_log_l_o_g_userdisablednotfound_22_present_71 = rail.IfOperator(
            task_id='if_log_l_o_g_userdisablednotfound_22_present_71',
            test='''{{ result('log_l_o_g_userdisablednotfound_22') | is_truthy }}''',
            yes_task="log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72",
            no_task="log_email_bodyforclientprojectcreated_f_i_n_a_l_73",
        )

        log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72',
            python_callable=lambda dag_run:
            "<p><strong>This is an Automated email, Please do not reply!</strong></p>\
            <p>Dear Customer,</p>\
            <p>Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' and the project '" + 'Sales-' + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' have been succesfully created in Replicon.\
            <p> "+rail.result('log_l_o_g_userdisablednotfound_22') + "</p>\
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        log_email_bodyforclientprojectcreated_f_i_n_a_l_73 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreated_f_i_n_a_l_73',
            python_callable=lambda: rail.result('log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72') if rail.result('log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72') else (rail.result(
                'log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70') if rail.result('log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70') else rail.result('log_email_bodyforclientprojectcreated_68'))
        )

        if_log_clienturitosearch_49_present_77 = rail.IfOperator(
            task_id='if_log_clienturitosearch_49_present_77',
            test='''{{ result('log_clienturitosearch_49') | is_truthy }}''',
            yes_task="get_client_details_78",
            no_task="log_final_e_m_a_i_l_b_o_d_y_110",
        )

        get_client_details_78 = rail.RepliconServiceOperator(
            task_id='get_client_details_78',
            endpoint="/services/ClientService1.svc/GetClientDetails",
            data={
                "clientUri": "{{result('log_clienturitosearch_49')}}"
            }
        )

        log_existing_client_manager_uri_79 = rail.PythonOperator(
            task_id='log_existing_client_manager_uri_79',
            python_callable=lambda:  rail.result('get_client_details_78')[
                'clientManager']['uri'] if rail.result('get_client_details_78') and rail.result('get_client_details_78')[
                'clientManager'] and rail.result('get_client_details_78')[
                'clientManager']['uri'] else null
        )

        log_projectnametosearch_80 = rail.PythonOperator(
            task_id='log_projectnametosearch_80',
            python_callable=lambda:  "Sales" + " - " +
            rail.result('log_clientnametosearch_50')
        )

        get_relationship_type_dropdown = rail.RepliconServiceOperator(
            task_id='get_relationship_type_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:394a7428-e31b-4081-acb8-24e51d7ad73b",
            }
        )

        get_status_reason_dropdown = rail.RepliconServiceOperator(
            task_id='get_status_reason_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:d20a4090-8955-4b80-8df5-e8a0895d5a0d",
            }

        )

        get_service_level_dropdown = rail.RepliconServiceOperator(
            task_id='get_service_level_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:f4a3ef4b-da5c-4d5b-96fb-3f9d987ae034",
            }

        )

        get_spectrastatus_dropdown = rail.RepliconServiceOperator(
            task_id='get_spectrastatus_dropdown',
            endpoint='/services/CustomFieldService1.svc/GetAllCustomFieldDropDownOptions',
            data=lambda: {
                "customFieldUri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:98c3403f-c56b-4e50-8028-cd6678952f32"
            }
        )

        if_request_statusreason_equals_to_inactive_81 = rail.IfOperator(
            task_id='if_request_statusreason_equals_to_inactive_81',
            test='''{{ dag_run.conf.webhook.data.payload.StatusReason == 'Inactive'  or dag_run.conf.webhook.data.payload.StatusReason == 'Former' }}''',
            yes_task="update_client_82",
            no_task="update_client_84",
        )

        update_client_82 = rail.RepliconServiceOperator(
            task_id='update_client_82',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                    "target": {
                        "uri": rail.result('log_clienturitosearch_49'),
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                "modifications": {
                        "nameToApply": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientName'],
                        },
                        "descriptionToApply": {
                            "value": dag_run.conf['webhook']['data']['payload']['Description'],
                        },
                        "statusToApply": False,
                        "clientContactToApply": null,
                        "clientAddressToApply": {
                            "address": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientStreet'],
                            },
                            "city": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientCity'],
                            },
                            "stateProvince": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientStateProvince'],
                            },
                            "country": {
                                "value": {
                                    "uri": null,
                                    "name": dag_run.conf['webhook']['data']['payload']['ClientCountry']
                                }
                            },
                            "zipPostalCode": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientZipPostal'],
                            },
                            "phoneNumber": null,
                            "faxNumber": null,
                            "email": null,
                            "website": null
                        },
                        "billingAddressToApply": {
                            "address": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingAddress'],
                            },
                            "city": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingCity'],
                            },
                            "stateProvince": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingStateProvince'],
                            },
                            "country": {
                                "value": {
                                    "uri": null,
                                    "name": dag_run.conf['webhook']['data']['payload']['ClientCountry']
                                }
                            },
                            "zipPostalCode": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingZipPostal'],
                            },
                            "phoneNumber": null,
                            "faxNumber": null,
                            "email": null,
                            "website": null
                        },
                        "billingRatesToApply": null,
                        "clientSharingToApply": null,
                        "expenseCodesToApply": null,
                        "customFieldsToApply": [
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:394a7428-e31b-4081-acb8-24e51d7ad73b",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_relationship_type_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['RelationshipType'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:d20a4090-8955-4b80-8df5-e8a0895d5a0d",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_status_reason_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['StatusReason'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:f4a3ef4b-da5c-4d5b-96fb-3f9d987ae034",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_level_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['ServiceLevel'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                        ],
                        "taxProfileToApply": null
                        },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        if_clientmanager_is_in_disable_state = rail.IfOperator(
            task_id='if_clientmanager_is_in_disable_state',
            test='''{{ result('log_required_client_manager_uri_35') | is_truthy}}''',
            yes_task="update_clientmanager1",
            no_task="searchprojectbyname_85",
        )

        update_clientmanager1 = rail.RepliconServiceOperator(
            task_id='update_clientmanager1',
            endpoint="services/ClientService1.svc/UpdateClientManager",
            data=lambda:
                {
                    "clientUri": rail.result('log_clienturitosearch_49'),
                    "clientManagerUri": rail.result('log_required_client_manager_uri_35'),
                }
        )

        update_client_84 = rail.RepliconServiceOperator(
            task_id='update_client_84',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run: {
                    "target": {
                        "uri": rail.result('log_clienturitosearch_49'),
                        "name": null,
                        "code": null,
                        "parameterCorrelationId": null
                    },
                "modifications": {
                        "nameToApply": {
                            "value": dag_run.conf['webhook']['data']['payload']['ClientName'],
                        },
                        "descriptionToApply": {
                            "value": dag_run.conf['webhook']['data']['payload']['Description'],
                        },
                        "statusToApply": True,
                        "clientContactToApply": null,
                        "clientAddressToApply": {
                            "address": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientStreet'],
                            },
                            "city": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientCity'],
                            },
                            "stateProvince": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientStateProvince'],
                            },
                            "country": {
                                "value": {
                                    "uri": null,
                                    "name": dag_run.conf['webhook']['data']['payload']['ClientCountry']
                                }
                            },
                            "zipPostalCode": {
                                "value": dag_run.conf['webhook']['data']['payload']['ClientZipPostal'],
                            },
                            "phoneNumber": null,
                            "faxNumber": null,
                            "email": null,
                            "website": null
                        },
                        "billingAddressToApply": {
                            "address": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingAddress'],
                            },
                            "city": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingCity'],
                            },
                            "stateProvince": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingStateProvince'],
                            },
                            "country": {
                                "value": {
                                    "uri": null,
                                    "name": dag_run.conf['webhook']['data']['payload']['ClientCountry']
                                }
                            },
                            "zipPostalCode": {
                                "value": dag_run.conf['webhook']['data']['payload']['BillingZipPostal'],
                            },
                            "phoneNumber": null,
                            "faxNumber": null,
                            "email": null,
                            "website": null
                        },
                        "billingRatesToApply": null,
                        "clientSharingToApply": null,
                        "expenseCodesToApply": null,
                        "customFieldsToApply": [
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:394a7428-e31b-4081-acb8-24e51d7ad73b",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_relationship_type_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['RelationshipType'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:d20a4090-8955-4b80-8df5-e8a0895d5a0d",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_status_reason_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['StatusReason'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                            {
                                "customField": {
                                    "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:f4a3ef4b-da5c-4d5b-96fb-3f9d987ae034",
                                    "name": null,
                                    "groupUri": null
                                },
                                "text": null,
                                "date": null,
                                "dropDownOption": {
                                    "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_service_level_dropdown'), 'displayText', dag_run.conf['webhook']['data']['payload']['ServiceLevel'], 'uri', ''),
                                    "name": null
                                },
                                "number": null
                            },
                        ],
                        "taxProfileToApply": null
                        },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        searchprojectbyname_85 = rail.RepliconServiceOperator(
            task_id='searchprojectbyname_85',
            endpoint="/services/ProjectListService1.svc/GetData",
            data={
                "page": "1",
                "pagesize": "1000",
                "columnUris": [
                    "urn:replicon:project-list-column:code",
                    "urn:replicon:project-list-column:name",
                    "urn:replicon:project-list-column:project"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": null,
                        "filterDefinitionUri": "urn:replicon:project-list-filter:text"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                        "leftExpression": null,
                        "operatorUri": null,
                        "rightExpression": null,
                        "value": {
                            "uri": null,
                            "uris": [],
                            "bool": null,
                            "date": null,
                            "money": null,
                            "number": null,
                            "text": "{{ result('log_projectnametosearch_80') }}",
                            "time": null,
                            "calendarDayDurationValue": null,
                            "workdayDurationValue": null,
                            "dateRange": null,
                            "dateTimeUtc": null
                        },
                        "filterDefinitionUri": null
                    },
                    "value": null,
                    "filterDefinitionUri": null
                }
            },
            data_handler=get_projectlist
        )

        log_projecturiifexists_87 = rail.PythonOperator(
            task_id='log_projecturiifexists_87',
            python_callable=lambda: rail.find_first_by_attr_and_get_attr(rail.result('searchprojectbyname_85')[
                'projectlist'], 'Name', rail.result('log_projectnametosearch_80'), 'URI', '') if rail.result('searchprojectbyname_85') and rail.result('searchprojectbyname_85')['projectlist'] and rail.result('searchprojectbyname_85')['projectlist'][0] and rail.result('searchprojectbyname_85')['projectlist'][0]['URI'] else null
        )

        if_log_projecturiifexists_87_blank_88 = rail.IfOperator(
            task_id='if_log_projecturiifexists_87_blank_88',
            test='''{{ result('log_projecturiifexists_87') | is_falsy }}''',
            yes_task="create_project_89",
            no_task="log_l_o_gclientupdatedandprojectcreated_93",
        )

        create_project_89 = rail.RepliconServiceOperator(
            task_id='create_project_89',
            endpoint="/services/ProjectService1.svc/CreateProjectOrApplyModifications",
            data=lambda: {
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": rail.result('log_projectnametosearch_80'),
                    },
                    "codeToApply": null,
                    "descriptionToApply": null,
                    "percentCompletedToApply": null,
                    "startDateToApply": {
                        "date": {
                            "year": datetime.now().strftime("%Y"),
                            "month": datetime.now().strftime("%m"),
                            "day": datetime.now().strftime("%d"),
                        }
                    },
                    "endDateToApply": null,
                    "billingTypeToApply": {
                        "value": "urn:replicon:billing-type:non-billable"
                    },
                    "clientBillingAllocationMethodToApply": null,
                    "clientAssignmentsSchedulesToApply": {
                        "clients": [
                            {
                                "client": {
                                    "uri": rail.result('log_clienturitosearch_49'),
                                    "name": null,
                                    "code": null,
                                    "parameterCorrelationId": null
                                },
                                "costAllocationPercentage": "100"
                            }
                        ],
                        "effectiveDate": null
                    },
                    "statusToApply": {
                        "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":project-status-label:e809a7ea-771c-4522-a855-0a93991bacc7",
                        "name": null
                    },
                    "projectWorkflowStateToApply": null,
                    "clientRepresentativeToApply": null,
                    "programToApply": {
                        "program": {
                            "uri": null,
                            "name": "Sales"
                        }
                    },
                    "projectLeaderToApply": null,
                    "isProjectLeaderApprovalRequired": null,
                    "costTypeToApply": null,
                    "isTimeEntryAllowed": null,
                    "estimatedHoursToApply": null,
                    "budgetedHoursToApply": null,
                    "estimatedCostToApply": {
                        "value": {
                            "amount": "0",
                            "currency": {
                                "uri": null,
                                "name": null,
                                "symbol": "USD$"
                            }
                        }
                    },
                    "budgetedCostToApply": null,
                    "expenseBudgetedCostToApply": null,
                    "totalEstimatedContractValueToApply": null,
                    "defaultBillingCurrencyToApply": null,
                    "timeAndMaterials": null,
                    "billingContractToApply": null,
                    "fixedBid": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": "urn:replicon-tenant:"+rail.get_tenant_slug()+":user-defined-field:98c3403f-c56b-4e50-8028-cd6678952f32",
                                "name": null,
                                "groupUri": null
                            },
                            "text": null,
                            "date": null,
                            "dropDownOption": {
                                "uri": rail.find_first_by_attr_and_get_attr(rail.result('get_spectrastatus_dropdown'), 'displayText', 'In Progress', 'uri', ''),
                                "name": null
                            },
                            "number": null
                        },
                    ],
                    "resourceAssignmentModifications": null,
                    "resourceProjectAssignmentModifications": null,
                    "billingContractModifications": null,
                    "keyValuesToApply": [],
                    "objectExtensionFieldsToApply": [],
                    "portfolioToApply": null,
                    "locationToApply": null,
                    "divisionToApply": null,
                    "serviceCenterToApply": null,
                    "costCenterToApply": null,
                    "departmentGroupToApply": null,
                    "employeeTypeGroupToApply": null
                },
                "projectModificationOptionUri": "urn:replicon:project-modification-option:save",
                "unitOfWorkId": str(uuid.uuid4())
            }
        )

        removeexistingprojectresources_90 = rail.RepliconServiceOperator(
            task_id='removeexistingprojectresources_90',
            endpoint="/services/ProjectService1.svc/PutProjectTeamMemberAssignments",
            data=lambda: {
                "projectUri": rail.result('create_project_89')['uri'],
                "resourceUris": []
            },
        )

        log_l_o_gclientupdatedandprojectcreated_91 = rail.PythonOperator(
            task_id='log_l_o_gclientupdatedandprojectcreated_91',
            python_callable=lambda dag_run:  "<p>Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' and the project '" +
            'Sales-' + dag_run.conf['webhook']['data']['payload']['ClientName'] +
            "' has been created successfully in Replicon."
        )

        log_l_o_gclientupdatedandprojectcreated_93 = rail.PythonOperator(
            task_id='log_l_o_gclientupdatedandprojectcreated_93',
            python_callable=lambda dag_run:  "<p>Client '" + dag_run.conf['webhook']['data']['payload']['ClientName'] + "' and the project '" + rail.result('log_projectnametosearch_80') +
            "' has been updated successfully  in Replicon.</p>"
        )

        log_projecturi_94 = rail.PythonOperator(
            task_id='log_projecturi_94',
            python_callable=lambda: rail.result('log_projecturiifexists_87') if rail.result(
                'log_projecturiifexists_87') else rail.result('create_project_89')['uri'],
        )

        if_log_projecturiifexists_87_present_check_existing_projectforpreviousclientmanager_95 = rail.IfOperator(
            task_id='if_log_projecturiifexists_87_present_check_existing_projectforpreviousclientmanager_95',
            test='''{{ result('log_projecturiifexists_87') | is_truthy }}''',
            yes_task="if_log_existing_client_manager_uri_79_present_96",
            no_task="if_log_required_client_manager_uri_35_present_99",
        )

        if_log_existing_client_manager_uri_79_present_96 = rail.IfOperator(
            task_id='if_log_existing_client_manager_uri_79_present_96',
            test='''{{ result('log_existing_client_manager_uri_79') | is_truthy  and result('log_required_client_manager_uri_35') | is_truthy }}''',
            yes_task="if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97",
            no_task="if_log_required_client_manager_uri_35_present_99",
        )

        if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97 = rail.IfOperator(
            task_id='if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97',
            test='''{{ result('log_existing_client_manager_uri_79') != result('log_required_client_manager_uri_35') }}''',
            yes_task="removepreviousclientmanagerfrom_projectteam_98",
            no_task="if_log_required_client_manager_uri_35_present_99",
        )

        removepreviousclientmanagerfrom_projectteam_98 = rail.RepliconServiceOperator(
            task_id='removepreviousclientmanagerfrom_projectteam_98',
            endpoint="/services/ProjectService1.svc/UpdateProjectTeamMemberAssignment",
            data={
                "projectUri": "{{ result('log_projecturiifexists_87') }}",
                "resourceUri": "{{ result('log_existing_client_manager_uri_79') }}",
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:force-unassign"
            },
        )

        if_log_required_client_manager_uri_35_present_99 = rail.IfOperator(
            task_id='if_log_required_client_manager_uri_35_present_99',
            test='''{{ result('log_required_client_manager_uri_35') | is_truthy }}''',
            yes_task="assignprojectresources_100",
            no_task="if_log_required_client_manager_uri_35_blank_101",
        )

        assignprojectresources_100 = rail.RepliconServiceOperator(
            task_id='assignprojectresources_100',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
                "projectUri": "{{ result('log_projecturi_94') }}",
                "resourceUri": [
                    "{{ result('log_executivedepartment_uri_38') }}",
                    "{{ result('log_salessupportdepartment_uri_39') }}",
                    "{{ result('log_finance_admin_u_ri_40') }}",
                    "{{ result('log_required_client_manager_uri_35') }}"
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            },
        )

        if_log_required_client_manager_uri_35_blank_101 = rail.IfOperator(
            task_id='if_log_required_client_manager_uri_35_blank_101',
            test='''{{ result('log_required_client_manager_uri_35') | is_falsy }}''',
            yes_task="assignprojectresources_102",
            no_task="log_l_o_gclientprojectcreatedupdated_103",
        )

        assignprojectresources_102 = rail.RepliconServiceOperator(
            task_id='assignprojectresources_102',
            endpoint="/services/ProjectService1.svc/BulkUpdateProjectTeamMembersAssignment",
            data={
                "projectUri": "{{ result('log_projecturi_94') }}",
                "resourceUri": [
                    "{{ result('log_executivedepartment_uri_38') }}",
                    "{{ result('log_salessupportdepartment_uri_39') }}",
                    "{{ result('log_finance_admin_u_ri_40') }}"
                ],
                "projectTeamMemberAssignmentOptionUri": "urn:replicon:project-team-member-assignment-option:assign"
            },
        )

        log_l_o_gclientprojectcreatedupdated_103 = rail.PythonOperator(
            task_id='log_l_o_gclientprojectcreatedupdated_103',
            python_callable=lambda: rail.result('log_l_o_gclientupdatedandprojectcreated_91') or rail.result(
                'log_l_o_gclientupdatedandprojectcreated_93')
        )

        log_e_m_a_i_lbody_clientproject_updated_104 = rail.PythonOperator(
            task_id='log_e_m_a_i_lbody_clientproject_updated_104',
            python_callable=lambda:
            "<p><strong>This is an Automated email, Please do not reply!</strong></p>\
            <p>Dear Customer,</p>\
            <p> " + rail.result('log_l_o_gclientprojectcreatedupdated_103') + " </p>\
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_105 = rail.IfOperator(
            task_id='if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_105',
            test='''{{ result('log_l_o_g_clientmanagerpermissionnotassigned_34') | is_truthy }}''',
            yes_task="log_email_bodyforclientprojectcreated_106",
            no_task="if_log_l_o_g_userdisablednotfound_22_present_107",
        )

        log_email_bodyforclientprojectcreated_106 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreated_106',
            python_callable=lambda: ""
            "<p><strong>This is an Automated email, Please do not reply!</strong></p>\
            <p>Dear Customer,</p>\
            <p>" + rail.result('log_l_o_gclientprojectcreatedupdated_103') + rail.result('log_l_o_g_clientmanagerpermissionnotassigned_34') + "</p>\
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        if_log_l_o_g_userdisablednotfound_22_present_107 = rail.IfOperator(
            task_id='if_log_l_o_g_userdisablednotfound_22_present_107',
            test='''{{ result('log_l_o_g_userdisablednotfound_22') | is_truthy }}''',
            yes_task="log_email_bodyforclientprojectcreated_108",
            no_task="log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109",
        )

        log_email_bodyforclientprojectcreated_108 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreated_108',
            python_callable=lambda:
            "<p><strong>This is an Automated email, Please do not reply!</strong></p>\
            <p>Dear Customer,</p>\
            <p>" + rail.result('log_l_o_gclientprojectcreatedupdated_103') + rail.result('log_l_o_g_userdisablednotfound_22') + " </p>\
            <p>please contact our support team at https://support.deltek.com for any further assistance.</p>\
            <p>Regards,</p>\
            <p>Deltek Inc.</p>"
        )

        log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109 = rail.PythonOperator(
            task_id='log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109',
            python_callable=lambda:  rail.result('log_email_bodyforclientprojectcreated_108') if rail.result('log_email_bodyforclientprojectcreated_108') else (rail.result(
                'log_email_bodyforclientprojectcreated_106') if rail.result('log_email_bodyforclientprojectcreated_106') else rail.result('log_e_m_a_i_lbody_clientproject_updated_104'))
        )

        log_final_e_m_a_i_l_b_o_d_y_110 = rail.PythonOperator(
            task_id='log_final_e_m_a_i_l_b_o_d_y_110',
            python_callable=lambda: rail.result('log_email_bodyforclientprojectcreated_f_i_n_a_l_73') or rail.result(
                'log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109')
        )

        log_final_e_m_a_i_l_b_o_d_y_111 = rail.PythonOperator(
            task_id='log_final_e_m_a_i_l_b_o_d_y_111',
            python_callable=lambda: rail.result('log_final_e_m_a_i_l_b_o_d_y_110') if rail.result(
                'log_final_e_m_a_i_l_b_o_d_y_110') else rail.result('log_onerror_42')
        )

        update_variable_112 = rail.SetVariableOperator(
            task_id='update_variable_112',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value="Completed successfully"
        )

        send_mail_113 = rail.EmailOperator(
            task_id='send_mail_113',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Sectranorthamericainc | Client/Project Sync from MS_Dynamics - Completed on {{ current_time() }} ''',
            html_content='''{{result('log_final_e_m_a_i_l_b_o_d_y_111') }} ''',
        )

        def get_error_message():
            error_message = rail.render_template("{{get_error_message()}}")
            if rail.get_current_context()['dag_run'].get_task_instance('foreach_search_users_8_16').current_state() == 'success':
                result = "Error while updating client/project" + "|" + error_message
            else:
                result = "Error while searching user" + "|" + error_message
            return result

        catch_114 = rail.EmptyOperator(
            task_id='catch_114',
            trigger_rule='one_failed',
        )

        update_variable_115 = rail.SetVariableOperator(
            task_id='update_variable_115',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value=get_error_message
        )

        update_variable_116 = rail.SetVariableOperator(
            task_id='update_variable_116',
            append=False,
            name='{{ result("declare_variable_2").name }}',
            value=lambda: "Error while updating client/project" +
            "|" + rail.render_template("{{get_error_message()}}")
        )

        send_mail_117 = rail.EmailOperator(
            task_id='send_mail_117',
            to=config.tenant_email,
            bcc=config.internal_logs_email,
            subject='''Sectranorthamericainc | Client/Project Sync from MS_Dynamics - Completed with Errors - {{ current_time() }} ''',
            html_content="templates/emails/completed_with_error_mail.html"
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label('No') >> declare_variable_2
        declare_variable_2 >> log_clientcountrytouse_3 >> log_billingcountrytouse_4 >> if_request_clientmanager_present_7
        if_request_clientmanager_present_7 >> rail.Label(
            'Yes') >> search_users_8 >> foreach_search_users_8_16
        if_request_clientmanager_present_7 >> rail.Label(
            'No') >> send_mail_11 >> catch_114
        foreach_search_users_8_16 >> accumulate_list_items_17 >> foreach_search_users_8_16_end
        foreach_search_users_8_16 >> foreach_search_users_8_16_end >> log_loginname_18
        log_loginname_18 >> log_clientmanageruritoassign_19 >> log_loginenabled_20
        log_loginenabled_20 >> if_log_clientmanageruritoassign_19_blank_21
        if_log_clientmanageruritoassign_19_blank_21 >> rail.Label(
            'Yes') >> log_l_o_g_userdisablednotfound_22 >> if_log_clientmanageruritoassign_19_present_24
        if_log_clientmanageruritoassign_19_blank_21 >> rail.Label(
            'No') >> if_log_clientmanageruritoassign_19_present_24
        if_log_clientmanageruritoassign_19_present_24 >> rail.Label(
            'Yes') >> getpermissionsetassignedtouser_25 >> foreach_response_26
        foreach_response_26 >> if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27
        if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27 >> rail.Label(
            'Yes') >> log_permissionset_uri_28 >> foreach_response_26_end
        if_foreach_response_26_policyuri_equals_to_urnrepliconpolicyprojectmanagement_27 >> rail.Label(
            'No') >> foreach_response_26_end
        foreach_response_26 >> foreach_response_26_end >> log_requiredprojectmanagerpermissionuri_29
        log_requiredprojectmanagerpermissionuri_29 >> if_log_requiredprojectmanagerpermissionuri_29_present_30
        if_log_requiredprojectmanagerpermissionuri_29_present_30 >> rail.Label(
            'Yes') >> _adhoc_http_action_31 >> log_checkfor_clientmanageraccessinpermssion_32
        log_checkfor_clientmanageraccessinpermssion_32 >> if_log_checkfor_clientmanageraccessinpermssion_32_blank_33
        if_log_requiredprojectmanagerpermissionuri_29_present_30 >> rail.Label(
            'No') >> if_log_checkfor_clientmanageraccessinpermssion_32_blank_33
        if_log_checkfor_clientmanageraccessinpermssion_32_blank_33 >> rail.Label(
            'Yes') >> log_l_o_g_clientmanagerpermissionnotassigned_34 >> log_required_client_manager_uri_35
        if_log_checkfor_clientmanageraccessinpermssion_32_blank_33 >> rail.Label(
            'No') >> log_required_client_manager_uri_35
        if_log_clientmanageruritoassign_19_present_24 >> rail.Label(
            'No') >> log_required_client_manager_uri_35 >> getalldepartments_36
        getalldepartments_36 >> get_enabled_company_billing_rates_37 >> log_executivedepartment_uri_38
        log_executivedepartment_uri_38 >> log_salessupportdepartment_uri_39 >> log_finance_admin_u_ri_40
        log_finance_admin_u_ri_40 >> get_relationship_type_dropdown >> get_service_level_dropdown
        get_service_level_dropdown >> get_spectrastatus_dropdown >> get_status_reason_dropdown >> if_request_code_blank_41
        if_request_code_blank_41 >> rail.Label(
            'Yes') >> log_onerror_42 >> if_request_code_present_46
        if_request_code_blank_41 >> rail.Label(
            'No') >> if_request_code_present_46
        if_request_code_present_46 >> rail.Label(
            'Yes') >> searchclientbycode_47 >> log_clienturitosearch_49
        log_clienturitosearch_49 >> log_clientnametosearch_50 >> if_log_clienturitosearch_49_blank_clientnotpresentclientcreation_52

        if_log_clienturitosearch_49_blank_clientnotpresentclientcreation_52 >> rail.Label(
            'Yes') >> if_request_statusreason_equals_to_inactive_53
        if_request_statusreason_equals_to_inactive_53 >> rail.Label(
            'Yes') >> create_client_54 >> update_client_manager >> create_project_55 >> log_cilenturicreated_59
        if_request_statusreason_equals_to_inactive_53 >> rail.Label(
            'No') >> create_client_57 >> update_client_manager1 >> create_project_58 >> log_cilenturicreated_59 >> foreach_response_60
        foreach_response_60 >> update_billing_rate_is_allowed_by_default_on_new_projects_61 >> foreach_response_60_end

        foreach_response_60 >> foreach_response_60_end >> log_project_uri_62
        log_project_uri_62 >> removeexistingprojectresources_63 >> if_log_required_client_manager_uri_35_present_64
        if_log_required_client_manager_uri_35_present_64 >> rail.Label(
            'Yes') >> assignprojectresources_65 >> if_log_required_client_manager_uri_35_blank_66
        if_log_required_client_manager_uri_35_present_64 >> rail.Label(
            'No') >> if_log_required_client_manager_uri_35_blank_66

        if_log_required_client_manager_uri_35_blank_66 >> rail.Label(
            'Yes') >> assignprojectresources_67 >> log_email_bodyforclientprojectcreated_68
        if_log_required_client_manager_uri_35_blank_66 >> rail.Label(
            'No') >> log_email_bodyforclientprojectcreated_68 >> if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_69
        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_69 >> rail.Label(
            'Yes') >> log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70
        log_email_bodyforclientprojectcreatedbutclientmanagerpermissionnotassigned_70 >> if_log_l_o_g_userdisablednotfound_22_present_71
        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_69 >> rail.Label(
            'No') >> if_log_l_o_g_userdisablednotfound_22_present_71

        if_log_l_o_g_userdisablednotfound_22_present_71 >> rail.Label(
            'Yes') >> log_email_bodyforclientprojectcreatedbutuserdisablednotavailable_72 >> log_email_bodyforclientprojectcreated_f_i_n_a_l_73
        if_log_l_o_g_userdisablednotfound_22_present_71 >> rail.Label(
            'No') >> log_email_bodyforclientprojectcreated_f_i_n_a_l_73 >> if_log_clienturitosearch_49_present_77
        if_log_clienturitosearch_49_blank_clientnotpresentclientcreation_52 >> rail.Label(
            'No') >> if_log_clienturitosearch_49_present_77
        if_log_clienturitosearch_49_present_77 >> rail.Label(
            'Yes') >> get_client_details_78 >> log_existing_client_manager_uri_79 >> log_projectnametosearch_80
        log_projectnametosearch_80 >> if_request_statusreason_equals_to_inactive_81
        if_request_statusreason_equals_to_inactive_81 >> rail.Label(
            'Yes') >> update_client_82 >> if_clientmanager_is_in_disable_state
        if_clientmanager_is_in_disable_state >> rail.Label(
            'Yes') >> update_clientmanager1
        update_clientmanager1 >> searchprojectbyname_85
        if_clientmanager_is_in_disable_state >> rail.Label(
            'No') >> searchprojectbyname_85
        if_request_statusreason_equals_to_inactive_81 >> rail.Label(
            'No') >> update_client_84 >> if_clientmanager_is_in_disable_state
        searchprojectbyname_85 >> log_projecturiifexists_87 >> if_log_projecturiifexists_87_blank_88

        if_log_projecturiifexists_87_blank_88 >> rail.Label(
            'Yes') >> create_project_89 >> removeexistingprojectresources_90 >> log_l_o_gclientupdatedandprojectcreated_91
        log_l_o_gclientupdatedandprojectcreated_91 >> log_projecturi_94

        if_log_projecturiifexists_87_blank_88 >> rail.Label(
            'No') >> log_l_o_gclientupdatedandprojectcreated_93 >> log_projecturi_94
        log_projecturi_94 >> if_log_projecturiifexists_87_present_check_existing_projectforpreviousclientmanager_95

        if_log_projecturiifexists_87_present_check_existing_projectforpreviousclientmanager_95 >> rail.Label(
            'Yes') >> if_log_existing_client_manager_uri_79_present_96
        if_log_existing_client_manager_uri_79_present_96 >> rail.Label(
            'Yes') >> if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97
        if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97 >> rail.Label(
            'Yes') >> removepreviousclientmanagerfrom_projectteam_98 >> if_log_required_client_manager_uri_35_present_99

        if_log_existing_client_manager_uri_79_not_equals_to_dataloggerlog_required_client_manager_uri_35message_97 >> rail.Label(
            'No') >> if_log_required_client_manager_uri_35_present_99
        if_log_existing_client_manager_uri_79_present_96 >> rail.Label(
            'No') >> if_log_required_client_manager_uri_35_present_99

        if_log_projecturiifexists_87_present_check_existing_projectforpreviousclientmanager_95 >> rail.Label(
            'No') >> if_log_required_client_manager_uri_35_present_99

        if_log_required_client_manager_uri_35_present_99 >> rail.Label(
            'Yes') >> assignprojectresources_100 >> if_log_required_client_manager_uri_35_blank_101
        if_log_required_client_manager_uri_35_present_99 >> rail.Label(
            'No') >> if_log_required_client_manager_uri_35_blank_101

        if_log_required_client_manager_uri_35_blank_101 >> rail.Label(
            'Yes') >> assignprojectresources_102 >> log_l_o_gclientprojectcreatedupdated_103

        if_log_required_client_manager_uri_35_blank_101 >> rail.Label(
            'No') >> log_l_o_gclientprojectcreatedupdated_103 >> log_e_m_a_i_lbody_clientproject_updated_104
        log_e_m_a_i_lbody_clientproject_updated_104 >> if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_105

        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_105 >> rail.Label(
            'Yes') >> log_email_bodyforclientprojectcreated_106 >> if_log_l_o_g_userdisablednotfound_22_present_107

        if_log_l_o_g_clientmanagerpermissionnotassigned_34_present_105 >> rail.Label(
            'No') >> if_log_l_o_g_userdisablednotfound_22_present_107
        if_log_l_o_g_userdisablednotfound_22_present_107 >> rail.Label(
            'Yes') >> log_email_bodyforclientprojectcreated_108 >> log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109
        if_log_l_o_g_userdisablednotfound_22_present_107 >> rail.Label(
            'No') >> log_email_bodyforclientprojectcreatedupdated_f_i_n_a_l_109 >> log_final_e_m_a_i_l_b_o_d_y_110
        log_final_e_m_a_i_l_b_o_d_y_110 >> log_final_e_m_a_i_l_b_o_d_y_111

        if_log_clienturitosearch_49_present_77 >> rail.Label(
            'No') >> log_final_e_m_a_i_l_b_o_d_y_110 >> log_final_e_m_a_i_l_b_o_d_y_111

        if_request_code_present_46 >> rail.Label(
            'No') >> log_final_e_m_a_i_l_b_o_d_y_111 >> update_variable_112 >> send_mail_113 >> catch_114
        catch_114 >> update_variable_115 >> update_variable_116 >> send_mail_117 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
