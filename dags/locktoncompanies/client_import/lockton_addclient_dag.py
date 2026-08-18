
from datetime import timedelta
from rail.lib.ecid import get_dagrun_ecid
from airflow.models import Variable
import rail

null=None

def create_dag(config):
    # pylint: disable=too-many-statements
    with rail.create_airflow_dag(
        dag_id=f'locktoncompanies_client_import_add_new_client_{config.instance}',
        description=f'Lockton_AddClient {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs_child,
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
                config.can_run_batch_task, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_clientcode_present'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_clientcode_present',
            end_task='catch_and_log_error',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_clientcode_present=rail.IfOperator(
            task_id='if_clientcode_present',
            test='''{{ dag_run.conf.ClientCode | is_truthy }}''',
            yes_task="if_cllientname_not_present",
            no_task="catch_and_log_error",
        )

        if_cllientname_not_present=rail.IfOperator(
            task_id='if_cllientname_not_present',
            test='''{{ dag_run.conf.ClientName | is_falsy }}''',
            yes_task="add_log_clientname_not_provided",
            no_task="if_results_present",
        )

        add_log_clientname_not_provided=rail.WriteLogOperator(
            task_id='add_log_clientname_not_provided',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - Client Name not provided in Input file"
            }
        )

        if_results_present=rail.IfOperator(
            task_id='if_results_present',
            test='''{{ dag_run.conf.results | is_truthy }}''',
            yes_task="add_log_clientcode_already_present",
            no_task="if_clientname_present",
        )

        add_log_clientcode_already_present=rail.WriteLogOperator(
            task_id='add_log_clientcode_already_present',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Skipped",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Skipped",
                "details": "Add Client - {{ dag_run_ecid() }} - Client Code already present - {{ dag_run.conf.ClientCode }}"
            }
        )

        if_clientname_present=rail.IfOperator(
            task_id='if_clientname_present',
            test='''{{ dag_run.conf.ClientName | is_truthy }}''',
            yes_task="search_client_in_replicon",
            no_task="catch_and_log_error",
        )

        def get_matching_client(response,dag_run):
            matching_client = {}
            for client in response['rows']:
                if client['cells'][0]['textValue'] == dag_run.conf['ClientName']:
                    matching_client = client
                    break
            return matching_client['cells'][1]['uri'] if matching_client else {}

        search_client_in_replicon=rail.RepliconServiceOperator(
            task_id='search_client_in_replicon',
            endpoint='/services/ClientListService1.svc/GetData',
            data={
                "page": "1",
                "pagesize": "100",
                "columnUris": [
                    "urn:replicon:client-list-column:name",
                    "urn:replicon:client-list-column:client"
                ],
                "sort": [],
                "filterExpression": {
                    "leftExpression": {
                    "filterDefinitionUri": "urn:replicon:client-list-filter:name"
                    },
                    "operatorUri": "urn:replicon:filter-operator:text-search",
                    "rightExpression": {
                    "value": {
                        "text": "{{dag_run.conf.ClientName}}"
                    }
                    }
                }
            },
            data_handler=get_matching_client
        )

        if_client_present=rail.IfOperator(
            task_id='if_client_present',
            test=lambda: bool(rail.result('search_client_in_replicon')),
            yes_task="add_log_clientname_already_present",
            no_task="if_client_not_found",
        )

        add_log_clientname_already_present=rail.WriteLogOperator(
            task_id='add_log_clientname_already_present',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - Client Name already present -  {{ dag_run.conf.ClientName }}"
            }
        )

        if_client_not_found=rail.IfOperator(
            task_id='if_client_not_found',
            test=lambda: not bool(rail.result('search_client_in_replicon')),
            yes_task="create_client",
            no_task="catch_and_log_error",
        )

        create_client=rail.RepliconServiceOperator(
            task_id='create_client',
            endpoint="/services/ClientService1.svc/CreateClientOrApplyModifications",
            data=lambda dag_run:{
                "target": null,
                "modifications": {
                    "nameToApply": {
                        "value": dag_run.conf['ClientName']
                    },
                    "codeToApply": {
                        "value": dag_run.conf['ClientCode']
                    },
                    "descriptionToApply": null,
                    "statusToApply": "true",
                    "clientContactToApply": null,
                    "clientAddressToApply": {
                        "address": null,
                        "city": {
                            "value": dag_run.conf['Clientcity']
                        } if dag_run.conf['Clientcity'] else null,
                        "stateProvince": {
                            "value": dag_run.conf['ClientState']
                        } if dag_run.conf['ClientState'] else null,
                        "country": null,
                        "zipPostalCode": null,
                        "phoneNumber": null,
                        "faxNumber": null,
                        "email": null,
                        "website": null
                    },
                    "billingAddressToApply": null,
                    "billingRatesToApply": null,
                    "clientManagerToApply": null,
                    "clientSharingToApply": null,
                    "expenseCodesToApply": null,
                    "customFieldsToApply": [
                        {
                            "customField": {
                                "uri": dag_run.conf['clientdnbnumberfielduri'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": dag_run.conf['ClientDnBNumber'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": dag_run.conf['parentcompanyfielduri'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": dag_run.conf['ParentCompany'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": dag_run.conf['parentdnbnumberfielduri'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": dag_run.conf['ParentDnBNumber'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        },
                        {
                            "customField": {
                                "uri": dag_run.conf['clientdnbnamefielduri'],
                                "name": null,
                                "groupUri": null
                            },
                            "text": dag_run.conf['ClientDnBName'],
                            "date": null,
                            "dropDownOption": null,
                            "number": null
                        }
                    ],
                    "taxProfileToApply": null
                },
                "clientModificationOptionUri": "urn:replicon:client-modification-option:save",
                "unitOfWorkId": get_dagrun_ecid(dag_run)
            }
        )

        add_log_client_created=rail.WriteLogOperator(
            task_id='add_log_client_created',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Added",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Added",
                "details": "Add Client - {{ dag_run_ecid() }} - Client Added -  {{ dag_run.conf.ClientName }}"
            }
        )

        if_clientprospectnonclient_not_present_in_replicon=rail.IfOperator(
            task_id='if_clientprospectnonclient_not_present_in_replicon',
            test='''{{ dag_run.conf.Clientprospectnonclient | is_truthy  and dag_run.conf.clientprospectnonclientoptionuri | is_falsy }}''',
            yes_task="add_log_clientprospectnonclient_not_exists",
            no_task="if_clientprospectnonclient_present_in_replicon",
        )

        add_log_clientprospectnonclient_not_exists=rail.WriteLogOperator(
            task_id='add_log_clientprospectnonclient_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.Clientprospectnonclient }}' " +
                    "for Client/Prospect/Non-Client does not exist"
            }
        )

        if_clientprospectnonclient_present_in_replicon=rail.IfOperator(
            task_id='if_clientprospectnonclient_present_in_replicon',
            test='''{{ dag_run.conf.clientprospectnonclientoptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_client_prospect_non_client",
            no_task="if_locktonbenefitsservicingoffice_not_present_in_replicon",
        )

        update_dropdown_value_for_client_prospect_non_client=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_client_prospect_non_client',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['clientprospectnonclientfielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['clientprospectnonclientoptionuri']
            }
        )

        if_locktonbenefitsservicingoffice_not_present_in_replicon=rail.IfOperator(
            task_id='if_locktonbenefitsservicingoffice_not_present_in_replicon',
            test='''{{ dag_run.conf.LocktonBenefitsServicingOffice | is_truthy  and dag_run.conf.locktonbenefitsservicingofficeoptionuri  | is_falsy }}''',
            yes_task="add_log_locktonbenefitsservicingoffice_not_exists",
            no_task="if_locktonbenefitsservicingoffice_present_in_replicon",
        )

        add_log_locktonbenefitsservicingoffice_not_exists=rail.WriteLogOperator(
            task_id='add_log_locktonbenefitsservicingoffice_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.LocktonBenefitsServicingOffice }}' " +
                    "for Lockton Benefits Servicing Office does not exist"
            }
        )

        if_locktonbenefitsservicingoffice_present_in_replicon=rail.IfOperator(
            task_id='if_locktonbenefitsservicingoffice_present_in_replicon',
            test='''{{ dag_run.conf.locktonbenefitsservicingofficeoptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_locktonbenefitsservicingoffice",
            no_task="if_retirementproducers_not_present_in_replicon",
        )

        update_dropdown_value_for_locktonbenefitsservicingoffice=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_locktonbenefitsservicingoffice',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['locktonbenefitsservicingofficefielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['locktonbenefitsservicingofficeoptionuri'],
            }
        )

        if_retirementproducers_not_present_in_replicon=rail.IfOperator(
            task_id='if_retirementproducers_not_present_in_replicon',
            test='''{{ dag_run.conf.RetirementProducers | is_truthy  and dag_run.conf.retirementproducersoptionuri | is_falsy }}''',
            yes_task="add_log_retirementproducers_not_exists",
            no_task="if_retirementproducers_present_in_replicon",
        )

        add_log_retirementproducers_not_exists=rail.WriteLogOperator(
            task_id='add_log_retirementproducers_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.RetirementProducers }}' for Retirement Producers does not exist"
            }
        )

        if_retirementproducers_present_in_replicon=rail.IfOperator(
            task_id='if_retirementproducers_present_in_replicon',
            test='''{{ dag_run.conf.retirementproducersoptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_retirementproducers",
            no_task="if_ebproducer_not_present_in_replicon",
        )

        update_dropdown_value_for_retirementproducers=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_retirementproducers',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['retirementproducersfielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['retirementproducersoptionuri'],
            }
        )

        if_ebproducer_not_present_in_replicon=rail.IfOperator(
            task_id='if_ebproducer_not_present_in_replicon',
            test='''{{ dag_run.conf.EBProducer | is_truthy  and dag_run.conf.ebproduceroptionuri | is_falsy }}''',
            yes_task="add_log_ebproducer_not_exists",
            no_task="if_ebproducer_present_in_replicon",
        )

        add_log_ebproducer_not_exists=rail.WriteLogOperator(
            task_id='add_log_ebproducer_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.EBProducer }}'  for EB Producer does not exist "
            }
        )

        if_ebproducer_present_in_replicon=rail.IfOperator(
            task_id='if_ebproducer_present_in_replicon',
            test='''{{ dag_run.conf.ebproduceroptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_ebproducer",
            no_task="if_pncproducer_not_present_in_replicon",
        )

        update_dropdown_value_for_ebproducer=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_ebproducer',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['ebproducerfielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['ebproduceroptionuri'],
            }
        )

        if_pncproducer_not_present_in_replicon=rail.IfOperator(
            task_id='if_pncproducer_not_present_in_replicon',
            test='''{{ dag_run.conf.PnCProducer | is_truthy  and dag_run.conf.pncproduceroptionuri | is_falsy }}''',
            yes_task="add_log_pncproducer_not_exists",
            no_task="if_pncproducer_present_in_replicon",
        )

        add_log_pncproducer_not_exists=rail.WriteLogOperator(
            task_id='add_log_pncproducer_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.PnCProducer }}' for  P&C Producer does not exist"
            }
        )

        if_pncproducer_present_in_replicon=rail.IfOperator(
            task_id='if_pncproducer_present_in_replicon',
            test='''{{ dag_run.conf.pncproduceroptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_pncproducer",
            no_task="if_locktonretirementservicingoffice_not_present_in_replicon",
        )

        update_dropdown_value_for_pncproducer=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_pncproducer',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['pncproducerfielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['pncproduceroptionuri'],
            }
        )

        if_locktonretirementservicingoffice_not_present_in_replicon=rail.IfOperator(
            task_id='if_locktonretirementservicingoffice_not_present_in_replicon',
            test='''{{ dag_run.conf.LocktonRetirementServicingOffice | is_truthy  and dag_run.conf.locktonretirementservicingofficeoptionuri | is_falsy }}''',
            yes_task="add_log_locktonretirementservicingoffice_not_exists",
            no_task="if_locktonretirementservicingoffice_present_in_replicon",
        )

        add_log_locktonretirementservicingoffice_not_exists=rail.WriteLogOperator(
            task_id='add_log_locktonretirementservicingoffice_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.LocktonRetirementServicingOffice }}' " +
                    "for Lockton Retirement Servicing Office does not exist"
            }
        )

        if_locktonretirementservicingoffice_present_in_replicon=rail.IfOperator(
            task_id='if_locktonretirementservicingoffice_present_in_replicon',
            test='''{{ dag_run.conf.locktonretirementservicingofficeoptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_locktonretirementservicingoffice",
            no_task="if_locktonpncservicingoffice_not_present_in_replicon",
        )

        update_dropdown_value_for_locktonretirementservicingoffice=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_locktonretirementservicingoffice',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['locktonretirementservicingofficefielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['locktonretirementservicingofficeoptionuri'],
            }
        )

        if_locktonpncservicingoffice_not_present_in_replicon=rail.IfOperator(
            task_id='if_locktonpncservicingoffice_not_present_in_replicon',
            test='''{{ dag_run.conf.locktonPnCServicingOffice | is_truthy  and dag_run.conf.locktonpncservicingofficeoptionuri  | is_falsy }}''',
            yes_task="add_log_locktonpncservicingoffice_not_exists",
            no_task="if_locktonpncservicingoffice_present_in_replicon",
        )

        add_log_locktonpncservicingoffice_not_exists=rail.WriteLogOperator(
            task_id='add_log_locktonpncservicingoffice_not_exists',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - UDF value: '{{ dag_run.conf.locktonPnCServicingOffice }}' " +
                    "for Lockton P&C Servicing Office does not exist "
            }
        )

        if_locktonpncservicingoffice_present_in_replicon=rail.IfOperator(
            task_id='if_locktonpncservicingoffice_present_in_replicon',
            test='''{{ dag_run.conf.locktonpncservicingofficeoptionuri | is_truthy }}''',
            yes_task="update_dropdown_value_for_locktonpncservicingoffice",
            no_task="catch_and_log_error",
        )

        update_dropdown_value_for_locktonpncservicingoffice=rail.RepliconServiceOperator(
            task_id='update_dropdown_value_for_locktonpncservicingoffice',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data=lambda dag_run:{
                "objectUri": rail.result('create_client')['uri'],
                "customFieldUri": dag_run.conf['locktonpncservicingofficefielduri'],
                "customFieldDropDownOptionUri": dag_run.conf['locktonpncservicingofficeoptionuri'],
            }
        )

        catch_and_log_error=rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.logslookuptable }}",
            message="na",
            severity="Error",
            properties={
                "jobid": "{{dag_run.conf.callerjobid}}",
                "locktonmasterid": "{{ dag_run.conf.ClientCode }}",
                "clientname": "{{ dag_run.conf.ClientName }}",
                "status": "Error",
                "details": "Add Client - {{ dag_run_ecid() }} - {{get_error_message()}}"
            }
        )

        log_to_sumo=rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            trigger_rule='all_done',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> catch_and_log_error
        can_run_batch_task >> rail.Label('No') >> if_clientcode_present
        if_clientcode_present >> rail.Label('Yes')  >> if_cllientname_not_present
        if_cllientname_not_present >> rail.Label('Yes')  >> add_log_clientname_not_provided >> catch_and_log_error
        if_cllientname_not_present >> rail.Label('No') >> if_results_present
        if_results_present >> rail.Label('Yes')  >> add_log_clientcode_already_present >> catch_and_log_error
        if_results_present >> rail.Label('No') >> if_clientname_present
        if_clientname_present >> rail.Label('Yes')  >> search_client_in_replicon >> if_client_present
        if_client_present >> rail.Label('Yes')  >> add_log_clientname_already_present >> catch_and_log_error
        if_client_present >> rail.Label('No') >> if_client_not_found
        if_client_not_found >> rail.Label('Yes')  >> create_client >> add_log_client_created >> if_clientprospectnonclient_not_present_in_replicon
        if_clientprospectnonclient_not_present_in_replicon >> rail.Label(
            'Yes') >> add_log_clientprospectnonclient_not_exists >> if_clientprospectnonclient_present_in_replicon
        if_clientprospectnonclient_not_present_in_replicon >> rail.Label(
            'No') >> if_clientprospectnonclient_present_in_replicon
        if_clientprospectnonclient_present_in_replicon >> rail.Label(
            'Yes') >> update_dropdown_value_for_client_prospect_non_client >> if_locktonbenefitsservicingoffice_not_present_in_replicon
        if_clientprospectnonclient_present_in_replicon >> rail.Label(
            'No') >> if_locktonbenefitsservicingoffice_not_present_in_replicon
        if_locktonbenefitsservicingoffice_not_present_in_replicon >> rail.Label(
            'Yes') >> add_log_locktonbenefitsservicingoffice_not_exists >> if_locktonbenefitsservicingoffice_present_in_replicon
        if_locktonbenefitsservicingoffice_not_present_in_replicon >> rail.Label(
            'No') >> if_locktonbenefitsservicingoffice_present_in_replicon
        if_locktonbenefitsservicingoffice_present_in_replicon >> rail.Label(
            'Yes')  >> update_dropdown_value_for_locktonbenefitsservicingoffice >> if_retirementproducers_not_present_in_replicon
        if_locktonbenefitsservicingoffice_present_in_replicon >> rail.Label(
            'No') >> if_retirementproducers_not_present_in_replicon
        if_retirementproducers_not_present_in_replicon >> rail.Label(
            'Yes') >> add_log_retirementproducers_not_exists >> if_retirementproducers_present_in_replicon
        if_retirementproducers_not_present_in_replicon >> rail.Label(
            'No') >> if_retirementproducers_present_in_replicon
        if_retirementproducers_present_in_replicon >> rail.Label(
            'Yes') >> update_dropdown_value_for_retirementproducers >> if_ebproducer_not_present_in_replicon
        if_retirementproducers_present_in_replicon >> rail.Label('No') >> if_ebproducer_not_present_in_replicon
        if_ebproducer_not_present_in_replicon >> rail.Label('Yes')  >> add_log_ebproducer_not_exists >> if_ebproducer_present_in_replicon
        if_ebproducer_not_present_in_replicon >> rail.Label('No') >> if_ebproducer_present_in_replicon
        if_ebproducer_present_in_replicon >> rail.Label('Yes')  >> update_dropdown_value_for_ebproducer >> if_pncproducer_not_present_in_replicon
        if_ebproducer_present_in_replicon >> rail.Label('No') >> if_pncproducer_not_present_in_replicon
        if_pncproducer_not_present_in_replicon >> rail.Label('Yes')  >> add_log_pncproducer_not_exists >> if_pncproducer_present_in_replicon
        if_pncproducer_not_present_in_replicon >> rail.Label('No') >> if_pncproducer_present_in_replicon
        if_pncproducer_present_in_replicon >> rail.Label(
            'Yes') >> update_dropdown_value_for_pncproducer >> if_locktonretirementservicingoffice_not_present_in_replicon
        if_pncproducer_present_in_replicon >> rail.Label('No') >> if_locktonretirementservicingoffice_not_present_in_replicon
        if_locktonretirementservicingoffice_not_present_in_replicon >> rail.Label(
            'Yes') >> add_log_locktonretirementservicingoffice_not_exists >> if_locktonretirementservicingoffice_present_in_replicon
        if_locktonretirementservicingoffice_not_present_in_replicon >> rail.Label(
            'No') >> if_locktonretirementservicingoffice_present_in_replicon
        if_locktonretirementservicingoffice_present_in_replicon >> rail.Label(
            'Yes') >> update_dropdown_value_for_locktonretirementservicingoffice >> if_locktonpncservicingoffice_not_present_in_replicon
        if_locktonretirementservicingoffice_present_in_replicon >> rail.Label('No') >> if_locktonpncservicingoffice_not_present_in_replicon
        if_locktonpncservicingoffice_not_present_in_replicon >> rail.Label(
            'Yes') >> add_log_locktonpncservicingoffice_not_exists >> if_locktonpncservicingoffice_present_in_replicon
        if_locktonpncservicingoffice_not_present_in_replicon >> rail.Label('No') >> if_locktonpncservicingoffice_present_in_replicon
        if_locktonpncservicingoffice_present_in_replicon >> rail.Label(
            'Yes') >> update_dropdown_value_for_locktonpncservicingoffice >> catch_and_log_error
        if_locktonpncservicingoffice_present_in_replicon >> rail.Label('No') >> catch_and_log_error >> log_to_sumo
        if_client_not_found >> rail.Label('No') >> catch_and_log_error
        if_clientname_present >> rail.Label('No') >> catch_and_log_error
        if_clientcode_present >> rail.Label('No') >> catch_and_log_error

    return dag

rail.for_each_instance(create_dag)
