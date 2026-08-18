from pike.add_billing_rates_global_level.utils import request_payload
import rail

null=None

def create_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'pike_adding_billing_rates_global_level_child_{config.instance}',
        description=f'Pike Adding Billing Rates Global level Child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.max_active_runs,
        default_args={
            'sftp_conn_id': config.sftp_conn_id,
        },
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        is_action_available = rail.IfOperator(
            task_id='is_action_available',
            test='{{ dag_run.conf.item.action | is_truthy }}',
            yes_task='process_billing_rate',
            no_task='log_action_not_available'
        )

        log_action_not_available = rail.WriteLogOperator(
            task_id='log_action_not_available',
            message="Action not Available",
            severity="Skipped",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Action not Available"
            }
        )

        process_billing_rate = rail.EmptyOperator(
            task_id='process_billing_rate'
        )

        is_action_add = rail.IfOperator(
            task_id='is_action_add',
            test='{{ dag_run.conf.item.action == "Add" }}',
            yes_task='is_billing_rate_available'
        )

        is_billing_rate_available = rail.IfOperator(
            task_id='is_billing_rate_available',
            test='{{ dag_run.conf.billing_rate_uri | is_truthy }}',
            yes_task='get_company_billing_rate_details',
            no_task='get_currency_uri'
        )

        get_company_billing_rate_details = rail.RepliconServiceOperator(
            task_id='get_company_billing_rate_details',
            endpoint='/services/BillingRateService1.svc/GetCompanyBillingRateDetails',
            data={
                "companyBillingRateUri": '{{ dag_run.conf.billing_rate_uri }}',
                "asOfDate": null
            }
        )

        is_billrate_equals_amount = rail.IfOperator(
            task_id='is_billrate_equals_amount',
            test=lambda dag_run: bool(
                        rail.result("get_company_billing_rate_details")["effectiveBillingRate"]["value"]["amount"]
                            == float(dag_run.conf["item"]["billrate"])),
            yes_task='log_already_available',
            no_task='update_company_billing_rate_amount'
        )

        log_already_available = rail.WriteLogOperator(
            task_id='log_already_available',
            message="Already Present",
            severity="Exception",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Already Present"
            }
        )

        update_company_billing_rate_amount = rail.RepliconServiceOperator(
            task_id='update_company_billing_rate_amount',
            endpoint='/services/BillingRateService1.svc/UpdateCompanyBillingRateAmount',
            data=request_payload.get_update_billing_rate_amount_payload
        )

        log_billing_rate_update = rail.WriteLogOperator(
            task_id='log_billing_rate_update',
            message="Updated the billing rate",
            severity="Success",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Updated the billing rate"
            }
        )

        get_currency_uri = rail.RepliconServiceOperator(
            task_id="get_currency_uri",
            endpoint="/services/CurrencyService2.svc/GetAllCurrencies",
            response_filter=lambda response: rail.find_first_by_attr_and_get_attr(response.json()['d'], 'displayText', '$', 'uri')
        )

        add_billing_rate = rail.RepliconServiceOperator(
            task_id='add_billing_rate',
            endpoint='/services/BillingRateService1.svc/PutCompanyBillingRate',
            data=request_payload.get_add_billing_rate_payload
        )

        log_add_billing_rate = rail.WriteLogOperator(
            task_id='log_add_billing_rate',
            message="Added",
            severity="Success",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Added"
            }
        )

        is_action_disable = rail.IfOperator(
            task_id='is_action_disable',
            test='{{ dag_run.conf.item.action == "Disable" }}',
            yes_task='is_billing_rate_available_2'
        )

        is_billing_rate_available_2 = rail.IfOperator(
            task_id='is_billing_rate_available_2',
            test='{{ dag_run.conf.billing_rate_uri | is_truthy }}',
            yes_task='disable_billing_rate',
            no_task='log_billing_rate_not_present'
        )

        disable_billing_rate = rail.RepliconServiceOperator(
            task_id='disable_billing_rate',
            endpoint='/services/BillingRateService1.svc/Disable',
            data={
                "billingRateUri": '{{ dag_run.conf.billing_rate_uri }}'
            }
        )

        log_disable_billing_rate = rail.WriteLogOperator(
            task_id='log_disable_billing_rate',
            message="Disabled",
            severity="Success",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Disabled"
            }
        )

        log_billing_rate_not_present = rail.WriteLogOperator(
            task_id='log_billing_rate_not_present',
            message="Not Present",
            severity="Skipped",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": "Not Present"
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity="Error",
            properties={
                "billing_rate_name": "{{ dag_run.conf.item.Billingratename }}",
                "action": '{{ dag_run.conf.item.action }}',
                "results": '{{ get_error_message() }}'
            }
        )

        is_action_available >> rail.Label("Yes") >> process_billing_rate >> is_action_add >> rail.Label("Yes") >> is_billing_rate_available
        is_billing_rate_available >> rail.Label("Yes") >> get_company_billing_rate_details >> is_billrate_equals_amount

        is_billrate_equals_amount >> rail.Label("Yes") >> log_already_available >> catch_and_log_errors
        is_billrate_equals_amount >> rail.Label("No") >> update_company_billing_rate_amount >> log_billing_rate_update >> catch_and_log_errors

        is_billing_rate_available >> rail.Label("No") >> get_currency_uri >> add_billing_rate >> log_add_billing_rate >> catch_and_log_errors

        process_billing_rate >> is_action_disable >> rail.Label("Yes") >> is_billing_rate_available_2

        is_billing_rate_available_2 >> rail.Label("Yes") >> disable_billing_rate >> log_disable_billing_rate >> catch_and_log_errors
        is_billing_rate_available_2 >> rail.Label("No") >> log_billing_rate_not_present >> catch_and_log_errors

        is_action_available >> rail.Label("No") >> log_action_not_available >> catch_and_log_errors

    return dag

rail.for_each_instance(create_dag)
