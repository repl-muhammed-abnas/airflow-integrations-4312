from datetime import datetime, timedelta
import rail


def create_dag_instance(config):
    with rail.create_airflow_dag(
        dag_id=config.child_dag_id,
        description='AP Invoice Sync Child - Process Individual Invoice to Procore',
        integration_type='generic',
        company_key=config.instance,
        max_active_runs=config.child_dag_max_active_runs,
        is_paused_upon_creation=config.is_paused_upon_creation,
        default_args={
            'procore_conn_id': config.procore_conn_id,
            'computerease_conn_id': config.computerease_conn_id,
            'execution_timeout': timedelta(days=config.execution_timeout_days)
        }
    ) as dag:

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='has_project_id',
            end_task='catch_unhandled_error',
            execution_timeout=timedelta(days=config.execution_timeout_days)
        )

        has_project_id = rail.IfOperator(
            task_id='has_project_id',
            test=lambda dag_run:  dag_run.conf['project_id'] and dag_run.conf['project_id'] != None,
            yes_task='fetch_work_order_contract_id',
            no_task='has_sync_failed'
        )

        fetch_work_order_contract_id = rail.ProcoreApiOperator(
            task_id='fetch_work_order_contract_id',
            endpoint='/work_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=lambda resp, dag_run: [{
                'work_order_contract_id': work_order_contract['id'], 'accounting_method': work_order_contract['accounting_method']
            } for work_order_contract in resp
                if work_order_contract['origin_id'] == f"CE_{dag_run.conf['ap_invoice']['value'][0]['PO_Number']}" and work_order_contract['status'].lower() == 'approved']
        )

        has_work_order_contract_id = rail.IfOperator(
            task_id='has_work_order_contract_id',
            test='{{ result("fetch_work_order_contract_id") | length > 0 }}',
            yes_task='fetch_work_order_contract_line_items',
            no_task='fetch_purchase_order_contract_id'
        )

        fetch_work_order_contract_line_items = rail.ProcoreApiOperator(
            task_id='fetch_work_order_contract_line_items',
            endpoint="work_order_contracts/{{result('fetch_work_order_contract_id')[0]['work_order_contract_id']}}/line_items",
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            }
        )

        check_worker_order_accounting_method = rail.IfOperator(
            task_id='check_worker_order_accounting_method',
            test=lambda: 'accounting_method' in rail.result('fetch_work_order_contract_id')[
                0] and rail.result('fetch_work_order_contract_id')[0]['accounting_method'] == 'amount',
            yes_task='fetch_work_order_contract_detail_line_items',
            no_task='fetch_billing_period_for_project'
        )

        def get_wbs_code(phase, category, cost_type):
            wbs_code = ''
            if phase and category and cost_type:
                wbs_code = f'{phase}-{category}.{cost_type}'
            elif phase and cost_type:
                wbs_code = f'{phase}.{cost_type}'
            elif category and cost_type:
                wbs_code = f'{category}.{cost_type}'
            elif cost_type:
                wbs_code = cost_type
            return wbs_code

        fetch_work_order_contract_detail_line_items = rail.ProcoreApiOperator(
            task_id='fetch_work_order_contract_detail_line_items',
            endpoint="work_order_contracts/{{result('fetch_work_order_contract_id')[0]['work_order_contract_id']}}/line_item_contract_details",
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            }
        )

        fetch_purchase_order_contract_id = rail.ProcoreApiOperator(
            task_id='fetch_purchase_order_contract_id',
            endpoint='/purchase_order_contracts',
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            },
            data_handler=lambda resp, dag_run: [{
                'purchase_order_contract_id': purchase_order_contract['id'], 'accounting_method': purchase_order_contract['accounting_method']
            } for purchase_order_contract in resp
                if purchase_order_contract['origin_id'] == f"CE_{dag_run.conf['ap_invoice']['value'][0]['PO_Number']}" and purchase_order_contract['status'].lower() == 'approved']
        )

        has_purchase_order_contract_id = rail.IfOperator(
            task_id='has_purchase_order_contract_id',
            test='{{ result("fetch_purchase_order_contract_id") | length > 0 }}',
            yes_task='fetch_purchase_order_contract_line_items',
            no_task='has_sync_failed'
        )

        fetch_purchase_order_contract_line_items = rail.ProcoreApiOperator(
            task_id='fetch_purchase_order_contract_line_items',
            endpoint="purchase_order_contracts/{{result('fetch_purchase_order_contract_id')[0]['purchase_order_contract_id']}}/line_items",
            method='GET',
            query_params=lambda dag_run: {
                'project_id': dag_run.conf['project_id']
            }
        )

        fetch_billing_period_for_project = rail.ProcoreApiOperator(
            task_id='fetch_billing_period_for_project',
            endpoint="projects/{{dag_run.conf['project_id']}}/billing_periods",
            method='GET'
        )

        def check_billing_period_id(dag_run):
            billing_periods = rail.result('fetch_billing_period_for_project')
            invoice_date = dag_run.conf['ap_invoice']['value'][0]['Invoice_Date']
            parsed_invoice_date = datetime.strptime(invoice_date, "%m/%d/%y")
            formatted_invoice_date = parsed_invoice_date.strftime("%Y-%m-%d")
            for billing_period in billing_periods:
                start_date = datetime.strptime(
                    billing_period['start_date'], "%Y-%m-%d").strftime("%Y-%m-%d")
                end_date = datetime.strptime(
                    billing_period['end_date'], "%Y-%m-%d").strftime("%Y-%m-%d")
                if start_date <= formatted_invoice_date <= end_date and billing_period['status'] == 'open':
                    dag_run.conf['billing_period'] = billing_period
                    return True

        has_billing_period_id = rail.IfOperator(
            task_id='has_billing_period_id',
            test=lambda dag_run: check_billing_period_id(dag_run),
            yes_task='sync_ap_invoice',
            no_task='has_sync_failed'
        )

        def get_line_items(item_name, accounting_method, contract_line_items, distributed_description, wbs_code, balance_amount, retention_amount, retention_percentage, invoice_description):
            items = []
            for item in contract_line_items:
                sov_description = item.get('description', '').strip()
                if (sov_description == distributed_description or item_name == sov_description) and wbs_code == item['wbs_code']['flat_code']:
                    amount = float(item['amount'])
                    quantity = float(item['quantity'])
                    if amount > 0 and (balance_amount + retention_amount) > 0:
                        if amount >= (balance_amount + retention_amount):
                            qty = quantity * \
                                (balance_amount + retention_amount) / amount
                            if accounting_method == 'unit':
                                invoice_item = {
                                    "line_item_id": item['id'],
                                    "work_completed_retainage_retained_this_period": str(round(retention_amount, 2)),
                                    "work_completed_this_period_quantity": str(round(qty, 2)),
                                    "work_completed_this_period": str(round(balance_amount + retention_amount, 2)),
                                    "status": "approved",
                                    "comment": invoice_description
                                }
                            else:
                                invoice_item = {
                                    "line_item_id": item['id'],
                                    "work_completed_retainage_retained_this_period": str(round(retention_amount, 2)),
                                    "work_completed_this_period": str(round(balance_amount + retention_amount, 2)),
                                    "status": "approved",
                                    "comment": invoice_description
                                }

                            items.append(invoice_item)
                            balance_amount = 0
                            retention_amount = 0
                            break
                        else:
                            retention_amt = amount * retention_percentage
                            balance_amt = amount - retention_amt
                            balance_amount -= balance_amt
                            retention_amount -= retention_amt
                            qty = quantity * \
                                (balance_amt + retention_amt) / amount
                            if accounting_method == 'unit':
                                invoice_item = {
                                    "line_item_id": item['id'],
                                    "work_completed_retainage_retained_this_period": str(round(retention_amt, 2)),
                                    "work_completed_this_period_quantity": str(round(qty, 2)),
                                    "work_completed_this_period": str(round(balance_amt + retention_amt, 2)),
                                    "status": "approved",
                                    "comment": invoice_description
                                }
                            else:
                                invoice_item = {
                                    "line_item_id": item['id'],
                                    "work_completed_retainage_retained_this_period": str(round(retention_amt, 2)),
                                    "work_completed_this_period": str(round(balance_amt + retention_amt, 2)),
                                    "status": "approved",
                                    "comment": invoice_description
                                }
                            items.append(invoice_item)
            return [items, balance_amount, retention_amount]

        def get_ap_invoice_payload(dag_run):
            invoice_details = dag_run.conf['ap_invoice']['value']
            fetch_work_order_contract_id = rail.result(
                'fetch_work_order_contract_id')
            work_order_contract_id = fetch_work_order_contract_id[0]['work_order_contract_id'] if len(
                fetch_work_order_contract_id) > 0 else None
            commitment_contract_id = ''
            status = "approved" if dag_run.conf['ap_invoice']['value'][0]['Status'].lower(
            ) != "hold" else "draft"
            items = []

            for item in invoice_details:
                invoice_description = item.get('Description', '')
                phase = item['Phase']
                category = item['Category']
                cost_type = item['Cost_Type']
                retention_amount = float(item['Retention_Amount'])
                balance_amount = float(item['Balance_Payable'])
                retention_percentage = float(item['Retention_Pct']) / 100
                distributed_description = item['Distribution_Description']
                item_name = item['Item_Name']

                wbs_code = get_wbs_code(phase, category, cost_type)

                if work_order_contract_id:
                    accounting_method = rail.result('fetch_work_order_contract_id')[
                        0]['accounting_method']
                    commitment_contract_id = work_order_contract_id
                    work_order_contract_line_items = rail.result(
                        'fetch_work_order_contract_line_items')
                    work_order_contract_detail_line_items = rail.result(
                        'fetch_work_order_contract_detail_line_items')

                    if accounting_method == 'unit':
                        item_details, balance_amount, retention_amount = get_line_items(
                            item_name, accounting_method, work_order_contract_line_items, distributed_description, wbs_code, balance_amount, retention_amount, retention_percentage, invoice_description)
                        items += item_details
                    else:
                        for work_order_contract_line_item in work_order_contract_line_items:
                            sov_description = work_order_contract_line_item.get('description', '').strip(
                            )
                            if sov_description == distributed_description and wbs_code == work_order_contract_line_item['wbs_code']['flat_code']:
                                line_item_id = work_order_contract_line_item['id']
                            else:
                                continue

                            for work_order_contract_detail_line_item in work_order_contract_detail_line_items:
                                if work_order_contract_detail_line_item['line_item_id'] == line_item_id:
                                    amount = float(work_order_contract_detail_line_item['amount']) - float(
                                        work_order_contract_detail_line_item['billed_to_date'])
                                    if amount > 0 and (balance_amount + retention_amount) > 0:
                                        if amount >= (balance_amount + retention_amount):
                                            invoice_item = {
                                                "detail_line_item_id": work_order_contract_detail_line_item['id'],
                                                "work_completed_retainage_retained_this_period": str(round(retention_amount, 2)),
                                                "work_completed_this_period": str(round(balance_amount + retention_amount, 2)),
                                                "status": "approved",
                                                "comment": invoice_description
                                            }
                                            items.append(invoice_item)
                                            balance_amount = 0
                                            retention_amount = 0
                                            break
                                        else:
                                            retention_amt = amount * retention_percentage
                                            balance_amt = amount - retention_amt
                                            balance_amount -= balance_amt
                                            retention_amount -= retention_amt
                                            invoice_item = {
                                                "detail_line_item_id": work_order_contract_detail_line_item['id'],
                                                "work_completed_retainage_retained_this_period": str(round(retention_amt, 2)),
                                                "work_completed_this_period": str(round(balance_amt + retention_amt, 2)),
                                                "status": "approved",
                                                "comment": invoice_description
                                            }
                                            items.append(invoice_item)

                else:
                    commitment_contract_id = rail.result('fetch_purchase_order_contract_id')[
                        0]['purchase_order_contract_id']
                    accounting_method = rail.result('fetch_purchase_order_contract_id')[
                        0]['accounting_method']
                    purchase_order_contract_line_items = rail.result(
                        'fetch_purchase_order_contract_line_items')

                    item_details, balance_amount, retention_amount = get_line_items(
                        item_name, accounting_method, purchase_order_contract_line_items, distributed_description, wbs_code, balance_amount, retention_amount, retention_percentage, invoice_description)
                    items += item_details

                if (balance_amount + retention_amount) > 0:
                    dag_run.conf['reason'] = 'Computerease invoice amount is greator than the amount that can be invoced in Procore'

            payload = {
                "project_id": dag_run.conf['project_id'],
                "commitment_id": commitment_contract_id,
                "requisition": {
                    "items": items,
                    "period_id": dag_run.conf['billing_period']['id'],
                    "requisition_start": dag_run.conf['billing_period']['start_date'],
                    "requisition_end": dag_run.conf['billing_period']['end_date'],
                    "billing_date": datetime.strptime(dag_run.conf['ap_invoice']['value'][0]['Invoice_Date'], "%m/%d/%y").strftime("%Y-%m-%d"),
                    "invoice_number": dag_run.conf['ap_invoice']['key'],
                    "payment_date": datetime.strptime(dag_run.conf['ap_invoice']['value'][0]['Due_Date'], "%m/%d/%y").strftime("%Y-%m-%d"),
                    "status": status,
                    "origin_id": f'CE_{dag_run.conf["ap_invoice"]["key"]}'
                }
            }

            dag_run.conf['payload'] = payload

            return payload

        sync_ap_invoice = rail.ProcoreApiOperator(
            task_id='sync_ap_invoice',
            endpoint='/requisitions',
            method='POST',
            version='1.1',
            data=get_ap_invoice_payload
        )

        def get_error_details(dag_run):
            try:
                project_id = dag_run.conf['project_id']
                invoice_number = dag_run.conf['ap_invoice']['key']
                commitment_contract_id = 0
                reason = ''
                billing_period_id = ''
                payload = ''

                if not project_id:
                    reason = 'Project does not exist'
                elif len(rail.result('fetch_work_order_contract_id')) == 0 and len(rail.result('fetch_purchase_order_contract_id')) == 0:
                    reason = 'Contract details not found or contract is not approved'
                else:
                    if len(rail.result('fetch_work_order_contract_id')) > 0:
                        commitment_contract_id = rail.result('fetch_work_order_contract_id')[
                            0]['work_order_contract_id']
                    else:
                        commitment_contract_id = rail.result('fetch_purchase_order_contract_id')[
                            0]['purchase_order_contract_id']

                    if 'reason' in dag_run.conf:
                        reason = dag_run.conf['reason']

                    if 'billing_period' in dag_run.conf:
                        billing_period_id = dag_run.conf['billing_period']['id']
                    else:
                        reason += 'Billing period does not exist'

                    if 'payload' in dag_run.conf:
                        payload = dag_run.conf['payload']

                err = rail.render_template('{{ get_error_message() }}')
                if type(err) == str:
                    status = 'Error'
                    reason += err
                else:
                    status = err['response']['status_code'] \
                        if err.get('response') else 'Error'
                    reason += err['response']['json']['error']['reason'] \
                        if err.get('response') else err
            except:
                status = "An exception occurred"

            return {
                'invoice_number': invoice_number,
                'project_id': project_id,
                'commitment_contract_id': commitment_contract_id,
                'billing_period_id': billing_period_id,
                'payload': payload,
                'status': status,
                'reason': reason
            }

        def check_for_failure_and_reason(dag_run):
            project_id = dag_run.conf['project_id']
            invoice_number = dag_run.conf['ap_invoice']['key']
            commitment_contract_id = 0
            reason = ''
            billing_period_id = ''

            if not project_id:
                reason = 'Project does not exist'
            elif len(rail.result('fetch_work_order_contract_id')) == 0 and len(rail.result('fetch_purchase_order_contract_id')) == 0:
                reason = 'Contract details not found'
            else:
                if len(rail.result('fetch_work_order_contract_id')) > 0:
                    commitment_contract_id = rail.result('fetch_work_order_contract_id')[
                        0]['work_order_contract_id']
                else:
                    commitment_contract_id = rail.result('fetch_purchase_order_contract_id')[
                        0]['purchase_order_contract_id']

                if 'reason' in dag_run.conf:
                    reason = dag_run.conf['reason']

                if 'billing_period' in dag_run.conf:
                    billing_period_id = dag_run.conf['billing_period']['id']
                else:
                    reason += 'Billing period does not exist'

            return True if reason != '' else False

        has_sync_failed = rail.IfOperator(
            task_id='has_sync_failed',
            test=lambda dag_run: check_for_failure_and_reason(dag_run),
            yes_task='catch_error',
            no_task='catch_unhandled_error'
        )

        catch_error = rail.WriteLogOperator(
            task_id='catch_error',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        catch_unhandled_error = rail.WriteLogOperator(
            task_id='catch_unhandled_error',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            severity='Error/Exception',
            properties=get_error_details
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
            trigger_rule='all_done'
        )

        batch_task >> catch_unhandled_error >> log_to_sumo
        batch_task >> has_project_id

        has_project_id >> rail.Label(
            'Yes') >> fetch_work_order_contract_id >> has_work_order_contract_id
        has_project_id >> rail.Label('No') >> has_sync_failed

        has_work_order_contract_id >> rail.Label(
            'Yes') >> fetch_work_order_contract_line_items >> check_worker_order_accounting_method
        has_work_order_contract_id >> rail.Label(
            'No') >> fetch_purchase_order_contract_id >> has_purchase_order_contract_id

        check_worker_order_accounting_method >> rail.Label('Yes') >> fetch_work_order_contract_detail_line_items >> fetch_billing_period_for_project \
            >> has_billing_period_id >> sync_ap_invoice >> has_sync_failed
        check_worker_order_accounting_method >> rail.Label(
            'No') >> fetch_billing_period_for_project >> has_billing_period_id

        has_billing_period_id >> rail.Label(
            'Yes') >> sync_ap_invoice >> has_sync_failed
        has_billing_period_id >> rail.Label('No') >> has_sync_failed

        has_purchase_order_contract_id >> rail.Label('Yes') >> fetch_purchase_order_contract_line_items >> fetch_billing_period_for_project \
            >> has_billing_period_id >> sync_ap_invoice >> has_sync_failed
        has_purchase_order_contract_id >> rail.Label('No') >> has_sync_failed

        has_sync_failed >> rail.Label(
            'Yes') >> catch_error >> catch_unhandled_error >> log_to_sumo
        has_sync_failed >> rail.Label(
            'No') >> catch_unhandled_error >> log_to_sumo

        return dag


rail.for_each_instance(create_dag_instance)
