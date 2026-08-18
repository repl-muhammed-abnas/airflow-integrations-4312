from pendulum import datetime
from airflow import DAG
from airflow.utils.email import send_mime_email, build_mime_message
import rail
from rail.lib.alerts_email import send_dagrun_alert_email

def create_process_case_dag(config):

    with DAG(
        dag_id=config.child_dag_id,
        description=config.dag_description,
        schedule=None,
        default_view='graph',
        start_date=datetime(2022, 1, 1),
        default_args={
            "salesforce_conn_id": config.salesforce_connection_id,
            'owner': 'salesforce_auto_response',
        },
        user_defined_macros=rail.dag.get_macros(),
        user_defined_filters=rail.dag.get_filters(),
        max_active_runs=1,
        on_failure_callback=send_dagrun_alert_email,
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_conf")

        def get_query_for_case_details(dag_run):
            return f"""select FIELDS(ALL) from Case where CaseNumber = '{dag_run.conf["item"]["CaseNumber"]}' LIMIT 200"""

        get_case_details = rail.InternalSalesforceQueryOperator(
            task_id="get_case_details",
            salesforce_conn_id=config.salesforce_connection_id,
            query=get_query_for_case_details
        )

        def validate_sender_callable():
            SuppliedEmail = rail.result('get_case_details')[
                'records'][0]['SuppliedEmail']
            return (('admin' not in SuppliedEmail)
                    and ('integrations' not in SuppliedEmail)
                    and ('integrationalerts' not in SuppliedEmail)
                    and ('do-not-reply' not in SuppliedEmail)
                    and ('airflow' not in SuppliedEmail))

        validate_sender = rail.IfOperator(
            task_id="validate_sender",
            test=validate_sender_callable,
            yes_task="update_case_owner_and_issue"
        )

        update_case_owner_and_issue = rail.InternalSalesforceUpdateObjectOperator(
            task_id="update_case_owner_and_issue",
            salesforce_conn_id=config.salesforce_connection_id,
            object_name="Case",
            operation='update',
            payload=lambda: [{
                "Id": f"{rail.result('get_case_details')['records'][0]['Id']}",
                "Dev_Issue_Subject__c": "Input the project name",
                "OwnerId": "00G0g000004HUBNEA4"
            }],
            batch_size=1
        )

        def get_query_email_details_for_case():
            return f"SELECT FIELDS(ALL) FROM EmailMessage WHERE ParentId = '{rail.result('get_case_details')['records'][0]['Id']}' LIMIT 200"

        get_case_emails = rail.InternalSalesforceQueryOperator(
            task_id="get_case_emails",
            salesforce_conn_id=config.salesforce_connection_id,
            query=get_query_email_details_for_case
        )

        def validate_case_email_callable():
            Subject: str = rail.result('get_case_emails')[
                'records'][0]['Subject']
            return (Subject.startswith("Automatic reply") or
                    ("Out" in Subject) or ("replicon" not in rail.result('get_case_details')['records'][0]['SuppliedEmail']))

        validate_case_email = rail.IfOperator(
            task_id="validate_case_email",
            test=validate_case_email_callable,
            no_task="send_standard_response"
        )

        def send_standard_response_callable():
            case_details = rail.result('get_case_details')['records'][0]
            email_message = rail.result("get_case_emails")['records'][-1]
            supplied_name = case_details['SuppliedName'].split(" ")[0] if case_details['SuppliedName'] else ""
            # pylint: disable=line-too-long

            html_body = f"""<p>Hi {supplied_name},<br /><br />Your request has been logged into our queue and the Salesforce case no. is #{case_details['CaseNumber']}.<br /><br />This request would be actioned as per the priority and request type sent, if this is a severity incident, please report an on-call incident.</p>
                    <p>We shall revert to this request as soon as possible.<br /><br />Thanks, <br />Integration team.</p>
                    <hr />{email_message['HtmlBody']}"""

            subject = f"{case_details['Subject']} {case_details['Thread_Id__c']}"
            body = html_body

            to = [case_details['SuppliedEmail']] + email_message['ToAddress'].split('; ')
            # removing `integrations@replicon.com` from to address
            if 'integrations@replicon.com' in email_message['ToAddress']:
                to.remove('integrations@replicon.com')
            cc_address = ['integrations@replicon.com']
            if email_message['CcAddress']:
                cc_address += email_message['CcAddress'].split('; ')

            msg, recipients = build_mime_message(
                mail_from= config.FROM_ADDR,
                to=set(to),
                cc=set(cc_address),
                subject=subject,
                html_content=body)
            send_mime_email(e_from=config.FROM_ADDR, e_to=recipients, mime_msg=msg)

        send_standard_response = rail.PythonOperator(
            task_id="send_standard_response",
            python_callable=send_standard_response_callable
        )

        get_case_details >> validate_sender >> rail.Label(
            "Yes") >> update_case_owner_and_issue >> get_case_emails >> validate_case_email >> rail.Label(
            "Send response") >> send_standard_response
    return dag


rail.for_each_instance(create_process_case_dag)
