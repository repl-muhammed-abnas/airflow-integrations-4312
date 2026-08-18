import rail
from pendulum import datetime
from airflow import DAG as af_dag
from test_dags.read_email_operator_test.instances import dev as config


def process_email_results():
    """
    Process the results from ReadEmailOperator.
    This function demonstrates how to work with the returned email data.
    """
    emails = rail.result('read_emails_body')

    if not emails:
        print("No emails found matching the criteria")
        return

    print(f"\n{'='*60}")
    print(f"Found {len(emails)} email(s)")
    print(f"{'='*60}\n")

    for i, email in enumerate(emails, 1):
        print(f"Email #{i}:")
        print(f"  ID: {email['id']}")
        print(f"  Subject: {email['subject']}")
        print(f"  From: {email['from']}")
        print(f"  To: {email['to']}")
        print(f"  Date: {email['date']}")
        print(f"  Body (plain): {email['body_plain'][:100]}..." if len(email['body_plain']) > 100 else f"  Body (plain): {email['body_plain']}")

        # Check for attachments
        if email.get('attachments'):
            print(f"  Attachments: {len(email['attachments'])}")
            for att in email['attachments']:
                print(f"    - {att['filename']} ({att['content_type']}, {att['size']} bytes)")

        # Check for saved artifacts
        if email.get('attachment_artifacts'):
            print(f"  Saved Artifacts:")
            for artifact in email['attachment_artifacts']:
                print(f"    - {artifact}")



with af_dag(
    dag_id=f'{config.dag_id}_{config.instance}',
    description=config.dag_description,
    start_date=datetime(2025, 1, 1, tz='UTC'),
    schedule=None,  # Manual trigger only
    tags=['test', 'email', 'read_email_operator'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    catchup=False,
    default_args={
        'owner': 'test_dags',
        'imap_conn_id': 'imap_test',  # Default IMAP connection
        'integration_type': 'generic'
    },
    default_view="graph"
) as dag:

    # View DAG run configuration
    view_dagrun_config = rail.ViewDagRunConfOperator(
        task_id="view_dagrun_config")

    read_emails_body = rail.ReadEmailOperator(
        task_id='read_emails_body',
        subject_pattern="{{ dag_run.conf | attr_or_default('subject_pattern', 'Change order reports') }}",
        body_pattern="{{ dag_run.conf | attr_or_default('body_pattern', 'Change order reports') }}",
        limit=5,
        mail_folder="{{ dag_run.conf | attr_or_default('mail_folder', 'INBOX') }}",
        max_emails_to_check=50
    )

    # Process body pattern results
    process_body_results = rail.PythonOperator(
        task_id='process_body_results',
        python_callable=process_email_results
    )

    # Set task dependencies
    view_dagrun_config >> read_emails_body >> process_body_results

