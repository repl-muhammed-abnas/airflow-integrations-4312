import rail
from pendulum import datetime
from test_dags.email_trigger_test.instances import email_trigger_test as config
import airflow

with airflow.DAG(
    dag_id=config.dag_id,
    description=config.dag_description,
    start_date=datetime(2022, 4, 1, tz='UTC'),
    schedule=None,
    tags= ['invalid_email_trigger_test'],
    user_defined_macros=rail.dag.get_macros(),
    user_defined_filters=rail.dag.get_filters(),
    catchup=False,
    default_args={
        'owner': 'test_dags',
    },
    default_view="graph"
) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        """
            Sample Conf for reference
                {
                    "bcc_email_address": "test@domain.com",
                    "cc_email_address": "test@domain.com",
                    "to_email_address": "test@domain.com"
                }
        """

        rail.EmailOperator(
            task_id = "send_email",
            to = "{{dag_run.conf.to_email_address}}",
            cc = "{{dag_run.conf.cc_email_address}}",
            bcc = "{{dag_run.conf.bcc_email_address}}",
            html_content="<P>Ignore, this is test Email</P>",
            subject= "Testing"
        )
