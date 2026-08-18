import uuid
from pendulum import datetime
import rail
from adtalem.time_off_auto_approval_webhooks.utils import response_filter,request_payload


def create_main_dag(config):
    with rail.create_airflow_dag(
        dag_id=f'adtalem_time_off_auto_approval_{config.instance}',
        description=f'Adtalem Time Off Auto Approval {config.instance}',
        company_key=config.company_key,
        start_date=datetime(2022, 1, 1),
        replicon_conn_id=config.replicon_conn_id,
        webhook_conf=[
            rail.WebhookConf(hmac_secret_var=config.webhook_shared_secrate)
        ],
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        get_all_time_off_details = rail.RepliconServiceOperator(
            task_id='get_all_time_off_details',
            endpoint='/services/TimeOffService1.svc/GetTimeOffDetails2',
            data={
                "timeOffUri": '{{ dag_run.conf.webhook.data.timeOff.uri }}'
            },
            response_filter=response_filter.get_time_off_data
        )

        check_time_off_name = rail.IfOperator(
            task_id='check_time_off_name',
            test='{{result("get_all_time_off_details").name == "FTO" }}',
            yes_task='get_actual_user_identity',
            no_task='finish'
        )

        finish = rail.EmptyOperator(task_id='finish')

        get_actual_user_identity = rail.RepliconServiceOperator(
            task_id='get_actual_user_identity',
            endpoint='/services/UserAccessControlService1.svc/GetMyActualUserIdentity',
        )

        get_currently_waiting_on_approvers = rail.RepliconServiceOperator(
            task_id='get_currently_waiting_on_approvers',
            endpoint='/services/TimeOffApprovalService1.svc/GetCurrentlyWaitingOnApprovers',
            data={
                "timeOffUri": '{{ dag_run.conf.webhook.data.timeOff.uri }}'
            }
        )

        check_uri = rail.IfOperator(
            task_id='check_uri',
            test= request_payload.check_uri,
            no_task='finish',
            yes_task='force_approve'
        )

        force_approve = rail.RepliconServiceOperator(
            task_id='force_approve',
            endpoint='/services/TimeOffApprovalService1.svc/ForceApprove',
            data={
                "timeOffUri": '{{ dag_run.conf.webhook.data.timeOff.uri }}',
                    "unitOfWorkId": str(uuid.uuid4()),
                    "comments": "Auto approving on behalf of the supervisor"
            }
        )

        get_user_details = rail.RepliconServiceOperator(
            task_id='get_user_details',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": '{{ result("get_all_time_off_details").uri }}'
            }
        )

        check_user_uri = rail.IfOperator(
            task_id='check_user_uri',
            test=lambda: rail.result("get_user_details")['uri'] and rail.result("get_user_details")['supervisor'],
            yes_task='get_supervisor_details',
            no_task='finish'
        )

        get_supervisor_details = rail.RepliconServiceOperator(
            task_id='get_supervisor_details',
            endpoint='/services/UserService1.svc/GetUserDetails',
            data={
                "userUri": '{{ result("get_user_details").supervisor.uri }}'
            }
        )

        check_email_address = rail.IfOperator(
            task_id='check_email_address',
            test=bool('{{ result("get_supervisor_details").emailAddress }}'),
            yes_task='send_email',
            no_task='finish'
        )

        send_email = rail.EmailOperator(
            task_id='send_email',
            to='{{ result("get_supervisor_details").emailAddress }}',
            subject='A time off booking has been approved : {{ result("get_user_details").firstName }} {{ result("get_user_details").lastName }}',
            html_content='''<p>Hi {{ result("get_supervisor_details").firstName}} {{ result("get_supervisor_details").lastName }},</p>
            <p>The following time off booking has been auto approved for {{ result("get_user_details").firstName}} \
                {{ result("get_user_details").lastName }} (Emp ID:{{ result("get_user_details").employeeId}}) <br /><br />
            Time Off Type: FTO <br /> From: {{result("get_all_time_off_details").start_date }}<br /> To:{{result("get_all_time_off_details").end_date }}<br />
            Total number of hours requested: {{result("get_all_time_off_details").duration }} <br /> <br />
            Regards,<br /> Deltek Inc.,</p>''',
        )

        get_all_time_off_details >> check_time_off_name >> rail.Label(
            "Yes") >> get_actual_user_identity >> get_currently_waiting_on_approvers >> check_uri

        check_time_off_name >> rail.Label(
            "No") >> finish

        check_uri >> rail.Label(
            "Yes") >> force_approve >> get_user_details >> check_user_uri

        check_uri >> rail.Label(
            "No") >> finish

        check_user_uri >> rail.Label(
            "Yes") >> get_supervisor_details >> check_email_address

        check_email_address >> rail.Label(
            "Yes") >> send_email >> finish

        check_email_address >> rail.Label(
            "No") >> finish

        check_user_uri >> rail.Label(
            "No") >> finish

    return dag


rail.for_each_instance(create_main_dag)
