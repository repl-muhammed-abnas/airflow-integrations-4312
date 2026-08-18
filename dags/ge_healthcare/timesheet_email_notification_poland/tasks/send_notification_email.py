from ge_healthcare.timesheet_email_notification_poland.utils import custom_methods
import rail


def send_notification(config, suffix):
    with rail.TaskGroup(group_id=f'send_notification_email_{suffix}', prefix_group_id=False):

        send_mail = rail.EmailOperator(
            task_id=f'send_mail_{suffix}',
            to=config.tenant_email,
            subject='Waiting for approval: {{ result("get_users_timesheets_length") }} timesheets',
            html_content='templates/emails/' + custom_methods.get_email_template(suffix),
            params= {
                "sso_link": config.sso_link
            }
        )

        return send_mail
