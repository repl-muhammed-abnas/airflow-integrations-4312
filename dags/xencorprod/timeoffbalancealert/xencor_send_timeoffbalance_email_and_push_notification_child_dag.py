
from datetime import timedelta, datetime
import json
import base64
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=f'xencorprod_timeoffbalancealert_xencor_send_timeoffbalance_email_and_push_notification_child_{config.instance}',
        description=f'Xencor send timeoffbalance email and push notification - child {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
        default_args={
        },
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='policy_schedule_list'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='policy_schedule_list',
            end_task='log_to_sumo',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        def get_policy_scedule_list(dag_run):
            parsed_policy = []
            policies = json.loads(dag_run.conf['policySetSchedule'])
            for policy in policies:
                parsed_policy.append({
                    "description": policy['description'],
                    "effective_date": str(policy['effectiveDate']['day']).zfill(2) + "-" + str(policy['effectiveDate']['month']).zfill(2) + "-" + str(policy['effectiveDate']['year']),
                    "day": policy['effectiveDate']['day'],
                    "month": policy['effectiveDate']['month'],
                    "year": policy['effectiveDate']['year'],
                    "policy_set": policy['policySet']
                })
            return parsed_policy

        policy_schedule_list = rail.PythonOperator(
            task_id='policy_schedule_list',
            # pylint: disable=unnecessary-lambda
            python_callable=lambda dag_run: get_policy_scedule_list(dag_run)
        )

        def get_effective_policy_date(task_name):
            policy_set = rail.result(task_name)
            effective_dates = []
            current_date = datetime.now()
            for policy in policy_set:
                effective_date = datetime.strptime(
                    policy['effective_date'], '%d-%m-%Y')
                if effective_date <= current_date:
                    effective_dates.append(effective_date)
            current_effective_date = max(
                effective_dates) if effective_dates else None
            return current_effective_date.strftime('%d-%m-%Y') if current_effective_date else None

        log_gettheeffectivepolicydate_5 = rail.PythonOperator(
            task_id='log_gettheeffectivepolicydate_5',
            python_callable=lambda: get_effective_policy_date(
                'policy_schedule_list')
        )

        if_log_gettheeffectivepolicydate_5_present_6 = rail.IfOperator(
            task_id='if_log_gettheeffectivepolicydate_5_present_6',
            test='''{{ result('log_gettheeffectivepolicydate_5') | is_truthy }}''',
            yes_task="parse_json_policy_setfromthepolicyschedule_7",
            no_task="log_maxbalancevalue_9",
        )

        def get_effective_policy_set(list_task_name):
            policy_set = rail.result(list_task_name)
            effective_policies = list(filter(
                lambda item: item['effective_date'] == rail.result('log_gettheeffectivepolicydate_5'), policy_set))
            return effective_policies[0]['policy_set'] if effective_policies else None

        parse_json_policy_setfromthepolicyschedule_7 = rail.PythonOperator(
            task_id='parse_json_policy_setfromthepolicyschedule_7',
            python_callable=lambda: get_effective_policy_set(
                'policy_schedule_list')
        )

        def get_additional_parameter(policy_task_name):
            policy_set = rail.result(policy_task_name)
            timeoff_balance_event_scripts = policy_set['timeOffBalanceEventScripts'] if policy_set else [
            ]
            effective_policies = list(filter(
                lambda item: item['script']['name'] == 'Max Balance Limit', timeoff_balance_event_scripts))
            additional_parameters = []
            if effective_policies:
                for effective_policy in effective_policies:
                    for keyuri in effective_policy['additionalParameters']:
                        additional_parameters.append(keyuri)
            print("additional_parameters", additional_parameters)
            return additional_parameters

        parse_json_additionalparameterfromabovepolicyschedule_max_balance_8 = rail.PythonOperator(
            task_id='parse_json_additionalparameterfromabovepolicyschedule_max_balance_8',
            python_callable=lambda: get_additional_parameter(
                'parse_json_policy_setfromthepolicyschedule_7')
        )

        def get_max_balance_amount(policy_date_task_name):
            max_balance = 0
            effective_ploicy_date = rail.result(policy_date_task_name)
            if effective_ploicy_date:
                max_balance_info = rail.find_first_by_attr_and_get_attr(rail.result(
                    'parse_json_additionalparameterfromabovepolicyschedule_max_balance_8'), 'keyUri', "urn:replicon:script-key:parameter:daily-maximum-balance-amount", 'value')
                max_balance = max_balance_info['number'] if max_balance_info else 0
            return max_balance

        log_maxbalancevalue_9 = rail.PythonOperator(
            task_id='log_maxbalancevalue_9',
            python_callable=lambda:  get_max_balance_amount(
                'log_gettheeffectivepolicydate_5')
        )

        if_log_maxbalancevalue_9_equals_to_0_10 = rail.IfOperator(
            task_id='if_log_maxbalancevalue_9_equals_to_0_10',
            test='''{{ result('log_maxbalancevalue_9') == 0 }}''',
            yes_task="stop_11",
            no_task="if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12",
        )

        stop_11 = rail.EmptyOperator(
            task_id='stop_11',

        )

        def is_balance_greater_than_max_balance(dag_run):
            tobalance = float(dag_run.conf['userrecords'][0]['timeoffbalance'])
            return bool(tobalance >= rail.result('log_maxbalancevalue_9'))

        if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12 = rail.IfOperator(
            task_id='if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12',
            test=is_balance_greater_than_max_balance,
            yes_task="log_eligibleforsendingnotification_13",
            no_task="stop_31",
        )

        log_eligibleforsendingnotification_13 = rail.PythonOperator(
            task_id='log_eligibleforsendingnotification_13',
            python_callable=lambda:  "Yes"
        )

        log_email_subjectand_pushnotificationmessage_14 = rail.PythonOperator(
            task_id='log_email_subjectand_pushnotificationmessage_14',
            python_callable=lambda dag_run:  dag_run.conf['userrecords'][0]['timeofftype'] +
            "- Maximum accrual balance reached"
        )

        log_encode_16 = rail.PythonOperator(
            task_id='log_encode_16',
            python_callable=lambda:  base64.urlsafe_b64encode(json.dumps({
                "authorityUri": None,
                "resourceUri": None,
                "tenant": {
                    "companyKey": config.company_key,
                    "slug": None,
                    "uri": None
                },
                "user": {
                    "loginName": None,
                    "uri": None
                }
            }).encode()).decode()
        )

        log_firstname_17 = rail.PythonOperator(
            task_id='log_firstname_17',
            python_callable=lambda dag_run:  dag_run.conf['userrecords'][0]['username'].split(
                ",")[-1]
        )

        log_h_t_m_lbody_formatted_19 = rail.PythonOperator(
            task_id='log_h_t_m_lbody_formatted_19',
            python_callable=lambda:  rail.render_template('''<!-- header -->
            <table class="wrapper" style="width: 100%; padding: 0px; margin: 0px;" cellspacing="0" cellpadding="0" bgcolor="#ffffff">
            <tbody>
            <tr>
            <td>&nbsp;</td>
            <td class="container">
            <table style="width: 100%; max-width: 600px; margin: 0 auto; display: block; border-bottom: 2px solid #007ac9;" cellspacing="0" cellpadding="0">
            <tbody>
            <tr>
            <td style="padding: 15px 15px 7px 15px;"><a href="http://www.replicon.com" target="_blank" rel="noopener"><img title="Replicon" src="https://www.replicon.com/wp-content/uploads/2018/03/Replicon-TI-logo_RGB-small.png" alt="Replicon" width="276" height="42" align="left" border="0" /></a></td>
            <td align="right">&nbsp;</td>
            </tr>
            </tbody>
            </table>
            </td>
            <td>&nbsp;</td>
            </tr>
            </tbody>
            </table>
            <!-- body -->
            <table class="wrapper" style="width: 100%; padding: 0px; margin: 0px;" cellspacing="0" cellpadding="0" bgcolor="#ffffff">
            <tbody>
            <tr>
            <td>&nbsp;</td>
            <td class="container">
            <table style="width: 100%; max-width: 600px; margin: 0 auto; display: block;" cellspacing="0" cellpadding="0">
            <tbody>
            <tr>
            <td style="padding: 15px; font-family: 'Helvetica Neue','Helvetica',Helvetica, Arial,sans-serif; font-size: 14px; line-height: 1.6;">
            <p>Hi {{ result('log_firstname_17') }},</p>
            <p>Your "{{ dag_run.conf.userrecords[0].timeofftype }}" balance has reached the maximum accrual limit {{ dag_run.conf.userrecords[0].timeoffbalance }}. Hence, there wouldn&rsquo;t be any further accrual.</p>
            <p><a class="buttonLink" style="display: inline-block; padding: 6px 8px; color: #fff; text-decoration: none; background-color: #1071b0;" href="https://global.replicon.com/go/?d={{ result('log_encode_16') }}" target="_blank" rel="noopener">Login to Replicon</a></p>
            <p>Thank You,<br />Replicon Team</p>
            </td>
            </tr>
            </tbody>
            </table>
            </td>
            <td>&nbsp;</td>
            </tr>
            </tbody>
            </table>
            <!-- footer -->
            <table class="wrapper" style="width: 100%; padding: 0px; margin: 0px;" cellspacing="0" cellpadding="0" bgcolor="#ffffff">
            <tbody>
            <tr>
            <td>&nbsp;</td>
            <td class="container">
            <table style="width: 100%; max-width: 600px; margin: 0 auto; display: block; border-top: 2px solid #007ac9;" cellspacing="0" cellpadding="0">
            <tbody>
            <tr>
            <td style="padding: 15px; font-family: 'Helvetica Neue','Helvetica',Helvetica, Arial,sans-serif; font-size: 14px; line-height: 1.6;"><a style="text-decoration: none;" href="http://www.replicon.com/" target="_blank" rel="noopener">www.replicon.com</a> <span style="color: #888888;"> | The Time Intelligence<sup style="font-size: 8px;">TM</sup> Company</span></td>
            </tr>
            </tbody>
            </table>
            </td>
            <td>&nbsp;</td>
            </tr>
            </tbody>
            </table>''')
        )

        send_mail_21 = rail.EmailOperator(
            task_id='send_mail_21',
            to='{{ dag_run.conf.userrecords[0].useremail }}',
            cc='{{ dag_run.conf.userrecords[0].supervisoremail }}',
            subject='''{{ result('log_email_subjectand_pushnotificationmessage_14') }} ''',
            html_content="{{ result('log_h_t_m_lbody_formatted_19') }}",
            params=None,
        )

        log_json_encoded_notification_bodyparameter_24 = rail.PythonOperator(
            task_id='log_json_encoded_notification_bodyparameter_24',
            python_callable=lambda: {
                "aps": {
                    "alert": rail.result('log_email_subjectand_pushnotificationmessage_14'),
                    "badge": 1
                },
                "t": "timeoffs"
            }
        )

        send_push_notification_28 = rail.RepliconServiceOperator(
            task_id='send_push_notification_28',
            endpoint="/services/NotificationService1.svc/SendPushNotification",
            data=lambda dag_run: {
                "pushNotification": {
                    "recipients": [
                        {
                            "user": {
                                "uri": dag_run.conf['userrecords'][0]['useruri'],
                                "loginName": null
                            },
                            "notificationTokenUri": null
                        }
                    ],
                    "jsonEncodedNotificationBody": rail.render_template("{{result('log_json_encoded_notification_bodyparameter_24') | to_json }}")
                }
            }
        )

        # catch_29 = rail.EmptyOperator(
        #     task_id='catch_29',
        #     trigger_rule='one_failed',
        # )

        stop_31 = rail.EmptyOperator(
            task_id='stop_31',

        )

        # send_email2_32 = rail.RepliconServiceOperator(
        #     task_id='send_email2_32',
        #     endpoint="/services/NotificationService1.svc/SendEmail2",
        #     data={
        #         "email": {
        #             "to": [
        #                 {
        #                     "user": {
        #                         "uri": "{{ dag_run.conf.userrecords[0].useruri }}",
        #                         "loginName": null
        #                     },
        #                     "email": null
        #                 }
        #             ],
        #             "cc": [
        #                 {
        #                     "user": {
        #                         "uri": "{{ dag_run.conf.userrecords[0].supervisoruri }}",
        #                         "loginName": null
        #                     },
        #                     "email": null
        #                 }
        #             ],
        #             "bcc": [],
        #             "replyTo": null,
        #             "fromDisplayName": "Replicon",
        #             "subject": "{{ result('log_email_subjectand_pushnotificationmessage_14') }}",
        #             "htmlBody": "{{ result('log_h_t_m_lbody_formatted_19') }}",
        #             "textBody": null,
        #             "attachments": []
        #         }
        #     }
        # )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> log_to_sumo
        can_run_batch_task >> rail.Label(
            'No') >> policy_schedule_list >> log_gettheeffectivepolicydate_5 >> if_log_gettheeffectivepolicydate_5_present_6
        if_log_gettheeffectivepolicydate_5_present_6 >> rail.Label('Yes') >> parse_json_policy_setfromthepolicyschedule_7 >> \
            parse_json_additionalparameterfromabovepolicyschedule_max_balance_8 >> log_maxbalancevalue_9
        if_log_gettheeffectivepolicydate_5_present_6 >> rail.Label(
            'No') >> log_maxbalancevalue_9 >> if_log_maxbalancevalue_9_equals_to_0_10
        if_log_maxbalancevalue_9_equals_to_0_10 >> rail.Label(
            'Yes') >> stop_11 >> log_to_sumo
        if_log_maxbalancevalue_9_equals_to_0_10 >> rail.Label(
            'No') >> if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12
        if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12 >> rail.Label('Yes') >> \
            log_eligibleforsendingnotification_13 >> log_email_subjectand_pushnotificationmessage_14 >> \
            log_encode_16 >> log_firstname_17 >> log_h_t_m_lbody_formatted_19 >> send_mail_21 >> \
            log_json_encoded_notification_bodyparameter_24 >> \
            send_push_notification_28 >> log_to_sumo
        if_timeoffbalance_to_f_equals_to_dataloggerlog_maxbalancevalue_9messageto_f_12 >> rail.Label(
            'No') >> stop_31 >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
