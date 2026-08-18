import rail
null = None
def get_payload_sendemail(dag_run):
    return {
                "email": {
                    "to": [
                    {
                        "user": {
                        "uri": dag_run.conf['approveruri'],
                        "loginName": null
                        },
                        "email": null
                    }
                    ],
                    "cc": [],
                    "bcc": [],
                    "replyTo": null,
                    "fromDisplayName": "Do-Not-Reply@deltek.com",
                    "subject": "Timesheets Waiting for Approval: " + str(rail.result('query_user_and_timesheetperiod', 'length')),
                    "htmlBody": rail.result('get_email_body'),
                    "textBody": null,
                    "attachments": []
                }
            }

def get_payload_timesheet_report_generation():
    filterValues = []
    filterValues.append({
                        "reportFilterUri": rail.find_first_by_attr_and_get_attr(
                            rail.result(
                                "get_timesheet_report_details")['filterConfiguration']
                                    ['enabledFilters'],'displayText','ApprovalStatusFilter','uri'),
                        "value": 1
                    })
    return {
                "reportParameters": [{
                    "reportUri": rail.result('get_timesheet_report_details')['uri'],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv",
                    "filterValues": filterValues
                }],
                "step_time": "120"
            }


def get_payload_userlist_report_generation():
    return {
                "reportParameters": [
                    {
                    "reportUri": rail.result('get_userlist_report_details')['uri'],
                    "filterValues": [],
                    "outputFormatUri": "urn:replicon:report-output-format-option:csv"
                    }
                ]
            }

def get_send_notification_data(dag_run):
    length = rail.result('query_user_and_timesheetperiod','length')
    return {
                "pushNotification": {
                    "recipients": [
                            {
                                "user": {
                                "uri": dag_run.conf['approveruri'],
                                "loginName": null
                                },
                                "notificationTokenUri": null
                            }
                    ],
                    # pylint: disable=line-too-long
                    "jsonEncodedNotificationBody": '''{\"aps\":{\"alert\":\"Timesheets Waiting for Approval\",\"badge\":'''+str(length)+'''},\"t\":\"timesheets_approvals\"}'''
                }
            }

def get_user_notification_preference(response):
    return rail.find_first_by_attr_and_get_attr(
        response['notificationDeliveryPreferences'],
        'objectTypeUri','urn:replicon:object-type:timesheet',
        'notificationDeliveryOptionUri') if response['notificationDeliveryPreferences'] else ''
