
from datetime import timedelta
from airflow.models import Variable
import rail

null = None


def create_dag(config):
    # pylint: disable=too-many-statements line-too-long
    with rail.create_airflow_dag(
        dag_id=config.horizonmedia_user_import_disable_user_child,
        description=f'Horizonmedia - Child_ disable user V2.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs=config.child_dag_max_active_runs,
    ) as dag:

        rail.ViewDagRunConfOperator(
            task_id="view_dagrun_config",
            extra_config=config)

        can_run_batch_task = rail.IfOperator(
            task_id='can_run_batch_task',
            test=lambda: Variable.get(
                config.can_run_batch_task_var_name, default_var='true').lower() == 'true',
            yes_task='batch_task',
            no_task='if_request_termination_date_present_3'
        )

        batch_task = rail.BatchTaskRunOperator(
            task_id='batch_task',
            start_task='if_request_termination_date_present_3',
            end_task='finish',
            execution_timeout=timedelta(
                days=config.execution_timeout_days),
        )

        if_request_termination_date_present_3 = rail.IfOperator(
            task_id='if_request_termination_date_present_3',
            test='''{{ dag_run.conf.Termination_Date | is_truthy }}''',
            yes_task="apply_user_modifications2_updateenddate_5",
            no_task="finish",
        )

        apply_user_modifications2_updateenddate_5 = rail.RepliconServiceOperator(
            task_id='apply_user_modifications2_updateenddate_5',
            endpoint="/services/ImportService1.svc/ApplyUserModifications2",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "modifications": {
                    "userDetailsToApply": {
                        "firstName": null,
                        "lastName": null,
                        "emailAddress": null,
                        "language": null,
                        "employmentDateRange": null,
                        "employmentStartDate": null,
                        "employmentEndDate": {
                            "date": {
                                "year": "{{ dag_run.conf.Termination_Date.year }}",
                                "month": "{{ dag_run.conf.Termination_Date.month }}",
                                "day": "{{ dag_run.conf.Termination_Date.day }}",
                            }
                        },
                        "employeeId": null,
                        "displayNameParameter": null
                    }
                },
                "userModificationOptionUri": "urn:replicon:user-modification-option:save"
            }
        )

        update_login_name = rail.RepliconServiceOperator(
            task_id='update_login_name',
            endpoint="/services/securityService1.svc/SetSSOAuthenticationForUser",
            data={
                "userUri": "{{ dag_run.conf.useruri }}",
                "loginName": "{{ dag_run.conf.userid }}_{{ dag_run.conf.Termination_Date.month }}{{ dag_run.conf.Termination_Date.day }}{{ dag_run.conf.Termination_Date.year }}"
            }
        )

        disable_login_disable_login_6 = rail.RepliconServiceOperator(
            task_id='disable_login_disable_login_6',
            endpoint="/services/securityService1.svc/DisableLogin",
            data={
                "userUri": "{{ dag_run.conf.useruri }}"
            }
        )

        put_user_notification_preferences_7 = rail.RepliconServiceOperator(
            task_id='put_user_notification_preferences_7',
            endpoint="/services/NotificationScriptAdministrationService1.svc/PutUserNotificationPreferences",
            data={
                "user": {
                    "uri": "{{ dag_run.conf.useruri }}",
                    "loginName": null,
                    "parameterCorrelationId": null
                },
                "preferences": {
                    "notificationDeliveryPreferences": [
                        {
                            "objectTypeUri": "urn:replicon:object-type:project",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:user",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:timesheet",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-entry-revision-group",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:pay-rule-script",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:time-off",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        },
                        {
                            "objectTypeUri": "urn:replicon:object-type:holiday",
                            "notificationDeliveryOptionUri": "urn:replicon:user-notification-delivery-option:never-deliver"
                        }
                    ],
                    "sharedDeliveryPreferenceOptionUris": [
                        "urn:replicon:user-shared-delivery-preference-option:always-deliver"
                    ]
                }
            }
        )

        update_dropdown_value_workerstatus_8 = rail.RepliconServiceOperator(
            task_id='update_dropdown_value_workerstatus_8',
            endpoint="/services/CustomFieldService1.svc/UpdateDropdownValue",
            data={
                "objectUri": "{{ dag_run.conf.useruri }}",
                "customFieldUri": "{{ dag_run.conf.wokerstatusuri }}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.terminatedstatusuri }}"
            }
        )

        horizonmedia_user_import_logs_add_entry_9 = rail.WriteLogOperator(
            task_id='horizonmedia_user_import_logs_add_entry_9',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Success",
            properties={
                "employeeid": "{{ dag_run.conf.Employee_ID }}",
                "username": "{{ dag_run.conf.username }}",
                "action": "Disable",
                "status": "Success",
                "details": "Users disabled as not part of the Input file",
            }
        )

        finish = rail.EmptyOperator(
            task_id='finish',
        )

        catch_and_log_error = rail.WriteLogOperator(
            task_id='catch_and_log_error',
            trigger_rule='one_failed',
            log="{{ dag_run.conf.log }}",
            message="na",
            severity="Error",
            properties={
                "employeeid": "{{ dag_run.conf.Employee_ID }}",
                "username": "{{ dag_run.conf.username }}",
                "action": "Disable",
                "status": "Error",
                "details": '{{ get_error_message() }}',

            }
        )

        log_to_sumo = rail.DagRunLogToSumoOperator(
            task_id='log_to_sumo',
            sumo_conn_id='sumologic-dagrunlogger',
        )

        can_run_batch_task >> rail.Label('Yes') >> batch_task >> finish
        can_run_batch_task >> rail.Label(
            'No') >> if_request_termination_date_present_3
        if_request_termination_date_present_3
        if_request_termination_date_present_3 >> rail.Label(
            'Yes') >> apply_user_modifications2_updateenddate_5 >> update_login_name >> disable_login_disable_login_6 >> put_user_notification_preferences_7 \
            >> update_dropdown_value_workerstatus_8 >> horizonmedia_user_import_logs_add_entry_9 >> finish
        if_request_termination_date_present_3 >> rail.Label(
            'No') >> finish
        finish >> catch_and_log_error >> log_to_sumo

    return dag


rail.for_each_instance(create_dag)
