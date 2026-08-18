import rail
from galaxyusopcoinc.workday_user_sync.time_off_plan_v2.utils import request_payload

null = None

def create_child_dag(config):
    with rail.create_airflow_dag(
        dag_id=config.create_timeoff_type_dag_id,
        description=f'Vialto Partners Time Off Create Child V1.0 {config.instance}',
        company_key=config.company_key,
        replicon_conn_id=config.replicon_conn_id,
        max_active_runs = config.create_timeoff_type_max_active_run
    ) as dag:

        rail.ViewDagRunConfOperator(task_id="view_dagrun_config")

        put_time_off_type = rail.RepliconServiceOperator(
            task_id='put_time_off_type',
            endpoint='/services/TimeOffService1.svc/PutTimeOffType',
            data=request_payload.get_put_time_off_type_data
        )

        put_booking_refid_oef_and_validation_rule = rail.RepliconServiceOperator(
            task_id='put_booking_refid_oef_and_validation_rule',
            endpoint='/services/TimeOffPolicyService1.svc/PutTimeOffBookingPolicyForTimeOffType',
            data=lambda dag_run: {
                "timeOffTypeBookingPolicy": {
                    "target": {
                    "uri": rail.result("put_time_off_type")['uri'],
                    "name": null
                    },
                    "policyValues": [
                    {
                        "policyKeyUri": "urn:replicon:time-off-booking-policy-keys:validation-rule",
                        "policyValue": {
                        "collection": [
                            {
                            "uri": dag_run.conf['only_admin_can_book_to_uri'],
                            "collection": []
                            }
                        ]
                        }
                    },
                    {
                        "policyKeyUri": "urn:replicon:time-off-booking-policy-keys:object-extension-definition",
                        "policyValue": {
                        "collection": [
                            {
                            "uri": dag_run.conf['oef_def_uri'],
                            "collection": []
                            }
                        ]
                        }
                    },
                    {
                        "policyKeyUri": "urn:replicon:policy:time-off:start-end-time-specification-requirement",
                        "policyValue": {
                        "uri": "urn:replicon:policy:time-off:start-end-time-specification-requirement:no-start-end-time-for-partial-days"
                        }
                    }
                    ]
                }
                }
        )

        set_allow_editing_of_submitted_bookings_as_always = rail.RepliconServiceOperator(
            task_id= "set_allow_editing_of_submitted_bookings_as_always",
            endpoint="/services/TimeOffPolicyService1.svc/UpdateTimeOffEditPolicyForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('put_time_off_type').uri }}",
                "timeOffEditPolicyOptionUri": "urn:replicon:time-off-edit-policy-option:do-not-allow-users-to-reopen-and-modify-time-off"
            }
        )

        set_allow_deletion_of_submitted_bookings_as_always = rail.RepliconServiceOperator(
            task_id= "set_allow_deletion_of_submitted_bookings_as_always",
            endpoint="/services/TimeOffPolicyService1.svc/UpdateTimeOffDeletePolicyForTimeOffType",
            data={
                "timeOffTypeUri": "{{ result('put_time_off_type').uri }}",
                "timeOffDeletePolicyOptionUri": "urn:replicon:time-off-delete-policy-option:do-not-allow-users-to-delete-time-off"
            }
        )

        log_create_success = rail.WriteLogOperator(
            task_id='log_create_success',
            message=lambda dag_run: f"Time Off Type is Created in Replicon as {'Enabled' if dag_run.conf['create_as_enable'] else 'Disabled'}",
            severity='Success',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.time_off_type_desc}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Success'
            }
        )

        catch_and_log_errors = rail.WriteLogOperator(
            task_id='catch_and_log_errors',
            trigger_rule='one_failed',
            message='{{ get_error_message() }}',
            properties={
                'time_off_type_desc': "{{ dag_run.conf.time_off_type_desc}}",
                'time_off_type_name': "{{ dag_run.conf.time_off_type_name}}",
                'unit_of_time': "{{ dag_run.conf.unit_of_time}}",
                'country': "{{ dag_run.conf.country}}",
                'status': 'Error'
            },
        )

        put_time_off_type >> put_booking_refid_oef_and_validation_rule >> set_allow_editing_of_submitted_bookings_as_always
        set_allow_editing_of_submitted_bookings_as_always >> set_allow_deletion_of_submitted_bookings_as_always\
            >> log_create_success >> rail.Label("On Error")  >> catch_and_log_errors

        return dag

rail.for_each_instance(create_child_dag)
