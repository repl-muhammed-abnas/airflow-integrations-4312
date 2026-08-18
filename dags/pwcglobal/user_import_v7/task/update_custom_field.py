import rail
from pwcglobal.user_import_v7.utils import request_payload
from pwcglobal.user_import_v7.task.put_line_manager import put_line_manager_udf


def get_update_custom_field():
    with rail.TaskGroup(group_id='update_custom_field', prefix_group_id=False) as update_custom_field:

        user_uri = request_payload.get_user_uri_template_exp()

        is_wordayid_changed = rail.IfOperator(
            task_id='is_wordayid_changed',
            test=lambda: request_payload.get_conf()['workdayid'] and request_payload.get_conf()['workdayid'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                    'customField.displayText',
                    'Workday ID',
                    'text'),
            yes_task='update_workdayid_udf',
            no_task='is_loscode_changed',
        )

        update_workdayid_udf = rail.RepliconServiceOperator(
            task_id='update_workdayid_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.workdayid}}",
                "value": "{{ dag_run.conf.workdayid}}"
            }
        )

        is_loscode_changed = rail.IfOperator(
            task_id='is_loscode_changed',
            test=lambda: request_payload.get_conf()['loscode'] and request_payload.get_conf()['loscode'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                'customField.displayText',
                    'LoS Code',
                    'text'),
            yes_task='update_loscode_udf',
            no_task='is_grade_changed',
        )

        update_loscode_udf = rail.RepliconServiceOperator(
            task_id='update_loscode_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.loscode}}",
                "value": "{{ dag_run.conf.loscode}}"
            }
        )

        is_grade_changed = rail.IfOperator(
            task_id='is_grade_changed',
            test=lambda: request_payload.get_conf()['grade'] and request_payload.get_conf()['gradedropdownuri'] and
                    request_payload.get_conf()['grade'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                'customField.displayText',
                    'Grade',
                    'text'),
            yes_task='update_grade_udf',
            no_task='is_prefix_changed',
        )

        update_grade_udf = rail.RepliconServiceOperator(
            task_id='update_grade_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.grade}}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.gradedropdownuri}}"
            }
        )

        is_prefix_changed = rail.IfOperator(
            task_id='is_prefix_changed',
            test=lambda: request_payload.get_conf()['prefix'] and request_payload.get_conf()['prefix'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                'customField.displayText',
                    'Prefix',
                    'text'),
            yes_task='update_prefix_udf',
            no_task='is_homeofficelocation_changed',
        )

        update_prefix_udf = rail.RepliconServiceOperator(
            task_id='update_prefix_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.prefix}}",
                "value": "{{ dag_run.conf.prefix}}"
            }
        )

        is_homeofficelocation_changed = rail.IfOperator(
            task_id='is_homeofficelocation_changed',
            test=lambda: request_payload.get_conf()['homeofficelocation'] and request_payload.get_conf()['homeofficelocation'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                'customField.displayText',
                    'Home office location',
                    'text'),
            yes_task='update_homeofficelocation_udf',
            no_task='is_resourcerole_changed',
        )

        update_homeofficelocation_udf = rail.RepliconServiceOperator(
            task_id='update_homeofficelocation_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.homelocation}}",
                "value": "{{ dag_run.conf.homeofficelocation}}"
            }
        )

        is_resourcerole_changed = rail.IfOperator(
            task_id='is_resourcerole_changed',
            test=lambda: request_payload.get_conf()['resourcerole'] and request_payload.get_conf()['resourcerole'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                'customField.displayText',
                    'Resource Role',
                    'text'),
            yes_task='update_resourcerole_udf',
            no_task='is_profilestatus_changed',
        )

        update_resourcerole_udf = rail.RepliconServiceOperator(
            task_id='update_resourcerole_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateTextValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.resourcerole}}",
                "value": "{{ dag_run.conf.resourcerole}}"
            }
        )

        is_profilestatus_changed = rail.IfOperator(
            task_id='is_profilestatus_changed',
            test=lambda: request_payload.get_conf()['profilestatus'] and request_payload.get_conf()['profilestatusdropdownuri'] and
                    request_payload.get_conf()['profilestatus'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                    'customField.displayText',
                    'Profile Status',
                    'text'),
            yes_task='update_profilestatus_udf',
            no_task='is_toil_changed',
        )

        update_profilestatus_udf = rail.RepliconServiceOperator(
            task_id='update_profilestatus_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.profilestatus}}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.profilestatusdropdownuri}}"
            }
        )

        is_toil_changed = rail.IfOperator(
            task_id="is_toil_changed",
            test=lambda: request_payload.get_conf()['toil'] and request_payload.get_conf()['toildropdownuri'] and
                    request_payload.get_conf()['toil'] !=
                    rail.find_first_by_attr_and_get_attr(
                    rail.result('bulk_get_user3')[
                        'userDetails']['customFieldValues'],
                    'customField.displayText',
                    'TOIL',
                    'text'),
            yes_task="update_toil_udf",
            no_task="has_line_manager"
        )

        update_toil_udf = rail.RepliconServiceOperator(
            task_id='update_toil_udf',
            endpoint='/services/CustomFieldService1.svc/UpdateDropdownValue',
            data={
                "objectUri": user_uri,
                "customFieldUri": "{{ dag_run.conf.customfielduri.toil}}",
                "customFieldDropDownOptionUri": "{{ dag_run.conf.toildropdownuri}}"
            }
        )

        has_line_manager = rail.IfOperator(
            task_id="has_line_manager",
            test=lambda: bool(request_payload.get_conf()
                              ["linemanagerpartyid"]),
            yes_task="line_manager_start",
            no_task="udf_update_complete"
        )

        line_manager_start = rail.EmptyOperator(task_id="line_manager_start")

        put_line_manager = put_line_manager_udf("update")

        udf_update_complete = rail.EmptyOperator(
            task_id='udf_update_complete'
        )

        is_wordayid_changed >> rail.Label('Yes') >> update_workdayid_udf >> \
            is_loscode_changed
        is_wordayid_changed >> rail.Label('No') >> is_loscode_changed

        is_loscode_changed >> rail.Label('Yes') >> update_loscode_udf >> \
            is_grade_changed
        is_loscode_changed >> rail.Label('No') >> is_grade_changed
        is_grade_changed >> rail.Label('Yes') >> update_grade_udf >> \
            is_prefix_changed
        is_grade_changed >> rail.Label('No') >> is_prefix_changed
        is_prefix_changed >> rail.Label('Yes') >> update_prefix_udf >> \
            is_homeofficelocation_changed
        is_prefix_changed >> rail.Label('No') >> is_homeofficelocation_changed
        is_homeofficelocation_changed >> rail.Label('Yes') >> update_homeofficelocation_udf >> \
            is_resourcerole_changed
        is_homeofficelocation_changed >> rail.Label('No') >> \
            is_resourcerole_changed
        is_resourcerole_changed >> rail.Label('Yes') >> \
            update_resourcerole_udf >> is_profilestatus_changed
        is_resourcerole_changed >> rail.Label('No') >> is_profilestatus_changed
        is_profilestatus_changed >> rail.Label('Yes') >> \
            update_profilestatus_udf >> is_toil_changed
        is_profilestatus_changed >> rail.Label('No') >> \
            is_toil_changed >> rail.Label("Yes") >>\
            update_toil_udf >> has_line_manager
        is_toil_changed >> rail.Label("No") >>\
            has_line_manager >> rail.Label("Yes") >>\
            line_manager_start >> put_line_manager >> udf_update_complete
        has_line_manager >> rail.Label("No") >> udf_update_complete

    return update_custom_field
