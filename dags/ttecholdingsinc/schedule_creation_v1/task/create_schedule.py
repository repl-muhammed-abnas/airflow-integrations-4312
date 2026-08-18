import rail
from ttecholdingsinc.schedule_creation_v1.utils import request_payload

def create_schedule_task():
    with rail.TaskGroup(group_id='create_schedule_task', prefix_group_id=False) as create_schedule:

        create_shift_schedule_draft = rail.RepliconServiceOperator(
            task_id = 'create_shift_schedule_draft',
            endpoint= '/services/ShiftService1.svc/CreateNewDraft',
        )

        update_name_for_new_shift_schedule=  rail.RepliconServiceOperator(
            task_id = 'update_name_for_new_shift_schedule',
            endpoint= '/services/ShiftService1.svc/UpdateName',
            data=lambda dag_run: {
                "shiftUri": rail.result("create_shift_schedule_draft"),
                "name": dag_run.conf['schedulename']
            }
        )

        update_description_for_new_shift_schedule=  rail.RepliconServiceOperator(
            task_id = 'update_description_for_new_shift_schedule',
            endpoint= '/services/ShiftService1.svc/UpdateDescription',
            data=lambda dag_run: {
                "shiftUri": rail.result("create_shift_schedule_draft"),
                "description": rail.result("get_query_data")['description']
            }
        )

        update_start_time_for_new_shift_schedule=  rail.RepliconServiceOperator(
            task_id = 'update_start_time_for_new_shift_schedule',
            endpoint= '/services/ShiftService1.svc/UpdateStartTime',
            data=lambda: request_payload.get_start_and_end_time_for_create('startTime')
        )

        update_end_time_for_new_shift_schedule=  rail.RepliconServiceOperator(
            task_id = 'update_end_time_for_new_shift_schedule',
            endpoint= '/services/ShiftService1.svc/UpdateEndTime',
            data= lambda: request_payload.get_start_and_end_time_for_create('endTime')
        )

        is_breaks_available_for_shift = rail.IfOperator(
            task_id = 'is_breaks_available_for_shift',
            test= '{{ result("get_query_data").break1 != "NULL" or result("get_query_data").break2 != "NULL" }}',
            yes_task= 'update_breaks_for_new_shift_schedule',
            no_task= 'publish_create_draft'
        )

        update_breaks_for_new_shift_schedule = rail.RepliconServiceOperator(
            task_id="update_breaks_for_new_shift_schedule",
            endpoint="/services/ShiftService1.svc/PutShiftBreakSegments",
            data=lambda: request_payload.get_break_hours_payload('create')
        )

        publish_create_draft=  rail.RepliconServiceOperator(
            task_id = 'publish_create_draft',
            endpoint= '/services/ShiftService1.svc/PublishDraft',
            data=lambda: {
                "shiftDraftUri": rail.result("create_shift_schedule_draft"),
            }
        )

        create_shift_schedule_draft >> update_name_for_new_shift_schedule >> update_description_for_new_shift_schedule >>\
            update_start_time_for_new_shift_schedule >> update_end_time_for_new_shift_schedule >> is_breaks_available_for_shift

        is_breaks_available_for_shift >> rail.Label(
            "Yes") >> update_breaks_for_new_shift_schedule >> publish_create_draft

        is_breaks_available_for_shift >> rail.Label(
            "No") >> publish_create_draft


    return create_schedule
