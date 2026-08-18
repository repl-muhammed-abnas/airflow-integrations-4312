import json
import rail
from ttecholdingsinc.schedule_creation_v1.utils import custom_methods,request_payload

def update_schedule_task():
    with rail.TaskGroup(group_id='update_schedule_task', prefix_group_id=False) as update_schedule:

        is_name_changed = rail.IfOperator(
            task_id = 'is_name_changed',
            test= custom_methods.check_shift_name,
            yes_task= 'update_name',
            no_task= 'is_description_changed'
        )

        update_name =  rail.RepliconServiceOperator(
            task_id = 'update_name',
            endpoint= '/services/ShiftService1.svc/UpdateName',
            data=lambda dag_run: {
                "shiftUri": rail.result("shift_details_in_replicon")[0]['uri'],
                "name": dag_run.conf['schedulename']
            }
        )

        is_description_changed = rail.IfOperator(
            task_id = 'is_description_changed',
            test= custom_methods.check_shift_description,
            yes_task= 'update_description',
            no_task= 'is_break_hours_available_in_feed'
        )

        update_description=  rail.RepliconServiceOperator(
            task_id = 'update_description',
            endpoint= '/services/ShiftService1.svc/UpdateDescription',
            data=lambda dag_run: {
                "shiftUri": rail.result("shift_details_in_replicon")[0]['uri'],
                "description": rail.result("get_query_data")['description']
            }
        )

        is_break_hours_available_in_feed = rail.IfOperator(
            task_id = 'is_break_hours_available_in_feed',
            test= '{{ result("get_query_data").break1 | is_truthy or result("get_query_data").break2 | is_truthy }}',
            yes_task= 'is_break_hours_available',
            no_task= 'finish'
        )

        is_break_hours_available = rail.IfOperator(
            task_id = 'is_break_hours_available',
            test=lambda: rail.result("shift_details_in_replicon")[0]['break_hours'],
            yes_task= 'get_shift_break_details',
            no_task= 'update_break_hours_for_shift'
        )

        get_shift_break_details = rail.RepliconServiceOperator(
            task_id="get_shift_break_details",
            endpoint="/services/ShiftService1.svc/BulkGetShiftDetails",
            data=lambda: json.dumps({
                    "shiftUris": [rail.result("shift_details_in_replicon")[0]['uri']]
                }),
            data_handler=custom_methods.get_shift_details
        )

        update_break_hours_for_shift = rail.RepliconServiceOperator(
            task_id="update_break_hours_for_shift",
            endpoint="/services/ShiftService1.svc/PutShiftBreakSegments",
            data=lambda: request_payload.get_break_hours_payload('update')
        )

        finish = rail.EmptyOperator(
            task_id = 'finish'
        )

        is_name_changed >> rail.Label(
            "Yes") >> update_name >> is_description_changed

        is_name_changed >> rail.Label(
            "No") >> is_description_changed

        is_description_changed >> rail.Label(
            "Yes") >> update_description >> is_break_hours_available_in_feed

        is_description_changed >> rail.Label(
            "No") >> is_break_hours_available_in_feed

        is_break_hours_available_in_feed >> rail.Label(
            "Yes") >> is_break_hours_available

        is_break_hours_available_in_feed >> rail.Label(
            "No") >> finish

        is_break_hours_available >> rail.Label(
            "Yes") >> get_shift_break_details >> update_break_hours_for_shift

        is_break_hours_available >> rail.Label(
            "No") >> update_break_hours_for_shift >> finish

    return update_schedule
