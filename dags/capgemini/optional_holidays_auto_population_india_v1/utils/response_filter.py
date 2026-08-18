from datetime import datetime
import itertools
import json
from capgemini.optional_holidays_auto_population_india_v1.utils.custom_methods import get_task_state
from airflow.models import Variable
import rail

null = None


def get_holiday_calendar_list(response):
    return list(map(lambda calendar_data: {
        "holiday_calendar_name": calendar_data["name"],
        "holiday_calendar_uri": calendar_data["uri"]
    }, response))


def get_optional_holiday_for_user(response):
    user_info = rail.result("get_user_info")
    return list(map(lambda calendar_data: {
        "user_uri": user_info["userDetails"]["uri"],
        "holiday_calendar_name": user_info["holidayCalendar"]["name"],
        "optional_holiday_calendar_name": user_info["holidayCalendar"]["name"] + "_Optional",
        "optional_holiday_calendar_uri": calendar_data["uri"]
    }, filter(lambda data: user_info["holidayCalendar"]["name"]+"_Optional" == data["name"], response)))


def get_allowed_locations_uris(response):
    states_holiday_calendar_mapper = rail.result(
        "logging_details")["states_optional_holiday_calendars"]
    all_holiday_calendars = rail.result("get_all_holiday_calendars")
    return list(map(lambda holiday_mapper: {
        "state_name": holiday_mapper["state_name"],
        "state_uri": rail.find_first_by_attr_and_get_attr(response, 'displayText', holiday_mapper["state_name"], 'uri'),
        "optional_holiday_cal_name": holiday_mapper["optional_holiday_calendar"],
        "optional_holiday_cal_uri": rail.find_first_by_attr_and_get_attr(all_holiday_calendars, 'holiday_calendar_name',
                                                                         holiday_mapper["optional_holiday_calendar"], 'holiday_calendar_uri'),
    }, filter(lambda holiday_data: holiday_data["allowed"].lower() == "yes", states_holiday_calendar_mapper)))


def get_locations_list(response):
    return list(map(lambda locations: {
        "location_name": locations["cells"][1]["textValue"],
        "location_uri": locations["cells"][1]["uri"].split(':')[-1]
    }, response["rows"]))


def get_holidays_list(response):
    return list(map(lambda holiday_list: {
        "holiday_name": holiday_list["name"],
        "holiday_date": datetime(holiday_list["date"]["year"], holiday_list["date"]["month"], holiday_list["date"]["day"]).strftime("%d-%m-%Y")
    }, response))


def get_user_parent_location(response):
    return list(itertools.chain.from_iterable(list(map(lambda rows: list(map(lambda cell_collection: {
        "user_location": rail.result("get_user_info")["locationSchedule"][-1]["location"]["displayText"],
        "parent_name": cell_collection["textValue"],
        "parent_uri": cell_collection["uri"]
    }, filter(lambda cell_collection: cell_collection["textValue"] == "India",
              rows["cells"][0]["cellCollection"]))),
        filter(lambda row_data: row_data["cells"][1]["textValue"] == rail.result("get_user_info")["locationSchedule"][-1]["location"]["displayText"],
               response["rows"])))))


def get_user_timeoff_bookings(response, excepted_timeoff_types_mapper):
    excepted_timeoff_types = list(map(lambda data: data["time_off_type"],
                                      filter(lambda data: data["except"].lower() == "yes", json.loads(Variable.get(excepted_timeoff_types_mapper)))))
    return list(map(lambda booking_data: {
        "booking_uri": booking_data["uri"],
        "timeoff_type": booking_data["timeOffType"]["name"]
    }, filter(lambda booking_data: booking_data["uri"] and booking_data["timeOffType"]["name"] in excepted_timeoff_types, response)))


def get_timeoff_booking_uri():
    status = get_task_state("put_and_submit_timeoff_booking_for_user")
    if status.lower() == "success":
        put_submit_timeoff_response = rail.result(
            "put_and_submit_timeoff_booking_for_user")
    else:
        error_message = rail.result(
            "put_and_submit_timeoff_booking_for_user", key='error')
    return put_submit_timeoff_response["uri"] if status.lower() == "success" \
        else (error_message["response"]["json"]["error"]["details"]["timeOff"]["uri"]
              if status.lower() == "failed" and error_message and error_message["response"] and
              error_message["response"]["json"]["error"]["details"] and
              error_message["response"]["json"]["error"]["details"]["timeOff"]["uri"] else null)

def get_user_current_holiday_calendar(response, states_optional_holiday_calendars):
    optional_holiday_calendars = list(map(lambda states_data: states_data["optional_holiday_calendar"],
        filter(lambda states_data: states_data["allowed"].lower() == "yes", json.loads(Variable.get(states_optional_holiday_calendars)))))
    user_optional_holiday_calendar = f'{response[0]["holidayCalendar"]["displayText"]}_Optional' if response else null
    return user_optional_holiday_calendar if user_optional_holiday_calendar and user_optional_holiday_calendar in optional_holiday_calendars else null
