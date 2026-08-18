from datetime import datetime
import rail

def valid_schedule(schedule):
    count = 0
    schedule_list = schedule.split("|")
    try:
        schedule_int_list = [float(sch) for sch in schedule_list]
        for schedule_hours in schedule_int_list:
            count += schedule_hours
        if count > 0:
            return True
        return False
    except:  # pylint: disable=bare-except
        return False


def schedule_all_blank_zero(schedule):
    schedule_list = schedule.split("|")
    if set(schedule_list) == {"0"}:
        return True
    return False

def schedule_is_blank(schedule):
    schedule_list = schedule.split("|")
    if set(schedule_list) == {""}:
        return True
    return False

def get_today_date():
    now = datetime.now()
    return {
        'year': now.year,
        'month': now.month,
        'day': now.day
    }


def get_replicon_date(date_str):
    if not date_str:
        return None
    try:
        date = datetime.strptime(date_str, '%Y-%m-%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_work_duration_for_validation(schedule_name, day):
    day_index = ['monday', 'tuesday',
                 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'].index(day)
    time = float(schedule_name.split('|')[day_index])
    return {
        "hours": int(time),
        "minutes": int((time*60) % 60),
        "seconds": int((time*60*60) % 60),
        "milliseconds": 0,
        "microseconds": 0,
    }

def get_work_duration(day):
    day_index = ['monday', 'tuesday',
                 'wednesday', 'thursday', 'friday', 'saturday', 'sunday'].index(day)
    time = float(rail.get_current_context()[
        'dag_run'].conf['scheduletype'].split('|')[day_index])
    return {
        "hours": int(time),
        "minutes": int((time*60) % 60),
        "seconds": int((time*60*60) % 60),
        "milliseconds": 0,
        "microseconds": 0,
    }
