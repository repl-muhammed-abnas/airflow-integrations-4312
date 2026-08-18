from datetime import datetime
from dateutil import relativedelta
import rail

def get_replicon_date(date_str):
    if not date_str:
        return None

    try:
        date = datetime.strptime(date_str, "%b %d, %Y") + relativedelta.relativedelta(days=1)
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None

def get_user_schedule_policy_list(response, dag_run):
    if response and bool(list(filter(lambda schedule:schedule["officeSchedule"] is not None and "displayText" in schedule["officeSchedule"], response))):
        filtered_reponse = list(filter(
                lambda schedule:schedule["officeSchedule"] is not None and "displayText" in schedule["officeSchedule"]
                and schedule["effectiveDate"] is not None and "day" in schedule["effectiveDate"],
                response )
            )
        return list(map(lambda schedule:{
            "userUri": dag_run.conf["useruri"],
            "effectiveMonth": schedule["effectiveDate"]["month"],
            "scheduletype": schedule["scheduleTypeUri"],
            "effectiveDay": schedule["effectiveDate"]["day"],
            "effectiveYear": schedule["effectiveDate"]["year"],
            "user": dag_run.conf["username"],
            "effectiveDate": schedule["effectiveDate"],
            "schedule": schedule["officeSchedule"]["displayText"],
            "scheduleuri": schedule["officeSchedule"]["uri"]
        }, filtered_reponse))
    return False

def create_schedule_entries_and_policy(dag_run):
    schedule_entries = []
    null=None
    if rail.result("get_schedule_policy_for_user"):
        schedule_entries = list(map(lambda schedule:{
                    "schedulePolicy": {
                        "officeScheduleUri": null,
                        "name": schedule["schedule"],
                        "officeSchedule": null,
                        "scheduleTypeUri": schedule["scheduletype"]
                    },
                    "effectiveDate": {
                        'year': schedule["effectiveYear"],
                        'month':schedule["effectiveMonth"],
                        'day': schedule["effectiveDay"]
                    }
            }, rail.result("get_schedule_policy_for_user")))

    schedule_entries.append({
                "schedulePolicy": {
                    "officeScheduleUri": null,
                    "name": "EMPTY",
                    "officeSchedule": null,
                    "scheduleTypeUri": "urn:replicon:schedule-type:office-schedule"
                },
                "effectiveDate": rail.result("get_enddate")
    })
    return  {
                "userUri": dag_run.conf["useruri"],
                "scheduleEntries": schedule_entries
            }
