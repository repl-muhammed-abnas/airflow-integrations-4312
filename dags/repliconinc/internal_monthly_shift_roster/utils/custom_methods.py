from dateutil.relativedelta import relativedelta
from datetime import timedelta, datetime
import calendar


null = None


def end_of_month(date):
    """
    Return the last day of the month for the given date.
    """
    last_day = calendar.monthrange(date.year, date.month)[1]
    return date.replace(day=last_day)


def get_end_date():
    """
    Calculate an end-of-month date based on current date.

    - If the month is December, return the end of the month
      13 months ahead.
    - Otherwise, return the end of next month.
    """
    today = datetime.today()
    scheduled_month = today.strftime("%B")
    current_end_of_month = end_of_month(today)

    if scheduled_month == "December":
        future_date = current_end_of_month + relativedelta(months=13)
        result = end_of_month(future_date)
    else:
        next_month_start_date = current_end_of_month + timedelta(days=1)
        result = end_of_month(next_month_start_date)
    
    return result.date()


def get_first_day_of_next_month(date):
    """
    Return the first day of the next month based on the given date.
    """
    last_day = calendar.monthrange(date.year, date.month)[1]
    end_of_month = date.replace(day=last_day)
    return end_of_month + timedelta(days=1)


def generate_weekdays(dag_run):
    """
    Generate weekdays CSV data with date calculations.
    
    For each sequence number between start and end date, calculates:
    - seq: The sequence number
    - date: The actual date (StartDate + (SeqNo - 1) days)
    - day: Day of week (0=Monday, 6=Sunday)
    - dateday: Day of month (1-31)
    - datemonth: Month (1-12)
    - dateyear: Year
    - week: ISO week number
    
    Returns:
        list: List of dictionaries with date information
    """
    # Extract start date from dag_run config
    start_date = datetime(
        year=dag_run.conf["start_date_year"],
        month=dag_run.conf["start_date_month"],
        day=dag_run.conf["start_date_day"]
    )
    
    # Get end date
    end_date = datetime(
        year=dag_run.conf["end_date_year"],
        month=dag_run.conf["end_date_month"],
        day=dag_run.conf["end_date_day"]
    )
    
    # Calculate days count
    days_count = (end_date - start_date).days + 1
    
    # Generate data for each day
    weekdays_data = []
    for seq_no in range(1, days_count + 1):
        # Calculate date for this sequence number
        current_date = start_date + timedelta(days=seq_no - 1)

        # Exclude Saturdays and Sundays
        if current_date.weekday() in (5, 6):
            continue
        
        weekdays_data.append({
            'dateday': current_date.day,
            'datemonth': current_date.month,
            'dateyear': current_date.year,
        })
    
    return weekdays_data


def create_final_shift_assignment(dag_run):
    weekdays = generate_weekdays(dag_run)
    if not weekdays:
        return []
    
    final_shift_assignment = []
    for weekday in weekdays:
        assignment = {}
        assignment["date"] = {
            "year": int(weekday["dateyear"]),
            "month": int(weekday["datemonth"]),
            "day": int(weekday["dateday"])
        }
        assignment["target"] = {
            "uri": null
        }
        assignment["shift"] = {
            "uri": null,
            "name": dag_run.conf["shift_name"],
        }
        assignment["user"] = {
            "uri": dag_run.conf["user_uri"],
            "name": null,
            "loginName": null
        }
        assignment["startTime"] = null
        assignment["endTime"] = null
        assignment["note"] = "Published by shift automation"
        assignment["publishState"] = "urn:replicon:shift-assignment-publish-state:published"
        final_shift_assignment.append(assignment)
    return final_shift_assignment