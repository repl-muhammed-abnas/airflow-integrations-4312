from datetime import datetime
import rail


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_replicon_date(date_str):
    if not date_str:
        return None
    # date format in 2006040
    try:
        date = datetime.strptime(date_str, '%Y%m%d')
        return {
            'year': date.year,
            'month': date.month,
            'day': date.day
        }
    except:  # pylint: disable=bare-except
        return None


def get_task_state(task_id):
    return rail.get_current_context()['dag_run'].get_task_instance(task_id).current_state()
