from datetime import datetime, timedelta
import rail

null = None


def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf


def get_data_from_document(document):
    with rail.lib.readers.get_data_reader(document) as reader:
        return list(reader)


def get_csv_rows(item):
    dag_conf = get_dag_run_conf()
    start_date_str = dag_conf['startdate']
    start_date_time = datetime.strptime(start_date_str, "%Y-%m-%d")
    day_seq = (start_date_time +
               timedelta(days=item['seq'])) - timedelta(days=1)
    row_data = [
        item['seq'],
        day_seq,
        day_seq.weekday(),
        day_seq.strftime("%d"),
        day_seq.strftime("%m"),
        day_seq.strftime("%Y"),
        day_seq.isocalendar()[1],
        day_seq.timetuple().tm_yday
    ]
    return row_data
