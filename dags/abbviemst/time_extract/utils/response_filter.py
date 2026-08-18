import rail
from datetime import datetime
null = None

def get_dag_run_conf():
    return rail.get_current_context()['dag_run'].conf

def translate_rows(row):
    dag_run_conf = get_dag_run_conf()
    ignored_keys = ('period_month', 'year')
    if row:
        return {
            **{k: v for k, v in row.items() if k not in ignored_keys},
            **{
                'period_month': datetime.strptime(row['entry_date'], "%d/%m/%Y").month,
                'year': datetime.strptime(row['entry_date'], "%d/%m/%Y").year,
            }
        }
    return None