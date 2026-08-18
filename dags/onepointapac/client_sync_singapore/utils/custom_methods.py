from datetime import datetime, timezone
from airflow.models import Variable
import rail


def read_lastsync_time(config):
    last = Variable.get(
        config.last_sync_time_var_name, default_var=None)
    if not last:
        last = datetime(1970, 1, 1, tzinfo=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    current = datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')
    return {'last_synctime': last, 'current_time': current}


def write_lastsync_time(config):
    Variable.set(config.last_sync_time_var_name,
                 rail.result('get_lastsync_time')['current_time'])
