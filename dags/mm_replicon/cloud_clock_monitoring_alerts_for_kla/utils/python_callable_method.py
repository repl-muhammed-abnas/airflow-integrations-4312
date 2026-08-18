from datetime import datetime, timedelta


def build_query_params():
    thirty_days_ago = (datetime.now()-timedelta(days=30)
                       ).strftime("%Y-%m-%dT00:01:00-00:00")
    fifteen_mins_ago = (datetime.now()-timedelta(minutes=15)
                        ).strftime("%Y-%m-%dT%H:%M:00-00:00")
    return {'minLastUpdate': thirty_days_ago, 'maxLastUpdate': fifteen_mins_ago}
