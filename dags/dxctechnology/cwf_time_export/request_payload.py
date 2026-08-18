from datetime import datetime, timezone


def get_today_utc_date():
    return datetime.now(timezone.utc)
