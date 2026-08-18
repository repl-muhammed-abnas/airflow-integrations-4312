import rail
    
def convert_decimal_to_seconds():
    conf = rail.get_dag_run_conf()
    try:
        hours = float(conf.get("timeoff_hrs", {}).get("hours", 0))
        minutes = float(conf.get("timeoff_hrs", {}).get("minutes", 0))
        decimal_hours = hours + (minutes / 60.0)
        seconds = int(decimal_hours * 3600)
    except (TypeError, ValueError, KeyError):
        seconds = 0
    return {"seconds": seconds}