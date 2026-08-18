from datetime import datetime as dat

def get_effective_date_for_new_policyset(dag_run, add_one_year=False):
    
    # For Adhoc runs
    if bool(dag_run.conf.get('skip_rundate_validation')):
        if bool(dag_run.conf['report_run_date']):
            date_obj = dat.strptime(dag_run.conf['report_run_date'], "%Y-%m-%d")
            if date_obj.month == 12:
                year = date_obj.year + 1
            else:
                year = date_obj.year
            if add_one_year:
                return dat(year+1,1,1).strftime("%Y-%m-%d")

            return dat(year,1,1).strftime("%Y-%m-%d")

    # For Scheduled runs
    today = dat.today()
    if today.month == 12:
        year = today.year+1
    else:
        year = today.year

    if add_one_year:
        return dat(year+1,1,1).strftime("%Y-%m-%d")

    return dat(year,1,1).strftime("%Y-%m-%d")

def get_balace_to_transfer(item, config):
    current_balance = float(str(item['timeoff_balance']).replace(",", ""))
    max_carry_over_balance = float(item['std_hrs']) * float(2)
    return{
            'name': config.VACATION_TIMEOFF_CARRY_OVER,
            'balance': current_balance if current_balance <= max_carry_over_balance  else  max_carry_over_balance
        }
