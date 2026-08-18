from datetime import datetime as dt

import pendulum
import rail

null = None

def get_compose_item_payout_data_row(items,config):
    current_date = pendulum.now(config.time_zone).strftime("%d-%m-%Y")
    return [
        "P2010",
        items['empid'],
        "US",
        "000",
        "INS",
        "2010",
        items['paycode'],
        dt.strptime(rail.find_first_by_attr_and_get_attr(
                config.USA_PAYROLL_CALENDER_MAPPER_TO_USE, "payroll_processing_date", current_date,"pay_period_end_date"), "%d-%m-%Y").strftime(
                "%Y%m%d") if config.instance == 'prod' else dt.strptime(current_date,"%d-%m-%Y").strftime("%Y%m%d"),
        "",
        "",
        "",
        "000",
        "",
        items['paycode'],
        "",
        "",
        "",
        "",
        "USD",
        items['timeoff_balance'],
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
        "",
    ]
