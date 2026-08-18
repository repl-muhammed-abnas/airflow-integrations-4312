
from datetime import timedelta
from decimal import Decimal
from dxctechnology.workday_user_import_v1.user_import.common_utils.custom_methods import get_day_diff_between_two_dates, convert_json_date_to_date, get_tenure_value
import rail

def exp_to_decimal_best(exp_str):
    try:
        decimal_num = Decimal(str(exp_str))
        result = format(decimal_num.quantize(Decimal('1.00')), '.2f')
        return result
    except Exception as e:
        return f"Error: {str(e)}"

def get_transaction_description(item):
    return rail.find_first_by_attr_and_get_attr(
        item['metadata'],
        'keyUri',
        'urn:replicon:time-off-transaction-key-value-key:transaction-description'
        'value',
        default = {}
    ).get('text')

def get_total_balance(item):
    return item['timeRemaining']


def get_transactions_history_aus_prorata_accrual_timeoff_data_handler(response, dag_run):
    # added here to make sure the service call response is present even in case of any failure on below logic    
    rail.set_result(key="response", val= response)

    location_effective_date = convert_json_date_to_date(dag_run.conf['json_formatted_dates']['location_effective_date'])
    tenure = get_tenure_value(location_effective_date, convert_json_date_to_date(dag_run.conf['json_formatted_dates']['schedule_change_date']))
    previous_location_state = dag_run.conf['old_location_state']
    location_effective_date_minus_one = location_effective_date - timedelta(days=1)
    mapped_data =  list(map(lambda record : {
        "transaction_description": get_transaction_description(record),
        "total_balance": record['timeRemaining'],
        "amount": record['amount'],
        "date": f"{record['date']['year']}/{record['date']['month']}/{record['date']['day']}",
        "date_json": record['date'],
        "day_diff": get_day_diff_between_two_dates(date_1=convert_json_date_to_date(record['date']), date_2=location_effective_date),
        "day_diff_2": get_day_diff_between_two_dates(date_1=convert_json_date_to_date(record['date']), date_2=location_effective_date_minus_one)
    }, response))

    accrual_amount_for_prorata_balance = rail.find_first_by_attr_and_get_attr(
        mapped_data,
        'date',
        f"{location_effective_date_minus_one.year}/{location_effective_date_minus_one.month}/{location_effective_date_minus_one.day}",
        'total_balance'
    )

    """
        For Victoria and QueensLand the accrual amount will be added as the starting balance not as prorata balance
    """
    prorata_balance_to_be_added_in_policy = accrual_amount_for_prorata_balance
    if previous_location_state.lower() == "victoria" and int(tenure) > 7:
        prorata_balance_to_be_added_in_policy = 0
    else:
        if previous_location_state.lower() == "queensland" and int(tenure) > 15:
            prorata_balance_to_be_added_in_policy = 0

    accrual_amount_for_starting_balance = rail.find_first_by_attr_and_get_attr(
        mapped_data,
        'date',
        f"{location_effective_date.year}/{location_effective_date.month}/{location_effective_date.day}",
        'total_balance'
    )

    starting_balance_to_update = exp_to_decimal_best(str(rail.result('get_user_timeoff_balance_summary')["timeRemaining"]))
    if accrual_amount_for_prorata_balance:
        if previous_location_state.lower() == "victoria" and int(tenure) > 7:
            starting_balance_to_update = float(starting_balance_to_update) + float(accrual_amount_for_starting_balance)
        else:
            if previous_location_state.lower() == "queensland" and int(tenure) > 15:
                starting_balance_to_update = float(starting_balance_to_update) + float(accrual_amount_for_starting_balance)

    return {
        "mapper_data": mapped_data,
        "starting_balance_to_update_value": starting_balance_to_update,
        "prorata_balance_to_be_added_in_policy": prorata_balance_to_be_added_in_policy,
        "accrual_amount_for_starting_balance": accrual_amount_for_starting_balance,
        "accrual_amount_for_prorata_balance": accrual_amount_for_prorata_balance,
        "tenure": tenure
    }
