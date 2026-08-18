import rail

def get_user_defined_data():
    user_details = rail.result('get_user_details')
    first_name = user_details['firstName'][0]
    last_name = user_details['lastName'][0]
    employee_id = str(user_details['employeeId'])
    start_date = rail.result('get_expensesheet_details')['date']
    start_year = str(start_date['year'])
    start_month = str(start_date['month']).zfill(2)
    start_day = str(start_date['day']).zfill(2)

    combined_expense_description = f"{first_name}{last_name}_{employee_id}_{start_year}_{start_month}_{start_day}"
    return combined_expense_description
