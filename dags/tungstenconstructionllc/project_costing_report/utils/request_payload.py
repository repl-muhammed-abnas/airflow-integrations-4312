# pylint: disable=too-many-statements
from datetime import datetime

def get_row_data(item):
    return [
        item.get('Login Name'),
        datetime.strptime(item['Timesheet Period'].split('-')[0].strip(), "%b %d, %Y").strftime("%Y-%m-%d"),
        datetime.strptime(item['Timesheet Period'].split('-')[-1].strip(), "%b %d, %Y").strftime("%Y-%m-%d"),
        item.get('Project Name'),
        item['Regular Time Hours'].replace(",", "") if item.get('Regular Time Hours') else 0,
        item['Overtime Hours'].replace(",", "") if item.get('Overtime Hours') else 0
    ]


def get_row_hourly_cost_data(item):
    return [
        item.get('Client Name'),
        item.get('Project Name'),
        datetime.strptime(item['Timesheet Start Date'].strip(), "%b %d, %Y").strftime("%Y-%m-%d") if item.get('Timesheet Start Date') else None,
        datetime.strptime(item['Timesheet End Date'].strip(), "%b %d, %Y").strftime("%Y-%m-%d") if item.get('Timesheet End Date') else None,
        item.get('Total Hrs',''),
        item['User Name'].split(', ')[-1] + " " + item['User Name'].split(', ')[0] if item.get('User Name') else None,
        item.get('Login Name'),
        item['HourlyCostAmount__amount'].replace(",", "") if item.get('HourlyCostAmount__amount') else 0,
        item['Equipment Cost'].replace(",", "") if item.get('Equipment Cost') else 0
    ]

def get_row_expense_data(item):
    return [
        item.get('Client Name'),
        item.get('Project Name'),
        item['User Name'].split(', ')[-1] + " " + item['User Name'].split(', ')[0] if item.get('User Name') else None,
        item.get('Login Name'),
        item.get('Expense Code'),
        item.get('Tracking Number'),
        datetime.strptime(item['Incurred Date'].strip(), "%b %d, %Y").strftime("%Y-%m-%d") if item.get('Incurred Date') else None,
        item['Amount__amount'].replace(",", "") if item.get('Amount__amount') else 0,
        item.get('Approval Status')
    ]

def get_row_final_data(item):
    print(f"get_row_final_data item: {item}")
    return [
        item['properties'].get('Client'),
        item['properties'].get('Project'),
        item['properties'].get('Daterange'),
        item['properties'].get('Perdiemtotals'),
        item['properties'].get('Grossincometotals'),
        item['properties'].get('Weeklytotals')
    ]
