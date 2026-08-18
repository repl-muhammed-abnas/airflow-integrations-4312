from datetime import timedelta
import json
import rail


def for_each_item_from_report():
    def get_actual_cost(cost):
        return float(str(cost).replace(",",""))
    result_list = []
    for item in rail.load_all_records(rail.result("parse_csv")):
        if ('ProjectCode' in item and 'ActualCost' in item and item['ActualCost'] and
            get_actual_cost(item['ActualCost']) > 0 and
            item['ProjectName'] and item['ProjectName'].strip() and
            item['ProjectCode'] and item['ProjectCode'].strip()):
            result_list.append(item)
    return json.dumps(result_list)

def format_payload(dag_run):
    def get_actual_cost(cost):
        return float(str(cost).replace(",",""))
    
    execution_date = dag_run.execution_date
    first_day_current_month = execution_date.replace(day=1)
    last_day_previous_month = first_day_current_month - timedelta(days=1)
    data = {
            "ManualJournals": [
                {
                    "Narration": "Allocation of Monthly Salaries to COGS",
                    "Date": last_day_previous_month.strftime("%Y-%m-%d"),
                    #"Amountsare": "No tax",  # change this when pushing the code to prod it might be Line Amount Types
                    "JournalLines": []
                }
            ]
        }
    for item in json.loads(rail.result("loop_through_csv")):
        data["ManualJournals"][0]["JournalLines"].append({
            "LineAmount": get_actual_cost(item['ActualCost']),
            "AccountCode": "481",
            "Description": item['ProjectCode'],
            "Project": item['ProjectName']
        })
        data["ManualJournals"][0]["JournalLines"].append({
            "LineAmount": get_actual_cost(item['ActualCost']) * -1,
            "AccountCode": "482",
            "Description": item['ProjectCode'],
            "Project": item['ProjectName']
        })
    return data
