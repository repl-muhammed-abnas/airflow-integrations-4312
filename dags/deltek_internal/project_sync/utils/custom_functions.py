"""
Custom utility functions for Salesforce to Polaris Project Sync
Contains helper functions used by master and child DAGs
"""


def generate_processing_summary(opportunities, results):
    """
    Generate summary of processing results

    Args:
        opportunities: List of opportunities that were processed
        results: Results from child DAG runs

    Returns:
        Dictionary with summary statistics
    """
    total_opportunities = len(opportunities) if opportunities else 0
    successful_results = [r for r in results if r.get('status') == 'success']
    failed_results = [r for r in results if r.get('status') == 'failed']

    return {
        'total_opportunities': total_opportunities,
        'successful_count': len(successful_results),
        'failed_count': len(failed_results),
        'success_rate': f"{(len(successful_results) / total_opportunities * 100):.1f}%" if total_opportunities > 0 else '0%',
        'opportunities': opportunities,
        'results': results
    }


def get_projects_list(response):
    check_list = []
    if response:
        project_detail_dict = response[0].get("projectDetails")
        if project_detail_dict:
            check_list.append(project_detail_dict)
            return check_list
        else:
            return check_list
    else:
        return check_list  
    
def customFieldsToApply_for_modification_payload(listOfDict, filter_value):
    filtered_data = next((item['customField'] for item in listOfDict if item["customField"]["displayText"] == filter_value), None)

    customField = {
        "groupUri": filtered_data.get("groupUri"),
        "name": filtered_data.get("name"),
        "uri": filtered_data.get("uri")
    }

    return customField

def dropdown_uri_for_modification_payload(listOfDict, filter_value):
    dropDownOption_uri = next((item['dropDownOption'] for item in listOfDict if item["customField"]["displayText"] == filter_value), None)
    return dropDownOption_uri