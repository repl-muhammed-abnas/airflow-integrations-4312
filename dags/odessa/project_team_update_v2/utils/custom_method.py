import math
import rail


def convert_input_data_to_task_data(item):
    if not item:
        return []

    if rail.get_company_key().lower() == 'odessasandbox':
        return {
        'key': item['key'] if item['key'] else None,
        'summary': item['fields']['summary'] if item['fields']['summary'] else None,
        'customer': item['fields']['customfield_10108']['value'].strip() if item['fields']['customfield_10108'] else None,
        'wing': item['fields']['customfield_10068']['value'] if item['fields']['customfield_10068'] else None,
        'task_type': item['fields']['issuetype']['name'] if item['fields']['issuetype'] else None,
        'parent_jira': item['fields']['customfield_10138'] if item['fields']['customfield_10138'] else None,
        'epic_id': item['fields']['customfield_10014'] if item['fields']['customfield_10014'] else None
    }

    return {
        'key': item['key'] if item['key'] else None,
        'summary': item['fields']['summary'] if item['fields']['summary'] else None,
        'customer': item['fields']['customfield_10108']['value'].strip() if item['fields']['customfield_10108'] else None,
        'wing': item['fields']['customfield_10103']['value'] if item['fields']['customfield_10103'] else None,
        'task_type': item['fields']['issuetype']['name'] if item['fields']['issuetype'] else None,
        'parent_jira': item['fields']['customfield_10205'] if item['fields']['customfield_10205'] else None,
        'epic_id': item['fields']['customfield_10014'] if item['fields']['customfield_10014'] else None
    }


def generate_pagination_pages(jira_response):
    """
    Generate pagination data for Jira API v3 response.
    First page uses the base response, subsequent pages use nextPageToken.
    Only creates the first page - subsequent pages will be handled by chaining.
    """

    pages = []

    # First page (page 1) - use base response
    pages.append({
        'page_number': 1,
        'next_page_token': None,
        'is_base_page': True,
        'base_response': rail.write_json_artifact(jira_response),  # Serialize for safe passing
        'is_last_page': jira_response.get('isLast', True),
        'next_page_token_for_chaining': jira_response.get('nextPageToken')
    })

    # Only create second page if there are more pages
    # Subsequent pages will be handled by each child DAG triggering the next one
    if 'nextPageToken' in jira_response and jira_response['nextPageToken'] and not jira_response.get('isLast', True):
        pages.append({
            'page_number': 2,
            'next_page_token': jira_response['nextPageToken'],
            'is_base_page': False,
            'base_response': None,
            'is_last_page': False,  # Will be determined at runtime
            'next_page_token_for_chaining': None  # Will be determined at runtime
        })

    return pages


def should_trigger_next_page(response_data, current_page_number):
    """
    Determine if we should trigger the next page based on the response.
    """
    if not response_data:
        return False, None

    # Check if this response indicates there are more pages
    has_next_token = 'nextPageToken' in response_data and response_data['nextPageToken']
    is_not_last = not response_data.get('isLast', True)

    if has_next_token and is_not_last:
        return True, {
            'page_number': current_page_number + 1,
            'next_page_token': response_data['nextPageToken'],
            'is_base_page': False,
            'base_response': None,
            'is_last_page': False,
            'next_page_token_for_chaining': None
        }

    return False, None
