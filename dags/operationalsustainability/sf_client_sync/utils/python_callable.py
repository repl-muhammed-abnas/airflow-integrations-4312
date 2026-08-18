import rail
import itertools


def convert_to_country_name(country_code):
    if not country_code:
        return None

    # Mapping for common country codes
    country_map = {
        'US': 'United States',
        'USA': 'United States',
        'CA': 'Canada',
        'UK': 'United Kingdom',
        'GB': 'United Kingdom',
        'AU': 'Australia',
        'NZ': 'New Zealand',
        'DE': 'Germany',
        'FR': 'France',
        'IT': 'Italy',
        'ES': 'Spain',
        'NL': 'Netherlands',
        'BE': 'Belgium',
        'CH': 'Switzerland',
        'AT': 'Austria',
        'SE': 'Sweden',
        'NO': 'Norway',
        'DK': 'Denmark',
        'FI': 'Finland',
        'IE': 'Ireland',
        'PT': 'Portugal',
        'GR': 'Greece',
        'PL': 'Poland',
        'CZ': 'Czech Republic',
        'HU': 'Hungary',
        'RO': 'Romania',
        'BG': 'Bulgaria',
        'JP': 'Japan',
        'CN': 'China',
        'IN': 'India',
        'KR': 'South Korea',
        'SG': 'Singapore',
        'HK': 'Hong Kong',
        'TW': 'Taiwan',
        'MY': 'Malaysia',
        'TH': 'Thailand',
        'ID': 'Indonesia',
        'PH': 'Philippines',
        'VN': 'Vietnam',
        'MX': 'Mexico',
        'BR': 'Brazil',
        'AR': 'Argentina',
        'CL': 'Chile',
        'CO': 'Colombia',
        'PE': 'Peru',
        'ZA': 'South Africa',
        'EG': 'Egypt',
        'NG': 'Nigeria',
        'KE': 'Kenya',
        'IL': 'Israel',
        'AE': 'United Arab Emirates',
        'SA': 'Saudi Arabia',
        'TR': 'Turkey',
        'RU': 'Russia'
    }

    # If it's a code, map it; otherwise return as-is
    return {
          "name": country_map.get(country_code, country_code)
            }
# country_map.get(country_code, country_code)


def should_skip_account_type(dag_run, config):
    if config.account_types_to_sync == 'All':
        return False

    account_type = dag_run.conf.get('Type')
    if account_type is None:
        return True

    allowed_types = [t.strip() for t in config.account_types_to_sync.split(',')]
    return account_type not in allowed_types


def page_handler(request, result_resp):
    if len(result_resp['rows']) > 0:
        request['page'] += 1
        return request
    return None


def get_client_details(response, dag_run):
    client_name = dag_run.conf.get('item').get('Name')
    flatten_rows = list(
        itertools.chain(*[x['rows'] for x in response])
    )
    filtered_client = [
        x for x in flatten_rows if x['cells'][1]['textValue'] == client_name
    ]

    if not filtered_client:
        return {}

    # Take first match if multiple found
    first_match = filtered_client[0]
    return {
        'client_uri': first_match['cells'][0]['uri'],
        'client_name': first_match['cells'][1]['textValue']
    }

