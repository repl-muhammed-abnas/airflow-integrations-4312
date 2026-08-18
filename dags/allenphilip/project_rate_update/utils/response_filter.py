null = None


def get_project_teammembers_data(response):
    return [{
        "loginname": data['resource']['user']['loginName'] if data['resource'] and
        data['resource']['user'] and data['resource']['user']['loginName'] else None,
        "username": data['resource']['user']['displayText'] if data['resource'] and
        data['resource']['user'] and data['resource']['user']['displayText'] else None,
        "useruri": data['resource']['uri'] if data['resource'] and data['resource']['uri'] else None
    } for data in response]


def get_billing_rates(response):
    return [{
        "loginname": rate['user']['loginName'] if rate['user'] and rate['user']['loginName'] else None,
        "useruri": rate['user']['uri'] if rate['user'] and rate['user']['uri'] else None,
        "isenabled": rate['isEnabled'] if rate['isEnabled'] else None,
        "amount": round(rate['effectiveBillingRate']['value']['amount'], 2) if rate['effectiveBillingRate'] and
        rate['effectiveBillingRate']['value'] and rate['effectiveBillingRate']['value']['amount'] else None,
        "currencysymbol": rate['effectiveBillingRate']['value']['currency']['symbol'] if rate['effectiveBillingRate'] and
        rate['effectiveBillingRate']['value'] and rate['effectiveBillingRate']['value']['currency'] and
        rate['effectiveBillingRate']['value']['currency']['symbol'] else None
    } for rate in response]
