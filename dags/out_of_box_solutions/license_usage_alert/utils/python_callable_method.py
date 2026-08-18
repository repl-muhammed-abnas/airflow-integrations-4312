import base64
import json


def encode(config, **_):
    data = {
        "authorityUri": None,
        "resourceUri": f"replicon://{config.company_key}/administration/products",
        "tenant": {
            "companyKey": config.company_key,
            "slug": None,
            "uri": None
        },
        "user": {
            "loginName": None,
            "uri": None
        }
    }
    return base64.urlsafe_b64encode(json.dumps(data).encode()).decode()
