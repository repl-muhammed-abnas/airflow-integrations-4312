from macquariegroup.clientimport.utils import custom_methods
import rail

null = None


def get_conf_client(item, index, dag_run, action):
    bu_rows = custom_methods.get_data_from_document(
        rail.result('query_bu_raw_collection'))
    locations_rows = custom_methods.get_data_from_document(
        rail.result('query_locations_raw_collection'))
    custom_fields = rail.result('get_all_client_custom_fields')
    dag_run.conf['time']

    def get_bu_values(bu_item, item):
        return [row[bu_item] for row in bu_rows if item['clientcode'] and item['clientcode'] == row['businessunit']]

    def get_locations_values(location_item, item):
        return [row[location_item] for row in locations_rows if item['location'] and item['location'] == row['location']]

    return {'client_input': list(map(lambda x: {
        "action": action,
        "index": index,
        "clienturi": x['uri'] if action in ('update', 'disable') else null,
        "clientname": x['clientname'] if x['clientname'] else null,
        "clientcode": x['clientcode'] if x['clientcode'] else null,
        "location": x['location'] if x['location'] else null,
        "group": get_bu_values('businessgroup', x)[0] if get_bu_values('businessgroup', x) else null,
        "groupuri": custom_fields["groupuri"],
        "division": get_bu_values('division', x)[0] if get_bu_values('division', x) else null,
        "divisionuri": custom_fields["divisionuri"],
        "locationname": get_locations_values('locationdescription', x)[0] if get_locations_values('locationdescription', x) else null,
        "locationnameuri": custom_fields["locationnameuri"],
        "businessunitname":  get_bu_values('businessunitname', x)[0] if get_bu_values('businessunitname', x) else null,
        "businessunitnameuri": custom_fields["businessunitnameuri"]
    }, item)) if item else []}


def get_put_client_param(item):

    def get_custom_fields():
        custom_fields = []
        if item['group']:
            custom_fields.append({
                "customField": {
                    "uri": item['groupuri'],
                    "name": null,
                    "groupUri": null
                },
                "text": item['group'],
                "date": null,
                "dropDownOption": null,
                "number": null
            })

        if item['group']:
            custom_fields.append({
                "customField": {
                    "uri": item['divisionuri'],
                    "name": null,
                    "groupUri": null
                },
                "text": item['division'],
                "date": null,
                "dropDownOption": null,
                "number": null
            })

        if item['locationname']:
            custom_fields.append({
                "customField": {
                    "uri": item['locationnameuri'],
                    "name": null,
                    "groupUri": null
                },
                "text": item['locationname'],
                "date": null,
                "dropDownOption": null,
                "number": null
            })

        if item['businessunitname']:
            custom_fields.append({
                "customField": {
                    "uri": item['businessunitnameuri'],
                    "name": null,
                    "groupUri": null
                },
                "text": item['businessunitname'],
                "date": null,
                "dropDownOption": null,
                "number": null
            })

        return custom_fields

    return {
        "client": {
            "target": {
                "uri": null,
                "name": item['clientname'],
                "code": null,
                "parameterCorrelationId": null
            },
            "name": item['clientname'],
            "code": item['clientcode'],
            "comment": null,
            "clientManager": null,
            "billingContact": null,
            "clientAddress": null,
            "billingAddress": null,
            "isActive": True,
            "customFieldValues": get_custom_fields(),
            "billingRates": [],
            "expenseCodesAllowedByDefaultOnNewProjects": [],
            "defaultBillingCurrency": null
        }
    }


def client_disable_payload(item):
    return {
        "client": {
            "target": {
                "uri": null,
                "name": item['clientname'],
                "code": null,
                "parameterCorrelationId": null
            },
            "name": item['clientname'],
            "code": item['clientcode'],
            "comment": null,
            "clientManager": null,
            "billingContact": null,
            "clientAddress": null,
            "billingAddress": null,
            "isActive": False,
            "customFieldValues": [],
            "billingRates": [],
            "expenseCodesAllowedByDefaultOnNewProjects": [],
            "defaultBillingCurrency": null
        }
    }
