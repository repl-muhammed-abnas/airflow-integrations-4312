import rail


def get_product_licensing_summary_response_filter(response):
    response = response.json()['d']
    if not response:
        return []

    return list(filter(lambda x: x['uri'] == rail.find_first_by_attr_and_get_attr(
        rail.result('get_all_public_licennsed_products'), 'uri', x['uri'], 'uri', default=""), list(map(lambda item: {
            "uri": item['product']['uri'],
            "product": item['product']['displayText'],
            "licensespuchased": item['seatsPurchased'],
            "licensesassigned": item['seatsAssigned'],
            "licensesremaining": item['seatsPurchased'] - item['seatsInUse'],
            "licensesremainingpercentage": round(
                (float(item['seatsAssigned']) / float(item['seatsPurchased']))*100, 1) if item['seatsPurchased'] >= 1 else None
        }, response['licensedProducts']))))

def project_list(response):
    response = response.json()['d']['rows']
    return list(map(lambda item: {
            "Projectname": item['cells'][0]['textValue'],
            "Projecturi": item['cells'][0]['uri'],
            "Projectstatus": item['cells'][1]['textValue'],
            "Projectstartdate": item['cells'][2]['textValue'],
            "Projectenddate": item['cells'][3]['textValue']
        }, response))

def guru_test(response):
    response= response.json()['d']

    return rail.find_first_by_attr_and_get_attr(
        response, 'displayText', 'TaskOutlineNumber', 'uri', default=""),rail.find_first_by_attr_and_get_attr(
        response, 'displayText', 'TaskOutlinelevel', 'uri', default="")
