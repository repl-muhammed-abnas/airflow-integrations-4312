import rail
null = None

def get_tags_available():
    taglist = rail.result('get_dropdown_options')['tags']
    tags = [ {
            "name": tag['name'],
            "uri": tag['uri'],
            "status": tag['isEnabled']
        } for tag in taglist ]
    return tags

def get_newoeftoadd():
    rows = rail.load_all_records(rail.result('query_oef_not_available_in_replicon'))
    newoeftoadd = [ {
            'name': row['authorizername']
        } for row in rows]
    return newoeftoadd
