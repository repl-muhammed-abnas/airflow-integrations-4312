

null = None

def get_filtered_time_off_details_on_booking_id(response, dag_run):
    return list(filter(lambda x: x['booking_id'] == dag_run.conf['booking_id'], map(lambda row: {
        "timeoff_uri": row['cells'][0]['uri'],
        "timeoff_type": row['cells'][1]['textValue'],
        "timeoff_type_uri": row['cells'][1]['uri'],
        "booking_id": row['cells'][2]['textValue'],
        "timeoff_start_date": str(row['cells'][3]['dateValue']['year'])+'-'+str(row['cells'][3]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][3]['dateValue']['day']).zfill(2),
        "timeoff_end_date": str(row['cells'][4]['dateValue']['year'])+'-'+str(row['cells'][4]['dateValue']['month']).zfill(2)
        + '-'+str(row['cells'][4]['dateValue']['day']).zfill(2),
        "timeoff_start_time":str(row['cells'][5]['timeValue']['hour']).zfill(2)+':'+str(row['cells'][5]['timeValue']['minute']).zfill(2)
        + ':'+str(row['cells'][5]['timeValue']['second']).zfill(2) if row['cells'][5]['dataType'] != 'urn:replicon:list-type:null' else null,
        "timeoff_end_time":str(row['cells'][6]['timeValue']['hour']).zfill(2)+':'+str(row['cells'][6]['timeValue']['minute']).zfill(2)
        + ':'+str(row['cells'][6]['timeValue']['second']).zfill(2) if row['cells'][6]['dataType'] != 'urn:replicon:list-type:null' else null
    }, response['rows'])))
