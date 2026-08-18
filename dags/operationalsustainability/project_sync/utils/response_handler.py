def get_required_client(response, account_name):
    required_client={}
    for client in response['rows']:
        if client['cells'][0]['textValue'] == account_name:
            required_client={
                "client_name": client['cells'][0]['textValue'],
                "client_uri": client['cells'][0]['uri']
            }
            break
    return required_client


def get_required_user(response):
    if not response:
        return {"user_uri": None}
    user = {
        "user_uri": response[0]["userDetails"]["uri"]
    }
    return user