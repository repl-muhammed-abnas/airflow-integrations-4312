from datetime import datetime
import json
import shutil
import requests
import logging
from airflow.models import Variable
logging.basicConfig(level=logging.INFO)

class OutlookConnection:

    def __decode_value(self, value:str, key:str):
        from base64 import urlsafe_b64decode
        return urlsafe_b64decode(value.replace(f"{key}_", "").encode('utf-8')).decode('utf-8')

    def __encode_value(self, key:str, value:str):
        from base64 import urlsafe_b64encode
        return f"{key}_{urlsafe_b64encode(value.encode('utf-8')).decode('utf-8')}"

    def __init__(self, outlook_connection_var_name):
        self.outlook_connection_var_name: str = outlook_connection_var_name
        outlook_connection_var_data: dict = Variable.get(outlook_connection_var_name, deserialize_json=True)
        self.tenant_id: str = self.__decode_value(outlook_connection_var_data.get("TENANT_ID"), "TENANT_ID")
        self.access_token: str = self.__decode_value(outlook_connection_var_data.get('access_token'), "access_token")
        self.refresh_token: str = self.__decode_value(outlook_connection_var_data.get('refresh_token'), "refresh_token")
        self.client_id: str = self.__decode_value(outlook_connection_var_data.get('client_id'), "client_id")
        self.client_secret: str = self.__decode_value(outlook_connection_var_data.get('client_secret'), "client_secret")
        self.scope: str = outlook_connection_var_data.get('scope', "mail.read")
        self.token_url: str = "https://login.microsoftonline.com/organizations/oauth2/v2.0/token"

    def timestamp(self) -> str:
        return datetime.now().strftime("%Y%m%d%H%M%S")

    def _update_token_details(self, token_data: dict):
        self.access_token = token_data.get('access_token')
        self.refresh_token = token_data.get('refresh_token')
        self.expires_in = token_data.get('expires_in')
        self.token_type = token_data.get('token_type')
        self.scope = token_data.get('scope')

        token_data['access_token'] = self.__encode_value("access_token", token_data.get('access_token'))
        token_data['refresh_token'] = self.__encode_value("refresh_token", token_data.get('refresh_token'))
        # adding timestamp and other details to token_data
        token_data['timestamp'] = self.timestamp()
        token_data['client_id'] = self.__encode_value("client_id", self.client_id)
        token_data['client_secret'] = self.__encode_value("client_secret", self.client_secret)
        token_data['TENANT_ID'] = self.__encode_value("TENANT_ID", self.tenant_id)
        # Update the Airflow Variable
        Variable.set(self.outlook_connection_var_name, token_data, serialize_json=True)
        logging.info("Token details updated in Airflow Variable.")
        return {
            "status": "success",
            "message": "Token details updated successfully."
        }

    def _refresh_access_token(self):
        if not all([self.refresh_token, self.client_id, self.client_secret]):
            raise Exception("Missing parameters for refreshing access token.")
        
        payload = {
            'client_id': self.client_id,
            'client_secret': self.client_secret,
            'grant_type': 'refresh_token',
            'refresh_token': self.refresh_token,
            'scope': self.scope,
        }

        response = requests.post(self.token_url, data=payload)
        if response.status_code == 200:
            token_data = response.json()
            logging.info("Access token refreshed.")
            return self._update_token_details(token_data)
        logging.error("Failed to refresh access token.")
        raise Exception(f"Token refresh failed. Response: {response.text}")

    def _test_connection(self, max_retry_count=3, retry_count=1):
        logging.info(f"Testing connection to Outlook API for {self.outlook_connection_var_name}.")
        url = "https://graph.microsoft.com/v1.0/me/messages?$top=1"
        headers = self.get_headers()
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            logging.info("Connection test successful.")
            return True
        elif response.status_code == 401:
            if retry_count > max_retry_count:
                logging.error("Max retry count reached. Unable to refresh access token.")
                raise Exception("Access token expired and can not generate new access token.")

            logging.warning(f"Access token expired. Attempting to refresh. attempt: {retry_count}")
            self._refresh_access_token()
            return self._test_connection(retry_count=retry_count + 1)
        else:
            logging.error(f"Connection failed with status code: {response.status_code} and response: {response.text}")
            return False

    def get_connection(self, force_refresh=False):
        if force_refresh:
            # should be used when you want to force refresh the token and not use the existing one
            # common scenario is when you updated the permission on of the user
            logging.info("force_refresh is set to True - Force refreshing access token")
            self._refresh_access_token()
        if not self._test_connection():
            raise Exception("Failed to establish a connection to Outlook API.")
        session = requests.Session()
        session.headers.update(self.get_headers())
        return session

    def get_headers(self):
        return {
            'Authorization': f'Bearer {self.access_token}',
            'Accept': 'application/json'
        }
