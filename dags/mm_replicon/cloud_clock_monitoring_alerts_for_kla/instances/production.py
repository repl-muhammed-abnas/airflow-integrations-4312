# pylint: disable=wildcard-import unused-wildcard-import
from mm_replicon.cloud_clock_monitoring_alerts_for_kla.config import *

instance = 'production'
environment = 'production'

company_key = 'airflow'
replicon_conn_id = 'airflow-replicon-admin'
http_conn_id = 'mm_replicon'

alert_email = '''
                dl-it-apps-webapps@kla-tencor.com,
                Venkat.Dasigi@kla-tencor.com,Jim.Nordin@kla.com,diana.wyland@kla-tencor.com,
                Jimmy.yen@kla-tencor.com,Emilio.flores@kla-tencor.com,Daniel.Morales@kla-tencor.com,
                marcos.fierros@kla-tencor.com,Ferdinand.Lewis@kla-tencor.com
              '''

client_company_name = 'kla'
