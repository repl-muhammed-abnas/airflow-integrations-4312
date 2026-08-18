# pylint: disable=wildcard-import unused-wildcard-import
from itvdaytime.schedule_sync.config import *

instance = "trial"
environment = 'pre-production'
company_key = "itvdaytimetrial01"

BATCH_SIZE = 4000

pgp_connection_id = f"pgp_{company_key}"

disabled = True
