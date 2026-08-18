# pylint: disable=wildcard-import unused-wildcard-import
from galaxyusopcoinc.decrypt_files.config import *

instance = "production"
environment = 'production'

company_key = 'GalaxyUSOpcoInc'

replicon_conn_id = 'galaxyusopcoinc_replicon_admin'
sftp_conn_id = 'sftp_galaxyusopcoinc_676273'
pgp_conn_id = "pgp_vialto_partners"

input_filepath = "/Tiger/Prod/Input"
archive_input_filepath = "/Tiger/Prod/archiveencryptedfiles"
decrypted_file_upload_path = "/Tiger/Prod/decryptedfiles"

master_dag_active_runs = 1
