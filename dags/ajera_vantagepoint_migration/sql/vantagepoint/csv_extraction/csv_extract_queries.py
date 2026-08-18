"""
csv_extract_queries.py
----------------------
SELECT queries for extracting converted records from cnv* staging tables in the Ajera database.

SQL_MAP keys correspond to the sql_key values in config.CSV_EXTRACTIONS and the
dag_run.conf['sql_key'] passed to each extract_csv child DAG.

The {ajera_db} placeholder is replaced at runtime by sql_source() in custom_methods.py
with the bracketed Ajera database name (e.g. '[ACMECORP_AJ_20250327_143022]').

Queries / output files:
    client         → Client.csv
    client_address → ClientAddress.csv
    contact        → Contact.csv
    vendor         → Vendor.csv
    vendor_address → VendorAddress.csv
"""

CLIENT_SQL = """select  ClientID,
        Client,
        [Name],
        coalesce(ClientType, '') ClientType,
        [Status],
        coalesce(Website, '') Website,
        coalesce(Memo, '') Memo,
        ClientInd
    from {ajera_db}.dbo.cnvClient
    order by 1;"""


CLIENT_ADDRESS_SQL = """select 	ClientID,
		Client,
		[Address],
		replace(coalesce(Address1, ''), ',', '') Address1,
		replace(coalesce(Address2, ''), ',', '') Address2,
		replace(coalesce(Address3, ''), ',', '') Address3,
		replace(coalesce(City, ''), ',', '') City,
		coalesce([State], '') [State],
		coalesce(ZIP, '') ZIP,
		coalesce(Country, '') Country,
		coalesce(Phone, '') Phone,
		coalesce(FAX, '') FAX,
		coalesce(EMail, '') EMail,
		PrimaryInd,
		Billing
from {ajera_db}.dbo.cnvClientAddress
order by 1, 2, 14 desc;"""


CONTACT_SQL = """select ContactID,
     coalesce(ClientID, '') ClientID,
     coalesce(LastName, '') LastName,
     coalesce(FirstName, '') FirstName,
     coalesce(MiddleName, '') MiddleName,
     coalesce(Title, '') Title,
     coalesce(Address1, '') Address1,
     coalesce(Address2, '') Address2,
     coalesce(Address3, '') Address3,
     coalesce(City, '') City,
     coalesce([State], '') [State],
     coalesce(ZIP, '') ZIP,
     coalesce(Country, '') Country,
     coalesce(Phone, '') Phone,
     coalesce(Fax, '') Fax,
     coalesce(Mobile, '') CellPhone,
     coalesce(Email, '') Email,
     coalesce(Memo, '') Memo, ContactStatus,
     coalesce(Website, '') Website
from {ajera_db}.dbo.cnvContact
order by 1;"""


VENDOR_SQL = """select 	ClientID,
		Vendor,
		Client,
		[Name],
		[Status],
		coalesce(Website, '') Website,
		coalesce(Memo, '') Memo,
		VendorInd,
		VendorType,
		AvailableForCRM,
		ReadyForApproval,
		ReadyForProcessing,
		coalesce(FedID, '') FedID,
		PayTerms,
		coalesce(RegAccount, '') RegAccount,
		coalesce(OHAccount, '') OHAccount,
		ThisYear1099,
		LastYear1099,
		Req1099,
		coalesce(AccountNumber, '') AccountNumber
from {ajera_db}.dbo.cnvVendor
order by 1;"""


VENDOR_ADDRESS_SQL = """select 	ClientID,
		[Address],
		coalesce(Address1, '') Address1,
		coalesce(Address2, '') Address2,
		coalesce(Address3, '') Address3,
		coalesce(City, '') City,
		coalesce([State], '') [State],
		coalesce(ZIP, '') ZIP,
		coalesce(Country, '') Country,
		coalesce(Phone, '') Phone,
		coalesce(FAX, '') FAX,
		coalesce(EMail, '') EMail,
		PrimaryInd,
		Payment
from {ajera_db}.dbo.cnvVendorAddress
order by 1, 2, 3;"""


SQL_MAP = {
    'client':         CLIENT_SQL,
    'client_address': CLIENT_ADDRESS_SQL,
    'contact':        CONTACT_SQL,
    'vendor':         VENDOR_SQL,
    'vendor_address': VENDOR_ADDRESS_SQL,
}
