"""
table_create_queries.py
-----------------------
DDL statements for creating the cnv* conversion staging tables in the Ajera database.

TABLE_SQL_MAP maps short keys to DROP-IF-EXISTS + CREATE TABLE SQL strings.
All tables are created inside the Ajera database (the master DAG issues
'USE [<ajera_db_name>]' before executing these statements).

Tables:
    cnv_client         → cnvClient         — client master records
    cnv_client_address → cnvClientAddress  — client address records
    cnv_contact        → cnvContact        — contact records linked to clients
    cnv_vendor         → cnvVendor         — vendor master records with payment fields
    cnv_vendor_address → cnvVendorAddress  — vendor address records
"""

CNV_CLIENT_SQL = """
DROP TABLE IF EXISTS cnvClient;

CREATE TABLE cnvClient (
    ClientID VARCHAR(32) NULL DEFAULT NULL,
    Client NVARCHAR(20) NULL DEFAULT NULL,
    srcClientKey INT NOT NULL DEFAULT ((0)),
    Relationship NVARCHAR(50) NULL DEFAULT NULL,
    Name NVARCHAR(100) NULL DEFAULT NULL,
    Status VARCHAR(1) NULL DEFAULT NULL,
    ClientType NVARCHAR(10) NULL DEFAULT NULL,
    ClientTypeDescription VARCHAR(255) NULL DEFAULT NULL,
    AvailableforCRM VARCHAR(1) NULL DEFAULT NULL,
    ReadyforApproval VARCHAR(1) NULL DEFAULT NULL,
    ClientInd VARCHAR(1) NULL DEFAULT NULL,
    Competitor VARCHAR(1) NULL DEFAULT NULL,
    GovernmentAgency VARCHAR(1) NULL DEFAULT NULL,
    Market NVARCHAR(10) NULL DEFAULT NULL,
    Website NVARCHAR(255) NULL DEFAULT NULL,
    Memo NVARCHAR(MAX) NULL DEFAULT NULL,
    OwnerFirstName NVARCHAR(20) NULL DEFAULT NULL,
    OwnerLastName NVARCHAR(20) NULL DEFAULT NULL
);"""


CNV_CLIENT_ADDRESS_SQL = """
DROP TABLE IF EXISTS cnvClientAddress;

CREATE TABLE cnvClientAddress (
    ClientID VARCHAR(32) NULL DEFAULT NULL,
    Client NVARCHAR(20) NULL DEFAULT NULL,
    srcClientKey INT NOT NULL DEFAULT ((0)),
    Address NVARCHAR(20) NULL DEFAULT NULL,
    Address1 NVARCHAR(50) NULL DEFAULT NULL,
    Address2 NVARCHAR(50) NULL DEFAULT NULL,
    Address3 NVARCHAR(50) NULL DEFAULT NULL,
    Address4 NVARCHAR(50) NULL DEFAULT NULL,
    City NVARCHAR(30) NULL DEFAULT NULL,
    State NVARCHAR(10) NULL DEFAULT NULL,
    ZIP NVARCHAR(10) NULL DEFAULT NULL,
    Country NVARCHAR(2) NULL DEFAULT NULL,
    Phone NVARCHAR(24) NULL DEFAULT NULL,
    Fax NVARCHAR(24) NULL DEFAULT NULL,
    Email NVARCHAR(50) NULL DEFAULT NULL,
    PrimaryInd VARCHAR(1) NULL DEFAULT NULL,
    Billing VARCHAR(1) NULL DEFAULT NULL,
    Accounting VARCHAR(1) NULL DEFAULT NULL,
    TaxCountry NVARCHAR(2) NULL DEFAULT NULL,
    TaxRegNumber NVARCHAR(20) NULL DEFAULT NULL
);"""


CNV_CONTACT_SQL = """
DROP TABLE IF EXISTS cnvContact;

CREATE TABLE cnvContact (
    ContactID VARCHAR(32) NULL DEFAULT NULL,
    srcContactKey INT NOT NULL DEFAULT ((0)),
    ClientID VARCHAR(32) NULL DEFAULT NULL,
    srcClientKey INT  NULL DEFAULT ((0)),
    FirmName NVARCHAR(100) NULL DEFAULT NULL,
    CLAddress NVARCHAR(20) NULL DEFAULT NULL,
    FirstName NVARCHAR(25) NULL DEFAULT NULL,
    MiddleName NVARCHAR(30) NULL DEFAULT NULL,
    LastName NVARCHAR(30) NULL DEFAULT NULL,
    PreferredName NVARCHAR(60) NULL DEFAULT NULL,
    Prefix NVARCHAR(5) NULL DEFAULT NULL,
    Suffix NVARCHAR(20) NULL DEFAULT NULL,
    Title NVARCHAR(50) NULL DEFAULT NULL,
    Owner NVARCHAR(20) NULL DEFAULT NULL,
    Source NVARCHAR(50) NULL DEFAULT NULL,
    Status NVARCHAR(10) NULL DEFAULT NULL,
    QualifiedStatus NVARCHAR(10) NULL DEFAULT NULL,
    MailingAddress VARCHAR(1) NULL DEFAULT NULL,
    Address1 NVARCHAR(50) NULL DEFAULT NULL,
    Address2 NVARCHAR(50) NULL DEFAULT NULL,
    Address3 NVARCHAR(50) NULL DEFAULT NULL,
    Address4 NVARCHAR(50) NULL DEFAULT NULL,
    City NVARCHAR(30) NULL DEFAULT NULL,
    State NVARCHAR(10) NULL DEFAULT NULL,
    ZIP NVARCHAR(10) NULL DEFAULT NULL,
    Country NVARCHAR(50) NULL DEFAULT NULL,
    Phone NVARCHAR(24) NULL DEFAULT NULL,
    Mobile NVARCHAR(24) NULL DEFAULT NULL,
    Home NVARCHAR(24) NULL DEFAULT NULL,
    Fax NVARCHAR(24) NULL DEFAULT NULL,
    Pager NVARCHAR(24) NULL DEFAULT NULL,
    EMail NVARCHAR(255) NULL DEFAULT NULL,
    Memo NVARCHAR(MAX) NULL DEFAULT NULL,
    ContactStatus VARCHAR(1) NULL DEFAULT NULL,
    Website NVARCHAR(255) NULL DEFAULT NULL
);"""


CNV_VENDOR_SQL = """
DROP TABLE IF EXISTS cnvVendor;

CREATE TABLE cnvVendor (
    ClientID                    VARCHAR(32)         NOT NULL DEFAULT (''),
    Vendor                      NVARCHAR(20)        NOT NULL DEFAULT (''),
    Client                      NVARCHAR(20)        NOT NULL DEFAULT (''),
    srcVendorKey                INT                 NOT NULL DEFAULT ((0)),
    Relationship                NVARCHAR(50)        NOT NULL DEFAULT (''),
    [Name]                      NVARCHAR(100)       NOT NULL DEFAULT (''),
    [Status]                    VARCHAR(1)          NOT NULL DEFAULT (''),
    AvailableForCRM             VARCHAR(1)          NOT NULL DEFAULT ('Y'),
    ReadyForApproval            VARCHAR(1)          NOT NULL DEFAULT ('Y'),
    ReadyForProcessing          VARCHAR(1)          NOT NULL DEFAULT ('Y'),
    ClientInd                   VARCHAR(1)          NOT NULL DEFAULT ('N'),
    VendorInd                   VARCHAR(1)          NOT NULL DEFAULT ('Y'),
    Competitor                  VARCHAR(1)          NOT NULL DEFAULT ('N'),
    GovernmentAgency            VARCHAR(1)          NOT NULL DEFAULT ('N'),
    Market                      NVARCHAR(10)        NOT NULL DEFAULT (''),
    Website                     NVARCHAR(255)       NOT NULL DEFAULT (''),
    Memo                        NVARCHAR(MAX)       NOT NULL DEFAULT (''),
    OwnerFirstName              NVARCHAR(20)        NOT NULL DEFAULT (''),
    OwnerLastName               NVARCHAR(20)        NOT NULL DEFAULT (''),
    VendorType                  VARCHAR(1)          NOT NULL DEFAULT (''),
    PayTerms                    NVARCHAR(4)         NOT NULL DEFAULT (''),
    PaymentNotes                NVARCHAR(MAX)       NOT NULL DEFAULT (''),
    SeparateChecks              VARCHAR(1)          NOT NULL DEFAULT (''),
    RegAccount                  NVARCHAR(13)        NOT NULL DEFAULT (''),
    OHAccount                   NVARCHAR(13)        NOT NULL DEFAULT (''),
    IndirectAccount             NVARCHAR(13)        NOT NULL DEFAULT (''),
    TaxRegNumber                NVARCHAR(20)        NOT NULL DEFAULT (''),
    FedID                       NVARCHAR(11)        NOT NULL DEFAULT (''),
    TypeofTIN                   VARCHAR(1)          NOT NULL DEFAULT (''),
    LastYear1099                DECIMAL(19,4)       NOT NULL DEFAULT ((0)),
    ThisYear1099                DECIMAL(19,4)       NOT NULL DEFAULT ((0)),
    Req1099                     VARCHAR(1)          NOT NULL DEFAULT (''),
    DefaultTaxCode              NVARCHAR(10)        NOT NULL DEFAULT (''),
    AccountNumber               NVARCHAR(25)        NOT NULL DEFAULT (''),
    ElectronicPaymentMethod     NVARCHAR(20)        NOT NULL DEFAULT (''),
    EFTAccountNumber            NVARCHAR(17)        NOT NULL DEFAULT (''),
    EFTAccountType              VARCHAR(1)          NOT NULL DEFAULT (''),
    EFTAddenda                  VARCHAR(1)          NOT NULL DEFAULT (''),
    EFTBankID                   NVARCHAR(8)         NOT NULL DEFAULT (''),
    EFTRemittance               VARCHAR(1)          NOT NULL DEFAULT (''),
    EFTEmail                    NVARCHAR(MAX)       NOT NULL DEFAULT (''),
    EFTStatus                   NVARCHAR(1)         NOT NULL DEFAULT (''),
    MatchMethod                 NVARCHAR(1)         NOT NULL DEFAULT (''),
    WireAccountID               NVARCHAR(35)        NOT NULL DEFAULT (''),
    WireAccountIDType           NVARCHAR(10)        NOT NULL DEFAULT (''),
    WireBankName                NVARCHAR(100)       NOT NULL DEFAULT (''),
    WireBankID                  NVARCHAR(35)        NOT NULL DEFAULT (''),
    WireBankIDType              NVARCHAR(10)        NOT NULL DEFAULT (''),
    WireBankAddressLine1        NVARCHAR(50)        NOT NULL DEFAULT (''),
    WireBankAddressLine2        NVARCHAR(50)        NOT NULL DEFAULT (''),
    WireBankAddressLine3        NVARCHAR(50)        NOT NULL DEFAULT (''),
    WireBankAddressLine4        NVARCHAR(50)        NOT NULL DEFAULT (''),
    WireBankCity                NVARCHAR(30)        NOT NULL DEFAULT (''),
    WireBankState               NVARCHAR(10)        NOT NULL DEFAULT (''),
    WireBankZip                 NVARCHAR(10)        NOT NULL DEFAULT (''),
    WireBankCountry             NVARCHAR(2)         NOT NULL DEFAULT (''),
    SEPABIC                     NVARCHAR(35)        NOT NULL DEFAULT (''),
    SEPAIBAN                    NVARCHAR(35)        NOT NULL DEFAULT ('')
);"""


CNV_VENDOR_ADDRESS_SQL = """
DROP TABLE IF EXISTS cnvVendorAddress;

CREATE TABLE cnvVendorAddress (
    ClientID VARCHAR(32) NULL DEFAULT NULL,
    Vendor NVARCHAR(20) NULL DEFAULT NULL,
    srcVendorKey INT NOT NULL DEFAULT ((0)),
    Address NVARCHAR(20) NULL DEFAULT NULL,
    Address1 NVARCHAR(50) NULL DEFAULT NULL,
    Address2 NVARCHAR(50) NULL DEFAULT NULL,
    Address3 NVARCHAR(50) NULL DEFAULT NULL,
    Address4 NVARCHAR(50) NULL DEFAULT NULL,
    City NVARCHAR(30) NULL DEFAULT NULL,
    State NVARCHAR(10) NULL DEFAULT NULL,
    ZIP NVARCHAR(10) NULL DEFAULT NULL,
    Country NVARCHAR(2) NULL DEFAULT NULL,
    Phone NVARCHAR(24) NULL DEFAULT NULL,
    Fax NVARCHAR(24) NULL DEFAULT NULL,
    Email NVARCHAR(50) NULL DEFAULT NULL,
    PrimaryInd VARCHAR(1) NULL DEFAULT NULL,
    Payment VARCHAR(1) NULL DEFAULT NULL,
    Accounting VARCHAR(1) NULL DEFAULT NULL,
    TaxCountry NVARCHAR(2) NULL DEFAULT NULL,
    TaxRegNumber NVARCHAR(20) NULL DEFAULT NULL
);"""


table_sql_map = {
    'cnv_client':         CNV_CLIENT_SQL,
    'cnv_client_address': CNV_CLIENT_ADDRESS_SQL,
    'cnv_contact':        CNV_CONTACT_SQL,
    'cnv_vendor':         CNV_VENDOR_SQL,
    'cnv_vendor_address': CNV_VENDOR_ADDRESS_SQL,
}
