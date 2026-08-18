"""
sp_007_cnv_contact.py
---------------------
SQL script: populate the cnvContact conversion staging table.

Reads contact records from Ajera and inserts them into cnvContact, mapping contact
fields (name, address, phone, email) to the VantagePoint schema.
Runs in parallel with 006 after 004 (cnvClientAddress) is complete.

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_007_SQL = """
-- =============================================
-- 022_Ajera_cnvContact.sql
-- =============================================
-- Author:		Data Team
-- Create date:
-- Description:	Ajera to VP cnvContact table
-- Modified by:	Noemi Leonardo
--          Modified date: 2025-08-20
--          Modification: Changed to use Mailing Address instead of Contact Address
--          Converted from stored procedure to direct SQL script
--          Refactored to use JOINs instead of functions
-- =============================================

SET NOCOUNT ON;

-- =============================================
-- Main Contact Data Insert
-- =============================================

-- Check if there are any qualifying records
IF NOT EXISTS (SELECT 1 FROM [Ajera_db].dbo.AxContact)
BEGIN
    PRINT 'No Contact Data found';
END
ELSE
BEGIN
    -- Insert records into CnvContact
    INSERT INTO [Ajera_db].dbo.cnvContact
    (
        ContactID,
        srcContactKey,
        ClientID,
        srcClientKey,
        FirmName,
        CLAddress,
        LastName,
        FirstName,
        MiddleName,
        Title,
        Address1,
        Address2,
        Address3,
        City,
        [State],
        ZIP,
        Country,
        Phone,
        Fax,
        Mobile,
        EMail,
        Memo,
        ContactStatus,
        Website
    )
    SELECT
        ContactID           = a.cntKey,
        srcContactKey       = a.cntKey,
        ClientID            = c.ClientID,
        srcClientKey        = ISNULL(b.vecKey, ''),
        FirmName            = b.vecDescription,
        CLAddress           = d.[Address],
        LastName            = LEFT(LTRIM(RTRIM(COALESCE(NULLIF(a.cntLastName, ''), '.'))), 30),
        FirstName           = LEFT(LTRIM(RTRIM(NULLIF(a.cntFirstName, ''))), 25),
        MiddleName          = LEFT(LTRIM(RTRIM(NULLIF(a.cntMiddleName, ''))), 30),
        Title               = LEFT(LTRIM(RTRIM(NULLIF(a.cntTitle, ''))), 50),
        Address1            = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingAddress1, ''))), 50),
        Address2            = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingAddress2, ''))), 50),
        Address3            = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingAddress3, ''))), 50),
        City                = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingCity, ''))), 30),
        [State]             = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingState, ''))), 10),
        ZIP                 = LEFT(LTRIM(RTRIM(NULLIF(a.cntMailingZip, ''))), 10),
        Country             = CASE
                                WHEN a.cntMailingCountry = 'United States' THEN 'US'
                                ELSE LEFT(LTRIM(RTRIM(COALESCE(NULLIF(a.cntMailingCountry, ''), 'US'))), 2)
                              END,
        Phone               = LEFT(LTRIM(RTRIM(NULLIF(a.cntPhone1, ''))), 24),
        Fax                 = LEFT(LTRIM(RTRIM(NULLIF(a.cntFax, ''))), 24),
        Mobile              = LEFT(LTRIM(RTRIM(NULLIF(a.cntPhone2, ''))), 24),
        EMail               = LEFT(LTRIM(RTRIM(NULLIF(a.cntEmail, ''))), 255),
        Memo                = NULLIF(CAST(a.cntNotes AS VARCHAR(MAX)), ''),
        ContactStatus       = CASE a.cntStatus WHEN 1 THEN 'A' ELSE 'I' END,
        Website             = LEFT(LTRIM(RTRIM(NULLIF(a.cntWebsite, ''))), 255)
    FROM [Ajera_db].dbo.AxContact a
    LEFT OUTER JOIN [Ajera_db].dbo.AxVEC b ON a.cntDescription = b.vecDescription AND b.vecIsClient = 1
    LEFT OUTER JOIN [Ajera_db].dbo.cnvClient c ON c.[Name] = b.vecDescription
    LEFT OUTER JOIN [Ajera_db].dbo.cnvClientAddress d ON d.ClientID = c.ClientID AND d.PrimaryInd = 'Y'
    ORDER BY 6, 7;

    PRINT 'Records processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
"""
