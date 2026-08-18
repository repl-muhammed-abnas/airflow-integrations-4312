"""
sp_004_cnv_client_address.py
----------------------------
SQL script: populate the cnvClientAddress conversion staging table.

Reads client address records from Ajera and inserts them into cnvClientAddress,
mapping address fields to the VantagePoint schema. Depends on 003 (cnvClient populated).

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_004_SQL = """
-- =============================================
-- 019_Ajera_cnvClientAddress.sql
-- =============================================
-- Author:		Data Team
-- Create date:
-- Description:	Ajera to VP cnvClientAddress table
-- Modified by:	Noemi Leonardo
--              Charisse Manalo 08/19/2025 - update [Address] logic
--              Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT ON;

-- Check if we have client address data
IF NOT EXISTS (SELECT 1 FROM [Ajera_db].dbo.AxVEC a WHERE a.vecIsClient = 1)
BEGIN
    PRINT 'No Client Address Data found';
END
ELSE
BEGIN
    -- Insert records into CnvClientAddress
    INSERT INTO [Ajera_db].dbo.cnvClientAddress
    (
        ClientID,
        Client,
        srcClientKey,
        [Address],
        Address1,
        Address2,
        Address3,
        City,
        [State],
        ZIP,
        Country,
        Phone,
        Fax,
        Email,
        PrimaryInd,
        Billing
    )
    -- Client AxVEC Primary Address
    SELECT
        ClientID						=	b.ClientID,
        Client							=	b.Client,
        srcClientKey					=	a.vecKey,
        [Address]						=  'Main',
        Address1						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecAddress1, ''))), 50),
        Address2						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecAddress2, ''))), 50),
        Address3						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecAddress3, ''))), 50),
        City							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecCity, ''))), 30),
        [State]							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecState, ''))), 10),
        ZIP								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecZIP, ''))), 10),
        Country							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecCountry, ''))), 2),
        Phone							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecPhone1, ''))), 24),
        Fax								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecFax, ''))), 24),
        Email							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecEmail, ''))), 50),
        PrimaryInd						=	NULL,
        Billing							=	NULL
    FROM [Ajera_db].dbo.AxVEC a
    JOIN [Ajera_db].dbo.cnvClient b ON b.srcClientKey = a.vecKey
    WHERE a.vecIsClient = 1
        AND a.vecAddress1 + a.vecAddress2 + a.vecAddress3 + a.vecCity + a.vecState + a.vecZIP + a.vecCountry <> ''

    UNION

    -- Client AxVEC Secondary Address
    SELECT
        ClientID						=	b.ClientID,
        Client							=	b.Client,
        srcClientKey					=	a.vecKey,
        [Address]						= 	'Mailing Address',
        Address1						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress1, ''))), 50),
        Address2						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress2, ''))), 50),
        Address3						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress3, ''))), 50),
        City							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingCity, ''))), 30),
        [State]							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingState, ''))), 10),
        ZIP								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingZip, ''))), 10),
        Country							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingCountry, ''))), 2),
        Phone							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecPhone1, ''))), 24),
        Fax								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecFax, ''))), 24),
        Email							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecEmail, ''))), 50),
        PrimaryInd						=	NULL,
        Billing							=	NULL
    FROM [Ajera_db].dbo.AxVEC a
    JOIN [Ajera_db].dbo.cnvClient b ON b.srcClientKey = a.vecKey
    WHERE a.vecIsClient = 1
        AND a.vecMailingAddressSame = 0
        AND a.vecMailingAddress1 + a.vecMailingAddress2 + a.vecMailingAddress3 + a.vecMailingCity + a.vecMailingState + a.vecMailingZIP + a.vecMailingCountry <> '';

    -- Update Primary and Billing indicators
    UPDATE [Ajera_db].dbo.CnvClientAddress
    SET PrimaryInd = CASE [Address] WHEN 'Main' THEN 'Y' ELSE 'N' END,
        Billing = CASE [Address] WHEN 'Main' THEN 'Y' ELSE 'N' END;

    PRINT 'Records processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
"""
