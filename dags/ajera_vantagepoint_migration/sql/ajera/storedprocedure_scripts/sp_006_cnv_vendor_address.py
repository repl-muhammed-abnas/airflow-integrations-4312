"""
sp_006_cnv_vendor_address.py
----------------------------
SQL script: populate the cnvVendorAddress conversion staging table.

Reads vendor address records from Ajera and inserts them into cnvVendorAddress,
mapping address fields to the VantagePoint schema. Depends on 005 (cnvVendor populated).

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_006_SQL = """
-- =============================================
-- 021_Ajera_cnvVendorAddress.sql
-- =============================================
-- Author:		Data Team
-- Create date:	7/2/2025
-- Description:	Ajera to VP cnvVendorAddress table
-- Modified by:
--              Charisse Manalo 08/19/2025 - update [Address] logic
--              Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT ON;

-- Check if we have vendor address data
IF NOT EXISTS (SELECT 1 FROM [Ajera_db].dbo.AxVEC a WHERE a.vecIsVendor = 1)
BEGIN
    PRINT 'No Vendor Address Data found';
END
ELSE
BEGIN
    -- Insert records into CnvVendorAddress
    INSERT INTO [Ajera_db].dbo.cnvVendorAddress
    (
        ClientID,
        Vendor,
        srcVendorKey,
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
        Payment
    )
    -- Vendor AxVEC Primary Address
    SELECT
        ClientID						=	b.ClientID,
        Vendor							=	b.Vendor,
        srcVendorKey					=	a.vecKey,
        [Address]						= 	'Main',
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
    JOIN [Ajera_db].dbo.cnvVendor b ON b.srcVendorKey = a.vecKey
    WHERE a.vecIsVendor = 1
        AND a.vecAddress1 + a.vecAddress2 + a.vecAddress3 + a.vecCity + a.vecState + a.vecZIP + a.vecCountry <> ''

    UNION

    -- Vendor AxVEC Secondary Address
    SELECT
        ClientID						=	b.ClientID,
        Vendor							=	b.Vendor,
        srcVendorKey					=	a.vecKey,
        [Address]						=  'Mailing Address',
        Address1						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress1, ''))), 50),
        Address2						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress2, ''))), 50),
        Address3						=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingAddress3, ''))), 50),
        City							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingCity, ''))), 30),
        [State]							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingState, ''))), 10),
        ZIP								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingZIP, ''))), 10),
        Country							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecMailingCountry, ''))), 2),
        Phone							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecPhone1, ''))), 24),
        Fax								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecFax, ''))), 24),
        Email							=	LEFT(LTRIM(RTRIM(NULLIF(a.vecEmail, ''))), 50),
        PrimaryInd						=	NULL,
        Billing							=	NULL
    FROM [Ajera_db].dbo.AxVEC a
    JOIN [Ajera_db].dbo.cnvVendor b ON b.srcVendorKey = a.vecKey
    WHERE a.vecIsVendor = 1
        AND a.vecMailingAddressSame = 0
        AND a.vecMailingAddress1 + a.vecMailingAddress2 + a.vecMailingAddress3 + a.vecMailingCity + a.vecMailingState + a.vecMailingZIP + a.vecMailingCountry <> '';

    -- Update Primary and Payment indicators
    UPDATE [Ajera_db].dbo.CnvVendorAddress
    SET PrimaryInd = CASE [Address] WHEN 'Main' THEN 'Y' ELSE 'N' END,
        Payment = CASE [Address] WHEN 'Main' THEN 'Y' ELSE 'N' END;

    PRINT 'Records processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
"""
