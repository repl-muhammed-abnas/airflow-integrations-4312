"""
sp_005_cnv_vendor.py
--------------------
SQL script: populate the cnvVendor conversion staging table.

Reads vendor master records from Ajera and inserts them into cnvVendor,
mapping vendor fields to the VantagePoint schema. Depends on 004 (cnvClientAddress).

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_005_SQL = """
-- =============================================
-- 020_Ajera_cnvVendor.sql
-- =============================================
-- Author:		Data Team
-- Create date:	06/24/2025
-- Description:	Ajera to VP cnvVendor table
-- Modified by:	Noemi Leonardo
--              Charisse Manalo 08/19/2025 - changed Category to VendorType
--              Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT ON;

-- Check if AxVEC has vendor data
IF NOT EXISTS (SELECT 1 FROM [Ajera_db].dbo.AxVEC WHERE vecIsVendor = 1)
BEGIN
    PRINT 'No Vendor Data found';
END
ELSE
BEGIN
    -- Insert records into CnvVendor
    INSERT INTO [Ajera_db].dbo.cnvVendor
    (
        ClientID,
        Vendor,
        Client,
        srcVendorKey,
        [Name],
        [Status],
        WebSite,
        Memo,
        VendorInd,
        VendorType,
        AvailableForCRM,
        ReadyForApproval,
        ReadyForProcessing,
        FedID,
        PayTerms,
        RegAccount,
        OHAccount,
        ThisYear1099,
        LastYear1099,
        Req1099,
        AccountNumber
    )
    SELECT
        ClientID							=	'ZDELTEKV'+RIGHT(REPLICATE('0', 5) + CAST(ROW_NUMBER() OVER(ORDER BY vecDateEstablished, vecDescription) AS VARCHAR), 5),
        Vendor								=	'V'+RIGHT(REPLICATE('0', 5) + CAST(ROW_NUMBER() OVER(ORDER BY vecDateEstablished, vecDescription) AS VARCHAR), 5),
        Client								=	'V'+RIGHT(REPLICATE('0', 5) + CAST(ROW_NUMBER() OVER(ORDER BY vecDateEstablished, vecDescription) AS VARCHAR), 5),
        srcVendorKey						=	a.vecKey,
        [Name]								=	LEFT(LTRIM(RTRIM(ISNULL(a.vecDescription, ''))), 100),
        [Status]							=	CASE a.vecStatus WHEN 1 THEN 'A' ELSE 'D' END,
        WebSite								=	LEFT(LTRIM(RTRIM(ISNULL(a.vecWebsite, ''))), 255),
        Memo								=	ISNULL(CAST(a.vecVendorNotes AS VARCHAR(MAX)), ''),
        VendorInd							=	'Y',
        VendorType							=	CASE WHEN d.vtIsConsultant = 1 THEN 'C' ELSE 'T' END,
        AvailableForCRM						=	'Y',
        ReadyForApproval					=	'Y',
        ReadyForProcessing					=	'Y',
        FedID								=	LEFT(LTRIM(RTRIM(ISNULL(a.vec1099RecipientID, ''))), 11),
        PayTerms							=	CASE WHEN a.vecDaysType = 1 THEN CAST(a.vecDaysToPay AS VARCHAR(MAX)) ELSE 'NEXT' END,
        RegAccount							=	ISNULL(e.Account, ''),
        OHAccount							=	ISNULL(f.Account, ''),
        ThisYear1099						=	COALESCE(c.Amount, 0),
        LastYear1099						=	COALESCE(b.Amount, 0),
        Req1099								=	CASE a.vecReceives1099 WHEN 1 THEN 'Y' ELSE 'N' END,
        AccountNumber						=	LEFT(LTRIM(RTRIM(ISNULL(a.vecAccountID, ''))), 25)
    FROM [Ajera_db].dbo.AxVEC a
    LEFT OUTER JOIN
    (
        SELECT be.beVendor, -SUM(gld.gldAmount) Amount
        FROM [Ajera_db].dbo.AxGLDetail gld
        INNER JOIN [Ajera_db].dbo.AxBankEntry be ON gld.gldBankEntry = be.beKey
        INNER JOIN [Ajera_db].dbo.AxVEC ve ON be.beVendor = ve.vecKey
        WHERE gld.gldIsCurrent = 1
            AND gld.gldControlAccountType IN (1, 8)
            AND NOT (gld.gldIsBeginningBalance = 1 AND be.beType = 1)
            AND ve.vecIsVendor = 1
            AND ve.vecReceives1099 = 1
            AND YEAR(be.beDate) = YEAR(GETDATE()) -1
        GROUP BY be.beVendor
    ) b ON a.vecKey = b.beVendor
    LEFT OUTER JOIN
    (
        SELECT be.beVendor, -SUM(gld.gldAmount) Amount
        FROM [Ajera_db].dbo.AxGLDetail gld
        INNER JOIN [Ajera_db].dbo.AxBankEntry be ON gld.gldBankEntry = be.beKey
        INNER JOIN [Ajera_db].dbo.AxVEC ve ON be.beVendor = ve.vecKey
        WHERE gld.gldIsCurrent = 1
            AND gld.gldControlAccountType IN (1, 8)
            AND NOT (gld.gldIsBeginningBalance = 1 AND be.beType = 1)
            AND ve.vecIsVendor = 1
            AND ve.vecReceives1099 = 1
            AND YEAR(be.beDate) = YEAR(GETDATE())
        GROUP BY be.beVendor
    ) c ON a.vecKey = c.beVendor
    LEFT OUTER JOIN [Ajera_db].dbo.AxVendorType d ON a.vecVendorType = d.vtKey
    LEFT OUTER JOIN
    (
        SELECT srcGLAccountKey,srcGLAccountType,Account
        FROM [Ajera_db].dbo.xwkAccount
        WHERE srcGLAccountType BETWEEN 7 AND 8
    ) e ON a.vecVendorAccount = e.srcGLAccountKey
    LEFT OUTER JOIN
    (
        SELECT srcGLAccountKey,srcGLAccountType,Account
        FROM [Ajera_db].dbo.xwkAccount
        WHERE srcGLAccountType = 9
    ) f ON a.vecVendorAccount = f.srcGLAccountKey
    WHERE a.vecIsVendor = 1
    ORDER BY 4;

    PRINT 'Records processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
"""
