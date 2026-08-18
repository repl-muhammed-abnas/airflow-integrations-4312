"""
sp_003_cnv_client.py
--------------------
SQL script: populate the cnvClient conversion staging table.

Reads client master records from the Ajera source tables and inserts them into
cnvClient in the Ajera database, mapping Ajera client fields to VantagePoint schema.
Depends on 001 (CFGClientType lookup) and 002 (Country codes standardised).

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_003_SQL = """
-- =============================================
-- 018_Ajera_cnvClient.sql
-- =============================================
-- Author:		Data Team
-- Create date:	06/24/2025
-- Description:	Ajera to VP cnvClient table
-- Modified by:	Noemi Leonardo
-- NOTE:		Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT ON;

-- Check if AxVEC has client data
IF NOT EXISTS (SELECT 1 FROM [Ajera_db].dbo.AxVEC WHERE vecIsClient = 1)
BEGIN
    PRINT 'No Client Data found';
END
ELSE
BEGIN
    -- Insert records into CnvClient
    INSERT INTO [Ajera_db].dbo.cnvClient
    (
        ClientID,
        Client,
        srcClientKey,
        [Name],
        ClientType,
        ClientTypeDescription,
        [Status],
        WebSite,
        Memo,
        ClientInd
    )
    SELECT
        ClientID							=	'ZDELTEK'+RIGHT(REPLICATE('0', 6) + CAST(ROW_NUMBER() OVER(ORDER BY a.vecDateEstablished, a.vecDescription) AS VARCHAR), 6),
        Client								=	RIGHT(REPLICATE('0', 6) + CAST(ROW_NUMBER() OVER(ORDER BY a.vecDateEstablished, a.vecDescription) AS VARCHAR), 6),
        vecKey								=	a.vecKey,
        [Name]								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecDescription, ''))), 100),
        ClientType							=	c.Code,
        ClientTypeDescription				=	LEFT(LTRIM(RTRIM(NULLIF(b.ctDescription, ''))), 50),
        [Status]							=	CASE a.vecStatus WHEN 1 THEN 'A' ELSE 'D' END,
        WebSite								=	LEFT(LTRIM(RTRIM(NULLIF(a.vecWebsite, ''))), 255),
        Memo								=	NULLIF(CAST(a.vecClientNotes AS VARCHAR(MAX)), ''),
        ClientInd							=	'Y'
    FROM [Ajera_db].dbo.AxVEC a
    LEFT JOIN  [Ajera_db].dbo.AxClientType b ON a.vecClientType = b.ctKey
    LEFT JOIN  [Vantagepoint_db].dbo.CFGClientType c ON c.[Description] = LEFT(b.ctDescription,50)
    WHERE a.vecIsClient = 1
    ORDER BY 4;

    PRINT 'Records processed: ' + CAST(@@ROWCOUNT AS VARCHAR);
END
"""
