"""
sp_001_list_insert.py
---------------------
SQL script: insert VP lookup-table rows from Ajera source tables.

Populates the following VantagePoint tables with values sourced from Ajera:
  - BTLaborCatsDescriptions / BTLaborCatsData  (from AXEMPLOYEETYPE)
  - CFGClientTypeDescriptions / CFGClientTypeData (from AXCLIENTTYPE)
  - CFGProjectTypeDescriptions / CFGProjectTypeData (from AXPROJECTTYPE)

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_001_SQL = """
-- =============================================
-- 000_Ajera_Vantagepoint_List_Inserts.sql
-- =============================================
-- Author:		Data Team
-- Create date:	10/7/2025
-- Description:	Master Vantagepoint Script Inserts - Consolidated
-- Modified by:
-- NOTE:		Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT OFF;

PRINT ''
PRINT '========== VANTAGEPOINT SCRIPT INSERTS =========='
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT ''

-- =============================================
-- Vantagepoint List
-- =============================================
PRINT '--- VANTAGEPOINT LIST INSERTS ---'
PRINT ''

--1. BTLaborCats
-- Get the current count of records in BTLaborCatsData to use as offset
DECLARE @BTLaborCatsDataCount INT;
DECLARE @AXEmployeeTypeRecordsAdded INT;

SELECT @BTLaborCatsDataCount = COUNT(*) FROM [Vantagepoint_db].dbo.BTLaborCatsData;

-- Insert into BTLaborCatsDescriptions with row_number starting after existing count
INSERT INTO [Vantagepoint_db].dbo.BTLaborCatsDescriptions (Category, UICultureName, [Description])
SELECT DISTINCT
    CAST(ROW_NUMBER() OVER(ORDER BY etDescription) + @BTLaborCatsDataCount AS VARCHAR),
    'en-US',
    LEFT(etDescription, 50)
FROM [Ajera_db].dbo.AXEMPLOYEETYPE
WHERE LEFT(etDescription, 50) NOT IN (SELECT [Description] FROM [Vantagepoint_db].dbo.BTLaborCats)
ORDER BY 1;

-- Capture how many records were added from AXEMPLOYEETYPE
SET @AXEmployeeTypeRecordsAdded = @@ROWCOUNT;
PRINT 'Records added from AXEMPLOYEETYPE to BTLaborCatsDescriptions: ' + CAST(@AXEmployeeTypeRecordsAdded AS VARCHAR);

-- Insert into BTLaborCatsData
INSERT INTO [Vantagepoint_db].dbo.BTLaborCatsData (Category, CategoryCode)
SELECT Category, Category
FROM [Vantagepoint_db].dbo.BTLaborCatsDescriptions
WHERE Category NOT IN (SELECT Category FROM [Vantagepoint_db].dbo.BTLaborCatsData);

-- Print how many records were added to BTLaborCatsData as well
PRINT 'Records added to BTLaborCatsData: ' + CAST(@@ROWCOUNT AS VARCHAR);


--3. ClientTypes (Market)
-- Get the current count of records in CFGClientTypeData to use as offset
DECLARE @CFGClientTypeDataCount INT;
DECLARE @ClientTypeMaxSeqNumber INT;
DECLARE @AXClientTypeRecordsAdded INT;

SELECT @CFGClientTypeDataCount = COUNT(*) FROM [Vantagepoint_db].dbo.CFGClientTypeData;
SELECT @ClientTypeMaxSeqNumber = ISNULL(MAX(Seq), 0) FROM [Vantagepoint_db].dbo.CFGClientTypeDescriptions;

-- Insert into CFGClientTypeDescriptions with row_number starting after existing count
INSERT INTO [Vantagepoint_db].dbo.CFGClientTypeDescriptions (Code, UICultureName, [Description], Seq)
SELECT DISTINCT
    RIGHT(REPLICATE('0', 2) + CAST(ROW_NUMBER() OVER(ORDER BY ctDescription) + @CFGClientTypeDataCount AS VARCHAR), 2),
    'en-US',
    LEFT(ctDescription, 50),
    @ClientTypeMaxSeqNumber + ROW_NUMBER() OVER(ORDER BY ctDescription)
FROM [Ajera_db].dbo.AXCLIENTTYPE
WHERE LEFT(ctDescription, 50) NOT IN (SELECT [Description] FROM [Vantagepoint_db].dbo.CFGClientType)
ORDER BY 1;

-- Capture how many records were added from AXCLIENTTYPE
SET @AXClientTypeRecordsAdded = @@ROWCOUNT;
PRINT 'Records added from AXCLIENTTYPE to CFGClientTypeDescriptions: ' + CAST(@AXClientTypeRecordsAdded AS VARCHAR);

-- Insert into CFGClientTypeData
INSERT INTO [Vantagepoint_db].dbo.CFGClientTypeData (Code)
SELECT Code
FROM [Vantagepoint_db].dbo.CFGClientTypeDescriptions
WHERE Code NOT IN (SELECT Code FROM [Vantagepoint_db].dbo.CFGClientTypeData);

-- Print how many records were added to CFGClientTypeData as well
PRINT 'Records added to CFGClientTypeData: ' + CAST(@@ROWCOUNT AS VARCHAR);


--4. ProjectTypes
-- Get the current count of records in CFGProjectTypeData to use as offset
DECLARE @CFGProjectTypeDataCount INT;
DECLARE @ProjectTypeMaxSeqNumber INT;
DECLARE @AXProjectTypeRecordsAdded INT;

SELECT @CFGProjectTypeDataCount = COUNT(*) FROM [Vantagepoint_db].dbo.CFGProjectTypeData;
SELECT @ProjectTypeMaxSeqNumber = ISNULL(MAX(Seq), 0) FROM [Vantagepoint_db].dbo.CFGProjectTypeDescriptions;

-- Insert into CFGProjectTypeDescriptions with row_number starting after existing count
INSERT INTO [Vantagepoint_db].dbo.CFGProjectTypeDescriptions (Code, UICultureName, [Description], Seq)
SELECT DISTINCT
    RIGHT(REPLICATE('0', 5) + CAST(ROW_NUMBER() OVER(ORDER BY ptDescription) + @CFGProjectTypeDataCount AS VARCHAR), 5),
    'en-US',
    LEFT(ptDescription, 50),
    @ProjectTypeMaxSeqNumber + ROW_NUMBER() OVER(ORDER BY ptDescription)
FROM [Ajera_db].dbo.AXPROJECTTYPE
WHERE LEFT(ptDescription, 50) NOT IN (SELECT [Description] FROM [Vantagepoint_db].dbo.CFGProjectType)
ORDER BY 1;

-- Capture how many records were added from AXPROJECTTYPE
SET @AXProjectTypeRecordsAdded = @@ROWCOUNT;
PRINT 'Records added from AXPROJECTTYPE to CFGProjectTypeDescriptions: ' + CAST(@AXProjectTypeRecordsAdded AS VARCHAR);

-- Insert into CFGProjectTypeData
INSERT INTO [Vantagepoint_db].dbo.CFGProjectTypeData (Code)
SELECT Code
FROM [Vantagepoint_db].dbo.CFGProjectTypeDescriptions
WHERE Code NOT IN (SELECT Code FROM [Vantagepoint_db].dbo.CFGProjectTypeData);

-- Print how many records were added to CFGProjectTypeData
PRINT 'Records added to CFGProjectTypeData: ' + CAST(@@ROWCOUNT AS VARCHAR);


PRINT ''
PRINT 'End Time: ' + CONVERT(VARCHAR, GETDATE(), 120)
PRINT '=========================================='
PRINT ''
"""
