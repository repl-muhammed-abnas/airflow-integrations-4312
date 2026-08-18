"""
sp_002_state_country.py
-----------------------
SQL script: standardise State and Country values in Ajera source tables.

Updates state/country codes in Ajera data to match the values expected by VantagePoint
before the cnv* conversion tables are populated.

Placeholder databases replaced at runtime by run_sp_sql_file() in custom_methods.py:
  [Ajera_db]        → actual Ajera DB name
  [Vantagepoint_db] → actual VantagePoint DB name
"""

SP_002_SQL = """
-- =============================================
-- 003_Ajera_StateCountry_Updates.sql
-- =============================================
-- Author:		Data Team
-- Create date:	10/7/2025
-- Description:	Ajera to VP State and Country Code Standardization
-- Modified by:
-- NOTE:		Standardizes state and country codes across AxVEC and AxContact tables
--              Converted from stored procedure to direct SQL script
-- =============================================

SET NOCOUNT ON;

PRINT '';
PRINT '========== STATE AND COUNTRY CODE STANDARDIZATION STARTED ==========';
PRINT 'Start Time: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';

-- Variables to track overall results
DECLARE @TotalRecordsProcessed INT = 0;

-- Section 1 variables
DECLARE @RecordsToUpdateMissingVecCountry INT = 0;
DECLARE @UpdatedCountMissingVecCountry INT = 0;
DECLARE @RecordsToUpdateVecCountry INT = 0;
DECLARE @UpdatedCountVecCountryCode INT = 0;
DECLARE @RecordsToUpdateVecState INT = 0;
DECLARE @UpdatedCountVecState INT = 0;

-- Section 2 variables
DECLARE @RecordsToUpdateMissingVecMailingCountry INT = 0;
DECLARE @UpdatedCountMissingVecMailingCountry INT = 0;
DECLARE @RecordsToUpdateVecMailingCountry INT = 0;
DECLARE @UpdatedCountVecMailingCountryCode INT = 0;
DECLARE @RecordsToUpdateVecMailingState INT = 0;
DECLARE @UpdatedCountVecMailingState INT = 0;

-- Section 3 variables
DECLARE @RecordsToUpdateMissingCntCountry INT = 0;
DECLARE @UpdatedCountMissingCntCountry INT = 0;
DECLARE @RecordsToUpdateCntCountry INT = 0;
DECLARE @UpdatedCountCntCountryCode INT = 0;
DECLARE @RecordsToUpdateCntState INT = 0;
DECLARE @UpdatedCountCntState INT = 0;

-- Section 4 variables
DECLARE @RecordsToUpdateMissingCntMailingCountry INT = 0;
DECLARE @UpdatedCountMissingCntMailingCountry INT = 0;
DECLARE @RecordsToUpdateCntMailingCountry INT = 0;
DECLARE @UpdatedCountCntMailingCountryCode INT = 0;
DECLARE @RecordsToUpdateCntMailingState INT = 0;
DECLARE @UpdatedCountCntMailingState INT = 0;

-- ==============================================================================================================================================================================================================================================================================
-- SECTION 1: AxVEC Table - Primary Address (vecCountry, vecState)
-- ==============================================================================================================================================================================================================================================================================

PRINT '';
PRINT '========== SECTION 1: AxVEC PRIMARY ADDRESS UPDATES ==========';
PRINT '';

-- 1a. Missing vecCountry but matching state in AxVEC table
PRINT '1a. Starting update of missing vecCountry fields based on vecState...';
PRINT '';

SELECT @RecordsToUpdateMissingVecCountry = COUNT(*)
FROM [Ajera_db].dbo.AxVEC v
WHERE ISNULL(v.vecState, '') <> '';

PRINT 'Found: ' + CAST(@RecordsToUpdateMissingVecCountry AS VARCHAR(10)) + ' records with missing country but matching state';

IF @RecordsToUpdateMissingVecCountry > 0
BEGIN
    UPDATE [Ajera_db].dbo.AxVEC
    SET vecCountry = CASE
        WHEN vecState IN ('Aberdeen', 'ABER') THEN 'GB'
        WHEN vecState IN ('Aberdeenshire', 'ABERS') THEN 'GB'
        WHEN vecState IN ('Alabama', 'AL') THEN 'US'
        WHEN vecState IN ('Alaska', 'AK') THEN 'US'
        WHEN vecState IN ('Alberta', 'AB') THEN 'CA'
        WHEN vecState IN ('American Samoa', 'AS') THEN 'US'
        WHEN vecState IN ('Angus', 'ANGU') THEN 'GB'
        WHEN vecState IN ('Argyll', 'ARGY') THEN 'GB'
        WHEN vecState IN ('Arizona', 'AZ') THEN 'US'
        WHEN vecState IN ('Arkansas', 'AR') THEN 'US'
        WHEN vecState IN ('Australian Capital Territory', 'ACT') THEN 'AU'
        WHEN vecState IN ('Avon', 'AVON') THEN 'GB'
        WHEN vecState IN ('Ayrshire AND Arran', 'AYAR') THEN 'GB'
        WHEN vecState IN ('Bedfordshire', 'BEDS') THEN 'GB'
        WHEN vecState IN ('Berkshire', 'BERKS') THEN 'GB'
        WHEN vecState IN ('Blaenau Gwent', 'BLAE') THEN 'GB'
        WHEN vecState IN ('BridgEND', 'BRID') THEN 'GB'
        WHEN vecState IN ('British Columbia', 'BC') THEN 'CA'
        WHEN vecState IN ('Buckinghamshire', 'BUCKS') THEN 'GB'
        WHEN vecState IN ('Caerphilly', 'CAER') THEN 'GB'
        WHEN vecState IN ('California', 'CA') THEN 'US'
        WHEN vecState IN ('Cambridgeshire', 'CAMBS') THEN 'GB'
        WHEN vecState IN ('Cardiff', 'CARD') THEN 'GB'
        WHEN vecState IN ('CarmarTHENshire', 'CARM') THEN 'GB'
        WHEN vecState IN ('Ceredigion', 'CERE') THEN 'GB'
        WHEN vecState IN ('Channel IslANDs', 'CHAN') THEN 'GB'
        WHEN vecState IN ('Cheshire', 'CHES') THEN 'GB'
        WHEN vecState IN ('Clackmannanshire', 'CLAC') THEN 'GB'
        WHEN vecState IN ('Colorado', 'CO') THEN 'US'
        WHEN vecState IN ('Connecticut', 'CT') THEN 'US'
        WHEN vecState IN ('Conwy', 'CONW') THEN 'GB'
        WHEN vecState IN ('Cornwall AND Isles of Scilly', 'CORN') THEN 'GB'
        WHEN vecState IN ('County Antrim', 'COANTRIM') THEN 'GB'
        WHEN vecState IN ('County Armagh', 'COARMAGH') THEN 'GB'
        WHEN vecState IN ('County Down', 'CODOWN') THEN 'GB'
        WHEN vecState IN ('County Fermanagh', 'COFERMANAG') THEN 'GB'
        WHEN vecState IN ('County Londonderry', 'COLONDONDE') THEN 'GB'
        WHEN vecState IN ('County Tyrone', 'COTYRONE') THEN 'GB'
        WHEN vecState IN ('CumberlAND', 'CUMB') THEN 'GB'
        WHEN vecState IN ('Cumbria', 'CUMBRIA') THEN 'GB'
        WHEN vecState IN ('Delaware', 'DE') THEN 'US'
        WHEN vecState IN ('Denbighshire', 'DENB') THEN 'GB'
        WHEN vecState IN ('Derbyshire', 'DERBYS') THEN 'GB'
        WHEN vecState IN ('Devon', 'DEVO') THEN 'GB'
        WHEN vecState IN ('DorSET', 'DORS') THEN 'GB'
        WHEN vecState IN ('Dumfries AND Galloway', 'DUMF') THEN 'GB'
        WHEN vecState IN ('Dunbartonshire', 'DUNB') THEN 'GB'
        WHEN vecState IN ('Dundee', 'DUND') THEN 'GB'
        WHEN vecState IN ('Durham', 'DURH') THEN 'GB'
        WHEN vecState IN ('East Yorkshire', 'EYORKS') THEN 'GB'
        WHEN vecState IN ('Edinburgh', 'EDIN') THEN 'GB'
        WHEN vecState IN ('Essex', 'ESSEX') THEN 'GB'
        WHEN vecState IN ('Falkirk', 'FALK') THEN 'GB'
        WHEN vecState IN ('Fife', 'FIFE') THEN 'GB'
        WHEN vecState IN ('FlINTshire', 'FLINT') THEN 'GB'
        WHEN vecState IN ('Florida', 'Florida.', 'FL') THEN 'US'
        WHEN vecState IN ('Georgia', 'GA') THEN 'US'
        WHEN vecState IN ('Glamorgan', 'GLAM') THEN 'GB'
        WHEN vecState IN ('Glasgow', 'GLOS') THEN 'GB'
        WHEN vecState IN ('Gloucestershire', 'GLOUCS') THEN 'GB'
        WHEN vecState IN ('Greater London', 'GLONDON') THEN 'GB'
        WHEN vecState IN ('Greater Manchester', 'MAN') THEN 'GB'
        WHEN vecState IN ('Guam', 'GU') THEN 'US'
        WHEN vecState IN ('Gwynedd', 'GWYN') THEN 'GB'
        WHEN vecState IN ('Hampshire', 'HANTS') THEN 'GB'
        WHEN vecState IN ('Hawaii', 'HI') THEN 'US'
        WHEN vecState IN ('Herefordshire', 'HEREFORD') THEN 'GB'
        WHEN vecState IN ('Hertfordshire', 'HERTS') THEN 'GB'
        WHEN vecState IN ('HighlANDs', 'HIGH') THEN 'GB'
        WHEN vecState IN ('Huntingdonshire', 'HUNTS') THEN 'GB'
        WHEN vecState IN ('Idaho', 'ID') THEN 'US'
        WHEN vecState IN ('Illinois', 'IL') THEN 'US'
        WHEN vecState IN ('Indiana', 'IN') THEN 'US'
        WHEN vecState IN ('Inverclyde', 'INVE') THEN 'GB'
        WHEN vecState IN ('Iowa', 'IA') THEN 'US'
        WHEN vecState IN ('Isle of Anglesey', 'ISANGL') THEN 'GB'
        WHEN vecState IN ('Isle of Man', 'ISMAN') THEN 'GB'
        WHEN vecState IN ('Isle of Wight', 'ISWIGHT') THEN 'GB'
        WHEN vecState IN ('Kansas', 'KS') THEN 'US'
        WHEN vecState IN ('Kent', 'KENT') THEN 'GB'
        WHEN vecState IN ('Kentucky', 'KY') THEN 'US'
        WHEN vecState IN ('Lanarkshire', 'LANA') THEN 'GB'
        WHEN vecState IN ('Lancashire', 'LANCS') THEN 'GB'
        WHEN vecState IN ('Leicestershire', 'LEICS') THEN 'GB'
        WHEN vecState IN ('Lincolnshire', 'LINCS') THEN 'GB'
        WHEN vecState IN ('London', 'LONDON') THEN 'GB'
        WHEN vecState IN ('Lothian', 'LOTH') THEN 'GB'
        WHEN vecState IN ('Louisiana', 'LA') THEN 'US'
        WHEN vecState IN ('Maine', 'ME') THEN 'US'
        WHEN vecState IN ('Manitoba', 'MB') THEN 'CA'
        WHEN vecState IN ('MarylAND', 'MD') THEN 'US'
        WHEN vecState IN ('MassachuSETts', 'MA') THEN 'US'
        WHEN vecState IN ('Merseyside', 'MERS') THEN 'GB'
        WHEN vecState IN ('Merthyr Tydfil', 'MERT') THEN 'GB'
        WHEN vecState IN ('Michigan', 'MI') THEN 'US'
        WHEN vecState IN ('Middlesex', 'MIDDX') THEN 'GB'
        WHEN vecState IN ('Minnesota', 'MN') THEN 'US'
        WHEN vecState IN ('Mississippi', 'MS') THEN 'US'
        WHEN vecState IN ('Missouri', 'MO') THEN 'US'
        WHEN vecState IN ('Monmouthshire', 'MONS') THEN 'GB'
        WHEN vecState IN ('Montana', 'MT') THEN 'US'
        WHEN vecState IN ('Moray', 'MORA') THEN 'GB'
        WHEN vecState IN ('Neath Port Talbot', 'NEAT') THEN 'GB'
        WHEN vecState IN ('Nebraska', 'NE') THEN 'US'
        WHEN vecState IN ('Nevada', 'NV') THEN 'US'
        WHEN vecState IN ('New Brunswick', 'NB') THEN 'CA'
        WHEN vecState IN ('New Hampshire', 'NH') THEN 'US'
        WHEN vecState IN ('New Jersey', 'N J,', 'NJ') THEN 'US'
        WHEN vecState IN ('New Mexico', 'NM') THEN 'US'
        WHEN vecState IN ('New South Wales', 'NSW') THEN 'AU'
        WHEN vecState IN ('New York', 'NY') THEN 'US'
        WHEN vecState IN ('NewfoundlAND AND Labrador', 'NL') THEN 'CA'
        WHEN vecState IN ('Newport', 'NEWP') THEN 'GB'
        WHEN vecState IN ('Norfolk', 'NORF') THEN 'GB'
        WHEN vecState IN ('North Carolina', 'NC') THEN 'US'
        WHEN vecState IN ('North Dakota', 'ND') THEN 'US'
        WHEN vecState IN ('North Yorkshire', 'NYORKS') THEN 'GB'
        WHEN vecState IN ('Northamptonshire', 'NORTHANTS') THEN 'GB'
        WHEN vecState IN ('Northern Territory', 'NT') THEN 'AU'
        WHEN vecState IN ('NorthumberlAND', 'NORTHD') THEN 'GB'
        WHEN vecState IN ('Northwest Territories', 'NT') THEN 'CA'
        WHEN vecState IN ('Nottinghamshire', 'NOTTS') THEN 'GB'
        WHEN vecState IN ('Nova Scotia', 'NS') THEN 'CA'
        WHEN vecState IN ('Nunavut', 'NU') THEN 'CA'
        WHEN vecState IN ('Ohio', 'OH') THEN 'US'
        WHEN vecState IN ('Oklahoma', 'OK') THEN 'US'
        WHEN vecState IN ('Ontario', 'ON') THEN 'CA'
        WHEN vecState IN ('Oregon', 'OR') THEN 'US'
        WHEN vecState IN ('Orkney IslANDs', 'ORKN') THEN 'GB'
        WHEN vecState IN ('Outer Hebrides', 'OUTH') THEN 'GB'
        WHEN vecState IN ('Oxfordshire', 'OXON') THEN 'GB'
        WHEN vecState IN ('Pembrokeshire', 'PEMBS') THEN 'GB'
        WHEN vecState IN ('Pennsylvania', 'PA') THEN 'US'
        WHEN vecState IN ('Perth AND Kinross', 'PERTH') THEN 'GB'
        WHEN vecState IN ('Powys', 'POWYS') THEN 'GB'
        WHEN vecState IN ('Prince Edward IslAND', 'PE') THEN 'CA'
        WHEN vecState IN ('Puerto Rico', 'PR') THEN 'US'
        WHEN vecState IN ('Quebec', 'QC') THEN 'CA'
        WHEN vecState IN ('QueenslAND', 'QLD') THEN 'AU'
        WHEN vecState IN ('Redcar AND ClevelAND', 'REDC') THEN 'GB'
        WHEN vecState IN ('Renfrewshire', 'RENS') THEN 'GB'
        WHEN vecState IN ('Rhode IslAND', 'RI') THEN 'US'
        WHEN vecState IN ('Rhondda Cynon Taff', 'RHON') THEN 'GB'
        WHEN vecState IN ('RutlAND', 'RUTL') THEN 'GB'
        WHEN vecState IN ('Saskatchewan', 'SK') THEN 'CA'
        WHEN vecState IN ('Scottish Borders', 'SCOTB') THEN 'GB'
        WHEN vecState IN ('ShetlAND IslANDs', 'SHET') THEN 'GB'
        WHEN vecState IN ('Shropshire', 'SALOP') THEN 'GB'
        WHEN vecState IN ('SomerSET', 'SOMT') THEN 'GB'
        WHEN vecState IN ('South Australia', 'SA') THEN 'AU'
        WHEN vecState IN ('South Carolina', 'SC') THEN 'US'
        WHEN vecState IN ('South Dakota', 'SD') THEN 'US'
        WHEN vecState IN ('South Yorkshire', 'SYORKS') THEN 'GB'
        WHEN vecState IN ('Staffordshire', 'STAFFS') THEN 'GB'
        WHEN vecState IN ('Stirling', 'STIR') THEN 'GB'
        WHEN vecState IN ('Suffolk', 'SUFF') THEN 'GB'
        WHEN vecState IN ('Surrey', 'SURR') THEN 'GB'
        WHEN vecState IN ('Sussex', 'SX') THEN 'GB'
        WHEN vecState IN ('Swansea', 'SWAN') THEN 'GB'
        WHEN vecState IN ('Tasmania', 'TAS') THEN 'AU'
        WHEN vecState IN ('Tennessee', 'TN') THEN 'US'
        WHEN vecState IN ('Texas', 'TX') THEN 'US'
        WHEN vecState IN ('Torfaen', 'TORF') THEN 'GB'
        WHEN vecState IN ('Tyne AND Wear', 'TYNE') THEN 'GB'
        WHEN vecState IN ('Utah', 'UT') THEN 'US'
        WHEN vecState IN ('Vermont', 'VT') THEN 'US'
        WHEN vecState IN ('Victoria', 'VIC') THEN 'AU'
        WHEN vecState IN ('Virgin IslANDs', 'VI') THEN 'US'
        WHEN vecState IN ('Virginia', 'VA') THEN 'US'
        WHEN vecState IN ('Warwickshire', 'WARKS') THEN 'GB'
        WHEN vecState IN ('Washington', 'WA') THEN 'US'
        WHEN vecState IN ('Washington, DC', 'WDC', 'D C', 'D.C.', 'DC') THEN 'US'
        WHEN vecState IN ('West MidlANDs', 'WMIDLANDS') THEN 'GB'
        WHEN vecState IN ('West Virginia', 'WV') THEN 'US'
        WHEN vecState IN ('West Yorkshire', 'WYORKS') THEN 'GB'
        WHEN vecState IN ('Western Australia', 'WA') THEN 'AU'
        WHEN vecState IN ('WestmorlAND', 'WMLD') THEN 'GB'
        WHEN vecState IN ('Wiltshire', 'WILTS') THEN 'GB'
        WHEN vecState IN ('Wisconsin', 'WI') THEN 'US'
        WHEN vecState IN ('Worcestershire', 'WORCS') THEN 'GB'
        WHEN vecState IN ('Wrexham', 'WREX') THEN 'GB'
        WHEN vecState IN ('Wyoming', 'WY') THEN 'US'
        WHEN vecState IN ('Yukon', 'YT') THEN 'CA'
        ELSE ''
    END
    WHERE ISNULL(vecState, '') <> '';

    SET @UpdatedCountMissingVecCountry = @@ROWCOUNT;
    SET @TotalRecordsProcessed = @TotalRecordsProcessed + @UpdatedCountMissingVecCountry;
    PRINT 'Update completed. Records processed: ' + CAST(@UpdatedCountMissingVecCountry AS VARCHAR(10));
END
ELSE
BEGIN
    PRINT 'No records with missing country and matching state found';
END

PRINT '';

-- ==============================================================================================================================================================================================================================================================================
-- FINAL CLEANUP
-- ==============================================================================================================================================================================================================================================================================

PRINT '';
PRINT '========== FINAL CLEANUP ==========';
PRINT '';

UPDATE [Ajera_db].dbo.AxVEC SET vecCountry = '' WHERE vecState = ' ' AND vecCountry <> '';
UPDATE [Ajera_db].dbo.AxVEC SET vecMailingCountry = '' WHERE vecMailingState = ' ' AND vecMailingCountry <> '';
UPDATE [Ajera_db].dbo.AxContact SET cntCountry = '' WHERE cntState = ' ' AND cntCountry <> '';
UPDATE [Ajera_db].dbo.AxContact SET cntMailingCountry = '' WHERE cntMailingState = ' ' AND cntMailingCountry <> '';

PRINT 'Cleanup completed';
PRINT '';

-- ==============================================================================================================================================================================================================================================================================
-- FINAL SUMMARY
-- ==============================================================================================================================================================================================================================================================================

PRINT '';
PRINT '==================== FINAL SUMMARY ====================';
PRINT 'Script completed at: ' + CONVERT(VARCHAR, GETDATE(), 120);
PRINT '';
PRINT 'State and country code standardization completed successfully.';
PRINT '=======================================================';
"""
