"""
restore.py
----------
Jinja-templated T-SQL script for restoring a SQL Server database from a .bak backup file.

RESTORE_DATABASE_SQL performs:
  1. Validates the .bak file exists on disk via xp_fileexist.
  2. Reads the logical file list from the backup (RESTORE FILELISTONLY).
  3. Builds dynamic MOVE clauses using the SQL Server default data path.
  4. Terminates existing connections and sets the database to SINGLE_USER if it exists.
  5. Restores the database with REPLACE + RECOVERY.
  6. Sets the database back to MULTI_USER.
  7. Runs DBCC CHECKDB and sp_updatestats for integrity verification.

Template parameters (rendered by render_and_execute_sql() via Jinja before execution):
    {{ params.file_path }}     — full path to the .bak file on the SQL Server host
    {{ params.database_name }} — target database name

Must be executed with autocommit=True (RESTORE is a DDL statement incompatible with
pymssql implicit transactions).
"""

RESTORE_DATABASE_SQL = """
-- =============================================
-- restore_database.sql
-- =============================================
-- Description: Restores the .bak file to the source database
-- Parameters rendered by Jinja before execution:
--   {{ params.file_path }}     - Full path to the .bak file on the SQL server
--   {{ params.database_name }} - Target database name
-- Compatible with Airflow/pymssql (no GO statements)
-- =============================================
USE master;

-- Variables passed from Airflow
DECLARE @BackupFile VARCHAR(500) = '{{ params.file_path }}';
DECLARE @DatabaseName VARCHAR(100) = '{{ params.database_name }}';

-- Declare variables for logical and physical file names
DECLARE @LogicalDataFileName VARCHAR(100);
DECLARE @LogicalLogFileName VARCHAR(100);

PRINT '========================================';
PRINT 'Starting Database Restore Process';
PRINT 'Server: ' + CAST(SERVERPROPERTY('ServerName') AS VARCHAR) + '\\' + CAST(ISNULL(SERVERPROPERTY('InstanceName'), 'MSSQLSERVER') AS VARCHAR);
PRINT 'Database: ' + @DatabaseName;
PRINT 'Backup File: ' + @BackupFile;
PRINT '========================================';

BEGIN TRY
    -- Step 1: Check if backup file exists (requires xp_fileexist)
    DECLARE @FileExists INT;
    EXEC master.dbo.xp_fileexist @BackupFile, @FileExists OUTPUT;
    IF @FileExists = 0
    BEGIN
        RAISERROR('Backup file does not exist: %s', 16, 1, @BackupFile);
        RETURN;
    END
    PRINT 'Backup file found: ' + @BackupFile;

    -- Step 2: Get logical and physical file names from the backup
    DECLARE @LogicalFiles TABLE (
        LogicalName VARCHAR(128),
        PhysicalName VARCHAR(260),
        Type CHAR(1),
        FileGroupName VARCHAR(128),
        Size NUMERIC(20,0),
        MaxSize NUMERIC(20,0),
        FileID INT,
        CreateLSN NUMERIC(25,0),
        DropLSN NUMERIC(25,0),
        UniqueID UNIQUEIDENTIFIER,
        ReadOnlyLSN NUMERIC(25,0),
        ReadWriteLSN NUMERIC(25,0),
        BackupSizeInBytes BIGINT,
        SourceBlockSize INT,
        FileGroupID INT,
        LogGroupGUID UNIQUEIDENTIFIER,
        DifferentialBaseLSN NUMERIC(25,0),
        DifferentialBaseGUID UNIQUEIDENTIFIER,
        IsReadOnly BIT,
        IsPresent BIT,
        TDEThumbprint VARBINARY(32),
        SnapshotURL NVARCHAR(360)
    );

    INSERT INTO @LogicalFiles
    EXEC('RESTORE FILELISTONLY FROM DISK = ''' + @BackupFile + '''');

    -- Step 2b: Resolve SQL Server default data directory
    DECLARE @DataPath NVARCHAR(500) = CAST(SERVERPROPERTY('InstanceDefaultDataPath') AS NVARCHAR(500));
    PRINT 'SQL Server data path: ' + @DataPath;

    -- Print all logical files found in the backup
    DECLARE @FileCount INT = (SELECT COUNT(*) FROM @LogicalFiles WHERE IsPresent = 1);
    PRINT 'Logical file count in backup: ' + CAST(@FileCount AS VARCHAR);

    -- Step 2c: Build MOVE clause for every logical file in the backup
    -- Uses FileID to generate unique physical filenames, handles any number of files
    DECLARE @MoveCommands NVARCHAR(MAX) = '';
    SELECT @MoveCommands = @MoveCommands +
        ',MOVE ''' + LogicalName + ''' TO ''' + @DataPath + @DatabaseName + '_' + CAST(FileID AS VARCHAR) +
        CASE Type WHEN 'L' THEN '.ldf' ELSE '.mdf' END + ''''
    FROM @LogicalFiles
    WHERE IsPresent = 1
    ORDER BY FileID;

    -- Remove the leading comma
    SET @MoveCommands = STUFF(@MoveCommands, 1, 1, '');
    PRINT 'MOVE clauses: ' + @MoveCommands;

    -- Step 3: Kill existing connections to the database if it exists
    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
    BEGIN
        PRINT 'Database exists. Terminating existing connections...';
        DECLARE @KillCommand VARCHAR(MAX) = '';
        SELECT @KillCommand = @KillCommand + 'KILL ' + CAST(session_id AS VARCHAR) + '; '
        FROM sys.dm_exec_sessions
        WHERE database_id = DB_ID(@DatabaseName)
            AND session_id <> @@SPID;
        IF LEN(@KillCommand) > 0
        BEGIN
            EXEC(@KillCommand);
            PRINT 'Terminated active connections.';
        END
        -- Set database to single user mode
        DECLARE @SetSingleUser VARCHAR(500) =
            'ALTER DATABASE [' + @DatabaseName + '] SET SINGLE_USER WITH ROLLBACK IMMEDIATE;';
        EXEC(@SetSingleUser);
        PRINT 'Database set to SINGLE_USER mode.';
    END

    -- Step 4: Restore the database
    PRINT 'Starting database restore...';
    DECLARE @RestoreCommand NVARCHAR(MAX) =
        'RESTORE DATABASE [' + @DatabaseName + ']
         FROM DISK = ''' + @BackupFile + '''
         WITH
            ' + @MoveCommands + ',
            REPLACE,
            RECOVERY,
            STATS = 10';
    PRINT @RestoreCommand;
    EXEC(@RestoreCommand);
    PRINT 'Database restored successfully!';

    -- Step 5: Set database to multi-user mode
    DECLARE @SetMultiUser VARCHAR(500) =
        'ALTER DATABASE [' + @DatabaseName + '] SET MULTI_USER;';
    EXEC(@SetMultiUser);
    PRINT 'Database set to MULTI_USER mode.';

    -- Step 6: Verify database integrity
    PRINT 'Verifying database integrity...';
    DBCC CHECKDB(@DatabaseName) WITH NO_INFOMSGS;
    PRINT 'Database integrity check completed successfully!';

    -- Step 7: Update statistics
    PRINT 'Updating statistics...';
    DECLARE @UpdateStatsCommand VARCHAR(500) =
        'USE [' + @DatabaseName + ']; EXEC sp_updatestats;';
    EXEC(@UpdateStatsCommand);
    PRINT 'Statistics updated successfully!';

    PRINT '========================================';
    PRINT 'Database Restore Completed Successfully!';
    PRINT '========================================';

END TRY
BEGIN CATCH
    DECLARE @ErrorMessage VARCHAR(MAX) = ERROR_MESSAGE();
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY();
    DECLARE @ErrorState INT = ERROR_STATE();

    PRINT '========================================';
    PRINT 'ERROR: Database Restore Failed';
    PRINT 'Error Message: ' + @ErrorMessage;
    PRINT '========================================';

    -- Try to set database back to multi-user if it exists
    IF EXISTS (SELECT 1 FROM sys.databases WHERE name = @DatabaseName)
    BEGIN
        BEGIN TRY
            DECLARE @SetMultiUserError VARCHAR(500) =
                'ALTER DATABASE [' + @DatabaseName + '] SET MULTI_USER;';
            EXEC(@SetMultiUserError);
        END TRY
        BEGIN CATCH
            -- Ignore errors in cleanup
        END CATCH
    END

    RAISERROR(@ErrorMessage, @ErrorSeverity, @ErrorState);
END CATCH;
"""
