/*
Database Migration Script for Simplified Dropoff System
Migrates existing Reference Data Management System to support PDF workflow
Preserves all existing data and adds new workflow tracking capabilities
*/

-- Migration script version and metadata
PRINT 'Starting migration to Simplified Dropoff System v1.0'
PRINT 'Migration Date: ' + CONVERT(VARCHAR, GETDATE(), 120)
GO

-- Create migration tracking table if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[System_Migrations]') AND type in (N'U'))
BEGIN
    CREATE TABLE [ref].[System_Migrations] (
        migration_id INT IDENTITY(1,1) PRIMARY KEY,
        migration_name NVARCHAR(255) NOT NULL,
        migration_version NVARCHAR(50) NOT NULL,
        applied_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        applied_by NVARCHAR(100) NOT NULL DEFAULT SYSTEM_USER,
        rollback_script NVARCHAR(MAX),
        migration_notes NVARCHAR(MAX)
    )
    
    PRINT 'Created ref.System_Migrations table'
END
GO

-- Check if this migration has already been applied
IF EXISTS (SELECT 1 FROM ref.System_Migrations WHERE migration_name = 'SimplifiedDropoffSystem')
BEGIN
    PRINT 'Migration already applied. Use rollback script if you need to revert.'
    PRINT 'Exiting migration script.'
    RETURN
END
GO

-- Begin transaction for atomic migration
BEGIN TRANSACTION MigrateToSimplified
BEGIN TRY

    PRINT 'Step 1: Creating PDF workflow schema...'
    
    -- Execute the PDF workflow schema creation
    -- (This is the content from pdf_workflow_schema.sql)
    
    -- Ensure ref schema exists
    IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'ref')
    BEGIN
        EXEC('CREATE SCHEMA [ref]')
        PRINT '  Created ref schema'
    END

    -- Create PDF_Workflow_Tracking table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[PDF_Workflow_Tracking]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [ref].[PDF_Workflow_Tracking] (
            [workflow_id] VARCHAR(50) NOT NULL PRIMARY KEY,
            [csv_file_path] NVARCHAR(500) NOT NULL,
            [pdf_file_path] NVARCHAR(500) NULL,
            [status] VARCHAR(50) NOT NULL,
            [created_at] DATETIME2 NOT NULL DEFAULT GETDATE(),
            [pdf_generated_at] DATETIME2 NULL,
            [approved_at] DATETIME2 NULL,
            [completed_at] DATETIME2 NULL,
            [error_message] NVARCHAR(MAX) NULL,
            [user_config] NVARCHAR(MAX) NULL,
            [processed_by] NVARCHAR(100) NULL,
            [retry_count] INT NOT NULL DEFAULT 0,
            [original_csv_size_mb] DECIMAL(10,2) NULL,
            [processing_duration_seconds] INT NULL,
            [created_by_system] VARCHAR(100) DEFAULT 'SimplifiedDropoffSystem'
        )
        
        PRINT '  Created PDF_Workflow_Tracking table'
    END

    -- Create PDF_Processing_Stats table
    IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[PDF_Processing_Stats]') AND type in (N'U'))
    BEGIN
        CREATE TABLE [ref].[PDF_Processing_Stats] (
            [stats_id] INT IDENTITY(1,1) PRIMARY KEY,
            [workflow_id] VARCHAR(50) NOT NULL,
            [stat_type] VARCHAR(50) NOT NULL,
            [stat_value] DECIMAL(10,3) NOT NULL,
            [recorded_at] DATETIME2 NOT NULL DEFAULT GETDATE(),
            [additional_info] NVARCHAR(500) NULL
        )
        
        -- Foreign key to workflow tracking
        ALTER TABLE [ref].[PDF_Processing_Stats]
        ADD CONSTRAINT [FK_PDF_Stats_Workflow]
        FOREIGN KEY ([workflow_id]) REFERENCES [ref].[PDF_Workflow_Tracking]([workflow_id])
        ON DELETE CASCADE
        
        PRINT '  Created PDF_Processing_Stats table'
    END

    PRINT 'Step 2: Creating indexes for performance...'
    
    -- Indexes for workflow tracking
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('ref.PDF_Workflow_Tracking') AND name = 'IX_PDF_Workflow_Status')
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_PDF_Workflow_Status]
        ON [ref].[PDF_Workflow_Tracking] ([status])
        INCLUDE ([workflow_id], [csv_file_path], [created_at])
        
        PRINT '  Created workflow status index'
    END

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('ref.PDF_Workflow_Tracking') AND name = 'IX_PDF_Workflow_CreatedAt')
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_PDF_Workflow_CreatedAt]
        ON [ref].[PDF_Workflow_Tracking] ([created_at])
        INCLUDE ([status], [completed_at])
        
        PRINT '  Created workflow creation date index'
    END

    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('ref.PDF_Workflow_Tracking') AND name = 'IX_PDF_Workflow_ApprovedAt')
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_PDF_Workflow_ApprovedAt]
        ON [ref].[PDF_Workflow_Tracking] ([approved_at])
        WHERE [approved_at] IS NOT NULL
        INCLUDE ([workflow_id], [csv_file_path], [pdf_file_path], [user_config])
        
        PRINT '  Created workflow approval date index'
    END

    -- Index for stats queries
    IF NOT EXISTS (SELECT * FROM sys.indexes WHERE object_id = OBJECT_ID('ref.PDF_Processing_Stats') AND name = 'IX_PDF_Stats_Type_Date')
    BEGIN
        CREATE NONCLUSTERED INDEX [IX_PDF_Stats_Type_Date]
        ON [ref].[PDF_Processing_Stats] ([stat_type], [recorded_at])
        INCLUDE ([stat_value])
        
        PRINT '  Created processing stats index'
    END

    PRINT 'Step 3: Creating constraints and validation...'
    
    -- Check constraint for valid workflow statuses
    IF NOT EXISTS (SELECT * FROM sys.check_constraints WHERE object_id = OBJECT_ID('ref.CK_PDF_Workflow_Status'))
    BEGIN
        ALTER TABLE [ref].[PDF_Workflow_Tracking]
        ADD CONSTRAINT [CK_PDF_Workflow_Status] 
        CHECK ([status] IN ('pending_pdf', 'pdf_generated', 'user_reviewing', 'approved', 'processing', 'completed', 'error'))
        
        PRINT '  Created workflow status constraint'
    END

    PRINT 'Step 4: Creating views and functions...'
    
    -- Create workflow summary view
    IF EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID('ref.VW_PDF_Workflow_Summary'))
    BEGIN
        DROP VIEW [ref].[VW_PDF_Workflow_Summary]
    END

    EXEC('CREATE VIEW [ref].[VW_PDF_Workflow_Summary]
    AS
    SELECT 
        w.workflow_id,
        w.csv_file_path,
        w.pdf_file_path,
        w.status,
        w.created_at,
        w.pdf_generated_at,
        w.approved_at,
        w.completed_at,
        w.processed_by,
        w.retry_count,
        w.original_csv_size_mb,
        w.processing_duration_seconds,
        CASE 
            WHEN w.status = ''completed'' THEN 
                DATEDIFF(SECOND, w.created_at, w.completed_at)
            ELSE 
                DATEDIFF(SECOND, w.created_at, GETDATE())
        END AS total_elapsed_seconds,
        CASE 
            WHEN w.pdf_generated_at IS NOT NULL THEN 
                DATEDIFF(SECOND, w.created_at, w.pdf_generated_at)
            ELSE NULL 
        END AS pdf_generation_seconds,
        CASE 
            WHEN w.approved_at IS NOT NULL AND w.pdf_generated_at IS NOT NULL THEN 
                DATEDIFF(SECOND, w.pdf_generated_at, w.approved_at)
            ELSE NULL 
        END AS user_review_seconds
    FROM [ref].[PDF_Workflow_Tracking] w')
    
    PRINT '  Created VW_PDF_Workflow_Summary view'

    -- Create cleanup stored procedure
    IF EXISTS (SELECT * FROM sys.procedures WHERE object_id = OBJECT_ID('ref.SP_Cleanup_Completed_Workflows'))
    BEGIN
        DROP PROCEDURE [ref].[SP_Cleanup_Completed_Workflows]
    END

    EXEC('CREATE PROCEDURE [ref].[SP_Cleanup_Completed_Workflows]
        @DaysToKeep INT = 7,
        @DeletedCount INT OUTPUT
    AS
    BEGIN
        SET NOCOUNT ON
        
        DECLARE @CutoffDate DATETIME2 = DATEADD(DAY, -@DaysToKeep, GETDATE())
        
        DELETE FROM [ref].[PDF_Workflow_Tracking]
        WHERE status = ''completed'' 
          AND completed_at < @CutoffDate
        
        SET @DeletedCount = @@ROWCOUNT
        
        INSERT INTO [ref].[PDF_Processing_Stats] (workflow_id, stat_type, stat_value, additional_info)
        VALUES (''SYSTEM'', ''cleanup_operation'', @DeletedCount, 
                CONCAT(''Cleaned up workflows older than '', @DaysToKeep, '' days''))
    END')
    
    PRINT '  Created SP_Cleanup_Completed_Workflows procedure'

    -- Create status function
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID('ref.FN_Get_Workflow_Status_Counts'))
    BEGIN
        DROP FUNCTION [ref].[FN_Get_Workflow_Status_Counts]
    END

    EXEC('CREATE FUNCTION [ref].[FN_Get_Workflow_Status_Counts]()
    RETURNS TABLE
    AS
    RETURN
    (
        SELECT 
            status,
            COUNT(*) as workflow_count,
            MIN(created_at) as oldest_workflow,
            MAX(created_at) as newest_workflow,
            AVG(CASE WHEN retry_count > 0 THEN retry_count ELSE NULL END) as avg_retries
        FROM [ref].[PDF_Workflow_Tracking]
        WHERE status != ''completed''
        GROUP BY status
    )')
    
    PRINT '  Created FN_Get_Workflow_Status_Counts function'

    PRINT 'Step 5: Preserving existing data and adding compatibility...'
    
    -- Check if existing File_Monitor_Tracking table exists and preserve it
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[File_Monitor_Tracking]') AND type in (N'U'))
    BEGIN
        PRINT '  Existing File_Monitor_Tracking table preserved - no changes needed'
        
        -- Add compatibility view if needed
        IF NOT EXISTS (SELECT * FROM sys.views WHERE object_id = OBJECT_ID('ref.VW_Legacy_File_Processing'))
        BEGIN
            EXEC('CREATE VIEW [ref].[VW_Legacy_File_Processing]
            AS
            SELECT 
                ''legacy'' as system_type,
                file_path as csv_file_path,
                status,
                created_at,
                NULL as pdf_file_path,
                NULL as workflow_id,
                ''N/A'' as processing_mode
            FROM [ref].[File_Monitor_Tracking]
            WHERE created_at >= DATEADD(DAY, -30, GETDATE())
            
            UNION ALL
            
            SELECT 
                ''simplified'' as system_type,
                csv_file_path,
                status,
                created_at,
                pdf_file_path,
                workflow_id,
                ISNULL(JSON_VALUE(user_config, ''$.processing_mode''), ''unknown'') as processing_mode
            FROM [ref].[PDF_Workflow_Tracking]')
            
            PRINT '  Created compatibility view for legacy and simplified systems'
        END
    END
    ELSE
    BEGIN
        PRINT '  No existing File_Monitor_Tracking table found - simplified system will be primary'
    END

    -- Preserve Reference_Data_Cfg table if it exists
    IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[Reference_Data_Cfg]') AND type in (N'U'))
    BEGIN
        PRINT '  Existing Reference_Data_Cfg table preserved - simplified system will use it'
    END

    PRINT 'Step 6: Creating system initialization data...'
    
    -- Insert system initialization record
    IF NOT EXISTS (SELECT 1 FROM [ref].[PDF_Workflow_Tracking] WHERE workflow_id = 'SYSTEM_MIGRATION_INIT')
    BEGIN
        INSERT INTO [ref].[PDF_Workflow_Tracking] 
        (workflow_id, csv_file_path, status, created_at, completed_at, processed_by, created_by_system)
        VALUES 
        ('SYSTEM_MIGRATION_INIT', 'MIGRATION_TO_SIMPLIFIED_SYSTEM', 'completed', GETDATE(), GETDATE(), 'SYSTEM_MIGRATION', 'DatabaseMigration')
        
        PRINT '  Created system initialization record'
    END

    -- Insert migration statistics
    INSERT INTO [ref].[PDF_Processing_Stats] (workflow_id, stat_type, stat_value, additional_info)
    VALUES 
    ('SYSTEM_MIGRATION_INIT', 'migration_completion', 1, 'Successfully migrated to Simplified Dropoff System v1.0')

    PRINT 'Step 7: Recording migration completion...'
    
    -- Record this migration in the tracking table
    INSERT INTO [ref].[System_Migrations] (
        migration_name, 
        migration_version, 
        rollback_script,
        migration_notes
    )
    VALUES (
        'SimplifiedDropoffSystem',
        '1.0',
        'DROP VIEW IF EXISTS [ref].[VW_PDF_Workflow_Summary]; DROP PROCEDURE IF EXISTS [ref].[SP_Cleanup_Completed_Workflows]; DROP FUNCTION IF EXISTS [ref].[FN_Get_Workflow_Status_Counts]; DROP TABLE [ref].[PDF_Processing_Stats]; DROP TABLE [ref].[PDF_Workflow_Tracking];',
        'Migration to Simplified Dropoff System with PDF workflow tracking. Preserves all existing data and functionality.'
    )

    -- Commit the transaction
    COMMIT TRANSACTION MigrateToSimplified
    
    PRINT 'Migration completed successfully!'
    PRINT ''
    PRINT 'Summary of changes:'
    PRINT '- Created PDF_Workflow_Tracking table for workflow management'
    PRINT '- Created PDF_Processing_Stats table for performance tracking'
    PRINT '- Added indexes for optimal query performance'
    PRINT '- Created workflow summary view and management procedures'
    PRINT '- Preserved all existing data and tables'
    PRINT '- Added compatibility views for legacy system integration'
    PRINT ''
    PRINT 'Next Steps:'
    PRINT '1. Deploy the Simplified Dropoff System application code'
    PRINT '2. Configure environment variables for new system'
    PRINT '3. Test with sample CSV files'
    PRINT '4. Monitor logs in simplified_file_monitor.log and pdf_approval_monitor.log'
    PRINT ''
    PRINT 'The system is now ready for Simplified Dropoff operations!'

END TRY
BEGIN CATCH
    -- Rollback transaction on error
    IF @@TRANCOUNT > 0
        ROLLBACK TRANSACTION MigrateToSimplified
    
    -- Log the error
    DECLARE @ErrorMessage NVARCHAR(4000) = ERROR_MESSAGE()
    DECLARE @ErrorSeverity INT = ERROR_SEVERITY()
    DECLARE @ErrorState INT = ERROR_STATE()
    
    PRINT 'Migration failed with error:'
    PRINT @ErrorMessage
    
    -- Optionally insert error into migration log
    IF EXISTS (SELECT 1 FROM sys.objects WHERE object_id = OBJECT_ID(N'[ref].[System_Migrations]'))
    BEGIN
        INSERT INTO [ref].[System_Migrations] (
            migration_name, 
            migration_version, 
            migration_notes
        )
        VALUES (
            'SimplifiedDropoffSystem_FAILED',
            '1.0',
            'Migration failed: ' + @ErrorMessage
        )
    END
    
    -- Re-raise the error
    RAISERROR (@ErrorMessage, @ErrorSeverity, @ErrorState)
END CATCH

-- Final verification
PRINT ''
PRINT 'Verification:'
SELECT 
    'PDF_Workflow_Tracking' as table_name,
    COUNT(*) as record_count
FROM [ref].[PDF_Workflow_Tracking]

UNION ALL

SELECT 
    'PDF_Processing_Stats' as table_name,
    COUNT(*) as record_count  
FROM [ref].[PDF_Processing_Stats]

UNION ALL

SELECT 
    'System_Migrations' as table_name,
    COUNT(*) as record_count
FROM [ref].[System_Migrations]
WHERE migration_name LIKE '%Simplified%'

PRINT 'Migration script completed.'
GO