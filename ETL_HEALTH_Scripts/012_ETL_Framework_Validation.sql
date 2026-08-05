/*
File: 012_ETL_Framework_Validation.sql
Purpose: Validates required ETL objects and exercises a rollback-only sample lifecycle.
Dependencies: 001 through 011.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
SET XACT_ABORT ON;
GO

/* Fail immediately when a required table is missing. */
IF EXISTS
(
    SELECT 1
    FROM (VALUES (N'RunHistory'), (N'FileHistory'), (N'RowRejection'),
                 (N'DuplicateRecord'), (N'Watermark'), (N'BatchHistory')) AS Required(Name)
    WHERE OBJECT_ID(N'ETL.' + QUOTENAME(Required.Name), N'U') IS NULL
)
    THROW 51001, 'Validation failed: one or more required ETL tables are missing.', 1;
GO

IF EXISTS
(
    SELECT 1
    FROM (VALUES (N'vw_LatestJobRun'), (N'vw_FailedRuns'), (N'vw_RunSummary'),
                 (N'vw_PendingQuarantine'), (N'vw_DuplicateSummary')) AS Required(Name)
    WHERE OBJECT_ID(N'ETL.' + QUOTENAME(Required.Name), N'V') IS NULL
)
    THROW 51002, 'Validation failed: one or more required ETL views are missing.', 1;
GO

IF EXISTS
(
    SELECT 1
    FROM (VALUES (N'usp_StartRun'), (N'usp_UpdateRunStatus'), (N'usp_CompleteRun'),
                 (N'usp_RegisterFile'), (N'usp_LogRejection'), (N'usp_LogDuplicate'),
                 (N'usp_SetPendingWatermark'), (N'usp_CommitWatermark'),
                 (N'usp_ClearPendingWatermark')) AS Required(Name)
    WHERE OBJECT_ID(N'ETL.' + QUOTENAME(Required.Name), N'P') IS NULL
)
    THROW 51003, 'Validation failed: one or more required ETL procedures are missing.', 1;
GO

IF EXISTS
(
    SELECT 1
    FROM (VALUES
        (N'RunHistory', N'PK_RunHistory'),
        (N'FileHistory', N'PK_FileHistory'),
        (N'RowRejection', N'PK_RowRejection'),
        (N'DuplicateRecord', N'PK_DuplicateRecord'),
        (N'Watermark', N'PK_Watermark'),
        (N'BatchHistory', N'PK_BatchHistory')) AS Required(TableName, ConstraintName)
    WHERE NOT EXISTS
    (
        SELECT 1 FROM sys.key_constraints AS KC
        WHERE KC.parent_object_id = OBJECT_ID(N'ETL.' + QUOTENAME(Required.TableName))
          AND KC.name = Required.ConstraintName AND KC.type = 'PK'
    )
)
    THROW 51004, 'Validation failed: one or more required primary keys are missing.', 1;
GO

IF EXISTS
(
    SELECT 1
    FROM (VALUES
        (N'RunHistory', N'FK_RunHistory_ParentETLRunID'),
        (N'FileHistory', N'FK_FileHistory_RunHistory'),
        (N'RowRejection', N'FK_RowRejection_RunHistory'),
        (N'RowRejection', N'FK_RowRejection_FileHistory'),
        (N'RowRejection', N'FK_RowRejection_ReprocessedRun'),
        (N'DuplicateRecord', N'FK_DuplicateRecord_RunHistory'),
        (N'DuplicateRecord', N'FK_DuplicateRecord_FileHistory'),
        (N'Watermark', N'FK_Watermark_LastSuccessfulRun'),
        (N'Watermark', N'FK_Watermark_PendingRun'),
        (N'BatchHistory', N'FK_BatchHistory_RunHistory')) AS Required(TableName, ConstraintName)
    WHERE NOT EXISTS
    (
        SELECT 1 FROM sys.foreign_keys AS FK
        WHERE FK.parent_object_id = OBJECT_ID(N'ETL.' + QUOTENAME(Required.TableName))
          AND FK.name = Required.ConstraintName
    )
)
    THROW 51005, 'Validation failed: one or more required foreign keys are missing.', 1;
GO

IF EXISTS
(
    SELECT 1
    FROM (VALUES
        (N'RunHistory', N'IX_RunHistory_JobName_StartedAt'),
        (N'RunHistory', N'IX_RunHistory_RunStatus_StartedAt'),
        (N'RunHistory', N'IX_RunHistory_Target'),
        (N'RunHistory', N'IX_RunHistory_Source'),
        (N'RunHistory', N'IX_RunHistory_ParentETLRunID'),
        (N'FileHistory', N'IX_FileHistory_SourceSystem_FileHash'),
        (N'FileHistory', N'IX_FileHistory_ETLRunID'),
        (N'FileHistory', N'IX_FileHistory_SourceSystem_FileName'),
        (N'FileHistory', N'IX_FileHistory_Status_DetectedAt'),
        (N'FileHistory', N'IX_FileHistory_FileHash'),
        (N'RowRejection', N'IX_RowRejection_ETLRunID'),
        (N'RowRejection', N'IX_RowRejection_ETLFileID'),
        (N'RowRejection', N'IX_RowRejection_JobName_RejectedAt'),
        (N'RowRejection', N'IX_RowRejection_ErrorCode_RejectedAt'),
        (N'RowRejection', N'IX_RowRejection_Status_RejectedAt'),
        (N'RowRejection', N'IX_RowRejection_Target'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_ETLRunID'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_ETLFileID'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_JobName_LoggedAt'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_Type_LoggedAt'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_BusinessKeyHash'),
        (N'DuplicateRecord', N'IX_DuplicateRecord_ResolutionAction'),
        (N'Watermark', N'UQ_Watermark_JobSource'),
        (N'Watermark', N'IX_Watermark_JobName'),
        (N'Watermark', N'IX_Watermark_Source'),
        (N'Watermark', N'IX_Watermark_LastSuccessfulRun'),
        (N'Watermark', N'IX_Watermark_PendingRun'),
        (N'BatchHistory', N'UQ_BatchHistory_RunBatch'),
        (N'BatchHistory', N'IX_BatchHistory_ETLRunID'),
        (N'BatchHistory', N'IX_BatchHistory_Status_StartedAt')) AS Required(TableName, IndexName)
    WHERE NOT EXISTS
    (
        SELECT 1 FROM sys.indexes AS I
        WHERE I.object_id = OBJECT_ID(N'ETL.' + QUOTENAME(Required.TableName))
          AND I.name = Required.IndexName
    )
)
    THROW 51006, 'Validation failed: one or more required ETL indexes are missing.', 1;
GO

/* Functional test: every insert/update is protected by this outer rollback. */
BEGIN TRY
    BEGIN TRANSACTION;

    DECLARE @Token NVARCHAR(36) = CONVERT(NVARCHAR(36), NEWID());
    DECLARE @JobName NVARCHAR(255) = N'ETL_FRAMEWORK_VALIDATION_' + @Token;
    DECLARE @SourceSystem NVARCHAR(255) = N'VALIDATION_SOURCE_' + @Token;
    DECLARE @SourceObject NVARCHAR(300) = N'VALIDATION_OBJECT_' + @Token;
    DECLARE @FileHash CHAR(64) = CONVERT(CHAR(64), HASHBYTES('SHA2_256', @Token), 2);
    DECLARE @ETLRunID BIGINT;
    DECLARE @ETLFileID BIGINT;
    DECLARE @TargetServer NVARCHAR(255) = CONVERT(NVARCHAR(255), @@SERVERNAME);
    DECLARE @TargetDatabase SYSNAME = DB_NAME();
    DECLARE @ExecutingUser NVARCHAR(255) = SUSER_SNAME();

    EXEC ETL.usp_StartRun
        @JobName = @JobName,
        @JobDescription = N'Rollback-only ETL framework validation',
        @SourceType = 'CSV',
        @SourceSystem = @SourceSystem,
        @SourceObject = @SourceObject,
        @TargetServer = @TargetServer,
        @TargetDatabase = @TargetDatabase,
        @TargetSchema = N'dbo',
        @TargetTable = N'ValidationTarget',
        @LoadType = 'UPSERT',
        @CreatedBy = @ExecutingUser;

    SELECT @ETLRunID = ETLRunID FROM ETL.RunHistory WHERE JobName = @JobName;
    IF @ETLRunID IS NULL THROW 51007, 'Functional validation failed to start a run.', 1;

    EXEC ETL.usp_RegisterFile
        @ETLRunID = @ETLRunID,
        @SourceSystem = @SourceSystem,
        @SourceFileName = N'validation.csv',
        @SourceFilePath = N'C:\validation\validation.csv',
        @FileExtension = N'.csv',
        @FileSizeBytes = 100,
        @FileHash = @FileHash,
        @ExpectedColumnCount = 3,
        @ActualColumnCount = 3,
        @ExpectedRowCount = 3,
        @ActualRowCount = 3,
        @HeaderRow = 1,
        @Delimiter = N',',
        @TextQualifier = N'"',
        @Encoding = N'UTF-8',
        @HasHeader = 1,
        @FileStatus = 'VALIDATED';

    SELECT @ETLFileID = ETLFileID
    FROM ETL.FileHistory
    WHERE ETLRunID = @ETLRunID AND FileHash = @FileHash;
    IF @ETLFileID IS NULL THROW 51008, 'Functional validation failed to register a file.', 1;

    EXEC ETL.usp_LogRejection
        @ETLRunID = @ETLRunID, @ETLFileID = @ETLFileID, @JobName = @JobName,
        @SourceSystem = @SourceSystem, @SourceObject = @SourceObject,
        @SourceFileName = N'validation.csv', @SourceRowNumber = 2,
        @SourceRecordID = N'ID=2', @TargetSchema = N'dbo', @TargetTable = N'ValidationTarget',
        @ColumnName = N'Amount', @OriginalValue = N'not-a-number', @ExpectedDataType = N'DECIMAL(18,2)',
        @ExpectedPrecision = 18, @ExpectedScale = 2, @ErrorCategory = 'DATATYPE',
        @ErrorCode = N'INVALID_DECIMAL', @ErrorMessage = N'Validation sample rejection.',
        @OriginalRowJSON = N'{"ID":2,"Amount":"not-a-number"}';

    EXEC ETL.usp_LogDuplicate
        @ETLRunID = @ETLRunID, @ETLFileID = @ETLFileID, @JobName = @JobName,
        @SourceSystem = @SourceSystem, @SourceObject = @SourceObject,
        @SourceFileName = N'validation.csv', @SourceRowNumber = 3,
        @MatchingSourceRowNumber = 1, @SourceRecordID = N'ID=1', @BusinessKey = N'ID=1',
        @BusinessKeyHash = 0x01, @DuplicateType = 'EXACT_DUPLICATE_IN_SOURCE',
        @ResolutionAction = 'SKIPPED', @WinnerSourceRowNumber = 1,
        @OriginalRowJSON = N'{"ID":1,"Amount":10}', @MatchingRowJSON = N'{"ID":1,"Amount":10}';

    EXEC ETL.usp_SetPendingWatermark
        @JobName = @JobName, @SourceSystem = @SourceSystem, @SourceObject = @SourceObject,
        @WatermarkColumn = N'UpdatedAt', @TieBreakerColumn = N'ID',
        @PendingWatermark = N'2026-01-01T00:00:00', @PendingTieBreaker = N'100',
        @ETLRunID = @ETLRunID, @WatermarkDataType = N'DATETIME2(0)', @UpdatedBy = @ExecutingUser;

    EXEC ETL.usp_CompleteRun
        @ETLRunID = @ETLRunID, @RowsReceived = 3, @RowsExtracted = 3, @RowsStaged = 3,
        @RowsValid = 2, @RowsInvalid = 1, @RowsExactDuplicate = 1,
        @RowsConflictingDuplicate = 0, @RowsAlreadyExist = 0, @RowsInserted = 1,
        @RowsUpdated = 0, @RowsDeleted = 0, @RowsUnchanged = 0, @RowsRejected = 1,
        @RowsSkipped = 0, @BatchCount = 0, @FileCount = 1, @RunStatus = 'COMPLETED';

    -- Promotion follows completion because usp_CommitWatermark correctly requires a committed-success status.
    EXEC ETL.usp_CommitWatermark
        @JobName = @JobName, @SourceSystem = @SourceSystem, @SourceObject = @SourceObject,
        @ETLRunID = @ETLRunID, @UpdatedBy = @ExecutingUser;

    SELECT * FROM ETL.vw_LatestJobRun WHERE JobName = @JobName;
    SELECT * FROM ETL.vw_FailedRuns WHERE JobName = @JobName;
    SELECT * FROM ETL.vw_RunSummary WHERE ETLRunID = @ETLRunID;
    SELECT * FROM ETL.vw_PendingQuarantine WHERE ETLRunID = @ETLRunID;
    SELECT * FROM ETL.vw_DuplicateSummary WHERE ETLRunID = @ETLRunID;
    SELECT N'PASS' AS ValidationResult, N'All functional test data was rolled back.' AS Detail;

    ROLLBACK TRANSACTION;
END TRY
BEGIN CATCH
    IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
    THROW;
END CATCH;
GO
