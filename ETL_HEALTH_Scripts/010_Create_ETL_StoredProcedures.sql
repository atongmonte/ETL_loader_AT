/*
File: 010_Create_ETL_StoredProcedures.sql
Purpose: Creates the ETL lifecycle, file, exception, duplicate, and watermark procedures.
Dependencies: 001 through 009.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'ETL.usp_StartRun', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_StartRun AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_StartRun
    @JobName NVARCHAR(255),
    @JobDescription NVARCHAR(1000) = NULL,
    @SourceType VARCHAR(20),
    @SourceSystem NVARCHAR(255) = NULL,
    @SourceObject NVARCHAR(500) = NULL,
    @TargetServer NVARCHAR(255) = NULL,
    @TargetDatabase SYSNAME,
    @TargetSchema SYSNAME,
    @TargetTable SYSNAME,
    @LoadType VARCHAR(30) = NULL,
    @ParentETLRunID BIGINT = NULL,
    @CreatedBy NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        INSERT ETL.RunHistory
            (ParentETLRunID, JobName, JobDescription, SourceType, SourceSystem, SourceObject,
             TargetServer, TargetDatabase, TargetSchema, TargetTable, LoadType,
             StartedAt, RunStatus, CreatedBy, CreatedAt)
        VALUES
            (@ParentETLRunID, @JobName, @JobDescription, @SourceType, @SourceSystem, @SourceObject,
             @TargetServer, @TargetDatabase, @TargetSchema, @TargetTable, @LoadType,
             SYSUTCDATETIME(), 'STARTED', @CreatedBy, SYSUTCDATETIME());

        DECLARE @ETLRunID BIGINT = CONVERT(BIGINT, SCOPE_IDENTITY());
        COMMIT TRANSACTION;
        SELECT @ETLRunID AS ETLRunID;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_SetPendingWatermark', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_SetPendingWatermark AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_SetPendingWatermark
    @JobName NVARCHAR(255),
    @SourceSystem NVARCHAR(255),
    @SourceObject NVARCHAR(300),
    @WatermarkColumn NVARCHAR(255),
    @TieBreakerColumn NVARCHAR(255) = NULL,
    @PendingWatermark NVARCHAR(500),
    @PendingTieBreaker NVARCHAR(500) = NULL,
    @ETLRunID BIGINT,
    @WatermarkDataType NVARCHAR(128),
    @UpdatedBy NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;

        DECLARE @ExistingPendingRunID BIGINT;
        SELECT @ExistingPendingRunID = PendingETLRunID
        FROM ETL.Watermark WITH (UPDLOCK, HOLDLOCK)
        WHERE JobName = @JobName AND SourceSystem = @SourceSystem AND SourceObject = @SourceObject;

        IF @ExistingPendingRunID IS NOT NULL AND @ExistingPendingRunID <> @ETLRunID
            THROW 50006, 'A different ETL run already owns the pending watermark.', 1;

        UPDATE ETL.Watermark
        SET WatermarkColumn = @WatermarkColumn,
            TieBreakerColumn = @TieBreakerColumn,
            PendingWatermark = @PendingWatermark,
            PendingTieBreaker = @PendingTieBreaker,
            PendingETLRunID = @ETLRunID,
            WatermarkDataType = @WatermarkDataType,
            UpdatedAt = SYSUTCDATETIME(),
            UpdatedBy = @UpdatedBy
        WHERE JobName = @JobName AND SourceSystem = @SourceSystem AND SourceObject = @SourceObject;

        IF @@ROWCOUNT = 0
            INSERT ETL.Watermark
                (JobName, SourceSystem, SourceObject, WatermarkColumn, TieBreakerColumn,
                 PendingWatermark, PendingTieBreaker, PendingETLRunID,
                 WatermarkDataType, UpdatedAt, UpdatedBy)
            VALUES
                (@JobName, @SourceSystem, @SourceObject, @WatermarkColumn, @TieBreakerColumn,
                 @PendingWatermark, @PendingTieBreaker, @ETLRunID,
                 @WatermarkDataType, SYSUTCDATETIME(), @UpdatedBy);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_CommitWatermark', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_CommitWatermark AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_CommitWatermark
    @JobName NVARCHAR(255),
    @SourceSystem NVARCHAR(255),
    @SourceObject NVARCHAR(300),
    @ETLRunID BIGINT,
    @UpdatedBy NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        DECLARE @RunStatus VARCHAR(40);
        SELECT @RunStatus = RunStatus
        FROM ETL.RunHistory WITH (UPDLOCK, HOLDLOCK)
        WHERE ETLRunID = @ETLRunID;

        IF @RunStatus IS NULL
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;
        IF @RunStatus NOT IN ('COMPLETED','COMPLETED_WITH_WARNINGS')
            THROW 50007, 'The watermark can be committed only for a successfully completed ETL run.', 1;

        UPDATE ETL.Watermark WITH (UPDLOCK)
        SET LastSuccessfulWatermark = PendingWatermark,
            LastSuccessfulTieBreaker = PendingTieBreaker,
            LastSuccessfulETLRunID = @ETLRunID,
            PendingWatermark = NULL,
            PendingTieBreaker = NULL,
            PendingETLRunID = NULL,
            UpdatedAt = SYSUTCDATETIME(),
            UpdatedBy = @UpdatedBy
        WHERE JobName = @JobName AND SourceSystem = @SourceSystem AND SourceObject = @SourceObject
          AND PendingETLRunID = @ETLRunID;

        IF @@ROWCOUNT = 0
            THROW 50008, 'No pending watermark owned by the supplied ETL run was found.', 1;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_ClearPendingWatermark', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_ClearPendingWatermark AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_ClearPendingWatermark
    @JobName NVARCHAR(255),
    @SourceSystem NVARCHAR(255),
    @SourceObject NVARCHAR(300),
    @ETLRunID BIGINT,
    @UpdatedBy NVARCHAR(255) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        UPDATE ETL.Watermark WITH (UPDLOCK, HOLDLOCK)
        SET PendingWatermark = NULL,
            PendingTieBreaker = NULL,
            PendingETLRunID = NULL,
            UpdatedAt = SYSUTCDATETIME(),
            UpdatedBy = @UpdatedBy
        WHERE JobName = @JobName AND SourceSystem = @SourceSystem AND SourceObject = @SourceObject
          AND PendingETLRunID = @ETLRunID;

        IF @@ROWCOUNT = 0
            THROW 50008, 'No pending watermark owned by the supplied ETL run was found.', 1;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_RegisterFile', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_RegisterFile AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_RegisterFile
    @ETLRunID BIGINT,
    @SourceSystem NVARCHAR(255),
    @SourceFileName NVARCHAR(500),
    @SourceFilePath NVARCHAR(2000) = NULL,
    @ArchiveFilePath NVARCHAR(2000) = NULL,
    @FailedFilePath NVARCHAR(2000) = NULL,
    @FileExtension NVARCHAR(20) = NULL,
    @FileSizeBytes BIGINT = NULL,
    @FileHashAlgorithm VARCHAR(20) = 'SHA256',
    @FileHash CHAR(64) = NULL,
    @FileCreatedAt DATETIME2(0) = NULL,
    @FileModifiedAt DATETIME2(0) = NULL,
    @FileDetectedAt DATETIME2(0) = NULL,
    @ExpectedColumnCount INT = NULL,
    @ActualColumnCount INT = NULL,
    @ExpectedRowCount BIGINT = NULL,
    @ActualRowCount BIGINT = NULL,
    @HeaderRow INT = NULL,
    @Delimiter NVARCHAR(20) = NULL,
    @TextQualifier NVARCHAR(20) = NULL,
    @Encoding NVARCHAR(100) = NULL,
    @HasHeader BIT = 1,
    @FileStatus VARCHAR(20) = 'DETECTED'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    DECLARE @ETLFileID BIGINT = NULL, @PreviousETLFileID BIGINT = NULL;

    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;

        IF @FileHash IS NOT NULL
        BEGIN
            -- The range lock and supporting SourceSystem/FileHash index serialize competing registrations.
            -- Active attempts are protected as well as completed files; failed/rejected attempts remain retryable.
            SELECT TOP (1) @PreviousETLFileID = ETLFileID
            FROM ETL.FileHistory WITH (UPDLOCK, HOLDLOCK)
            WHERE SourceSystem = @SourceSystem
              AND FileHash = @FileHash
              AND FileStatus IN ('DETECTED','READY','PROCESSING','VALIDATED','LOADED','ARCHIVED')
            ORDER BY ETLFileID DESC;
        END;

        IF @PreviousETLFileID IS NULL
        BEGIN
            INSERT ETL.FileHistory
                (ETLRunID, SourceSystem, SourceFileName, SourceFilePath, ArchiveFilePath, FailedFilePath,
                 FileExtension, FileSizeBytes, FileHashAlgorithm, FileHash, FileCreatedAt, FileModifiedAt,
                 FileDetectedAt, FileStatus, ExpectedColumnCount, ActualColumnCount, ExpectedRowCount,
                 ActualRowCount, HeaderRow, Delimiter, TextQualifier, Encoding, HasHeader)
            VALUES
                (@ETLRunID, @SourceSystem, @SourceFileName, @SourceFilePath, @ArchiveFilePath, @FailedFilePath,
                 @FileExtension, @FileSizeBytes, @FileHashAlgorithm, @FileHash, @FileCreatedAt, @FileModifiedAt,
                 ISNULL(@FileDetectedAt, CONVERT(DATETIME2(0), SYSUTCDATETIME())), @FileStatus,
                 @ExpectedColumnCount, @ActualColumnCount, @ExpectedRowCount, @ActualRowCount,
                 @HeaderRow, @Delimiter, @TextQualifier, @Encoding, @HasHeader);
            SET @ETLFileID = CONVERT(BIGINT, SCOPE_IDENTITY());
        END;

        COMMIT TRANSACTION;
        SELECT @ETLFileID AS ETLFileID,
               CONVERT(BIT, CASE WHEN @PreviousETLFileID IS NULL THEN 0 ELSE 1 END) AS IsDuplicateFile,
               @PreviousETLFileID AS PreviousETLFileID;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_LogRejection', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_LogRejection AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_LogRejection
    @ETLRunID BIGINT,
    @ETLFileID BIGINT = NULL,
    @JobName NVARCHAR(255),
    @SourceSystem NVARCHAR(255) = NULL,
    @SourceObject NVARCHAR(1000) = NULL,
    @SourceFileName NVARCHAR(500) = NULL,
    @SourceRowNumber BIGINT = NULL,
    @SourceRecordID NVARCHAR(1000) = NULL,
    @TargetSchema SYSNAME = NULL,
    @TargetTable SYSNAME = NULL,
    @ColumnName SYSNAME = NULL,
    @OriginalValue NVARCHAR(MAX) = NULL,
    @ExpectedDataType NVARCHAR(128) = NULL,
    @MaximumLength INT = NULL,
    @ExpectedPrecision TINYINT = NULL,
    @ExpectedScale TINYINT = NULL,
    @ErrorCategory VARCHAR(30),
    @ErrorCode NVARCHAR(100),
    @ErrorMessage NVARCHAR(MAX),
    @OriginalRowJSON NVARCHAR(MAX) = NULL,
    @QuarantineStatus VARCHAR(20) = 'PENDING'
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;
        IF @ETLFileID IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM ETL.FileHistory WITH (HOLDLOCK) WHERE ETLFileID = @ETLFileID AND ETLRunID = @ETLRunID)
            THROW 50005, 'The supplied ETLFileID does not belong to the supplied ETLRunID.', 1;

        INSERT ETL.RowRejection
            (ETLRunID, ETLFileID, JobName, SourceSystem, SourceObject, SourceFileName,
             SourceRowNumber, SourceRecordID, TargetSchema, TargetTable, ColumnName, OriginalValue,
             ExpectedDataType, MaximumLength, ExpectedPrecision, ExpectedScale, ErrorCategory,
             ErrorCode, ErrorMessage, OriginalRowJSON, QuarantineStatus, RejectedAt)
        VALUES
            (@ETLRunID, @ETLFileID, @JobName, @SourceSystem, @SourceObject, @SourceFileName,
             @SourceRowNumber, @SourceRecordID, @TargetSchema, @TargetTable, @ColumnName, @OriginalValue,
             @ExpectedDataType, @MaximumLength, @ExpectedPrecision, @ExpectedScale, @ErrorCategory,
             @ErrorCode, @ErrorMessage, @OriginalRowJSON, @QuarantineStatus, SYSUTCDATETIME());

        DECLARE @RejectionID BIGINT = CONVERT(BIGINT, SCOPE_IDENTITY());
        COMMIT TRANSACTION;
        SELECT @RejectionID AS RejectionID;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_LogDuplicate', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_LogDuplicate AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_LogDuplicate
    @ETLRunID BIGINT,
    @ETLFileID BIGINT = NULL,
    @JobName NVARCHAR(255),
    @SourceSystem NVARCHAR(255) = NULL,
    @SourceObject NVARCHAR(1000) = NULL,
    @SourceFileName NVARCHAR(500) = NULL,
    @SourceRowNumber BIGINT = NULL,
    @MatchingSourceRowNumber BIGINT = NULL,
    @SourceRecordID NVARCHAR(1000) = NULL,
    @BusinessKey NVARCHAR(2000) = NULL,
    @BusinessKeyHash VARBINARY(32) = NULL,
    @DuplicateType VARCHAR(50),
    @MatchingTargetKey NVARCHAR(2000) = NULL,
    @ComparisonHash VARBINARY(32) = NULL,
    @ResolutionAction VARCHAR(40),
    @WinnerSourceRowNumber BIGINT = NULL,
    @WinnerSourceRecordID NVARCHAR(1000) = NULL,
    @OriginalRowJSON NVARCHAR(MAX) = NULL,
    @MatchingRowJSON NVARCHAR(MAX) = NULL,
    @ResolutionComment NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;
        IF @ETLFileID IS NOT NULL AND NOT EXISTS
            (SELECT 1 FROM ETL.FileHistory WITH (HOLDLOCK) WHERE ETLFileID = @ETLFileID AND ETLRunID = @ETLRunID)
            THROW 50005, 'The supplied ETLFileID does not belong to the supplied ETLRunID.', 1;

        INSERT ETL.DuplicateRecord
            (ETLRunID, ETLFileID, JobName, SourceSystem, SourceObject, SourceFileName,
             SourceRowNumber, MatchingSourceRowNumber, SourceRecordID, BusinessKey, BusinessKeyHash,
             DuplicateType, MatchingTargetKey, ComparisonHash, ResolutionAction,
             WinnerSourceRowNumber, WinnerSourceRecordID, OriginalRowJSON, MatchingRowJSON,
             ResolutionComment, LoggedAt)
        VALUES
            (@ETLRunID, @ETLFileID, @JobName, @SourceSystem, @SourceObject, @SourceFileName,
             @SourceRowNumber, @MatchingSourceRowNumber, @SourceRecordID, @BusinessKey, @BusinessKeyHash,
             @DuplicateType, @MatchingTargetKey, @ComparisonHash, @ResolutionAction,
             @WinnerSourceRowNumber, @WinnerSourceRecordID, @OriginalRowJSON, @MatchingRowJSON,
             @ResolutionComment, SYSUTCDATETIME());

        DECLARE @DuplicateID BIGINT = CONVERT(BIGINT, SCOPE_IDENTITY());
        COMMIT TRANSACTION;
        SELECT @DuplicateID AS DuplicateID;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_UpdateRunStatus', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_UpdateRunStatus AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_UpdateRunStatus
    @ETLRunID BIGINT,
    @RunStatus VARCHAR(40),
    @ErrorCode NVARCHAR(100) = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;
    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (UPDLOCK, HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;

        UPDATE ETL.RunHistory
        SET RunStatus = @RunStatus,
            CompletedAt = CASE WHEN @RunStatus IN
                ('COMPLETED','COMPLETED_WITH_WARNINGS','FAILED_FILE_VALIDATION','FAILED_DATA_VALIDATION',
                 'FAILED_EXTRACTION','FAILED_DATABASE_LOAD','FAILED_RECONCILIATION','FAILED','CANCELLED')
                THEN ISNULL(CompletedAt, CONVERT(DATETIME2(0), SYSUTCDATETIME())) ELSE NULL END,
            ErrorCode = @ErrorCode,
            ErrorMessage = @ErrorMessage
        WHERE ETLRunID = @ETLRunID;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

IF OBJECT_ID(N'ETL.usp_CompleteRun', N'P') IS NULL
    EXEC(N'CREATE PROCEDURE ETL.usp_CompleteRun AS BEGIN SET NOCOUNT ON; END;');
GO
ALTER PROCEDURE ETL.usp_CompleteRun
    @ETLRunID BIGINT,
    @RowsReceived BIGINT,
    @RowsExtracted BIGINT,
    @RowsStaged BIGINT,
    @RowsValid BIGINT,
    @RowsInvalid BIGINT,
    @RowsExactDuplicate BIGINT,
    @RowsConflictingDuplicate BIGINT,
    @RowsAlreadyExist BIGINT,
    @RowsInserted BIGINT,
    @RowsUpdated BIGINT,
    @RowsDeleted BIGINT,
    @RowsUnchanged BIGINT,
    @RowsRejected BIGINT,
    @RowsSkipped BIGINT,
    @BatchCount BIGINT,
    @FileCount BIGINT,
    @RejectFilePath NVARCHAR(2000) = NULL,
    @DuplicateFilePath NVARCHAR(2000) = NULL,
    @LogFilePath NVARCHAR(2000) = NULL,
    @RunStatus VARCHAR(40) = 'COMPLETED',
    @ErrorCode NVARCHAR(100) = NULL,
    @ErrorMessage NVARCHAR(MAX) = NULL,
    @ValidateReconciliation BIT = 1
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @RunStatus NOT IN ('COMPLETED','COMPLETED_WITH_WARNINGS','FAILED_FILE_VALIDATION','FAILED_DATA_VALIDATION',
                          'FAILED_EXTRACTION','FAILED_DATABASE_LOAD','FAILED_RECONCILIATION','FAILED','CANCELLED')
        THROW 50002, 'usp_CompleteRun requires a final run status.', 1;

    IF @RowsReceived < 0 OR @RowsExtracted < 0 OR @RowsStaged < 0 OR @RowsValid < 0 OR
       @RowsInvalid < 0 OR @RowsExactDuplicate < 0 OR @RowsConflictingDuplicate < 0 OR
       @RowsAlreadyExist < 0 OR @RowsInserted < 0 OR @RowsUpdated < 0 OR @RowsDeleted < 0 OR
       @RowsUnchanged < 0 OR @RowsRejected < 0 OR @RowsSkipped < 0 OR @BatchCount < 0 OR @FileCount < 0
        THROW 50003, 'ETL completion counts cannot be negative.', 1;

    -- This general reconciliation is enforced at completion. Jobs whose load semantics differ
    -- should include the difference in RowsSkipped or finish with FAILED_RECONCILIATION.
    IF @ValidateReconciliation = 1 AND
       @RowsReceived <> @RowsInvalid + @RowsExactDuplicate + @RowsConflictingDuplicate +
                        @RowsInserted + @RowsUpdated + @RowsUnchanged + @RowsSkipped
        THROW 50004, 'RowsReceived does not reconcile to invalid, duplicate, loaded, unchanged, and skipped rows.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;
        IF NOT EXISTS (SELECT 1 FROM ETL.RunHistory WITH (UPDLOCK, HOLDLOCK) WHERE ETLRunID = @ETLRunID)
            THROW 50001, 'The supplied ETLRunID does not exist.', 1;

        UPDATE ETL.RunHistory
        SET RowsReceived = @RowsReceived, RowsExtracted = @RowsExtracted, RowsStaged = @RowsStaged,
            RowsValid = @RowsValid, RowsInvalid = @RowsInvalid,
            RowsExactDuplicate = @RowsExactDuplicate, RowsConflictingDuplicate = @RowsConflictingDuplicate,
            RowsAlreadyExist = @RowsAlreadyExist, RowsInserted = @RowsInserted,
            RowsUpdated = @RowsUpdated, RowsDeleted = @RowsDeleted, RowsUnchanged = @RowsUnchanged,
            RowsRejected = @RowsRejected, RowsSkipped = @RowsSkipped,
            BatchCount = @BatchCount, FileCount = @FileCount,
            RejectFilePath = @RejectFilePath, DuplicateFilePath = @DuplicateFilePath,
            LogFilePath = @LogFilePath, RunStatus = @RunStatus,
            ErrorCode = @ErrorCode, ErrorMessage = @ErrorMessage,
            CompletedAt = SYSUTCDATETIME()
        WHERE ETLRunID = @ETLRunID;
        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
