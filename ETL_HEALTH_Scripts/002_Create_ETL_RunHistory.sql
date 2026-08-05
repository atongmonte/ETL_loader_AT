/*
File: 002_Create_ETL_RunHistory.sql
Purpose: Creates the central ETL execution audit table.
Dependencies: 001_Create_ETL_Schema.sql.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET ANSI_PADDING ON;
SET ANSI_WARNINGS ON;
SET ARITHABORT ON;
SET CONCAT_NULL_YIELDS_NULL ON;
SET QUOTED_IDENTIFIER ON;
SET NUMERIC_ROUNDABORT OFF;
SET NOCOUNT ON;
GO
IF OBJECT_ID(N'ETL.RunHistory', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.RunHistory
    (
        ETLRunID BIGINT IDENTITY(1,1) NOT NULL,
        ParentETLRunID BIGINT NULL, -- Parent orchestration run, when applicable.
        JobName NVARCHAR(255) NOT NULL,
        JobDescription NVARCHAR(1000) NULL,
        SourceType VARCHAR(20) NOT NULL,
        SourceSystem NVARCHAR(255) NULL,
        SourceObject NVARCHAR(500) NULL,
        TargetServer NVARCHAR(255) NULL,
        TargetDatabase SYSNAME NOT NULL,
        TargetSchema SYSNAME NOT NULL,
        TargetTable SYSNAME NOT NULL,
        LoadType VARCHAR(30) NULL,
        StartedAt DATETIME2(0) NOT NULL CONSTRAINT DF_RunHistory_StartedAt DEFAULT (SYSUTCDATETIME()),
        CompletedAt DATETIME2(0) NULL,
        RunStatus VARCHAR(40) NOT NULL CONSTRAINT DF_RunHistory_RunStatus DEFAULT ('STARTED'),
        RowsReceived BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsReceived DEFAULT (0),
        RowsExtracted BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsExtracted DEFAULT (0),
        RowsStaged BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsStaged DEFAULT (0),
        RowsValid BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsValid DEFAULT (0),
        RowsInvalid BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsInvalid DEFAULT (0),
        RowsExactDuplicate BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsExactDuplicate DEFAULT (0),
        RowsConflictingDuplicate BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsConflictingDuplicate DEFAULT (0),
        RowsAlreadyExist BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsAlreadyExist DEFAULT (0),
        RowsInserted BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsInserted DEFAULT (0),
        RowsUpdated BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsUpdated DEFAULT (0),
        RowsDeleted BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsDeleted DEFAULT (0),
        RowsUnchanged BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsUnchanged DEFAULT (0),
        RowsRejected BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsRejected DEFAULT (0),
        RowsSkipped BIGINT NOT NULL CONSTRAINT DF_RunHistory_RowsSkipped DEFAULT (0),
        BatchCount BIGINT NOT NULL CONSTRAINT DF_RunHistory_BatchCount DEFAULT (0),
        FileCount BIGINT NOT NULL CONSTRAINT DF_RunHistory_FileCount DEFAULT (0),
        RejectFilePath NVARCHAR(2000) NULL,
        DuplicateFilePath NVARCHAR(2000) NULL,
        LogFilePath NVARCHAR(2000) NULL,
        ErrorCode NVARCHAR(100) NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        CreatedBy NVARCHAR(255) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_RunHistory_CreatedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RunHistory PRIMARY KEY CLUSTERED (ETLRunID),
        CONSTRAINT FK_RunHistory_ParentETLRunID FOREIGN KEY (ParentETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT CK_RunHistory_SourceType CHECK (SourceType IN ('CSV','SNOWFLAKE','DATABASE','API','EXCEL','OTHER')),
        CONSTRAINT CK_RunHistory_LoadType CHECK (LoadType IS NULL OR LoadType IN ('INSERT_ONLY','UPSERT','FULL_REFRESH','SNAPSHOT','INCREMENTAL','DELETE_INSERT')),
        CONSTRAINT CK_RunHistory_RunStatus CHECK (RunStatus IN ('STARTED','EXTRACTING','STAGING','VALIDATING','LOADING','COMPLETED','COMPLETED_WITH_WARNINGS','FAILED_FILE_VALIDATION','FAILED_DATA_VALIDATION','FAILED_EXTRACTION','FAILED_DATABASE_LOAD','FAILED_RECONCILIATION','FAILED','CANCELLED')),
        CONSTRAINT CK_RunHistory_NonnegativeCounts CHECK (RowsReceived >= 0 AND RowsExtracted >= 0 AND RowsStaged >= 0 AND RowsValid >= 0 AND RowsInvalid >= 0 AND RowsExactDuplicate >= 0 AND RowsConflictingDuplicate >= 0 AND RowsAlreadyExist >= 0 AND RowsInserted >= 0 AND RowsUpdated >= 0 AND RowsDeleted >= 0 AND RowsUnchanged >= 0 AND RowsRejected >= 0 AND RowsSkipped >= 0 AND BatchCount >= 0 AND FileCount >= 0),
        CONSTRAINT CK_RunHistory_CompletionOrder CHECK (CompletedAt IS NULL OR CompletedAt >= StartedAt)
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RunHistory') AND name = N'IX_RunHistory_JobName_StartedAt')
    CREATE NONCLUSTERED INDEX IX_RunHistory_JobName_StartedAt ON ETL.RunHistory (JobName, StartedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RunHistory') AND name = N'IX_RunHistory_RunStatus_StartedAt')
    CREATE NONCLUSTERED INDEX IX_RunHistory_RunStatus_StartedAt ON ETL.RunHistory (RunStatus, StartedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RunHistory') AND name = N'IX_RunHistory_Target')
    CREATE NONCLUSTERED INDEX IX_RunHistory_Target ON ETL.RunHistory (TargetDatabase, TargetSchema, TargetTable);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RunHistory') AND name = N'IX_RunHistory_Source')
    CREATE NONCLUSTERED INDEX IX_RunHistory_Source ON ETL.RunHistory (SourceSystem, SourceObject);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RunHistory') AND name = N'IX_RunHistory_ParentETLRunID')
    CREATE NONCLUSTERED INDEX IX_RunHistory_ParentETLRunID ON ETL.RunHistory (ParentETLRunID) WHERE ParentETLRunID IS NOT NULL;
GO
