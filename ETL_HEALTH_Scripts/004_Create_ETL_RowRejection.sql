/*
File: 004_Create_ETL_RowRejection.sql
Purpose: Creates centralized row/cell quarantine storage.
Dependencies: 001, 002, 003.
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
IF OBJECT_ID(N'ETL.RowRejection', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.RowRejection
    (
        RejectionID BIGINT IDENTITY(1,1) NOT NULL,
        ETLRunID BIGINT NOT NULL,
        ETLFileID BIGINT NULL,
        JobName NVARCHAR(255) NOT NULL,
        SourceSystem NVARCHAR(255) NULL,
        SourceObject NVARCHAR(1000) NULL,
        SourceFileName NVARCHAR(500) NULL,
        SourceRowNumber BIGINT NULL, -- Null for sources without physical row numbers.
        SourceRecordID NVARCHAR(1000) NULL, -- Simple, compound, or extraction identifier.
        TargetSchema SYSNAME NULL,
        TargetTable SYSNAME NULL,
        ColumnName SYSNAME NULL, -- Null denotes a row-level rejection.
        OriginalValue NVARCHAR(MAX) NULL,
        ExpectedDataType NVARCHAR(128) NULL,
        MaximumLength INT NULL,
        ExpectedPrecision TINYINT NULL,
        ExpectedScale TINYINT NULL,
        ErrorCategory VARCHAR(30) NOT NULL,
        ErrorCode NVARCHAR(100) NOT NULL,
        ErrorMessage NVARCHAR(MAX) NOT NULL,
        OriginalRowJSON NVARCHAR(MAX) NULL,
        QuarantineStatus VARCHAR(20) NOT NULL CONSTRAINT DF_RowRejection_Status DEFAULT ('PENDING'),
        ResolutionComment NVARCHAR(MAX) NULL,
        ResolvedBy NVARCHAR(255) NULL,
        ResolvedAt DATETIME2(0) NULL,
        ReprocessedETLRunID BIGINT NULL,
        RejectedAt DATETIME2(0) NOT NULL CONSTRAINT DF_RowRejection_RejectedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_RowRejection PRIMARY KEY CLUSTERED (RejectionID),
        CONSTRAINT FK_RowRejection_RunHistory FOREIGN KEY (ETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT FK_RowRejection_FileHistory FOREIGN KEY (ETLFileID) REFERENCES ETL.FileHistory (ETLFileID),
        CONSTRAINT FK_RowRejection_ReprocessedRun FOREIGN KEY (ReprocessedETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT CK_RowRejection_Category CHECK (ErrorCategory IN ('FILE_STRUCTURE','REQUIRED_VALUE','DATATYPE','LENGTH','PRECISION_SCALE','REFERENCE','BUSINESS_RULE','SCHEMA_CHANGE','OTHER')),
        CONSTRAINT CK_RowRejection_Status CHECK (QuarantineStatus IN ('PENDING','REVIEWED','CORRECTED','REPROCESSED','IGNORED','CLOSED')),
        CONSTRAINT CK_RowRejection_SourceRow CHECK (SourceRowNumber IS NULL OR SourceRowNumber > 0),
        CONSTRAINT CK_RowRejection_Metadata CHECK ((MaximumLength IS NULL OR MaximumLength >= 0) AND (ExpectedPrecision IS NULL OR ExpectedPrecision BETWEEN 1 AND 38) AND (ExpectedScale IS NULL OR ExpectedScale <= 38))
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_ETLRunID')
    CREATE NONCLUSTERED INDEX IX_RowRejection_ETLRunID ON ETL.RowRejection (ETLRunID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_ETLFileID')
    CREATE NONCLUSTERED INDEX IX_RowRejection_ETLFileID ON ETL.RowRejection (ETLFileID) WHERE ETLFileID IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_JobName_RejectedAt')
    CREATE NONCLUSTERED INDEX IX_RowRejection_JobName_RejectedAt ON ETL.RowRejection (JobName, RejectedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_ErrorCode_RejectedAt')
    CREATE NONCLUSTERED INDEX IX_RowRejection_ErrorCode_RejectedAt ON ETL.RowRejection (ErrorCode, RejectedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_Status_RejectedAt')
    CREATE NONCLUSTERED INDEX IX_RowRejection_Status_RejectedAt ON ETL.RowRejection (QuarantineStatus, RejectedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.RowRejection') AND name = N'IX_RowRejection_Target')
    CREATE NONCLUSTERED INDEX IX_RowRejection_Target ON ETL.RowRejection (TargetSchema, TargetTable);
GO
