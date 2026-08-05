/*
File: 005_Create_ETL_DuplicateRecord.sql
Purpose: Records source/target duplicates and their resolution.
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
IF OBJECT_ID(N'ETL.DuplicateRecord', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.DuplicateRecord
    (
        DuplicateID BIGINT IDENTITY(1,1) NOT NULL,
        ETLRunID BIGINT NOT NULL,
        ETLFileID BIGINT NULL,
        JobName NVARCHAR(255) NOT NULL,
        SourceSystem NVARCHAR(255) NULL,
        SourceObject NVARCHAR(1000) NULL,
        SourceFileName NVARCHAR(500) NULL,
        SourceRowNumber BIGINT NULL,
        MatchingSourceRowNumber BIGINT NULL,
        SourceRecordID NVARCHAR(1000) NULL,
        BusinessKey NVARCHAR(2000) NULL, -- Human-readable key or compound key.
        BusinessKeyHash VARBINARY(32) NULL, -- Optional SHA-256 key digest.
        DuplicateType VARCHAR(50) NOT NULL,
        MatchingTargetKey NVARCHAR(2000) NULL,
        ComparisonHash VARBINARY(32) NULL,
        ResolutionAction VARCHAR(40) NOT NULL,
        WinnerSourceRowNumber BIGINT NULL,
        WinnerSourceRecordID NVARCHAR(1000) NULL,
        OriginalRowJSON NVARCHAR(MAX) NULL,
        MatchingRowJSON NVARCHAR(MAX) NULL,
        ResolutionComment NVARCHAR(MAX) NULL,
        LoggedAt DATETIME2(0) NOT NULL CONSTRAINT DF_DuplicateRecord_LoggedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_DuplicateRecord PRIMARY KEY CLUSTERED (DuplicateID),
        CONSTRAINT FK_DuplicateRecord_RunHistory FOREIGN KEY (ETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT FK_DuplicateRecord_FileHistory FOREIGN KEY (ETLFileID) REFERENCES ETL.FileHistory (ETLFileID),
        CONSTRAINT CK_DuplicateRecord_Type CHECK (DuplicateType IN ('EXACT_DUPLICATE_IN_SOURCE','CONFLICTING_DUPLICATE_IN_SOURCE','DUPLICATE_IN_TARGET','UNCHANGED_EXISTING_ROW','MULTIPLE_SOURCE_ROWS_MATCH_TARGET')),
        CONSTRAINT CK_DuplicateRecord_Action CHECK (ResolutionAction IN ('KEPT_FIRST','KEPT_LAST','KEPT_LATEST','KEPT_HIGHEST_SEQUENCE','SKIPPED','UPDATED_TARGET','UNCHANGED','QUARANTINED','FAILED_FILE','MANUAL_REVIEW')),
        CONSTRAINT CK_DuplicateRecord_RowNumbers CHECK ((SourceRowNumber IS NULL OR SourceRowNumber > 0) AND (MatchingSourceRowNumber IS NULL OR MatchingSourceRowNumber > 0) AND (WinnerSourceRowNumber IS NULL OR WinnerSourceRowNumber > 0))
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_ETLRunID')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_ETLRunID ON ETL.DuplicateRecord (ETLRunID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_ETLFileID')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_ETLFileID ON ETL.DuplicateRecord (ETLFileID) WHERE ETLFileID IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_JobName_LoggedAt')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_JobName_LoggedAt ON ETL.DuplicateRecord (JobName, LoggedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_Type_LoggedAt')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_Type_LoggedAt ON ETL.DuplicateRecord (DuplicateType, LoggedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_BusinessKeyHash')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_BusinessKeyHash ON ETL.DuplicateRecord (BusinessKeyHash) WHERE BusinessKeyHash IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.DuplicateRecord') AND name = N'IX_DuplicateRecord_ResolutionAction')
    CREATE NONCLUSTERED INDEX IX_DuplicateRecord_ResolutionAction ON ETL.DuplicateRecord (ResolutionAction);
GO
