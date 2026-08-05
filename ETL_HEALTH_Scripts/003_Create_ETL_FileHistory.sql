/*
File: 003_Create_ETL_FileHistory.sql
Purpose: Tracks source-file discovery, validation, loading, and archival.
Dependencies: 001, 002.
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
IF OBJECT_ID(N'ETL.FileHistory', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.FileHistory
    (
        ETLFileID BIGINT IDENTITY(1,1) NOT NULL,
        ETLRunID BIGINT NOT NULL,
        SourceSystem NVARCHAR(255) NOT NULL,
        SourceFileName NVARCHAR(500) NOT NULL,
        SourceFilePath NVARCHAR(2000) NULL,
        ArchiveFilePath NVARCHAR(2000) NULL,
        FailedFilePath NVARCHAR(2000) NULL,
        FileExtension NVARCHAR(20) NULL,
        FileSizeBytes BIGINT NULL,
        FileHashAlgorithm VARCHAR(20) NOT NULL CONSTRAINT DF_FileHistory_HashAlgorithm DEFAULT ('SHA256'),
        FileHash CHAR(64) NULL, -- Lower- or upper-case hexadecimal SHA-256 digest.
        FileCreatedAt DATETIME2(0) NULL,
        FileModifiedAt DATETIME2(0) NULL,
        FileDetectedAt DATETIME2(0) NOT NULL CONSTRAINT DF_FileHistory_DetectedAt DEFAULT (SYSUTCDATETIME()),
        ProcessingStartedAt DATETIME2(0) NULL,
        ProcessingCompletedAt DATETIME2(0) NULL,
        FileStatus VARCHAR(20) NOT NULL CONSTRAINT DF_FileHistory_Status DEFAULT ('DETECTED'),
        ExpectedColumnCount INT NULL,
        ActualColumnCount INT NULL,
        ExpectedRowCount BIGINT NULL,
        ActualRowCount BIGINT NULL,
        HeaderRow INT NULL,
        Delimiter NVARCHAR(20) NULL,
        TextQualifier NVARCHAR(20) NULL,
        Encoding NVARCHAR(100) NULL,
        HasHeader BIT NOT NULL CONSTRAINT DF_FileHistory_HasHeader DEFAULT (1),
        ErrorCode NVARCHAR(100) NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_FileHistory_CreatedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_FileHistory PRIMARY KEY CLUSTERED (ETLFileID),
        CONSTRAINT FK_FileHistory_RunHistory FOREIGN KEY (ETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT CK_FileHistory_Status CHECK (FileStatus IN ('DETECTED','READY','PROCESSING','VALIDATED','LOADED','ARCHIVED','DUPLICATE','REJECTED','FAILED')),
        CONSTRAINT CK_FileHistory_Hash CHECK (FileHash IS NULL OR (FileHashAlgorithm = 'SHA256' AND FileHash NOT LIKE '%[^0-9A-Fa-f]%' AND LEN(FileHash) = 64)),
        CONSTRAINT CK_FileHistory_Nonnegative CHECK ((FileSizeBytes IS NULL OR FileSizeBytes >= 0) AND (ExpectedColumnCount IS NULL OR ExpectedColumnCount >= 0) AND (ActualColumnCount IS NULL OR ActualColumnCount >= 0) AND (ExpectedRowCount IS NULL OR ExpectedRowCount >= 0) AND (ActualRowCount IS NULL OR ActualRowCount >= 0) AND (HeaderRow IS NULL OR HeaderRow > 0))
    );
END;
GO
-- Supports the locked lookup in usp_RegisterFile. It is intentionally non-unique so failed files can be retried.
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.FileHistory') AND name = N'IX_FileHistory_SourceSystem_FileHash')
    CREATE NONCLUSTERED INDEX IX_FileHistory_SourceSystem_FileHash ON ETL.FileHistory (SourceSystem, FileHash) INCLUDE (ETLFileID, FileStatus) WHERE FileHash IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.FileHistory') AND name = N'IX_FileHistory_ETLRunID')
    CREATE NONCLUSTERED INDEX IX_FileHistory_ETLRunID ON ETL.FileHistory (ETLRunID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.FileHistory') AND name = N'IX_FileHistory_SourceSystem_FileName')
    CREATE NONCLUSTERED INDEX IX_FileHistory_SourceSystem_FileName ON ETL.FileHistory (SourceSystem, SourceFileName);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.FileHistory') AND name = N'IX_FileHistory_Status_DetectedAt')
    CREATE NONCLUSTERED INDEX IX_FileHistory_Status_DetectedAt ON ETL.FileHistory (FileStatus, FileDetectedAt);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.FileHistory') AND name = N'IX_FileHistory_FileHash')
    CREATE NONCLUSTERED INDEX IX_FileHistory_FileHash ON ETL.FileHistory (FileHash) WHERE FileHash IS NOT NULL;
GO
