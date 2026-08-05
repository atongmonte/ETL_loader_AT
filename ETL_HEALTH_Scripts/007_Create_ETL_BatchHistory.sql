/*
File: 007_Create_ETL_BatchHistory.sql
Purpose: Tracks load progress and outcomes by batch.
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
IF OBJECT_ID(N'ETL.BatchHistory', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.BatchHistory
    (
        ETLBatchID BIGINT IDENTITY(1,1) NOT NULL,
        ETLRunID BIGINT NOT NULL,
        BatchNumber INT NOT NULL,
        SourceStartRow BIGINT NULL,
        SourceEndRow BIGINT NULL,
        RowsReceived BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsReceived DEFAULT (0),
        RowsValid BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsValid DEFAULT (0),
        RowsRejected BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsRejected DEFAULT (0),
        RowsInserted BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsInserted DEFAULT (0),
        RowsUpdated BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsUpdated DEFAULT (0),
        RowsUnchanged BIGINT NOT NULL CONSTRAINT DF_BatchHistory_RowsUnchanged DEFAULT (0),
        BatchStatus VARCHAR(20) NOT NULL CONSTRAINT DF_BatchHistory_Status DEFAULT ('STARTED'),
        StartedAt DATETIME2(0) NOT NULL CONSTRAINT DF_BatchHistory_StartedAt DEFAULT (SYSUTCDATETIME()),
        CompletedAt DATETIME2(0) NULL,
        ErrorCode NVARCHAR(100) NULL,
        ErrorMessage NVARCHAR(MAX) NULL,
        CreatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_BatchHistory_CreatedAt DEFAULT (SYSUTCDATETIME()),
        CONSTRAINT PK_BatchHistory PRIMARY KEY CLUSTERED (ETLBatchID),
        CONSTRAINT UQ_BatchHistory_RunBatch UNIQUE (ETLRunID, BatchNumber),
        CONSTRAINT FK_BatchHistory_RunHistory FOREIGN KEY (ETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT CK_BatchHistory_Status CHECK (BatchStatus IN ('STARTED','VALIDATED','LOADED','COMPLETED','FAILED','ROLLED_BACK')),
        CONSTRAINT CK_BatchHistory_Nonnegative CHECK (BatchNumber > 0 AND RowsReceived >= 0 AND RowsValid >= 0 AND RowsRejected >= 0 AND RowsInserted >= 0 AND RowsUpdated >= 0 AND RowsUnchanged >= 0 AND (SourceStartRow IS NULL OR SourceStartRow > 0) AND (SourceEndRow IS NULL OR SourceEndRow >= SourceStartRow)),
        CONSTRAINT CK_BatchHistory_CompletionOrder CHECK (CompletedAt IS NULL OR CompletedAt >= StartedAt)
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.BatchHistory') AND name = N'IX_BatchHistory_ETLRunID')
    CREATE NONCLUSTERED INDEX IX_BatchHistory_ETLRunID ON ETL.BatchHistory (ETLRunID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.BatchHistory') AND name = N'IX_BatchHistory_Status_StartedAt')
    CREATE NONCLUSTERED INDEX IX_BatchHistory_Status_StartedAt ON ETL.BatchHistory (BatchStatus, StartedAt);
GO
