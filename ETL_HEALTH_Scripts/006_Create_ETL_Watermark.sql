/*
File: 006_Create_ETL_Watermark.sql
Purpose: Stores committed and pending incremental extraction boundaries.
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
IF OBJECT_ID(N'ETL.Watermark', N'U') IS NULL
BEGIN
    CREATE TABLE ETL.Watermark
    (
        WatermarkID BIGINT IDENTITY(1,1) NOT NULL,
        JobName NVARCHAR(255) NOT NULL,
        SourceSystem NVARCHAR(255) NOT NULL,
        SourceObject NVARCHAR(300) NOT NULL, -- Sized so the required unique key is valid on SQL Server 2016.
        WatermarkColumn NVARCHAR(255) NOT NULL,
        TieBreakerColumn NVARCHAR(255) NULL,
        LastSuccessfulWatermark NVARCHAR(500) NULL,
        LastSuccessfulTieBreaker NVARCHAR(500) NULL,
        PendingWatermark NVARCHAR(500) NULL, -- Upper bound captured before extraction.
        PendingTieBreaker NVARCHAR(500) NULL,
        LastSuccessfulETLRunID BIGINT NULL,
        PendingETLRunID BIGINT NULL,
        WatermarkDataType NVARCHAR(128) NOT NULL,
        UpdatedAt DATETIME2(0) NOT NULL CONSTRAINT DF_Watermark_UpdatedAt DEFAULT (SYSUTCDATETIME()),
        UpdatedBy NVARCHAR(255) NULL,
        CONSTRAINT PK_Watermark PRIMARY KEY CLUSTERED (WatermarkID),
        CONSTRAINT UQ_Watermark_JobSource UNIQUE (JobName, SourceSystem, SourceObject),
        CONSTRAINT FK_Watermark_LastSuccessfulRun FOREIGN KEY (LastSuccessfulETLRunID) REFERENCES ETL.RunHistory (ETLRunID),
        CONSTRAINT FK_Watermark_PendingRun FOREIGN KEY (PendingETLRunID) REFERENCES ETL.RunHistory (ETLRunID)
    );
END;
GO
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_JobName')
    CREATE NONCLUSTERED INDEX IX_Watermark_JobName ON ETL.Watermark (JobName);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_Source')
    CREATE NONCLUSTERED INDEX IX_Watermark_Source ON ETL.Watermark (SourceSystem, SourceObject);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_LastSuccessfulRun')
    CREATE NONCLUSTERED INDEX IX_Watermark_LastSuccessfulRun ON ETL.Watermark (LastSuccessfulETLRunID) WHERE LastSuccessfulETLRunID IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_PendingRun')
    CREATE NONCLUSTERED INDEX IX_Watermark_PendingRun ON ETL.Watermark (PendingETLRunID) WHERE PendingETLRunID IS NOT NULL;
GO
