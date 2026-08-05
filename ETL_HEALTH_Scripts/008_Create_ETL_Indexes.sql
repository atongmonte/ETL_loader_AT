/*
File: 008_Create_ETL_Indexes.sql
Purpose: Creates all supporting ETL indexes when they do not already exist.
Dependencies: 001 through 007.
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

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_JobName')
    CREATE NONCLUSTERED INDEX IX_Watermark_JobName ON ETL.Watermark (JobName);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_Source')
    CREATE NONCLUSTERED INDEX IX_Watermark_Source ON ETL.Watermark (SourceSystem, SourceObject);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_LastSuccessfulRun')
    CREATE NONCLUSTERED INDEX IX_Watermark_LastSuccessfulRun ON ETL.Watermark (LastSuccessfulETLRunID) WHERE LastSuccessfulETLRunID IS NOT NULL;
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.Watermark') AND name = N'IX_Watermark_PendingRun')
    CREATE NONCLUSTERED INDEX IX_Watermark_PendingRun ON ETL.Watermark (PendingETLRunID) WHERE PendingETLRunID IS NOT NULL;
GO

IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.BatchHistory') AND name = N'IX_BatchHistory_ETLRunID')
    CREATE NONCLUSTERED INDEX IX_BatchHistory_ETLRunID ON ETL.BatchHistory (ETLRunID);
IF NOT EXISTS (SELECT 1 FROM sys.indexes WHERE object_id = OBJECT_ID(N'ETL.BatchHistory') AND name = N'IX_BatchHistory_Status_StartedAt')
    CREATE NONCLUSTERED INDEX IX_BatchHistory_Status_StartedAt ON ETL.BatchHistory (BatchStatus, StartedAt);
GO
