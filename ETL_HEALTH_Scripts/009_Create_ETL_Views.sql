/*
File: 009_Create_ETL_Views.sql
Purpose: Creates reporting and operational views over the ETL control tables.
Dependencies: 001 through 008.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

IF OBJECT_ID(N'ETL.vw_LatestJobRun', N'V') IS NULL
    EXEC(N'CREATE VIEW ETL.vw_LatestJobRun AS SELECT 1 AS Placeholder;');
GO
ALTER VIEW ETL.vw_LatestJobRun
AS
WITH RankedRuns AS
(
    SELECT RH.*,
           ROW_NUMBER() OVER (PARTITION BY RH.JobName ORDER BY RH.StartedAt DESC, RH.ETLRunID DESC) AS RunRank
    FROM ETL.RunHistory AS RH
)
SELECT ETLRunID, ParentETLRunID, JobName, JobDescription, SourceType, SourceSystem, SourceObject,
       TargetServer, TargetDatabase, TargetSchema, TargetTable, LoadType, StartedAt, CompletedAt,
       RunStatus, RowsReceived, RowsExtracted, RowsStaged, RowsValid, RowsInvalid,
       RowsExactDuplicate, RowsConflictingDuplicate, RowsAlreadyExist, RowsInserted, RowsUpdated,
       RowsDeleted, RowsUnchanged, RowsRejected, RowsSkipped, BatchCount, FileCount,
       RejectFilePath, DuplicateFilePath, LogFilePath, ErrorCode, ErrorMessage, CreatedBy, CreatedAt
FROM RankedRuns
WHERE RunRank = 1;
GO

IF OBJECT_ID(N'ETL.vw_FailedRuns', N'V') IS NULL
    EXEC(N'CREATE VIEW ETL.vw_FailedRuns AS SELECT 1 AS Placeholder;');
GO
ALTER VIEW ETL.vw_FailedRuns
AS
SELECT ETLRunID, JobName, SourceType, SourceSystem, SourceObject, TargetDatabase, TargetSchema,
       TargetTable, StartedAt, CompletedAt, RunStatus, ErrorCode, ErrorMessage, RowsReceived,
       RowsRejected, RowsExactDuplicate + RowsConflictingDuplicate AS RowsDuplicate
FROM ETL.RunHistory
WHERE RunStatus IN ('FAILED_FILE_VALIDATION','FAILED_DATA_VALIDATION','FAILED_EXTRACTION',
                    'FAILED_DATABASE_LOAD','FAILED_RECONCILIATION','FAILED');
GO

IF OBJECT_ID(N'ETL.vw_RunSummary', N'V') IS NULL
    EXEC(N'CREATE VIEW ETL.vw_RunSummary AS SELECT 1 AS Placeholder;');
GO
ALTER VIEW ETL.vw_RunSummary
AS
WITH FileTotals AS
(
    SELECT ETLRunID, COUNT_BIG(*) AS FileCount
    FROM ETL.FileHistory
    GROUP BY ETLRunID
),
BatchTotals AS
(
    SELECT ETLRunID, COUNT_BIG(*) AS BatchCount
    FROM ETL.BatchHistory
    GROUP BY ETLRunID
),
RejectionTotals AS
(
    SELECT ETLRunID, COUNT_BIG(*) AS RejectionCount
    FROM ETL.RowRejection
    GROUP BY ETLRunID
),
DuplicateTotals AS
(
    SELECT ETLRunID, COUNT_BIG(*) AS DuplicateCount
    FROM ETL.DuplicateRecord
    GROUP BY ETLRunID
)
SELECT RH.ETLRunID, RH.ParentETLRunID, RH.JobName, RH.SourceType, RH.SourceSystem, RH.SourceObject,
       RH.TargetServer, RH.TargetDatabase, RH.TargetSchema, RH.TargetTable, RH.LoadType,
       RH.StartedAt, RH.CompletedAt, RH.RunStatus,
       RH.RowsReceived, RH.RowsExtracted, RH.RowsStaged, RH.RowsValid, RH.RowsInvalid,
       RH.RowsExactDuplicate, RH.RowsConflictingDuplicate, RH.RowsAlreadyExist,
       RH.RowsInserted, RH.RowsUpdated, RH.RowsDeleted, RH.RowsUnchanged, RH.RowsRejected, RH.RowsSkipped,
       ISNULL(FT.FileCount, 0) AS FileCount,
       ISNULL(BT.BatchCount, 0) AS BatchCount,
       ISNULL(RT.RejectionCount, 0) AS RejectionCount,
       ISNULL(DT.DuplicateCount, 0) AS DuplicateCount,
       CASE WHEN RH.CompletedAt IS NULL THEN NULL ELSE DATEDIFF(SECOND, RH.StartedAt, RH.CompletedAt) END AS DurationSeconds,
       CAST(CASE WHEN RH.RowsReceived = 0 THEN 0 ELSE 100.0 * RH.RowsRejected / NULLIF(RH.RowsReceived, 0) END AS DECIMAL(9,4)) AS RejectionPercentage,
       CAST(CASE WHEN RH.RowsReceived = 0 THEN 0 ELSE 100.0 * (RH.RowsInserted + RH.RowsUpdated) / NULLIF(RH.RowsReceived, 0) END AS DECIMAL(9,4)) AS LoadPercentage,
       RH.ErrorCode, RH.ErrorMessage
FROM ETL.RunHistory AS RH
LEFT JOIN FileTotals AS FT ON FT.ETLRunID = RH.ETLRunID
LEFT JOIN BatchTotals AS BT ON BT.ETLRunID = RH.ETLRunID
LEFT JOIN RejectionTotals AS RT ON RT.ETLRunID = RH.ETLRunID
LEFT JOIN DuplicateTotals AS DT ON DT.ETLRunID = RH.ETLRunID;
GO

IF OBJECT_ID(N'ETL.vw_PendingQuarantine', N'V') IS NULL
    EXEC(N'CREATE VIEW ETL.vw_PendingQuarantine AS SELECT 1 AS Placeholder;');
GO
ALTER VIEW ETL.vw_PendingQuarantine
AS
SELECT RejectionID, ETLRunID, ETLFileID, JobName, SourceSystem, SourceObject, SourceFileName,
       SourceRowNumber, SourceRecordID, TargetSchema, TargetTable, ColumnName, OriginalValue,
       ExpectedDataType, MaximumLength, ExpectedPrecision, ExpectedScale, ErrorCategory,
       ErrorCode, ErrorMessage, OriginalRowJSON, QuarantineStatus, ResolutionComment,
       ResolvedBy, ResolvedAt, ReprocessedETLRunID, RejectedAt
FROM ETL.RowRejection
WHERE QuarantineStatus IN ('PENDING','REVIEWED');
GO

IF OBJECT_ID(N'ETL.vw_DuplicateSummary', N'V') IS NULL
    EXEC(N'CREATE VIEW ETL.vw_DuplicateSummary AS SELECT 1 AS Placeholder;');
GO
ALTER VIEW ETL.vw_DuplicateSummary
AS
SELECT ETLRunID, JobName, DuplicateType, ResolutionAction, COUNT_BIG(*) AS DuplicateCount
FROM ETL.DuplicateRecord
GROUP BY ETLRunID, JobName, DuplicateType, ResolutionAction;
GO
