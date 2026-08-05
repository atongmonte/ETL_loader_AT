/*
File: 011_Create_ETL_SeedData.sql
Purpose: Documents the intentional absence of lookup seed data in the constraint-based design.
Dependencies: 001 through 010.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO

/*
Statuses and types are stable framework rules implemented by named CHECK constraints.
No lookup tables are created, avoiding duplicate enforcement in both constraints and seed data.
ErrorCode remains intentionally open-ended. Suggested starting codes are:
MISSING_REQUIRED_COLUMN, UNEXPECTED_COLUMN, DUPLICATE_HEADER, REQUIRED_VALUE_MISSING,
INVALID_INTEGER, INVALID_DECIMAL, INVALID_DATE, INVALID_DATETIME, INVALID_BOOLEAN,
LENGTH_EXCEEDED, PRECISION_EXCEEDED, SCALE_EXCEEDED, INVALID_REFERENCE,
INVALID_ENUM_VALUE, BUSINESS_RULE_FAILURE, and SCHEMA_MISMATCH.

This script is therefore an intentional, rerunnable no-op.
*/
GO
