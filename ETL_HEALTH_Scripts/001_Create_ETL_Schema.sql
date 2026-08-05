/*
File: 001_Create_ETL_Schema.sql
Purpose: Creates the namespace for reusable ETL control objects.
Dependencies: None.
Compatibility: Microsoft SQL Server 2016 or later.
*/
-- USE [YourDatabaseName];
SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
SET NOCOUNT ON;
GO
IF SCHEMA_ID(N'ETL') IS NULL
    EXEC(N'CREATE SCHEMA [ETL] AUTHORIZATION [dbo];');
GO
