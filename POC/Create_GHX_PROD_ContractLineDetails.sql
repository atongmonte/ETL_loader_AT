/*
    Source: Phase1 - Contract Core - ContractLineCCX(Egnyte).xlsx
    Sheet:  Future Status
    Table:  GHX.ContractLineDetails

    Notes:
      - NUMERIC(19,4) from the workbook is expressed as DECIMAL(19,4), its
        SQL Server synonym.
      - Legacy DATETIME definitions are expressed as DATETIME2.
      - No primary/unique key is imposed because IsPrimaryKey is 0 for every
        workbook row. The workbook identifies a possible future business key:
        ContractID, TierLevel, ManufacturerPartNumber, UOM, OrganizationEID,
        and ERPVendorID.
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'GHX') IS NULL
    EXEC(N'CREATE SCHEMA [GHX] AUTHORIZATION [dbo];');
GO

IF OBJECT_ID(N'[GHX].[ContractLineDetails]', N'U') IS NULL
BEGIN
    CREATE TABLE [GHX].[ContractLineDetails]
    (
        [Organization]                     varchar(100) NOT NULL,
        [ContractID]                       varchar(100) NOT NULL,
        [ContractDescription]              varchar(500) NOT NULL,
        [TierLevel]                        varchar(40) NOT NULL,
        [TierDescription]                  varchar(500) NOT NULL,
        [ManufacturerName]                 varchar(255) NOT NULL,
        [ManufacturerPartNumber]           varchar(100) NOT NULL,
        [PartDescription]                  varchar(500) NOT NULL,
        [VendorName]                       varchar(255) NOT NULL,
        [VendorPartNumber]                 varchar(100) NOT NULL,
        [BuyerPartNumber]                  varchar(100) NULL,
        [Source]                           varchar(20) NOT NULL,
        [ContractTierStartDate]            date NOT NULL,
        [ContractTierEndDate]              date NOT NULL,
        [UOM]                              varchar(10) NOT NULL,
        [QOE]                              decimal(19,4) NOT NULL,
        [Price]                            decimal(19,4) NOT NULL,
        [AdjustmentPercent]                decimal(19,4) NOT NULL,
        [AdjustedPrice]                    decimal(19,4) NOT NULL,
        [ItemPriceStartDate]               datetime2(0) NOT NULL,
        [ItemPriceEndDate]                 datetime2(0) NOT NULL,
        [IsMyItem]                         varchar(5) NULL,
        [ContractOwner]                    varchar(100) NULL,
        [ContractType]                     varchar(100) NOT NULL,
        [ReplacementForContractNumber]     varchar(255) NULL,
        [ProductDescriptionOrCategory]     varchar(500) NULL,
        [AdministrativeFee]                decimal(19,4) NULL,
        [Notes]                            varchar(500) NULL,
        [ContractReviewDate]               date NULL,
        [ContractState]                    varchar(50) NULL,
        [PriceProtectedDate]               date NULL,
        [ProductUNSPSC]                    varchar(255) NULL,
        [GPOContractReference]             varchar(255) NULL,
        [LeasePaymentTerms]                varchar(255) NULL,
        [RebateFrequency]                  varchar(50) NULL,
        [RebateAmount]                     decimal(19,4) NULL,
        [DateRebateExpected]               date NULL,
        [MethodOfPayment]                  varchar(50) NULL,
        [OtherContractDetails]             varchar(500) NULL,
        [RebateBasis]                      varchar(50) NULL,
        [RequiresCommitmentTo]             varchar(50) NULL,
        [CommitmentMeasure]                varchar(255) NULL,
        [OtherKnownCompetitors]            varchar(255) NULL,
        [OrganizationEID]                  varchar(10) NOT NULL,
        [ContractOrgID]                    varchar(40) NOT NULL,
        [ERPVendorID]                      varchar(20) NULL,
        [IsSUOM]                           varchar(1) NOT NULL,
        [SourceContractType]               varchar(40) NOT NULL,
        [ManufacturerEID]                  varchar(20) NULL,
        [VendorEID]                        varchar(20) NULL,
        [LastUpdate]                       datetime2(3) NOT NULL
            CONSTRAINT [DF_ContractLineDetails_LastUpdate]
            DEFAULT (SYSUTCDATETIME())
    );
END
ELSE
BEGIN
    PRINT 'Table [GHX].[ContractLineDetails] already exists; no changes made.';
END;
GO
