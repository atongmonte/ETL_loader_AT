/*
    Source: Phase1 - Contract Core - ContractLineCCX(Egnyte).xlsx
    Sheet:  Future Status
    Table:  GHX._STG_ContractLineDetails

    Staging columns intentionally retain the workbook's VARCHAR(500) contract.
    LastUpdate uses DATETIME2(3) and defaults to the UTC load timestamp because
    the workbook does not identify a source-file column for it.
*/

SET ANSI_NULLS ON;
SET QUOTED_IDENTIFIER ON;
GO

IF SCHEMA_ID(N'GHX') IS NULL
    EXEC(N'CREATE SCHEMA [GHX] AUTHORIZATION [dbo];');
GO

IF OBJECT_ID(N'[GHX].[_STG_ContractLineDetails]', N'U') IS NULL
BEGIN
    CREATE TABLE [GHX].[_STG_ContractLineDetails]
    (
        [Organization]                     varchar(500) NOT NULL,
        [ContractID]                       varchar(500) NOT NULL,
        [ContractDescription]              varchar(500) NOT NULL,
        [TierLevel]                        varchar(500) NOT NULL,
        [TierDescription]                  varchar(500) NOT NULL,
        [ManufacturerName]                 varchar(500) NOT NULL,
        [ManufacturerPartNumber]           varchar(500) NOT NULL,
        [PartDescription]                  varchar(500) NOT NULL,
        [VendorName]                       varchar(500) NOT NULL,
        [VendorPartNumber]                 varchar(500) NOT NULL,
        [BuyerPartNumber]                  varchar(500) NULL,
        [Source]                           varchar(500) NOT NULL,
        [ContractTierStartDate]            varchar(500) NOT NULL,
        [ContractTierEndDate]              varchar(500) NOT NULL,
        [UOM]                              varchar(500) NOT NULL,
        [QOE]                              varchar(500) NOT NULL,
        [Price]                            varchar(500) NOT NULL,
        [AdjustmentPercent]                varchar(500) NOT NULL,
        [AdjustedPrice]                    varchar(500) NOT NULL,
        [ItemPriceStartDate]               varchar(500) NOT NULL,
        [ItemPriceEndDate]                 varchar(500) NOT NULL,
        [IsMyItem]                         varchar(500) NULL,
        [ContractOwner]                    varchar(500) NULL,
        [ContractType]                     varchar(500) NOT NULL,
        [ReplacementForContractNumber]     varchar(500) NULL,
        [ProductDescriptionOrCategory]     varchar(500) NULL,
        [AdministrativeFee]                varchar(500) NULL,
        [Notes]                            varchar(500) NULL,
        [ContractReviewDate]               varchar(500) NULL,
        [ContractState]                    varchar(500) NULL,
        [PriceProtectedDate]               varchar(500) NULL,
        [ProductUNSPSC]                    varchar(500) NULL,
        [GPOContractReference]             varchar(500) NULL,
        [LeasePaymentTerms]                varchar(500) NULL,
        [RebateFrequency]                  varchar(500) NULL,
        [RebateAmount]                     varchar(500) NULL,
        [DateRebateExpected]               varchar(500) NULL,
        [MethodOfPayment]                  varchar(500) NULL,
        [OtherContractDetails]             varchar(500) NULL,
        [RebateBasis]                      varchar(500) NULL,
        [RequiresCommitmentTo]             varchar(500) NULL,
        [CommitmentMeasure]                varchar(500) NULL,
        [OtherKnownCompetitors]            varchar(500) NULL,
        [OrganizationEID]                  varchar(500) NOT NULL,
        [ContractOrgID]                    varchar(500) NOT NULL,
        [ERPVendorID]                      varchar(500) NULL,
        [IsSUOM]                           varchar(500) NOT NULL,
        [SourceContractType]               varchar(500) NOT NULL,
        [ManufacturerEID]                  varchar(500) NULL,
        [VendorEID]                        varchar(500) NULL,
        [LastUpdate]                       datetime2(3) NOT NULL
            CONSTRAINT [DF_STG_ContractLineDetails_LastUpdate]
            DEFAULT (SYSUTCDATETIME())
    );
END
ELSE
BEGIN
    PRINT 'Table [GHX].[_STG_ContractLineDetails] already exists; no changes made.';
END;
GO
