# Simple YAML ETL Loader

This version has three Python files:

- `main_loader.py` reads every YAML loader, validates the existing production table, and loads source data directly into it.
- `etl_health.py` writes one final `SUCCESS` or `FAILURE` row to the configured legacy ETL health table after each enabled loader runs.
- `graph_email.py` sends one summary email through Microsoft Graph after all loaders finish.

## Install and run

From this directory:

```powershell
py -3.12 -m pip install -r .\requirements.txt
Copy-Item .\etl_config.sample.yaml .\etl_config.yaml
py -3.12 .\main_loader.py --config .\etl_config.yaml --validate-only
py -3.12 .\main_loader.py --config .\etl_config.yaml --preflight
py -3.12 .\main_loader.py --config .\etl_config.yaml --table-info
py -3.12 .\main_loader.py --config .\etl_config.yaml
```

`etl_config.sample.yaml` contains placeholders only. Copy it to the Git-ignored `etl_config.yaml`, then put real server, database, source-file, destination, sender, and recipient settings in that local YAML file.

`.env.sample` is the only environment template tracked by Git and contains only the three Microsoft Graph authentication values: tenant ID, client ID, and client secret. Real `.env`, `.env.local`, and `.env.example` files are ignored. At startup, the loader automatically reads `.env` from the directory containing the selected YAML file. Variables already supplied by PowerShell, the scheduler, or a secret manager take precedence. Store all non-sensitive settings in YAML.

`--validate-only` checks YAML without accessing source files, databases, or the shared log folder. `--preflight` reads and validates the complete source plus live production metadata without writing anything. `--table-info` prints the live production columns and generated-column status. Add `--loader LoaderName` to limit either live read-only command to one loader.

## YAML behavior

`default_destination` is the shared SQL Server destination. Each loader inherits it and may override values:

```yaml
default_destination:
  connection:
    server: MainSqlServer
    database: Warehouse
    auth: {mode: trusted}
  schema: GHX

loaders:
  - name: orders
    destination:
      connection:
        database: OtherWarehouse  # Keeps the default server/auth.
      table: orders
    direct_load:
      strategy: truncate_insert
      batch_size: 10000
      allow_empty: false
      pk_check: [OrderID]
```

Health logging uses its own configured connection because the centralized status table can be on a different SQL Server from the load destination.

Console logs, run summaries, health records, and email timestamps use U.S. Eastern time (`America/New_York`) and automatically follow daylight-saving time.

Supported sources are `csv` (including delimited `.txt` files), `tsv`, `excel`, `json`, `parquet`, and `sql` query. CSV/TSV and SQL sources are read in `direct_load.batch_size` chunks. All converted chunks are retained until the complete source passes preflight, ensuring that no later source error can occur after production has been truncated. Every loader supplies its column mapping and SQL type:

For a file delivered daily, set `source.path` to its directory and use a Python-style date format in `filename_pattern`. The filename is resolved once at the start of the run in U.S. Eastern time and the same path is used for reading, ETL health, and archive/error movement:

```yaml
source:
  type: csv
  path: '\\server\share\inbound'
  filename_pattern: 'CCX_Extract_MHS_{date:%m%d%Y}.txt'
```

On August 5, 2026, this resolves to `CCX_Extract_MHS_08052026.txt`. Omit `filename_pattern` when `path` already names a fixed file.

```yaml
columns:
  - {source: ExternalId, target: customer_id, type: bigint, nullable: false}
  - {source: Name, target: customer_name, type: "nvarchar(150)"}
```

Supported common types are integer types, `decimal(p,s)`, `float`, `bit`, character types, `date`, `datetime`, `datetime2`, and `uniqueidentifier`.

For file sources, optional post-load handling moves a fully successful file to an archive folder. A source file that fails during file access, parsing, or column conversion moves to an error folder. Destination database failures leave the inbound file in place for retry:

```yaml
source:
  type: csv
  path: '\\your-file-server\share\inbound\sample.txt'
  file_move:
    success_directory: '\\your-file-server\share\archive'
    error_directory: '\\your-file-server\share\errors'
```

The destination folders must already exist. Existing files are never overwritten; a filename collision causes the move to fail and is included in the loader result.

## Direct production load

Every loader explicitly declares a direct load:

```yaml
direct_load:
  strategy: truncate_insert
  batch_size: 10000
  allow_empty: false
  pk_check: [BusinessKeyColumn]
```

Before changing production, the loader validates the entire source: required columns, conversions, integer and decimal bounds, string lengths, nullability, empty-file policy, and configured primary-key duplicates across all chunks. Configure `pk_check: []` only when the table genuinely has no enforced or approved business key.

The loader also requires configured target columns to match the live production table's names, order, SQL types, sizes/precision, and nullability. Unmapped columns are accepted when SQL Server generates them through an identity, computed expression, or default constraint, or when the column is nullable and can safely receive `NULL`. This permits the production `Load_TS` default and the staging table's optional `LAST_UPDATE` while rejecting unmapped required business columns.

After preflight, schema validation, `TRUNCATE TABLE`, every batched insert, and final `COUNT(*)` reconciliation execute inside one database transaction. Prepared rows, inserted rows, and the final production count must agree. Any failure rolls the truncate and all inserts back together.

## ETL health tracking

The newer `ETL.RunHistory` start/finish calls are temporarily disabled in `main_loader.py`. For now, configure the centralized legacy table connection:

```yaml
health:
  enabled: true
  connection:
    server: 'YOUR_SERVER\YOUR_INSTANCE'
    database: ETL
    auth: {mode: trusted}
  schema: dbo
  table: ETL_Health_Status
  required: true
  process_frequency: OnDemand
  owner: ETL Owner

loaders:
  - name: ExampleLoader
    health:
      data_flow_task_name: CSV_to_SQL_Direct_TruncateInsert
```

Each actual enabled loader execution inserts one row into `dbo.ETL_Health_Status` after the load attempt finishes. The row includes the resolved source file, completion time, production target, final committed row count, log path, error, frequency, and owner. Direct loads use `STGTableName = 'Not Applicable'` and the default task name `CSV_to_SQL_Direct_TruncateInsert`. Read-only commands and disabled loaders do not write status rows. Set `required: true` to report an otherwise successful load as failed when its status row cannot be written.

## Graph summary email

Create a Microsoft Entra application with Microsoft Graph application permission `Mail.Send` and tenant admin consent. Put only its tenant ID, client ID, and client secret in environment variables. Store sender and recipients in YAML, then enable the email section:

```yaml
email:
  enabled: true
  tenant_id: "${GRAPH_TENANT_ID}"
  client_id: "${GRAPH_CLIENT_ID}"
  client_secret: "${GRAPH_CLIENT_SECRET}"
  sender: etl-service@example.org
  recipients:
    - data-operations@example.org
```

The script uses the client-credentials flow and calls `POST /v1.0/users/{sender}/sendMail`. Keep secrets out of YAML and source control. See Microsoft's [sendMail API](https://learn.microsoft.com/en-us/graph/api/user-sendmail?view=graph-rest-1.0) and [client-credentials flow](https://learn.microsoft.com/en-us/entra/identity-platform/v2-oauth2-client-creds-grant-flow) documentation.

Exit codes are `0` for success, `1` for a loader failure, `2` for a required email failure, and `3` for invalid configuration/startup.

Every actual load writes a timestamped log named `etl_loader_YYYYMMDD_HHMMSS_microseconds.log` to `\\montefiore.org\centralfiles\data\Procurement PMO\_Data\CCX\LOGS`. Read-only commands do not create a log or require access to that share. An actual load exits with code `3` if the log directory cannot be accessed.
