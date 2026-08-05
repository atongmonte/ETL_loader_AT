# Simple YAML ETL Loader

This version has three Python files:

- `main_loader.py` reads every YAML loader, loads source data into staging, and optionally moves staging to production.
- `etl_health.py` writes `RUNNING`, `SUCCESS`, and `FAILED` information to an ETL health table.
- `graph_email.py` sends one summary email through Microsoft Graph after all loaders finish.

## Install and run

From this directory:

```powershell
py -3.12 -m pip install -r .\requirements.txt
Copy-Item .\etl_config.sample.yaml .\etl_config.yaml
py -3.12 .\main_loader.py --config .\etl_config.yaml --validate-only
py -3.12 .\main_loader.py --config .\etl_config.yaml
```

`etl_config.sample.yaml` contains placeholders only. Copy it to the Git-ignored `etl_config.yaml`, then put real server, database, source-file, destination, sender, and recipient settings in that local YAML file.

`.env.sample` is the only environment template tracked by Git and contains only the three Microsoft Graph authentication values: tenant ID, client ID, and client secret. Real `.env`, `.env.local`, and `.env.example` files are ignored. The loader does not automatically read those files; inject the values into the process through PowerShell, your scheduler, or an approved secret manager. Store all non-sensitive settings in YAML.

The `truncate_append` mode requires the staging table to exist, truncates it once, and then appends each chunk while preserving the table definition and database defaults.

## YAML behavior

`default_destination` is the shared SQL Server destination. Each loader inherits it and may override values:

```yaml
default_destination:
  connection:
    server: MainSqlServer
    database: Warehouse
    auth: {mode: trusted}
  staging_schema: GHX
  staging_load_mode: truncate_append

loaders:
  - name: orders
    destination:
      connection:
        database: OtherWarehouse  # Keeps the default server/auth.
      staging_table: orders_stg
```

The same effective connection is passed to `etl_health.py`, so health records automatically follow a loader-level server or database override. There is no separate health connection.

Console logs, run summaries, health records, and email timestamps use U.S. Eastern time (`America/New_York`) and automatically follow daylight-saving time.

Supported sources are `csv` (including delimited `.txt` files), `tsv`, `excel`, `json`, `parquet`, and `sql` query. CSV/TSV and SQL sources are streamed in `batch_size` chunks. Every loader supplies its column mapping and SQL type:

```yaml
columns:
  - {source: ExternalId, target: customer_id, type: bigint, nullable: false}
  - {source: Name, target: customer_name, type: "nvarchar(150)"}
```

Supported common types are integer types, `decimal(p,s)`, `float`, `bit`, character types, `date`, `datetime`, `datetime2`, and `uniqueidentifier`.

For file sources, optional post-load handling moves a fully successful file to an archive folder. A source file that fails during file access, parsing, or column conversion moves to an error folder. Destination database and production-promotion failures leave the inbound file in place for retry:

```yaml
source:
  type: csv
  path: '\\your-file-server\share\inbound\sample.txt'
  file_move:
    success_directory: '\\your-file-server\share\archive'
    error_directory: '\\your-file-server\share\errors'
```

The destination folders must already exist. Existing files are never overwritten; a filename collision causes the move to fail and is included in the loader result.

## Staging to production

The `staging_to_prod.mode` values are:

- `none`: stop after staging.
- `append`: insert all staging rows into an existing production table.
- `truncate_insert`: truncate the production table, then insert all staging rows.
- `stored_procedure`: execute a configured `schema.procedure` after staging.
- `sql`: execute trusted SQL from the YAML file.

For generic insert modes, the production table must already exist and have the configured target columns. Use `staging_load_mode: replace` or `truncate_append` so only the current source rows are promoted.

Stored-procedure example:

```yaml
staging_to_prod:
  mode: stored_procedure
  procedure: dbo.usp_customers_staging_to_prod
  parameters:
    run_id: $RUN_ID
```

Each staging chunk commits independently to avoid one extremely large SQL Server transaction. Production promotion runs only after every staging chunk succeeds. If a later staging chunk fails, production is not changed and the loader is reported as failed.

## ETL health table

Health logging is off in the sample. Enable it with:

```yaml
health:
  enabled: true
  schema: dbo
  table: etl_health
  auto_create: true
  required: false
```

`auto_create: true` creates the simple table used by `etl_health.py`; the schema itself must already exist. Set `required: true` if a health-write failure should fail the loader.

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
