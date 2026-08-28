# Cardinal Invoice Details Loader

This loader reads the daily Cardinal `Invoice Level` Excel worksheet from
`\\montefiore.org\centralfiles\data\Procurement PMO\_Data\CARDINAL\INBOUND`
and atomically appends to `cardinal.dbo.CARDINAL_INV_DETAILS` on
`YNBBSTVWP02\PROCDATASRVPROD`.

The loader is scheduled as a daily process, and its ETL health records use
`ProcessFrequency = Daily`.

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

### Batch file

From Command Prompt or PowerShell, the included batch file runs the production loader using the Cardinal virtual environment, YAML, and automatically loaded `.env`:

```bat
run_cardinal_loader.bat
```

It also passes optional command-line arguments to the loader. Use this command for a read-only configuration test:

```bat
run_cardinal_loader.bat --validate-only
```

The batch file returns the loader's exit code to the calling console or scheduler.

## Windows Task Scheduler

Task Scheduler should call the virtual environment's Python executable directly. It does not need to activate the virtual environment or start PowerShell/CMD.

1. Run `taskschd.msc`, select **Task Scheduler Library**, and choose **Create Task**.
2. On **General**, enter a name such as `Cardinal Invoice Details ETL`. Select the approved service account and **Run whether user is logged on or not**. Leave **Do not store password** cleared because the task needs network resources. Enable **Run with highest privileges** only if required by local policy.
3. On **Triggers**, create a daily trigger after the expected Cardinal file-delivery time.
4. On **Actions**, select **Start a program** and enter these values:

| Field | Value |
|---|---|
| Program/script | `<INSTALL_DIR>\Cardinal\.venv\Scripts\python.exe` |
| Add arguments | `main_loader.py --config etl_config.yaml` |
| Start in | `<INSTALL_DIR>\Cardinal` |

Replace `<INSTALL_DIR>` with the final local installation directory on the new machine. Do not omit **Start in**. It ensures relative paths and local modules resolve consistently. Do not use mapped drive `I:` in the task; scheduled sessions may not have user drive mappings. The YAML already uses the corresponding UNC path.

On **Settings**, enable **Allow task to be run on demand** and **Run task as soon as possible after a scheduled start is missed**. Set **If the task is already running** to **Do not start a new instance**, because overlapping runs could target the same table and inbound file. A retry such as every 15 minutes up to three times can help with transient database or network failures.

The task account needs:

- Read/execute access to the Cardinal project and `.venv`.
- Read, create, move, and delete access as appropriate on the Cardinal `INBOUND`, `ARCHIVE`, `ERRORS`, and `LOGS` UNC folders.
- Trusted SQL Server access to truncate/insert/select `cardinal.dbo.CARDINAL_INV_DETAILS` and insert into `ETL.dbo.ETL_Health_Status`.
- Permission to log on as a batch job and outbound HTTPS access to Microsoft Graph for summary email.
- Read access to `.env`. Restrict that file to the task account and administrators because it contains the Graph client secret.

Before enabling the production trigger, test under the scheduled account by temporarily using these read-only arguments:

```text
main_loader.py --config etl_config.yaml --validate-only
```

A successful test shows Task Scheduler result `0x0`. Then use `--preflight` to verify the current source workbook and live SQL schema without changing data:

```text
main_loader.py --config etl_config.yaml --preflight
```

Finally, restore the production arguments `main_loader.py --config etl_config.yaml`, run the task on demand once, and confirm the Cardinal log, ETL health row, summary email, destination row count, and archive movement.

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
      strategy: append
      batch_size: 10000
      allow_empty: false
      pk_check: [OrderID]
```

Health logging uses its own configured connection because the centralized status table can be on a different SQL Server from the load destination.

Console logs, run summaries, health records, and email timestamps use U.S. Eastern time (`America/New_York`) and automatically follow daylight-saving time.

Supported sources are `csv` (including delimited `.txt` files), `tsv`, `excel`, `json`, `parquet`, and `sql` query. CSV/TSV and SQL sources are read in `direct_load.batch_size` chunks. All converted chunks are retained until the complete source passes preflight, ensuring that the source is fully valid before the destination transaction begins. Every loader supplies its column mapping and SQL type:

For a file delivered daily, set `source.path` to its directory and use a Python-style date format in `filename_pattern`. The Cardinal loader also accepts a wildcard for the timestamp portion of the report name. Exactly one matching file must be present. The filename is resolved once at the start of the run in U.S. Eastern time and the same path is used for reading, ETL health, and archive/error movement:

```yaml
source:
  type: excel
  path: '\\montefiore.org\centralfiles\data\Procurement PMO\_Data\CARDINAL\INBOUND'
  filename_pattern: 'Montefiore_Purchase Invoice Detail Report_Daily{date:%Y-%m-%d}-*.xlsx'
```

On August 21, 2026, this matches a name such as `Montefiore_Purchase Invoice Detail Report_Daily2026-08-21-08-42-44.xlsx`. Omit `filename_pattern` when `path` already names a fixed file.

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

## Append production load

Every loader explicitly declares a direct load:

```yaml
direct_load:
  strategy: append
  batch_size: 10000
  allow_empty: false
  pk_check: []
```

Before changing production, the loader validates the entire source: required columns, conversions, integer and decimal bounds, string lengths, nullability, empty-file policy, and configured primary-key duplicates across all chunks. Configure `pk_check: []` only when the table genuinely has no enforced or approved business key.

The loader also requires configured target columns to match the live production table's names, order, SQL types, sizes/precision, and nullability. `Load_TS` is mapped explicitly and receives one Eastern-time `datetime2(2)` timestamp for the complete workbook load.

After preflight and schema validation, the loader records the starting table count, inserts every batch, and verifies that the final count increased by exactly the prepared row count. These database operations execute inside one transaction, and any failure rolls back every row from that workbook. The loader never truncates `CARDINAL_INV_DETAILS`.

The table currently has no configured duplicate key check. Do not return an already archived workbook to `INBOUND` unless its rows are intentionally meant to be appended again.

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
  process_frequency: Daily
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

Every actual load writes a timestamped log named `etl_loader_YYYYMMDD_HHMMSS_microseconds.log` to `\\montefiore.org\centralfiles\data\Procurement PMO\_Data\CARDINAL\LOGS`. Read-only commands do not create a log or require access to that share. An actual load exits with code `3` if the log directory cannot be accessed.
