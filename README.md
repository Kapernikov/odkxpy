# ODKX python API

Python API for accessing ODK-X. Support local sync to a SQL database

## Caveats/Known problems

* only supports PostgreSQL for now (makes use of queries using UPDATE...FROM syntax)
  need to fix this by switching to sqlalchemy core

* Managment of full local history is only possible with a [patched odkx server](#patched-odkx-server-with-pr-31).

## Getting data from the server (using REST)

```python
import odkxpy
from sqlalchemy import create_engine

con = odkxpy.OdkxConnection('https://odk_sync_endpoint.com/odktables/', 'user', 'password')
meta = odkxpy.OdkxServerMeta(con)

### get a list of tables , and find the table definition

tables = meta.getTables()
first_table = tables[0]
definition = first_table.getTableDefinition()

### get the general info on a table (dataETag, schemaETag, ...)

my_table = meta.getTable("my_table")
tableInfo = my_table.getTableInfo()

### get the all the rows

AllDataRows = my_table.getAllDataRows()
```

## Storing data locally

```python
engine = create_engine("postgresql://test:test@localhost:5432/test")
local_storage = odkxpy.local_storage_sql.SqlLocalStorage(engine, 'public','/home/attachments')

first_table_local = local_storage.getLocalTable(first_table)
first_table_local.sync(first_table)
```

## Making some changes and pushing the changes back to the server

Suppose you want to create a computation that updates the answer for question1 and question2, but does not touch any other field.
We don't use the ODKX ID but the ID of the record in our (proprietary) database in the example below (however, its also possible to use the odkx id as primary key).
We use `ONLY_EXISTING_RECORDS` so we are sure that we will not create new records, only update existing ones.

```python
import pandas as pd
local_storage.initializeExternalSource("my_calculation", first_table, ["question1", "question2","my_id"])

df = pd.DataFrame([
    {'my_id':'444','question1':64, 'question2': 88},
    {'my_id':'445','question1':68, 'question2': 80},
])
first_table_local.localSyncFromDataframe('my_calculation', 'my_id',df, odkxpy.LocalSyncMode.ONLY_EXISTING_RECORDS)
## now the changes are staged (in a separate table). lets upload them:

first_table_local.sync(first_table, "my_calculation")
```

## Uploading records to a table

It is possible to upload a full history of records (with multiple occurence of rowids) from an arbitrary table to an ODKX table.
The arbitrary table should have the same column types than the ODKX table. It the same for the column names unless a mapping dictionary is given.
This dictionary defines how the columns are renamed. The keys are the old names and the values are the new names.

```python
first_table_local.uploadHistory(first_table, an_history_table, a_mapping_dict):
```

## Migrating a table

If you want to migrate a table from one namespace to another, you can use the migrator class.
It will involve the `uploadHistory` function as migration keep the history of the old table.
Note that the attachments are also kept and that the migration is only possible if you have a [patched odkx server](#patched-odkx-server-with-pr-31).

```python
# odkx_application_path: path to the application root directory
# path_definition_csv: path to the definition.csv file (relative to appRoot)

Migrator = migrator(tableId, newTableId, meta, local_storage, odkx_application_path, path_definition_csv, a_mapping_dict)

# Get a report on the incompatibilities before the migration
Migrator.migrateReport()

# Create the new table
Migrator.createRemoteTable()

Migrator.migrate()
```

## Uploading application and table files
The library is also able to update application files. 
The appRoot paramter is the location of the application.
The mode parameter controls what is updated:
- "table": update the table files of the current table
- "app": update the application files
- "table_html_js": update the html/js table files of the current table
- "file": update a specific file

```python
AppManager = OdkxAppManager(tableId, meta, appRoot)
AppManager.putFiles(mode)
```

## Patched odkx server with PR 31

By default, the /diff API only returns the latest version of a row in one fetch block. 
An option has been added in the API to get all versions of rows.
When the full history is needed, now one can pass getFullLog=true to get all changes.

More information can be found on [github](https://github.com/opendatakit/sync-endpoint/pull/31)

## Authors

Frank Dekervel
Ludovic Santos
Ezechiël Syx
