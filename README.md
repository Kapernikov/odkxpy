# ODKX python API

Python API for accessing ODK-X. Support local sync to a SQL database

# Caveats/Known problems

* only supports PostgreSQL for now (makes use of queries using UPDATE...FROM syntax)
  need to fix this by switching to sqlalchemy core

* does not support uploading attachments yet (only downloading).

# Getting data from the server (using REST)

```python

import odkxpy
from sqlalchemy import create_engine

con = odkxpy.OdkxConnection('https://odk_sync_endpoint.com/odktables/', 'user', 'password')
meta = odkxpy.OdkxServerMeta(con)

### get a list of tables , and find the table definition

tables = meta.getTables()
first_table = tables[0]
definition = first_table.getTableDefinition()

```

# Storing data locally

```python

engine = create_engine("postgresql://test:test@localhost:5432/test")
local_storage = odkxpy.local_storage_sql.SqlLocalStorage(engine, 'public','/home/attachments')

first_table_local = local_storage.getLocalTable(first_table)
first_table_local.sync(first_table)
```

# Making some changes and pushing the changes back to the server

Suppose you want to create a computation that updates the answer for question1 and question2, but does not touch any other field.
We don't use the ODKX ID but the ID of the record in our (proprietary) database in the example below (however, its also possible to use the odkx id as primary key).
We use ONLY_EXISTING_RECORDS so we are sure that we will not create new records, only update existing ones.

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

## Migrating a table

If you want to migrate a table from one namespace to another, you can use the Migrator.
The history will be kept as well as the attachments.
The migration is only possible if you have activate the pull request 31.

con = odkxpy.OdkxConnection('https://odk_sync_endpoint.com/odktables/', 'user', 'password')
```python
Migrator = migrator(tableId, newTableId, meta, local_storage, odkx_application_path, path, pathMapping)
Migrator.migrateReport()
Migrator.migrate()
```

## Uploading app files
You can then update your application to take into account to changes. 
The parameter upload_mode controls with is updated from the application repository("table", "app", "table_html_js", "file").
```python
Migrator.putFiles(upload_mode)
```


# Authors

Frank Dekervel
Ludovic Santos
Ezechiël Syx
