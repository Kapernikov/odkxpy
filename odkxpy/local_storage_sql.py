import sqlalchemy
from .odkx_server_table import OdkxServerTable
from .odkx_local_table import OdkxLocalTable
from typing import Optional, List
import os

class SqlLocalStorage(object):
    def __init__(self, engine: sqlalchemy.engine.Engine, schema: str, file_storage_root: str):
        self.engine = engine
        self.schema = schema
        self.file_storage_root = file_storage_root

    def getLocalTable(self, server_table: OdkxServerTable) -> OdkxLocalTable:
        filestore = os.path.join(self.file_storage_root, server_table.tableId)
        os.makedirs(filestore, exist_ok=True)
        self.initializeLocalStorage(server_table)
        return OdkxLocalTable(server_table.tableId, self.engine, self.schema, filestore)

    def initializeLocalStorage(self, server_table: OdkxServerTable):
        self._createLocalTable(server_table, log_table=False, create_state_col=True)
        self._createLocalTable(server_table, log_table=True)
        self._createLocalTable(server_table, log_table=True, table_name_instead=server_table.tableId + '_staging')
        self._createStatusTable()

    def intializeExternalSource(self, source_prefix: str, server_table: OdkxServerTable, relevant_columns: Optional[List[str]] = None):
        self.initializeLocalStorage(server_table)
        self._createLocalTable(server_table, log_table=False, table_name_instead=server_table.tableId + '_' + source_prefix, create_hash_col=True,
                               create_state_col=True, only_create_datacols=relevant_columns)
        self._createLocalTable(server_table, log_table=False, table_name_instead=server_table.tableId + '_' + source_prefix + '_staging',
                               create_hash_col=True, create_state_col=True, only_create_datacols=relevant_columns, no_create_standard_pkey=True)

    def _getTableMeta(self, tablename: str) -> sqlalchemy.Table:
        meta = sqlalchemy.MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[tablename])
        return meta.tables.get(self.schema+ '.' + tablename)




    def _createStatusTable(self):
        s_tn = 'status_table'
        full_tn = self.schema + '.' + s_tn
        meta = sqlalchemy.MetaData()
        meta.bind = self.engine
        t = None
        try:
            meta.reflect(only=[s_tn], schema=self.schema, views=True)
            if not full_tn in meta:
                t = sqlalchemy.Table(s_tn, meta, schema=self.schema)
            else:
                t = meta.tables.get(full_tn)  # sqlalchemy.Table
        except sqlalchemy.exc.InvalidRequestError:
            t = sqlalchemy.Table(s_tn, meta, schema=self.schema)
        t.append_column(sqlalchemy.Column('table_name', sqlalchemy.String(80)))
        t.append_column(sqlalchemy.Column('dataETag', sqlalchemy.String(50)))
        t.append_column(sqlalchemy.Column('sync_date', sqlalchemy.DateTime))
        meta.create_all()


    def _createLocalTable(self, server_table: OdkxServerTable, log_table: bool = False, table_name_instead = None,
                          create_hash_col: bool = False, create_state_col: bool = False, only_create_datacols: Optional[List[str]] = None,
                          no_create_standard_pkey : bool = False):
        s_tn = server_table.tableId + ('_log' if log_table else '')
        if table_name_instead:
            s_tn = table_name_instead
        full_tn = self.schema + '.' + s_tn
        meta = sqlalchemy.MetaData()
        meta.bind = self.engine
        t = None
        try:
            meta.reflect(only=[s_tn], schema=self.schema, views=True)
            if not full_tn in meta:
                t = sqlalchemy.Table(s_tn, meta, schema=self.schema)
            else:
                t = meta.tables.get(full_tn)  # sqlalchemy.Table
        except sqlalchemy.exc.InvalidRequestError:
            t = sqlalchemy.Table(s_tn, meta, schema=self.schema)
        definition = server_table.getTableDefinition()
        column_names = [x.elementKey for x in definition if x.isMaterialized()]
        if not only_create_datacols is None:
            for c in only_create_datacols:
                if not c in column_names:
                    raise Exception("don't know about column " + c)
        for col in definition:
            if not col.isMaterialized():
                continue
            if not (only_create_datacols is None):
                if not col.elementKey in only_create_datacols:
                    continue
            cname = col.elementKey
            dt = sqlalchemy.Text
            if col.elementType == 'string':
                dt = sqlalchemy.Text
            elif col.elementType == 'number':
                dt = sqlalchemy.Float
            elif col.elementType == 'mimeType':
                dt = sqlalchemy.String(40)
            elif col.elementType == 'rowpath':
                dt = sqlalchemy.String(255)
            elif col.elementType == 'integer':
                dt = sqlalchemy.Integer
            elif col.elementType == 'array':
                dt = sqlalchemy.types.JSON
            if not cname in t.c:
                t.append_column(sqlalchemy.Column(cname, dt))


        for cn in ['createUser', 'lastUpdateUser', 'dataETagAtModification', 'savepointCreator', 'formId']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.String(50)))

        if create_hash_col:
            if not 'hash' in t.c:
                t.append_column(sqlalchemy.Column('hash', sqlalchemy.String(50)))
        if create_state_col:
            if not 'state' in t.c:
                t.append_column(sqlalchemy.Column('state', sqlalchemy.String(50)))


        if not 'rowETag' in t.c:
            t.append_column(sqlalchemy.Column('rowETag', sqlalchemy.String(50), primary_key=(log_table and not no_create_standard_pkey)))

        for cn in ['locale', 'savepointType']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.String(20)))

        for cn in ['defaultAccess', 'groupModify', 'groupPrivileged', 'groupReadOnly', 'rowOwner']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.Text))

        if not 'savepointTimestamp' in t.c:
            t.append_column(sqlalchemy.Column('savepointTimestamp', sqlalchemy.DateTime))
        if not 'deleted' in t.c:
            t.append_column(sqlalchemy.Column('deleted', sqlalchemy.Boolean))
        if not 'id' in t.c:
            pkey = ((not log_table) and not no_create_standard_pkey)
            nullable = not pkey
            t.append_column(sqlalchemy.Column('id', sqlalchemy.String(50), primary_key=pkey, nullable=nullable))

        meta.create_all(self.engine)



