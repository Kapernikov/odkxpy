import sqlalchemy
from .odkx_server_table import OdkxServerTable, OdkxServerTableDefinition
from .odkx_local_table import OdkxLocalTable
from typing import Optional, List
import os


class SqlLocalStorage(object):
    chache_table_name = "odkxpy_cached_defintions"

    def __init__(self, engine: sqlalchemy.engine.Engine, schema: str, file_storage_root: str, useWindowsCompatiblePaths: bool = False):
        self.engine = engine
        self.schema = schema
        self.file_storage_root = file_storage_root
        self.useWindowsCompatiblePaths = useWindowsCompatiblePaths
        self._create_cache()

    def getLocalTable(self, server_table: OdkxServerTable) -> OdkxLocalTable:
        filestore = os.path.join(self.file_storage_root, server_table.tableId)
        os.makedirs(filestore, exist_ok=True)
        self.initializeLocalStorage(server_table)
        return OdkxLocalTable(server_table.tableId, self.engine, self.schema, filestore, useWindowsCompatiblePaths=self.useWindowsCompatiblePaths)

    def _create_cache(self, create=True):
       

        table_name = self.chache_table_name

        meta = sqlalchemy.MetaData()
        meta.bind = self.engine

        # try:

        #     tabledef = sqlalchemy.Table(
        #         table_name, meta, schema=self.schema, autoload=True, autoload_with=self.engine)

        # except sqlalchemy.exc.NoSuchTableError:

        tabledef = sqlalchemy.Table(table_name, meta, 
            sqlalchemy.Column("tableId", sqlalchemy.types.Text(), index= True),
            sqlalchemy.Column("schemaETag", sqlalchemy.types.Text),
            sqlalchemy.Column("odkxpydef", sqlalchemy.types.JSON(none_as_null=False)),
            sqlalchemy.UniqueConstraint('tableId', 'schemaETag', name='uix_1'),
            schema=self.schema
        )
        if create:
            meta.create_all()
        return tabledef


    def _cache_table_defintion(self, table_defintion: OdkxServerTableDefinition):
        table = self._create_cache(False)
    
        # sql = f"""INSERT INTO {self.schema}.{self.chache_table_name} ("tableId", "schemaETag", "odkxpydef")
        #          VALUES ('{table_defintion.tableId}', '{table_defintion.schemaETag}', '{str(datetime.datetime.now())}')"""

        # store defintion

        with self.engine.connect() as c:
            result = c.execute(
                table.select(),
                tableId=table_defintion.tableId
            )
            if not result.fetchone():
                c.execute(
                    table.insert(),
                    tableId=table_defintion.tableId,
                    schemaETag=table_defintion.schemaETag,
                    odkxpydef=table_defintion._asdict()
                )

    def getCachedLocalTable(self, tableId: str) -> OdkxLocalTable:
        table = self._create_cache()
        with self.engine.connect() as c:
            result = c.execute(
                table.select(),
                tableId=tableId
            )
            obj = dict(result.fetchall()[0])["odkxpydef"]
        tabledef = OdkxServerTableDefinition.tableDefinitionOf(obj)
            

        # code duplication initializeLocalStorage, since we don't have a OdkxServerTable only a def and id
        filestore = os.path.join(self.file_storage_root, tableId)
        os.makedirs(filestore, exist_ok=True)
        self._cache_table_defintion(tabledef)
        self._createLocalTable(tabledef, log_table=False,
                               create_state_col=True)
        self._createLocalTable(tabledef, log_table=True)
        self._createLocalTable(tabledef, log_table=True,
                               table_name_instead=tableId + '_staging')
        self._createStatusTable()
        # end code duplication

        return OdkxLocalTable(tableId, self.engine, self.schema, filestore, useWindowsCompatiblePaths=self.useWindowsCompatiblePaths)

    def initializeLocalStorage(self, server_table: OdkxServerTable):
        tabledef = server_table.getTableDefinition()
        self._cache_table_defintion(tabledef)
        self._createLocalTable(tabledef, log_table=False,
                               create_state_col=True)
        self._createLocalTable(tabledef, log_table=True)
        self._createLocalTable(
            tabledef, log_table=True, table_name_instead=server_table.tableId + '_staging')
        self._createStatusTable()

    def intializeExternalSource(self, source_prefix: str, server_table: OdkxServerTable, relevant_columns: Optional[List[str]] = None):
        """
        initialize a staging table and an external modifications table for a certain external source.
        the staging table will be called [tableId]_[sourceprefix]_staging, and the external source table will be called [tableId]_[sourceprefix]

        when creating such a table it is important to only create this table for the fields that you really plan to update using this external source.
        this will prevent blanking out fields that you don't want to touch.

        it is then possible to use localTable.localSync to fill the external modifications table from the staging table (or from a dataframe)
        or to write directly to the external modifications table (eg using an interactive app)

        :param source_prefix:
        :param server_table:
        :param relevant_columns:
        :return:
        """
        self.initializeLocalStorage(server_table)
        tabledef = server_table.getTableDefinition()
        self._createLocalTable(tabledef, log_table=False, table_name_instead=server_table.tableId + '_' + source_prefix, create_hash_col=True,
                               create_state_col=True, only_create_datacols=relevant_columns)
        self._createLocalTable(tabledef, log_table=False, table_name_instead=server_table.tableId + '_' + source_prefix + '_staging',
                               create_hash_col=True, create_state_col=True, only_create_datacols=relevant_columns, no_create_standard_pkey=True)

    def _getTableMeta(self, tablename: str) -> sqlalchemy.Table:
        meta = sqlalchemy.MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[tablename])
        return meta.tables.get(self.schema + '.' + tablename)

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

    def _createLocalTable(self, server_table: OdkxServerTableDefinition, log_table: bool = False, table_name_instead=None,
                          create_hash_col: bool = False, create_state_col: bool = False, only_create_datacols: Optional[List[str]] = None,
                          no_create_standard_pkey: bool = False):
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

        definition = server_table.columns
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
                t.append_column(sqlalchemy.Column(
                    'hash', sqlalchemy.String(50)))
        if create_state_col:
            if not 'state' in t.c:
                t.append_column(sqlalchemy.Column(
                    'state', sqlalchemy.String(50)))

        if not 'rowETag' in t.c:
            t.append_column(sqlalchemy.Column('rowETag', sqlalchemy.String(
                50), primary_key=(log_table and not no_create_standard_pkey)))

        for cn in ['locale', 'savepointType']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.String(20)))

        for cn in ['defaultAccess', 'groupModify', 'groupPrivileged', 'groupReadOnly', 'rowOwner']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.Text))

        if not 'savepointTimestamp' in t.c:
            t.append_column(sqlalchemy.Column(
                'savepointTimestamp', sqlalchemy.DateTime))
        if not 'deleted' in t.c:
            t.append_column(sqlalchemy.Column('deleted', sqlalchemy.Boolean))
        if not 'id' in t.c:
            pkey = ((not log_table) and not no_create_standard_pkey)
            nullable = not pkey
            t.append_column(sqlalchemy.Column('id', sqlalchemy.String(
                50), primary_key=pkey, nullable=nullable))

        meta.create_all(self.engine)
