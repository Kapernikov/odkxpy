import sqlalchemy
from .odkx_server_table import OdkxServerTable

class SqlLocalStorage(object):
    def __init__(self, engine: sqlalchemy.engine.Engine, schema: str):
        self.engine = engine
        self.schema = schema

    def createLocalTable(self, server_table: OdkxServerTable):
        full_tn = self.schema + '.' + server_table.tableId
        meta = sqlalchemy.MetaData()
        meta.bind = self.engine
        t = None
        try:
            meta.reflect(only=[server_table.tableId], schema=self.schema, views=True)
            if not full_tn in meta:
                t = sqlalchemy.Table(server_table.tableId, meta, schema=self.schema)
            else:
                t = meta[full_tn]  # sqlalchemy.Table
        except sqlalchemy.exc.InvalidRequestError:
            t = sqlalchemy.Table(server_table.tableId, meta, schema=self.schema)
        for col in server_table.getTableDefinition():
            if not col.isMaterialized():
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


        for cn in ['createUser', 'lastUpdateUser', 'dataETagAtModification', 'rowETag', 'savepointCreator', 'formId', 'state', 'hash']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.String(50)))

        for cn in ['default', 'lastUpdateUser', 'dataETagAtModification', 'rowETag', 'savepointCreator', 'formId']:
            if not cn in t.c:
                t.append_column(sqlalchemy.Column(cn, sqlalchemy.String(50)))

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
            t.append_column(sqlalchemy.Column('id', sqlalchemy.String(50), primary_key=True, nullable=False))



        meta.create_all(self.engine)



