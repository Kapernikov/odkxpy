import sqlalchemy
from .odkx_server_table import OdkxServerTable

class OdkxLocalTable(object):
    def __init__(self, tableId: str, engine: sqlalchemy.engine.Engine, schema: str):
        self.tableId = tableId
        self.schema = schema
        self.engine : sqlalchemy.engine.Engine = engine

    def getLocalDataETag(self):
        with self.engine.connect() as c:
            rs = c.execute(f"""select * from {self.schema}.status_table where "table_name" = :tableid
                order by sync_date desc limit 1
                """, tableid=self.tableId)
            for row in rs:
                return row[0]
        return str('')

    def updateLogTable(self, remoteTable: OdkxServerTable):
        for rowset in remoteTable.getDiffGenerator(dataETag=self.getLocalDataETag()):

        records = self.remoteTable.getAllResults('AllDataChanges', dataETag=self.getLocalDataETag())
        if records['rows']:
            df = self.toDf(records)
            dfCurr = pd.read_sql(f"""select "rowETag" from {self.schema}.{self.tableId}""",
                                 db.get_postgres_cache_engine())
            alreadyInIds = dfCurr.rowETag.to_list()
            lstIds = [x for x in df.rowETag.to_list() if x not in alreadyInIds]
            df = df[df.rowETag.isin(lstIds)]
            df.to_sql(
                self.tableId + "_log",
                db.get_postgres_cache_engine(),
                schema=self.schema,
                index=False,
                if_exists='append')
