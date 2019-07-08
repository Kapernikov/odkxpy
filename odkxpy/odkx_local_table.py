import sqlalchemy
from .odkx_server_table import OdkxServerTable, OdkxServerTableRow, OdkxServerTableColumn
from sqlalchemy import MetaData, text
import os
from typing import Optional
import hashlib
import requests
import logging
import datetime

class FilesystemAttachmentStore(object):
    def __init__(self, path):
        self.path = path

    def getFileName(self, id, filename):
        return os.path.join(self.path, id, filename)

    def hasFile(self, id, filename):
        return os.path.isfile(self.getFileName(id, filename))

    def open(self, id, filename):
        return open(self.getFileName(id, filename), 'rb')

    def getMD5(self, id, filename):
        hash_md5 = hashlib.md5()
        with open(self.getFileName(id, filename), "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def storeFile(self, id, filename, response: requests.Response):
        target = self.getFileName(id, filename)
        os.makedirs(os.path.join(self.path, id),exist_ok=True)
        with open(target + '-tmp', 'wb') as out_file:
            for chunk in response.iter_content(1024):
                out_file.write(chunk)
        if os.path.isfile(target):
            os.remove(target)
        os.rename(target + '-tmp', target)
        del response


class OdkxLocalTable(object):
    def __init__(self, tableId: str, engine: sqlalchemy.engine.Engine, schema: str):
        self.tableId = tableId
        self.schema = schema
        self.attachments = FilesystemAttachmentStore(os.getcwd())
        self.engine: sqlalchemy.engine.Engine = engine

    def getLocalDataETag(self):
        with self.engine.connect() as c:
            rs = c.execute(text(f"""select "dataETag" from {self.schema}.status_table where "table_name" = :tableid order by sync_date desc limit 1
                """), tableid=self.tableId)
            for row in rs:
                return row[0]
        return str('')

    def _getStagingTable(self) -> sqlalchemy.Table:
        meta = MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[self.tableId + '_staging'])
        return meta.tables.get(self.schema+ '.' + self.tableId + '_staging')

    def _getLogTable(self) -> sqlalchemy.Table:
        meta = MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[self.tableId + '_log'])
        return meta.tables.get(self.schema+ '.' + self.tableId + '_log')

    def _getDataTable(self) -> sqlalchemy.Table:
        meta = MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[self.tableId])
        return meta.tables.get(self.schema+ '.' + self.tableId)


    def updateLocalStatusDb(self, dataETag):
        sql = f"""INSERT INTO {self.schema}.status_table ("table_name", "dataETag", "sync_date")
                 VALUES ('{self.tableId}', '{dataETag}', '{str(datetime.datetime.now())}')"""
        logging.info("SQL request:\n"+sql)
        with self.engine.connect() as con:
            con.execute(sql)


    def row_asdict(self, r: OdkxServerTableRow):
        dct = {}
        dct['rowETag'] = r.rowETag
        dct['createUser'] = r.createUser
        dct['lastUpdateUser'] = r.lastUpdateUser
        dct['dataETagAtModification'] = r.dataETagAtModification
        dct['savepointCreator'] = r.savepointCreator
        dct['formId'] = r.formId
        dct['locale'] = r.locale
        dct['savepointType'] = r.savepointType
        dct['savepointTimestamp'] = r.savepointTimestamp
        dct['deleted'] = r.deleted
        dct['id'] = r.id
        for col in r.orderedColumns:
            assert isinstance(col, OdkxServerTableColumn)
            dct[col.column] =col.value
        return dct

    def stageAllDataChanges(self, remoteTable: OdkxServerTable) -> Optional[str]:
        st = self._getStagingTable()
        last_rs = None
        with self.engine.begin() as transaction:
            transaction.execute(st.delete())
            for rowset in remoteTable.getDiffGenerator(dataETag=self.getLocalDataETag()):
                last_rs = rowset
                transaction.execute(st.insert(), [self.row_asdict(x) for x in rowset.rows])
        if not last_rs is None:
            return last_rs.dataETag

    def downloadAttachments(self, remoteTable: OdkxServerTable, rowId: str):
        for f in remoteTable.getAttachmentsManifest(rowId):
            if self.attachments.hasFile(rowId, f.filename):
                if self.attachments.getMD5(rowId, f.filename) == f.md5hash:
                    continue
            self.attachments.storeFile(rowId, f.filename,
                                       remoteTable.getAttachment(rowId, f.filename, stream=True, timeout=300))


    def _sync_pull_attachments(self, remoteTable: OdkxServerTable):
        ids = None
        with self.engine.connect() as c:
            result = c.execute("select id from {schema}.{table} where state='sync_attachments'".format(
                schema=self.schema, table=self.tableId
            ))
            ids = [r[0] for r in result]
        for id in ids:
            self.downloadAttachments(remoteTable, id)
            with self.engine.connect() as c:
                c.execute(sqlalchemy.sql.text("update {schema}.{table} set state='synced' where id=:rowid".format(
                    schema=self.schema, table=self.tableId)), rowid=id)

    def _staging_to_log(self):
        st = self._getStagingTable()
        colnames = [x.name for x in st.columns]
        fields = ','.join(['"{colname}"'.format(colname=colname) for colname in colnames])
        with self.engine.begin() as trans:
            trans.execute("insert into {schema}.{logtable} ({fields}) select {fields} from {schema}.{stagingtable}".format(
                schema= self.schema,
                logtable=self.tableId+'_log',
                fields=fields,
                stagingtable=self.tableId+'_staging'
            ))

    def _sync_iter_pull(self, remoteTable: OdkxServerTable):
        if remoteTable.getdataETag() == self.getLocalDataETag():
            return False
        new_etag = self.stageAllDataChanges(remoteTable)
        self._staging_to_log()
        st = self._getStagingTable()
        colnames = [x.name for x in st.columns]

        # sync up data
        with self.engine.begin() as trans:
            trans.execute("delete from {schema}.{table} where id in (select id from {schema}.{stagingtable})".format(
                schema=self.schema, table=self.tableId, stagingtable=self.tableId + '_staging'
            ))
            fields = ','.join(['"{colname}"'.format(colname=colname) for colname in colnames])
            trans.execute("insert into {schema}.{table} ({fields},state) select {fields}, 'sync_attachments' as state from {schema}.{stagingtable}".format(
                schema=self.schema, table=self.tableId, stagingtable=self.tableId+'_staging', fields=fields
            ))
        self._sync_pull_attachments(remoteTable)
        self.updateLocalStatusDb(new_etag)
        return True


    def sync(self, remoteTable: OdkxServerTable):
        self._sync_iter_pull(remoteTable)
