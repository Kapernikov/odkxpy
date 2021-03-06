import sqlalchemy
from .odkx_server_table import OdkxServerTable, OdkxServerTableRow, OdkxServerTableColumn, OdkxServerColumnDefinition, OdkxServerTableDefinition
from .odkx_local_file import OdkxLocalFile
from .odkx_manifest_cache import OdkTableManifestCache
from sqlalchemy import MetaData, text
import os
from typing import Optional, List
import hashlib
import requests
import datetime
import pandas as pd
from enum import Enum
from distutils.dir_util import copy_tree
from requests_toolbelt.multipart import decoder as multi_decoder

class LocalSyncMode(Enum):
    FULL = 1
    ONLY_NEW_RECORDS = 2
    ONLY_EXISTING_RECORDS = 3

class FilesystemAttachmentStore(object):
    def __init__(self, path, useWindowsPaths: bool = False):
        self.path = path
        self.useWindowsPaths = useWindowsPaths

    def okWindows(self, id):
        xid = id
        if self.useWindowsPaths:
            xid = id.replace(":", "")
        return xid

    def getFileName(self, id, filename):
        return os.path.join(self.path, self.okWindows(id), filename)

    def hasFile(self, id, filename):
        return os.path.isfile(self.getFileName(id, filename))

    def openLocalFile(self, id, filename):
        filename = os.path.join(self.path, self.okWindows(id), filename)
        return open(filename, 'rb')

    def getMD5(self, id, filename):
        hash_md5 = hashlib.md5()
        with open(self.getFileName(id, filename), "rb") as f:
            for chunk in iter(lambda: f.read(4096), b""):
                hash_md5.update(chunk)
        return hash_md5.hexdigest()

    def storeFile(self, id, filename, response: requests.Response):
        target = self.getFileName(id, filename)
        xid = id
        if self.useWindowsPaths:
            xid = id.replace(":","")
        os.makedirs(os.path.join(self.path, xid),exist_ok=True)
        with open(target + '-tmp', 'wb') as out_file:
            for chunk in response.iter_content(1024):
                out_file.write(chunk)
        if os.path.isfile(target):
            os.remove(target)
        os.rename(target + '-tmp', target)
        del response

    def storeFileData(self, id:str, filename:str, data:bytes):
        def chunked(n):
            for i in range(0, len(data), n):
                yield data[i:i + n]

        target = self.getFileName(id, filename)
        xid = id
        if self.useWindowsPaths:
            xid = id.replace(":", "")
        os.makedirs(os.path.join(self.path, xid), exist_ok=True)
        with open(target + '-tmp', 'wb') as out_file:
            for chunk in chunked(1024):
                out_file.write(chunk)
        if os.path.isfile(target):
            os.remove(target)
        os.rename(target + '-tmp', target)
        del data

    def getManifest(self, id) -> List[OdkxLocalFile]:
        pathDir = os.path.join(self.path, self.okWindows(id))
        if os.path.isdir(pathDir):
            listFilDir = os.listdir(pathDir)
        else:
            listFilDir = []
        return [OdkxLocalFile(**{'filename':f, 'md5hash': self.getMD5(id,f)}) for f in listFilDir if os.path.isfile(os.path.join(pathDir, f))]

    def copyLocalFiles(self, oldStorePath):
        print("copying files from:", oldStorePath, " to:", self.path)
        copy_tree(oldStorePath, self.path)


class OdkxLocalTable(object):
    def __init__(self, tableId: str, engine: sqlalchemy.engine.Engine, schema: str, attachment_store_path: Optional[str], useWindowsCompatiblePaths: bool, storage):
        self.tableId = tableId
        self._storage = storage
        self.schema = schema
        self.attachments = FilesystemAttachmentStore(os.getcwd() if attachment_store_path is None else attachment_store_path, useWindowsPaths=storage.useWindowsCompatiblePaths)
        self.engine: sqlalchemy.engine.Engine = engine
        self.genericCols = ['id', 'rowETag', 'savepointTimestamp', 'dataETagAtModification', 'savepointCreator', 'formId', 'savepointType', 'lastUpdateUser']
        self.colAccess = ['defaultAccess',  'groupModify', 'groupPrivileged', 'groupReadOnly', 'rowOwner']

    def getTableDefinition(self) -> OdkxServerTableDefinition:
        return self._storage.getCachedTableDefinition(self.tableId)

    def getLocalDataETag(self):
        with self.engine.connect() as c:
            rs = c.execute(text(f"""select "dataETag" from {self.schema}.status_table where "table_name" = :tableid order by sync_date desc limit 1
                """), tableid=self.tableId)
            for row in rs:
                return row[0]
        return str('')

    def getInitialDataETag(self):
        with self.engine.connect() as c:
            rs = c.execute(text(f"""select "dataETag" from {self.schema}.status_table where "table_name" = :tableid order by sync_date asc limit 1
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

    def _safeSql(self, sql, transaction: sqlalchemy.engine.Connection=None):
        if transaction is not None:
            transaction.execute(sql)
        else:
            with self.engine.begin() as con:
                con.execute(sql)

    def updateLocalStatusDb(self, dataETag, connection: sqlalchemy.engine.Connection=None):
        sql = f"""INSERT INTO {self.schema}.status_table ("table_name", "dataETag", "sync_date")
                 VALUES ('{self.tableId}', '{dataETag}', '{str(datetime.datetime.now())}')"""
        self._safeSql(sql, connection)


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
        dct['defaultAccess'] = r.filterScope.defaultAccess
        dct['groupModify'] = r.filterScope.groupModify
        dct['groupPrivileged'] = r.filterScope.groupPrivileged
        dct['groupReadOnly'] = r.filterScope.groupReadOnly
        dct['rowOwner'] = r.filterScope.rowOwner
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
            for rowset in remoteTable.getDiffGenerator(dataETag=self.getLocalDataETag(), getFullLog=True):
                last_rs = rowset
                if (len(rowset.rows) > 0):
                    #transaction.execute(st.insert(), [self.row_asdict(x) for x in rowset.rows])
                    transaction.execute(st.insert().values([self.row_asdict(x) for x in rowset.rows]))
        if not last_rs is None:
            return last_rs.dataETag

    def isMissingFiles(self, rowId: str, target_file_list: List[str], manifest_files: List[str], other_manifest_files: List[str] = None):
        missing_files = [x for x in target_file_list if x not in manifest_files]
        if other_manifest_files:
            missing_files.extend([x for x in other_manifest_files if x not in manifest_files])
        if len(missing_files) > 0:
            print("MISSING FILES (trying again on next sync) for ", rowId, str(missing_files), "\ngot\n", str(manifest_files))
            return True

    def attachmentsToDownload(self, remoteManifest: List[str], rowId: str):
        def filter_md5(f):
            return not (self.attachments.hasFile(rowId, f.filename) and self.attachments.getMD5(rowId, f.filename) == f.md5hash) and f.contentLength
        to_fetch = list(filter(filter_md5, remoteManifest))
        return to_fetch

    def attachmentsToUpload(self, remoteManifest, rowId: str):
        to_push = []
        for f in self.attachments.getManifest(rowId):
            remoteFileProperties = next((x for x in remoteManifest if x.filename == f.filename), None)
            if remoteFileProperties:
                if remoteFileProperties.md5hash == f.md5hash:
                    continue
            print(rowId, f.filename)
            with self.attachments.openLocalFile(rowId, f.filename) as datafile:
                data = datafile.read()
            to_push.append((f, data))
        return to_push

    def downloadAttachments(self, remoteTable: OdkxServerTable, rowId: str, target_file_list: List[str]):
        remoteManifest = remoteTable.getAttachmentsManifest(rowId)
        remote_manifest_files = [f.filename for f in remoteManifest]
        to_fetch = self.attachmentsToDownload(remoteManifest, rowId)
        if to_fetch:
            store_attachments = remoteTable.getAttachments(rowId, to_fetch)
            if store_attachments.status_code != 200:
                if self.isMissingFiles(rowId, target_file_list, []):
                    return False
            else:
                store_files = multi_decoder.MultipartDecoder.from_response(store_attachments)
                for part in store_files.parts:
                    self.attachments.storeFileData(rowId, str(part.headers[b'Content-Disposition']).split("=")[1][1:-2],
                                                   part.content)
                local_manifest_files = [f.filename for f in self.attachments.getManifest(rowId)]
                if self.isMissingFiles(rowId, target_file_list, remote_manifest_files, local_manifest_files):
                    return False
        return True

    def uploadAttachments(self, remoteTable: OdkxServerTable, rowId: str, target_file_list: List[str]):
        remoteManifest = remoteTable.getAttachmentsManifest(rowId)
        local_manifest_files = [f.filename for f in self.attachments.getManifest(rowId)]
        to_push = self.attachmentsToUpload(remoteManifest, rowId)
        if to_push:
            res = remoteTable.putAttachments(rowId, *zip(*to_push))
            print(res)

        remote_manifest_files = [f.filename for f in remoteManifest]
        if self.isMissingFiles(rowId, target_file_list, local_manifest_files, remote_manifest_files):
            return False
        return True

    def _writeSuccess(self, table, id, state_col):
        with self.engine.connect() as c:
            c.execute(sqlalchemy.sql.text(f"""update {self.schema}."{table}" set {state_col}='synced' where id=:rowid"""), rowid=id)

    def _sync_attachments(self, remoteTable: OdkxServerTable, state_col:str = "state", localTable: str = None):
        """ Sync the attachments for the rowids in state "sync_attachments"
        """
        attach_cols = [x.elementKey for x in self.getTableDefinition().columns if x.elementType == 'rowpath']
        if localTable:
            mode = "pushing"
            with self.engine.begin() as c:
                res = c.execute("""SELECT column_name FROM information_schema.columns
                             WHERE table_schema = '{schema}' AND table_name = '{table}'
                             AND column_name LIKE '%%_uriFragment';
                          """.format(schema=self.schema, table=localTable))
                resColumns = res.fetchall()
            attach_cols = [col[0] for col in resColumns]
            table = localTable
        else:
            mode = "pulling"
            table = self.tableId

        if len(attach_cols) == 0:
            return
        ids = []
        files_by_id = {}
        with self.engine.connect() as c:
            result = c.execute("""select DISTINCT ON (id) id, {cols} from {schema}."{table}" where {state_col} = 'sync_attachments'""".format(
                schema=self.schema, table=table,
                state_col=state_col, cols=",".join(['"{c}"'.format(c=c) for c in attach_cols])
            ))
            for r in result:
                ids.append(r['id'])
                files_by_id[r['id']] = [r[x] for x in attach_cols if not r[x] is None]
        print(mode + " ", len(ids), " rows attachments")

        for id in ids:
            if mode == "pushing":
                if self.uploadAttachments(remoteTable, id, files_by_id[id]):
                    self._writeSuccess(table, id, state_col)
            elif mode == "pulling":
                if self.downloadAttachments(remoteTable, id, files_by_id[id]):
                    self._writeSuccess(table, id, state_col)

    def _staging_to_log(self, connection: sqlalchemy.engine.Connection = None, stagingtable = None):
        if stagingtable is not None:
            st = stagingtable
        else:
            st = self._getStagingTable()
        colnames = [x.name for x in st.columns]
        fields = ','.join(['"{colname}"'.format(colname=colname) for colname in colnames])
        prefixed_fields = ','.join(['stage."{colname}"'.format(colname=colname) for colname in colnames])
        sql="""
        insert into {schema}."{logtable}" ({fields})
        select {prefixed_fields}
        from {schema}."{stagingtable}" stage left outer join {schema}."{logtable}" log
        on stage."rowETag" = log."rowETag"
        where log."rowETag" is null
        """.format(
                schema= self.schema,
                logtable=self.tableId+'_log',
                fields=fields,
                stagingtable=self.tableId+'_staging',
                prefixed_fields=prefixed_fields
            )
        self._safeSql(sql, connection)


    def hasIncomingChanges(self, remoteTable: OdkxServerTable) -> bool:
        """
        :param remoteTable:
        :return: true if there are changes on the server that have not been downloaded yet. use the "sync" function to download these changes
        """
        return remoteTable.getdataETag() != self.getLocalDataETag()

    def _sync_iter_pull(self, remoteTable: OdkxServerTable, no_attachments: bool = False):
        if remoteTable.getdataETag() == self.getLocalDataETag():
            ## we still need to check if we need to download attachments
            self._sync_attachments(remoteTable)
            return False
        new_etag = self.stageAllDataChanges(remoteTable)
        st = self._getStagingTable()
        colnames = [x.name for x in st.columns]
        with self.engine.begin() as trans:
            # delete rows to be updated
            trans.execute("""delete from {schema}."{table}" where id in (select id from {schema}."{stagingtable}")""".format(
                schema=self.schema, table=self.tableId, stagingtable=self.tableId + '_staging'
            ))
            fields = ','.join(['"{colname}"'.format(colname=colname) for colname in colnames])
            fields_v = ','.join(['st."{colname}"'.format(colname=colname) for colname in colnames])
            # insert new and updated rows
            insert_sql = """
            WITH latest AS (
                SELECT p."rowETag",
                       ROW_NUMBER() OVER(PARTITION BY p.id
                                            ORDER BY p."savepointTimestamp" DESC, p."rowETag" DESC) AS rk
                 FROM {schema}."{stagingtable}" p)
            insert into {schema}."{table}" ({fields},state) select {fields_v}, 'sync_attachments' as state
            from {schema}."{stagingtable}" st inner join
            (select latest."rowETag" from latest
            WHERE latest.rk = 1) f
            ON f."rowETag" = st."rowETag"
            """.format(
                schema=self.schema, table=self.tableId, stagingtable=self.tableId+'_staging', fields=fields, fields_v=fields_v)
            #print(insert_sql)
            trans.execute(insert_sql)
            self._staging_to_log(trans, stagingtable=st)
            self.updateLocalStatusDb(new_etag, trans)
        if not no_attachments:
            self._sync_attachments(remoteTable)
        return True


    def _qryState(self, localTable: str, tableDefinition: List[OdkxServerColumnDefinition], state: List[str], force_push: bool):
        locChanges = self._getTableMeta(localTable)
        locTable = self._getTableMeta(self.tableId)
        locChangesCols = [x.name for x in locChanges.columns]
        locTableCols = [x.name for x in locTable.columns]
        colsTakeLocally = [x.elementKey for x in tableDefinition if x.isMaterialized()] + self.genericCols
        if force_push:
            # take row ETag directly from server, making push always work even if we updated old data
            # it can still conflict but now only because somebody uploaded between us pulling and us pushing
            colsTakeLocally = [x for x in colsTakeLocally if not x in ['rowETag']]

        colsTakeLocally = [x for x in locChangesCols if x in colsTakeLocally]
        col_list = ['l."{x}"'.format(x=x) if x in colsTakeLocally else 'r."{x}"'.format(x=x) for x in locTableCols]
        qry = """SELECT {col_list} FROM {schema}."{loctable}" l LEFT OUTER JOIN {schema}."{table}" r ON l.id = r.id WHERE l.state in ({state})""".format(
            schema=self.schema,
            loctable=localTable,
            table=self.tableId,
            col_list=','.join(col_list),
            state=','.join(["'" + x + "'" for x in state])
        )
        return qry

    def _getHistoryBatch(self, localTable: str, state, mapping:dict = None):

        if mapping is not None:
            mappedCols = [v + '\" as \"' + k for k,v in mapping['mapping'].items()]
            unchangedCols = mapping['common']
            colsToTake = self.genericCols + mappedCols + unchangedCols + self.colAccess
        else:
            with self.engine.begin() as c:
                res = c.execute(f"""SELECT column_name FROM information_schema.columns
                             WHERE table_schema = '{self.schema}' AND table_name = '{localTable}';""")
                resColumns = res.fetchall()
            colsToTake = [col[0] for col in resColumns if col[0] != "state" and col[0] != "state_upload"]

        # take row ETag directly from server, making push always work even if we updated old data
        # it can still conflict but now only because somebody uploaded between us pulling and us pushing
        colsToTake = [x for x in colsToTake if x not in ['rowETag']]

        qry = """SELECT loc."{col_list}", rev."rowETag"
                 FROM {schema}."{loctable}" loc
                 LEFT JOIN {schema}."{loctable}_rev" as rev
                 ON loc.id = rev.id
                 WHERE state_upload in ({state})""".format(
            schema=self.schema,
            loctable=localTable,
            col_list='", loc."'.join(colsToTake),
            state=','.join(["'" + x + "'" for x in state])
        )
        return qry


    def _sync_iter_push(self, remoteTable: OdkxServerTable, localTable: str, mapping: dict = None,
                        fullHistory: bool = False, force_push: bool = False, no_attachments: bool = False):
        definition = remoteTable.getTableDefinition().columns
        id_list_good = []
        id_list_conflict = []
        if not fullHistory:
            state_col = "state"
        else:
            state_col = "state_upload"
            id_and_rowETag_list = []

        if (self.hasUnresolvedConflicts(localTable, state_col)):
            raise Exception("unresolved conflicts, cannot push changes")

        records = []
        if not fullHistory:
            state_qry = self._qryState(localTable, tableDefinition=definition, state=['new', 'modified'], force_push=force_push)
            with self.engine.connect() as c:
                for row in c.execute(state_qry):
                    records.append(self.row2rec(row, definition, remoteTable.connection.user))
                    dataETag = self.getLocalDataETag()
        else:
            state_qry = self._getHistoryBatch(localTable, state=['historyUpload'], mapping=mapping)
            with self.engine.connect() as c:
                for row in c.execute(state_qry):
                    records.append(self.row2rec(row, definition, remoteTable.connection.user, full=False))
                    dataETag = remoteTable.getdataETag()

        if (len(records) == 0):
            return None
        json = {'rows': records, 'dataETag': dataETag}

        rs = remoteTable.alterDataRows(json)

        for outcome in rs['rows']:
            if outcome['outcome'] == 'IN_CONFLICT':
                id_list_conflict.append(outcome['id'])
                if fullHistory:
                   raise Exception("During this process. No one should update the server")
            else:
                id_list_good.append(outcome['id'])
                if fullHistory:
                    id_and_rowETag_list.append([outcome['id'], outcome['rowETag']])

        def chunks(l, n):
            """Yield successive n-sized chunks from l."""
            for i in range(0, len(l), n):
                yield l[i:i + n]

        for chunk in chunks(id_list_good, 10):
            qry = """update {schema}."{localtable}" set {state_col}='sync_attachments' where id in ({ids})""".format(
                schema=self.schema,
                localtable=localTable,
                state_col=state_col,
                ids=','.join(["'{c}'".format(c=c) for c in chunk])
            )
            if fullHistory:
                qry = qry + f""" and {state_col} LIKE 'historyUpload' """
            with self.engine.begin() as c:
                c.execute(qry)
        for chunk in chunks(id_list_conflict, 10):
            qry = """update {schema}."{localtable}" set {state_col}='conflict' where id in ({ids})""".format(
                schema=self.schema,
                localtable=localTable,
                state_col=state_col,
                ids=','.join(["'{c}'".format(c=c) for c in chunk])
            )
            if fullHistory:
                qry = qry + f""" and {state_col} LIKE 'historyUpload' """
            with self.engine.begin() as c:
                print(qry)
                c.execute(qry)
        if fullHistory:
            df = pd.DataFrame(id_and_rowETag_list, columns=["id", "rowETag"])
            ids = df['id'].tolist()
            with self.engine.begin() as trans:
                trans.execute("""delete from {schema}."{table}" where id in ({ids})""".format(
                    schema=self.schema,
                    table=localTable+"_rev",
                    ids=','.join([f"'{id_}'" for id_ in ids])
                ))
            df.to_sql(localTable+"_rev", self.engine, schema=self.schema, if_exists='append', index=False)
        if not no_attachments and not fullHistory:
            self._sync_attachments(remoteTable, state_col, localTable)

    def row2rec(self,row: dict, definition: List[OdkxServerColumnDefinition], default_user: str, full: bool = True):
        datacols = [x.elementKey for x in definition if x.isMaterialized()]
        ## TODO refactor
        if full:
           for c in datacols:
               if not c in row.keys():
                   raise Exception("schema's have diverged: on ODKX server i got column {c} but i couldn't find it locally. please fix.".format(c=c))
        tupColAccess = ('defaultAccess',  'groupModify', 'groupPrivileged', 'groupReadOnly', 'rowOwner')
        tupColnames = tuple(datacols)
        filterScope = {}
        orderedColumns = []
        for c in tupColAccess:
            if (c == 'defaultAccess' and row[c] is None):
                filterScope[c] = 'FULL'
            else:
                filterScope[c] = row[c]
        for c in tupColnames:
            if full:
                orderedColumns.append({'column':c,'value':row[c]})
            else:
                if c in row.keys():
                    orderedColumns.append({'column':c,'value':row[c]})

        result = {}
        result['filterScope'] = filterScope
        result['orderedColumns'] = orderedColumns
        fix_row_fields = ['createUser', 'lastUpdateUser', 'dataETagAtModification', 'rowETag', 'savepointCreator',
                          'formId', 'locale', 'savepointType', 'savepointTimestamp', 'deleted', 'id']
        for k in [ x for x in row.keys() if x in fix_row_fields]:
            if (k == 'savepointTimestamp' and row[k] is None):
                result[k] = str(datetime.datetime.now())
            elif (k == 'savepointTimestamp' and isinstance(row[k], datetime.datetime)):
                result[k] = str(row[k])
            elif (k == 'savepointType' and row[k] is None):
                result[k] = 'COMPLETE'
            elif (k in ('createUser','lastUpdateUser', 'savepointCreator') and row[k] is None):
                result[k] = default_user
            else:
                result[k] = row[k]
        return result

    def _cache_manifest(self, remoteTable: OdkxServerTable):
        session = self._storage.Session()
        OdkTableManifestCache(session, self._storage, remoteTable.connection).do_sync(
            self.tableId, remoteTable.getFileManifest()
        )

    def sync(self, remoteTable: OdkxServerTable, local_changes_prefix: Optional[str] = None, force_push: bool = False, no_attachments: bool = False):
        """

        :param remoteTable: the OdkxServerTable you want to sync with
        :param local_changes_prefix: the prefix of the local changes to push (when left empty , it will not push, only pull)
        :param force_push: if the server has more recent changes than our local changes, push anyway, overwriting the changes on the server
        :param no_attachments: ignore the attachments for now (the rows will remain in sync_attachments state, so they will be synced next time when you don't pass no_attachments)
        :return:
        """
        self._cache_manifest(remoteTable)
        session = self._storage.Session()
        self._storage._cache_table_defintion(remoteTable.getTableDefinition(), session)
        self._sync_iter_pull(remoteTable, no_attachments)
        if local_changes_prefix is not None:
            localTable = self.tableId + '_' + local_changes_prefix
            self._sync_iter_push(remoteTable, localTable, force_push=force_push, no_attachments=no_attachments)
            rs = self._sync_iter_pull(remoteTable, no_attachments=no_attachments)
            return rs

    def _getTableMeta(self, tablename: str) -> sqlalchemy.Table:
        meta = sqlalchemy.MetaData()
        meta.reflect(self.engine, schema=self.schema, only=[tablename])
        return meta.tables.get(self.schema+ '.' + tablename)


    def _getHashedColumns(self, table_name):
        tm = self._getTableMeta(table_name)
        exclude_columns = ['hash', 'state', 'dataETagAtModification', 'formId', 'rowETag', 'savepointTimestamp',
                           'savepointCreator',
                           'createUser', 'lastUpdateUser', 'locale', 'savepointType']
        columns_to_hash = [x.name for x in tm.columns
                           if not x.name in exclude_columns]
        return columns_to_hash

    def fillHashColumn(self, table_name):
        columns_to_hash = self._getHashedColumns(table_name)
        qry = """UPDATE {schema}."{table}" set hash=md5(ROW({cols})::TEXT)""".format(
            schema=self.schema,
            table=table_name,
            cols=','.join(['"{c}"'.format(c=c) for c in columns_to_hash]))
        with self.engine.begin() as c:
            c.execute(qry)

    def _copyMissingData(self, tn1, tn2):
        t1 = self._getTableMeta(tn1)
        t2 = self._getTableMeta(tn2)
        t1_cols = [x.name for x in t1.columns]
        t2_cols = [x.name for x in t2.columns]
        common_cols = [x for x in t1_cols if x in t2_cols]
        qry = """
            INSERT INTO {schema}."{tn2}" ({cols}) select {cols} from {schema}."{tn1}" where not {schema}."{tn1}".id in (select id from {schema}."{tn2}")
        """.format(schema=self.schema, tn1= tn1, tn2 = tn2, cols=",".join(['"{c}"'.format(c=x) for x in common_cols]))
        with self.engine.begin() as c:
            c.execute(qry)

    def _fillUUIDs(self, tn, uuid_col):
        qry = """
            UPDATE {schema}."{tn}" set "{uuid_col}"=md5(random()::text || clock_timestamp()::text)::uuid where "{uuid_col}" is null
        """.format(schema=self.schema, tn=tn, uuid_col=uuid_col)
        with self.engine.begin() as c:
            c.execute(qry)

    def resetColumns(self, table: str, col_list: List[str],external_id: str):
        """ reset the given columns in this table from the latest version (eg you blanked them and you don't want the blanked version to be uploaded)
        warning: external_id needs to be a UNIQUE field!
        """
        if col_list:
            col_expr = ",".join([
                """"{c}" = {schema}."{master}"."{c}" """.format(schema=self.schema, tn=table, master=self.tableId, c=x)
                for x in col_list])

            qry = """
                UPDATE {schema}."{tn}" set {col_expr} FROM {schema}."{master}" WHERE {schema}."{tn}"."{extid}" = {schema}."{master}"."{extid}"
            """.format(
                schema=self.schema,
                master = self.tableId,
                tn = table,
                extid = external_id,
                col_expr = col_expr
            )
            with self.engine.begin() as c:
                c.execute(qry)

    def localSyncFromDataframe(self, source_prefix: str, external_id_column: str, df: pd.DataFrame, localSyncMode: LocalSyncMode = LocalSyncMode.FULL):
        """
        to sync changes from a dataframe:
          * first do initializeExternalSource
          * then do this function, with the same prefix. give a UNIQUE column as external_id (if you use the odkx ID then just pass 'id')
          * then do sync with the remote table and give the external source prefix as a parameter

        this will stage changes for syncing from the given database. note that every column missing from the database will NOT BE TOUCHED
        (it uses resetColumns to reset all columns that were not in the dataframe).


        note that if you didn't sync up the pending changes and you want to re-do the local sync, you must resetLocalChanges first (a localSync
        won't work when there are already changes pending)

        :param source_prefix: the prefix of the externalSource
        :param external_id_column: the "ID" you want to use as primary key for this operation. if you just want to use the ODKX id, pas "id"
        :param df: a dataframe containing at least the external_id_column and then one or more columns that also appear in the ODKX table.
        :param localSyncMode: FULL or ONLY_NEW_RECORDS. when ONLY_NEW_RECORDS then modifications will not be synced only additions.
        :return:
        """
        staging_tn = self.tableId + '_' + source_prefix + '_staging'
        hash_cols = self._getHashedColumns(staging_tn)
        missing_cols = [x for x in hash_cols if not x in list(df)]
        qry = """DELETE FROM {schema}."{tn}" """.format(schema=self.schema, tn=staging_tn)
        with self.engine.begin() as c:
            c.execute(qry)
        df.to_sql(staging_tn, schema=self.schema, if_exists='append', index=False, con=self.engine)
        self.resetColumns(staging_tn, missing_cols, external_id_column)
        self.localSyncFromStagingTable(source_prefix, external_id_column, localSyncMode)

    def hasPendingLocalChanges(self, source_prefix: str):
        def_tn = self.tableId + '_' + source_prefix
        q_test = """
        select count(id) as aantal from {schema}."{deftn}" where not state in ('unchanged', 'synced')
        """.format(schema=self.schema, deftn=def_tn)
        with self.engine.connect() as c:
            res = c.execute(q_test)
            for r in res:
                if r['aantal'] > 0:
                    return True
        return False

    def hasUnresolvedConflicts(self, def_tn: str, state_col:str):
        q_test = """
        select count(id) as aantal from {schema}."{deftn}" where {state_col} in ('conflict')
        """.format(schema=self.schema, deftn=def_tn, state_col=state_col)
        with self.engine.connect() as c:
            res = c.execute(q_test)
            for r in res:
                if r['aantal'] > 0:
                    return True
        return False


    def resetLocalChanges(self, source_prefix: str):
        def_tn = self.tableId + '_' + source_prefix
        q_test = """
        delete from {schema}."{deftn}"
        """.format(deftn=def_tn,schema=self.schema)
        with self.engine.begin() as c:
            c.execute(q_test)



    def localSyncFromStagingTable(self, source_prefix: str, external_id_column: str, localSyncMode: LocalSyncMode = LocalSyncMode.FULL):
        """
        DO NOT USE THIS FUNCTION if you are writing an interactive editing application.
        this function takes the sync time as edit time, which is not right for editing apps

        an editing app should do the following when saving a record:
         * taking the record from the master table and making modifications
         * setting savepointTimestamp / lastUpdateUser / savepointType accordingly
         * delete old record from the externalsource table
         * replace it by the updated record

        this is only for batch syncing with systems that don't know about record versions and edit timestamps

        it will consider the following tables:
         * [tableId]_[externalsource]_staging --> the staging table. just dump data there (to create a staging table, see SqlLocalStorage.initializeExternalSource)
         * [tableId]_[externalsource] --> the table containing the modifications coming from externalsource

        this function will compare the staging with the modif tables, apply changes to the modif table while setting the relevant metadata fields
        when an update happens on the odkX server on a record after the record has been modified by the localSync, a conflict will arise and the sync
        will fail. to prevent this, one can use force_push=True while syncing

        the table t_prefix contains the state after the previous sync. the staging table (t_prefix_staging) is
        compared with this table to determine what changed.
        :param source_prefix: the name of the external source (use lowercase no spaces)
        :param external_id_column: the primary key field of a record IN THE EXTERNAL SYSTEM (so not the odkx ID field)
        :return:
        """

        staging_tn = self.tableId + '_' + source_prefix + '_staging'
        def_tn = self.tableId + '_' + source_prefix
        self._copyMissingData(self.tableId, def_tn)
        self.fillHashColumn(def_tn)

        if (self.hasPendingLocalChanges(source_prefix)):
            raise Exception("unsynced local changes still pending. sync first")


        qry = """
            UPDATE {schema}."{stagingtable}" set id = {schema}."{realtable}".id, state='modified', "rowETag" = {schema}."{realtable}"."rowETag"
            FROM {schema}."{realtable}" WHERE {schema}."{stagingtable}"."{extid}" = {schema}."{realtable}"."{extid}"
        """
        with self.engine.begin() as c:
            c.execute(f"""update {self.schema}."{staging_tn}" set state=null""")
            c.execute(f"""update {self.schema}."{staging_tn}" set deleted=False where deleted is null""")
            c.execute(qry.format(schema=self.schema, stagingtable=staging_tn, realtable=def_tn, extid=external_id_column))
            c.execute(qry.format(schema=self.schema, stagingtable=staging_tn, realtable=self.tableId, extid=external_id_column))

        self.fillHashColumn(staging_tn)
        qry = """
            UPDATE {schema}."{stagingtable}" set "rowETag" = {schema}."{realtable}"."rowETag", state='unchanged'
            FROM {schema}."{realtable}" WHERE {schema}."{stagingtable}".id = {schema}."{realtable}".id AND
            {schema}."{stagingtable}".hash = {schema}."{realtable}".hash
        """
        with self.engine.begin() as c:
            c.execute(qry.format(schema=self.schema, stagingtable=staging_tn, realtable=def_tn))
            c.execute(f"""update {self.schema}."{staging_tn}" set state='new', "createUser" = 'localSync' where state is null""")
            c.execute("""update {schema}."{stagingtable}" set "savepointTimestamp"=now(),
                        "savepointCreator"='localSync',
                        "savepointType"='COMPLETE',
                        "formId"='localSync',
                        "lastUpdateUser"='localSync'
                        where state in ('new', 'modified')""".format(schema=self.schema, stagingtable=staging_tn))
            c.execute("""update {schema}."{stagingtable}" set "dataETagAtModification"='{etag}' where state in ('new', 'modified')""".format(schema=self.schema, stagingtable=staging_tn, etag=self.getLocalDataETag()))

        self._fillUUIDs(staging_tn, 'id')
        self._fillUUIDs(staging_tn, 'rowETag')

        with self.engine.begin() as c:
            w = ""
            if localSyncMode == LocalSyncMode.ONLY_NEW_RECORDS:
                w = " WHERE state in ('new') "
            if localSyncMode == LocalSyncMode.ONLY_EXISTING_RECORDS:
                w = " WHERE state in ('modified') "
            c.execute("""delete from {schema}."{def_tn}" where {schema}."{def_tn}".id in (select {schema}."{stagingtable}".id from {schema}."{stagingtable}" {w}) 
            """.format(schema=self.schema, def_tn=def_tn, stagingtable=staging_tn, w=w))
        self._copyMissingData(staging_tn, def_tn)

    def archiveTables(self, remoteTable: OdkxServerTable, historyPrefix: str, deleteOldTables: bool = False):
        """ Archive the local table and all the related tables
        """
        with self._storage.engine.begin() as trans:
            if deleteOldTables:
                sql = """  DO
                         $$
                         DECLARE
                             row record;
                         BEGIN
                             FOR row IN SELECT "table_name" FROM information_schema.tables WHERE "table_schema"='{schema}'
                                 and ("table_name" like '{tableId}\_%%' or "table_name" = '{tableId}')
                             LOOP
                                 EXECUTE 'ALTER TABLE {schema}.' || row."table_name" || ' RENAME TO _archive_{history_nb}_' || row."table_name";
                             END LOOP;
                         END;
                         $$;
                         """.format(schema=self.schema, tableId=self.tableId, history_nb=historyPrefix)
            else:
                sql = """  DO
                         $$
                         DECLARE
                             row record;
                         BEGIN
                             FOR row IN SELECT "table_name" FROM information_schema.tables WHERE "table_schema"='{schema}'
                                 and ("table_name" like '{tableId}\_%%' or "table_name" = '{tableId}')
                             LOOP
                                 EXECUTE 'CREATE TABLE archive_{history_nb}_' || row."table_name" ||
                                 'as SELECT * FROM {schema}.' || row."table_name";
                             END LOOP;
                         END;
                         $$;
                         """.format(schema=self.schema, tableId=self.tableId, history_nb=historyPrefix)
            self._safeSql(sql, trans)
            if deleteOldTables:
                self.updateLocalStatusDb(None, trans)
                remoteTable.deleteTable(True)

    def _checkStateUpload(self, table):
        with self.engine.begin() as c:
            res = c.execute("""SELECT count(id) FROM {schema}."{table}"
                               WHERE "{table}"."state_upload" is null
                               OR "{table}"."state_upload" like 'historyUpload';
                            """.format(schema=self.schema, table=table))
            if res.scalar() > 0:
                return True
            else:
                return False

    def _checkIfPreparedUpload(self, table):
        with self.engine.begin() as c:
            res = c.execute("""SELECT * FROM {schema}."{table}" WHERE "state_upload" = 'historyUpload'
                      """.format(schema=self.schema, table=table))
            if res.rowcount == 0:
                return False
            else:
                print('--> Already records prepared to be pushed: ', res.rowcount)
                return True

    def _prepareUpload(self, table):
        with self.engine.begin() as c:
            res = c.execute("""UPDATE {schema}."{table}" SET state_upload = 'historyUpload' WHERE "rowETag" in (
                                   SELECT DISTINCT ON (id) "rowETag" FROM {schema}."{table}"
                                   WHERE "{table}"."state_upload" is distinct from 'sync_attachments'
                                   AND "{table}"."state_upload" is distinct from 'synced'
                                   AND "{table}"."state_upload" is distinct from 'conflict'
                                   ORDER BY id ASC, "savepointTimestamp" ASC
                              );
                      """.format(schema=self.schema, table=table))
            print('--> Preparing to push records: ', res.rowcount)

    def _addStateUploadColumn(self, table):
        with self.engine.begin() as con:
            res = con.execute("""SELECT column_name FROM information_schema.columns
                         WHERE table_schema = '{schema}' AND table_name = '{table}'
                         AND column_name LIKE 'state_upload';""".format(schema=self.schema, table=table))
        if not res.first():
            with self.engine.begin() as con:
                con.execute("""ALTER TABLE {schema}."{table}" ADD COLUMN state_upload VARCHAR;
                      """.format(schema=self.schema, table=table))

    def uploadHistory(self, remoteTable: OdkxServerTable, historyTable: str = None, mapping: dict = None):
        """
        Upload history by batch from an history table.
        Each batch contains only unique occurence of rowids.
        The savepointTimestamp is used to order the records as we don't have historization locally with the dataETagAtModification.
        It is possible to rename old columns or even to duplicates old columns in the new table using the mapping dict.

        :param remoteTable: the target table where we will upload the history
        :param historyTable: the table that contains the historic records
        :param mapping: the mapping dict defines which old columns are renamed or duplicated (if the old column is present in the new table definition).
            The keys are the old columns, the values are the corresponding new columns.
        :return:
        """
        if historyTable is None:
            historyTable = self.tableId + "_log"
        self._storage._createRevisionTable(historyTable)
        self._addStateUploadColumn(historyTable)

        print('--> Importing history table: ', historyTable, " into remote table: ", remoteTable.tableId)
        while self._checkStateUpload(historyTable):
            if not self._checkIfPreparedUpload(historyTable):
                self._prepareUpload(historyTable)
            self._sync_iter_push(remoteTable, historyTable, mapping=mapping, fullHistory=True)
        print('--> Syncing the attachments')
        self._sync_attachments(remoteTable, "state_upload", historyTable)
