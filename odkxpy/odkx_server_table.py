import json
from collections import namedtuple
from .odkx_connection import OdkxConnection

OdkxServerTableInfo = namedtuple('OdkxServerTableInfo', [
        'tableId', 'dataETag', 'schemaETag', 'selfUri', 'definitionUri', 'dataUri', 'instanceFilesUri', 'diffUri', 'aclUri', 'tableLevelManifestETag'
])

OdkxServerTableInfo.__new__.__defaults__ = (None, ) * len(OdkxServerTableInfo._fields)


class OdkxServerColumnDefinition(object):
    def __init__(self, elementKey=None, elementName = None, elementType= None, childElements=None, parentElement=None):
        self.elementKey = elementKey
        self.elementName = elementName
        self.elementType = elementType
        self.childElements = childElements
        self.parentElement = parentElement
        self.properties = {}

    def isMaterialized(self):
        """
        :return: true if this column will be represented physically in a table
        """
        if self.parentElement is not None and self.parentElement.elementType == 'array':
            return False
        if len(self.childElements) > 0:
            if self.elementType == 'array':
                return True
            else:
                return False
        return True

    def __repr__(self):
        rpr =   (' - ' if self.parentElement is not None else '') + 'OdkxServerColumnDefinition' + \
                ('*' if self.isMaterialized() else '') + '(' + self.elementKey + ':' + self.elementType + ')'
        for p in self.properties.keys():
            sv = str(self.properties[p])
            if len(sv) > 40:
                sv = sv[:37]+'...'
            rpr += '\n\t{k}={v}'.format(k=p, v=sv)
        return rpr


OdkxServerTableDefinition = namedtuple('OdkxServerTableDefinition', [
    'schemaETag', 'tableId', 'orderedColumns', 'selfUri', 'tableUri'
])



from .odkx_server_file import OdkxServerFile


class OdkxServerTable(object):
    def __init__(self, con: OdkxConnection, tableId: str, schemaETag: str):
        self.connection = con
        self.tableId = tableId
        self.schemaETag = schemaETag

    def getTableInfo(self):
        return OdkxServerTableInfo(**self.connection.GET('tables/' + self.tableId))

    def getFileManifest(self):
        return [OdkxServerFile(**x) for x in self.connection.GET("manifest/2/" + self.tableId)['files']]

    def getFile(self, path):
        return self.connection.GET('files/2/tables/' + self.tableId + ('' if path.startswith('/') else '/') + path)

    def putFile(self, content_type, payload, path):
        headers = {"Content-Type": content_type}
        if type(payload) in(dict, list):
            payload = json.dumps(payload).encode('utf-8')
        if type(payload) == str:
            payload = payload.encode('utf-8')
        return self.connection.POST('/files/2/tables/' + self.tableId + ('' if path.startswith('/') else '/') + path,
                                    headers=headers, data=payload)

    def deleteFile(self, path):
        return self.connection.DELETE('files/2/tables/' + self.tableId + (
            '' if path.startswith('/') else '/') + path)

    def getdataETag(self):
        return self.getTableInfo().dataETag

    def getTableRoot(self):
        return "tables/" + self.tableId + "/ref/" + self.schemaETag

    def getTableDefinition(self):
        t_d = self.connection.GET(self.getTableRoot())
        col_props = [ x for x in self.connection.GET("tables/" + self.tableId + "/properties/2") if x['partition'] == 'Column']


        cols = {}
        for c in t_d['orderedColumns']:
            dd = {}
            dd.update(c)
            del dd['listChildElementKeys']
            cd = OdkxServerColumnDefinition(**dd)
            cd.childElements = []
            for prop in [x for x in col_props if x['aspect'] == cd.elementKey]:
                cd.properties[prop['key']] = prop['value']

            cols[cd.elementKey] = cd
        for c in t_d['orderedColumns']:
            children = json.loads(c['listChildElementKeys'])
            parent = cols[c['elementKey']]
            for c in children:
                cols[c].parentElement = parent
                parent.childElements.append(cols[c])
        return [cols[x['elementKey']] for x in t_d['orderedColumns']]


    #def setTableDefinition(self, json):
    #    return self.connection.PUT(self.getTableRoot(), json)

    def deleteTable(self, are_you_sure: bool):
        """To delete a table
        """
        if not are_you_sure:
            raise Exception("not sure ?")
        return self.connection.DELETE(self.getTableRoot())


    def getTableProperties(self):
        return self.connection.GET('tables/' + self.tableId + "/properties/2")

    def putJsonTableProperties(self, json):
        return self.connection.PUT('tables/' + self.tableId + "/properties/2", json)

#### I GOT HERE REFACTORING

    def getTableAcl(self):
        return self.connection.GET('tables/' + self.tableId + '/acl')

    def getAllDataChanges(self, dataETag=None, cursor=None, fetchLimit=None):
        params = {'data_etag': dataETag, 'cursor': cursor, 'fetchLimit': fetchLimit}
        return self.connection.GET(self.getTableRoot() + "/diff", params)

    def getChangesets(self, dataETag=None, sequence_value=None):
        # Not working - Problem API ?
        params = {'data_etag': dataETag, 'sequence_value': sequence_value}
        return self.req_odkx_server(self.getTableRoot() + "/diff/changeSets", params)

    def getChangesetRows(self, dataETag, cursor=None, fetchLimit=None, active_only=None):
        params = {'active_only': active_only, 'cursor': cursor, 'fetchLimit': fetchLimit}
        return self.req_odkx_server(self.getTableRoot() + "/diff/changeSets/" + dataETag, params)

    def getAllDataRows(self, cursor=None, fetchLimit=None):
        params = {'cursor': cursor, 'fetchLimit': fetchLimit}
        return self.req_odkx_server(self.getTableRoot() + "/rows", params)

    def getDataRow(self, rowId):
        return self.req_odkx_server(self.getTableRoot() + "/rows/" + rowId)

    def getAttachmentsManifest(self, rowId):
        response = self.session.get(
            self.server + self.appID + '/' + self.getTableRoot() + "/attachments/" + rowId + "/manifest")
        return self.treatResponse(response)

    def getAttachment(self, rowId, name, stream, timeout):
        return self.session.get(
            self.server + self.appID + '/' + self.getTableRoot() + "/attachments/" + rowId + "/file/" + name,
            stream=stream, timeout=timeout)

    def getAttachments(self, rowId, data):
        # Not working - TODO
        headers = {"Content-Type": "application/json"}
        payload = json.dumps(data)
        return self.session.post(
            self.server + self.appID + '/' + self.getTableRoot() + "/attachments/" + rowId + "/download",
            headers=headers, data=payload)

    def alterDataRows(self, json):
        """Insert, Update or Delete"""
        return self.put_odkx_server(self.getTableRoot() + "/rows", json)

    # Manipulate individual records

    def addRecord(self, dataETag, formId, **kwargs):
        orderedColumns = []
        for key, item in kwargs.items():
            orderedColumns.append({'column': key, 'value': item})
        json = {'rows':
            [{
                'rowETag': None,
                'dataETagAtModification': None,
                'deleted': False,
                'createUser': self.user,
                'lastUpdateUser': self.user,
                'formId': formId,
                'savepointTimestamp': str(datetime.datetime.now()),
                'savepointCreator': self.user,
                'orderedColumns': orderedColumns
            }],
            'dataETag': dataETag}
        output = self.alterDataRows(json)
        return output

    def alterRecord(self, dataETag, rowId, **kwargs):
        onerow = self.getDataRow(rowId)
        del onerow['selfUri']
        onerow['savepointTimestamp'] = str(datetime.datetime.now())
        onerow['savepointCreator'] = self.user
        orderedColumns = []
        for key, item in kwargs.items():
            orderedColumns.append({'column': key, 'value': item})
        onerow['orderedColumns'] = orderedColumns
        json = {'rows': [onerow], 'dataETag': dataETag}
        output = self.alterDataRows(json)
        return output

    def deleteRecord(self, dataETag, rowId):
        onerow = self.getDataRow(rowId)
        del onerow['selfUri']
        onerow['deleted'] = True
        json = {'rows': [onerow], 'dataETag': dataETag}
        output = self.alterDataRows(json)
        return output

    # dict needed to manipulate records

    def dictAddRecord(self, formId, kwargs):
        orderedColumns = []
        for key, item in kwargs.items():
            orderedColumns.append({'column': key, 'value': item})
        dict_ = {
            'rowETag': None,
            'dataETagAtModification': None,
            'deleted': False,
            'createUser': self.user,
            'lastUpdateUser': self.user,
            'formId': formId,
            'savepointTimestamp': str(datetime.datetime.now()),
            'savepointCreator': self.user,
            'orderedColumns': orderedColumns
        }
        return dict_

    def dictAlterRecord(self, rowId, kwargs):
        onerow = self.getDataRow(rowId)
        del onerow['selfUri']
        onerow['savepointTimestamp'] = str(datetime.datetime.now())
        onerow['savepointCreator'] = self.user
        orderedColumns = []
        for key, item in kwargs.items():
            orderedColumns.append({'column': key, 'value': item})
        onerow['orderedColumns'] = orderedColumns
        return onerow

    def dictDeleteRecord(self, rowId):
        onerow = self.getDataRow(rowId)
        del onerow['selfUri']
        onerow['deleted'] = True
        return onerow

    # Manipulate records

    def addRecords(self, dataETag, formId, lst_kwargs):
        lst_entry = []
        for kwargs in lst_kwargs:
            lst_entry.append(self.dictAddRecord(formId, kwargs))
        json = {'rows': lst_entry, 'dataETag': dataETag}
        return self.alterDataRows(json)

    def alterRecords(self, dataETag, lst_rowId, lst_kwargs):
        lst_entry = []
        for rowId, kwargs in zip(lst_rowId, lst_kwargs):
            lst_entry.append(self.dictAlterRecord(rowId, kwargs))
        json = {'rows': lst_entry, 'dataETag': dataETag}
        return self.alterDataRows(json)

    def deleteRecords(self, dataETag, lst_rowId):
        lst_entry = []
        for rowId in lst_rowId:
            lst_entry.append(self.dictDeleteRecord(rowId))
        json = {'rows': lst_entry, 'dataETag': dataETag}
        return self.alterDataRows(json)

    def addAlterDeleteRecords(self, dataETag, formId, local_records, remote_records):
        lst_entry = []
        remoteIDs = [x['id'] for x in remote_records]
        localIDs = [x['id'] for x in local_records]

        for item in local_records:
            if item['id'] not in remoteIDs:  # Add
                #                lst_entry.append(self.dictAddRecord(formId, item))
                lst_entry.append(item)
            elif item['id'] in remoteIDs:  # Alter
                #                lst_entry.append(self.dictAlterRecord(item['id'], item))
                lst_entry.append(item)

        # TODO Delete with the delete field
        # for item in remote_records:
        #    if item['id'] not in localIDs:  # Delete
        #        lst_entry.append(self.dictDeleteRecord(item['id']))

        json = {'rows': lst_entry, 'dataETag': dataETag}
        res = self.alterDataRows(json)
        return res

    def getRecords(self, dataETag, lst_rowId):
        lst_entry = []
        for rowId in lst_rowId:
            onerow = self.getDataRow(rowId)
            del onerow['selfUri']
            lst_entry.append(onerow)
        json = {'rows': lst_entry, 'dataETag': dataETag}
        return json

    # Sync process components

    def compareDataETag(self, dataETagLocal):
        dataETag = self.getTable()['dataETag']
        if dataETag == dataETagLocal:
            logging.info("same dataETag: " + dataETag)
            return True
        logging.info("different dataETag: " + str(dataETag) + ', ' + dataETagLocal)
        return False

    def getAllResults(self, mode, dataETag=None):
        if mode == "AllDataRows":
            dict_ = self.getAllDataRows()
        elif mode == "AllDataChanges":
            dict_ = self.getAllDataChanges(dataETag=dataETag)

        notFinished = dict_['hasMoreResults']
        cursor = dict_['webSafeResumeCursor']
        while notFinished:
            logging.info("more rows ...")
            if mode == 'AllDataChanges':
                moreRes = self.getAllDataChanges(dataETag=dataETag, cursor=cursor)
            elif mode == 'AllDataRows':
                moreRes = self.getAllDataRows(cursor=cursor)
            notFinished = moreRes['hasMoreResults']
            cursor = moreRes['webSafeResumeCursor']
            dict_["rows"].extend(moreRes["rows"])
        del dict_['tableUri']
        del dict_['webSafeRefetchCursor']
        del dict_['webSafeBackwardCursor']
        del dict_['webSafeResumeCursor']
        del dict_['hasMoreResults']
        del dict_['hasPriorResults']
        return dict_

    def push(self, local_records, formId):
        logging.info("Pushing")
        dataETag = self.getTable()['dataETag']
        remote_records = self.getAllResults('AllDataRows')['rows']
        return self.addAlterDeleteRecords(dataETag, formId, local_records, remote_records)

    def pull(self, dataETagLocal):
        logging.info("Pulling")
        return self.getAllResults('AllDataChanges', dataETag=dataETagLocal)

    # Sync process

    def tryPushOrPull(self, dataETagLocal, local_records, formId):
        """
        """
        if self.compareDataETag(dataETagLocal):
            logging.info("Update server")
            return self.push(local_records, formId)
        else:
            logging.info("Local client needs to be updated")
            return self.pull(dataETagLocal)

    def __repr__(self):
        return 'OdkxServerTable(' + self.tableId + ')'