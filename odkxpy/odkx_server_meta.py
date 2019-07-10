import json
from .odkx_connection import OdkxConnection
from .odkx_server_table import OdkxServerTable
from collections import namedtuple

from .odkx_server_file import OdkxServerFile

OdkxServerUser = namedtuple('OdkxServerUser', [
    'user_id', 'full_name', 'defaultGroup', 'roles'
])

OdkxServerUser.__new__.__defaults__ = (None, ) * len(OdkxServerUser._fields)

class OdkxServerMeta(object):
    """
    this is a wrapper around the global metadata REST API
    """
    def __init__(self, connection: OdkxConnection):
        self.connection = connection

    def getSupportedClientVersions(self):
        return self.connection.GET('clientVersions')

    def getPrivilegesInfo(self):
        return OdkxServerUser(**self.connection.GET('privilegesInfo'))

    def getUsersInfo(self):
        return [OdkxServerUser(**x) for x in self.connection.GET('usersInfo')]

    def getFileManifest(self):
        return [OdkxServerFile(**d) for d in self.connection.GET("manifest/2/")['files']]

    def getFile(self, path):
        return self.connection.GET('files/2' + ('' if path.startswith('/') else '/') + path)

    def putFile(self, content_type, payload, path):
        headers = {"Content-Type": content_type}
        if type(payload) in(dict, list):
            payload = json.dumps(payload).encode('utf-8')
        if type(payload) == str:
            payload = payload.encode('utf-8')
        return self.connection.POST('files/2' + ('' if path.startswith('/') else '/') + path, payload, headers=headers)

    def deleteFile(self, path):
        return self.connection.DELETE('files/2' + ('' if path.startswith('/') else '/') + path)

    def getTables(self):
        return [ OdkxServerTable(self.connection, x['tableId'], x['schemaETag']) for x in  self.connection.GET("tables")['tables'] ]


    def createTable(self, json):
        return self.connection.PUT("tables/" + json["tableId"], json)
