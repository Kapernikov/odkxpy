from .odkx_server_table import OdkxServerTable, OdkxServerTableDefinition
from .odkx_server_meta import OdkxServerMeta
from .local_storage_sql import SqlLocalStorage
import os
import json
import csv

ctypes_map = {
    '.js': 'application/x-javascript',
    '.css': 'text/css',
    '.csv': 'text/csv',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.html': 'text/html',
}

class bidict(dict):
    """
    https://stackoverflow.com/questions/3318625/how-to-implement-an-efficient-bidirectional-hash-table/3318808#3318808
    """
    def __init__(self, *args, **kwargs):
        super(bidict, self).__init__(*args, **kwargs)
        self.inverse = {}
        for key, value in self.items():
            self.inverse.setdefault(value,[]).append(key)

    def __setitem__(self, key, value):
        if key in self:
            self.inverse[self[key]].remove(key)
        super(bidict, self).__setitem__(key, value)
        self.inverse.setdefault(value,[]).append(key)

    def __delitem__(self, key):
        self.inverse.setdefault(self[key],[]).remove(key)
        if self[key] in self.inverse and not self.inverse[self[key]]:
            del self.inverse[self[key]]
        super(bidict, self).__delitem__(key)


class migrator(object):
    """
    Utility to migrate a table from one table definition to another while keeping the compatible data
    Process :
        - print compatibilities of the column between the 2 table definitions
        - sync the local table
        - migrated the local table and all the linked tables to _legacy1_tableId...
        - delete the old table definition
        - upload the new table definition
        - initialize the table with the new table definition
        - reupload the whole history for compatible columns if a legacy table exist

    appRoot : path to the application root directory
    path : path to the definition.csv file or to an arbitrary file (relative to appRoot)
    pathMapping : path to the mapping file (old : new) (relative to appRoot)
    """

    def __init__(self, tableId, meta: OdkxServerMeta, local_storage: SqlLocalStorage, appRoot: str, path: str = None, pathMapping: str = None):
        self.tableId = tableId
        self.meta = meta
        self.table = self.meta.getTable(self.tableId)
        self.local_storage = local_storage
        self.appRoot = appRoot
        self.pathAppFiles = self.appRoot + "/app/config/assets/"
        self.pathTableFiles = self.appRoot + "/app/config/tables/" + self.tableId
        self.path = self.appRoot + "/" + path if path is not None else self.pathTableFiles + "/definition.csv"
        self.pathMapping = self.appRoot + "/" + pathMapping

    def getNewTableDefinition(self) -> OdkxServerTableDefinition:
        with open(self.path, newline='') as csvfile:
            colList = list(csv.reader(csvfile))
        return OdkxServerTableDefinition._from_DefFile(self.tableId, colList)

    def getColumnMapping(self) -> bidict:
        with open(self.pathMapping) as file:
            mapping = json.load(file)["mapping"]
            bdMapping = bidict(mapping)
        return bdMapping

    def getValidMapping(self, mapping, newColumns, oldColumns):
        validMapping = bidict({})
        print("\nReport on the mapping: ")
        print("=========================")
        for k, v in mapping.items():
            if k in newColumns and v in oldColumns:
                validMapping[k] = v
            else:
                print(f"Unknown column:{k} or {v}")

        print("\n Valid mapping \n")
        print(validMapping)
        return validMapping

    def checkColumnsType(self, common, oldTableDef: OdkxServerTableDefinition, newTableDef: OdkxServerTableDefinition, validMapping: bidict):
        incompat = []
        for item in common:
            if oldTableDef.getColDef(item) is None:
                oldItem = validMapping[item]
                oldColDef = oldTableDef.getColDef(oldItem)
            else:
                oldColDef = oldTableDef.getColDef(item)

            if oldColDef.elementType != newTableDef.getColDef(item).elementType:
                incompat.append({oldColDef.elementKey: oldColDef.elementType, item: newTableDef.getColDef(item).elementType})
        return incompat


    def compareTableDef(self, oldTableDef: OdkxServerTableDefinition, newTableDef: OdkxServerTableDefinition):
        if self.pathMapping is not None:
            mapping = self.getColumnMapping()
            validMapping = self.getValidMapping(mapping, oldTableDef.columnsKeyList, newTableDef.columnsKeyList)
            if validMapping:
                oldColumnsMapped = []
                for col in oldTableDef.columnsKeyList:
                    if col in set(validMapping.values()):
                        oldColumnsMapped.extend(validMapping.inverse[col])
                    else:
                        oldColumnsMapped.append(col)
            else:
                oldColumnsMapped = oldTableDef.columnsKeyList
        else:
            validMapping = False
            oldColumnsMapped = oldTableDef.columnsKeyList

        deleted = sorted(list(set(oldColumnsMapped) - set(newTableDef.columnsKeyList)))
        new = sorted(list(set(newTableDef.columnsKeyList) - set(oldColumnsMapped)))
        common = sorted(list(set(newTableDef.columnsKeyList) & set(oldColumnsMapped)))
        incompat = self.checkColumnsType(common, oldTableDef, newTableDef, validMapping)


        print("\nReport on the migration: ")
        print("=========================")

        if validMapping:
            print("\nUsing the following \33[94mmapping\033[0m:")
            print(json.dumps(validMapping, indent=4))
        if deleted:
            print("\nThe following columns will be \033[94mdeleted\033[0m:")
            print(json.dumps(deleted, indent=4))
        if new:
            print("\nThe following columns will be \033[94mcreated\033[0m: ")
            print(json.dumps(new, indent=4))
        if common:
            print("\nThe following columns will \033[94mnot be changed\033[0m: ")
            print(json.dumps(common, indent=4))
        if incompat:
            print("\nThe following columns will not be keep as there is \033[94mincompatibility\033[0m:")
            print(json.dumps(incompat, indent=4))
        return {'mapping': validMapping, 'common': common, 'incompat': incompat}

    def getListOfFiles(self, dirName):
        # create a list of file and sub directories
        listOfFile = os.listdir(dirName)
        allFiles = list()
        # Iterate over all the entries
        for entry in listOfFile:
            # Create full path
            fullPath = os.path.join(dirName, entry)
            # If entry is a directory then get the list of files in this directory
            if os.path.isdir(fullPath):
                allFiles = allFiles + self.getListOfFiles(fullPath)
            else:
                allFiles.append(fullPath)
        return allFiles

    def putFiles(self, mode: str):
        """Upload files to the OdkxServer

           mode :
                [app] for putting application files
                [file] to put exactly one file
                [table] to put table files
        """
        if mode == "app":
            print("Putting global files")
            localFiles = self.getListOfFiles(self.pathAppFiles)
        elif mode == "file":
            print("Putting one file : {path}".format(path=self.path))
            localFiles = [self.path]
        elif (mode == "table") or (mode == "table_html_js"):
            print("Putting table files : {tableId}".format(tableId=self.tableId))
            localFiles = self.getListOfFiles(self.pathTableFiles)
        else:
            raise Exception("Unrecognized mode")

        for f in localFiles:
            if mode != "table_html_js" or (f.split('.')[-1] in ['html', 'js']):
                print("uploading: " + f)
                fhandle = open(f, "rb")
                data = fhandle.read()
                fhandle.close()
                ctype = 'application/octet-stream'
                for k, v in ctypes_map.items():
                    if f.endswith(k):
                        ctype = v
                if mode == "app":
                    self.meta.putFile(ctype, data, f)
                else:
                    el = f[len(self.pathTableFiles):]
                    self.table.putFile(ctype, data, el)

    def migrateReport(self):
        newTableDef = self.getNewTableDefinition()
        oldTableDef = self.table.getTableDefinition()
        return self.compareTableDef(oldTableDef, newTableDef)

    def createRemoteAndLocalTable(self, force=False):
        newTableDef = self.getNewTableDefinition()
        self.meta.createTable(newTableDef._asdict(True))
        # We update the info on the current loaded table in the migrator
        self.table = self.meta.getTable(newTableDef.tableId)
        self.putFiles("table")

    def uploadLegacyTable(self, table=None, res=None, force=False):
        self.local_table = self.local_storage.getLocalTable(self.table)
        if self.local_table._checkIfLegacy():
            self.local_table.uploadLegacy(self.table, specific_table=table, res=res, force_push=force)

    def migrate(self, force=False):
        res = self.migrateReport()
        if res['incompat'] and not force:
            print("The migration was aborted")
            return
        self.local_table = self.local_storage.getLocalTable(self.table)
        self.local_table.sync(self.table)
        with self.local_storage.engine.begin() as trans:
            self.local_table.toLegacy(trans)
            self.local_table.updateLocalStatusDb(None, trans)
        self.table.deleteTable(True)
        self.createRemoteAndLocalTable(force)
        self.uploadLegacyTable(res=res, force=force)
        self.local_table.sync(self.table)
