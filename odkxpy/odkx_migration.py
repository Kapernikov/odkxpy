from .odkx_server_table import OdkxServerTable, OdkxServerTableDefinition
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
    """

    def __init__(self, table: OdkxServerTable, pathDefFile: str, local_storage: SqlLocalStorage):
        self.table = table
        self.pathDefFile = pathDefFile
        self.local_storage = local_storage
        self.local_table = local_storage.getLocalTable(self.table.tableId)

    def getNewTableDefinition(self) -> OdkxServerTableDefinition:
        with open(self.pathDef, newline='') as csvfile:
            data = list(csv.reader(csvfile))
        return OdkxServerTableDefinition._from_DefFile(self.table.tableId, data)

    def getColumnMapping(self):
        with open(self.path) as file:
            mapping = json.load(file)
        return mapping

    def getValidMapping(self, mapping, oldColumns, newColumns):
        validMapping = {}
        for k, v in mapping.items():
            if k in oldColumns and v in newColumns:
                validMapping[k] = v
            else:
                print(f"Unknown column:{k} or {v}")
        return validMapping

    def checkColumnsType(self, common, oldTableDef: OdkxServerTableDefinition, newTableDef: OdkxServerTableDefinition):
        incompat = []
        for item in common:
            if oldTableDef.getColDef(item).elementType != newTableDef.getColDef(item).elementType:
                incompat.append({item: [oldTableDef.getColDef(item).elementType, newTableDef.getColDef(item).elementType]})
        return incompat


    def compareTableDef(self, oldTableDef: OdkxServerTableDefinition, newTableDef: OdkxServerTableDefinition):
        mapping = self.getColumnMapping()
        validMapping = self.getValidMapping(mapping, oldTableDef.columnsKeyList, newTableDef.columnsKeyList)

        if validMapping:
            oldMappedColumns = [validMapping[col] if col in validMapping.keys() else col for col in oldTableDef.columnsKeyList]
        else:
            oldMappedColumns = oldTableDef.columnsKeyList

        deleted = list(set(oldMappedColumns) - set(newTableDef.columnsKeyList))
        new = list(set(newTableDef.columnsKeyList) - set(oldMappedColumns))
        common = list(set(newTableDef.columnsKeyList) & set(oldMappedColumns))
        incompat = self.checkColumnsType(common, oldTableDef, newTableDef)


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

    def putFiles(self, mode, option=None):
        if mode == "app":
            print("Putting global files")
            localFiles = self.getListOfFiles(f".")
        elif mode == "file":
            print("Putting one file : {path}".format(path=self.path))
            localFiles = [self.path]
        else:
            print("Putting table files : {tableId}".format(tableid=self.table.tableId))
            localFiles = self.getListOfFiles("tables/{tableId}".format(tableid=self.table.tableId))

        for f in localFiles:
            if option != "assets" or (f.split('.')[-1] in ['html', 'js']):
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
                    pass
                else:
                    el = f[len('tables/{tableId}/'.format(tableid=self.table.tableId)):]
                    self.table.putFile(ctype, data, el)

    def uploadTableFiles(self):
        self.putFiles("table")

    def migrate(self, force=False):
        newTableDef = self.getNewTableDefinition()
        oldTableDef = self.table.getTableDefinition()
        res = self.compareTableDef(oldTableDef, newTableDef)
        if res['incompat'] and not force:
            print("The migration was aborted")
            return
        self.local_table.sync(self.table.tableId)
        self.local_table.toLegacy()
        self.table.deleteTableDefinition(False)
        self.table.setTableDefinition(newTableDef._asdict())
        self.uploadTableFiles()
        self.local_storage.initializeLocalStorage()
        self.local_table.uploadLegacy(self.table, res, force)
