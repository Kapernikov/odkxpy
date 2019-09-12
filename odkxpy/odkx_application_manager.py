from .odkx_server_meta import OdkxServerMeta
import os

ctypes_map = {
    '.js': 'application/x-javascript',
    '.css': 'text/css',
    '.csv': 'text/csv',
    '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    '.html': 'text/html',
}


class OdkxAppManager(object):
    """
    Utility to update the files of an ODKX application

    :param tableId: table to be migrated
    :param appRoot: path to the application root directory
    :param path: path to an arbitrary file (relative to appRoot)
    """

    def __init__(self, tableId: str, meta: OdkxServerMeta, appRoot: str, path: str = None):
        self.tableId = tableId
        self.meta = meta
        self.table = self.meta.getTable(self.tableId)
        self.appRoot = appRoot
        self.pathAppFiles = self.appRoot + "/app/config/assets/"
        self.pathTableFiles = self.appRoot + "/app/config/tables/" + self.tableId
        self.path = self.appRoot + "/" + path if path is not None else None

    def _getListOfFiles(self, dirName):
        # create a list of file and sub directories
        listOfFile = os.listdir(dirName)
        allFiles = list()
        # Iterate over all the entries
        for entry in listOfFile:
            # Create full path
            fullPath = os.path.join(dirName, entry)
            # If entry is a directory then get the list of files in this directory
            if os.path.isdir(fullPath):
                allFiles = allFiles + self._getListOfFiles(fullPath)
            else:
                allFiles.append(fullPath)
        return allFiles

    def putFiles(self, mode: str):
        """Upload files to the OdkxServer

        :param mode:
                [app] update the application files
                [file] update a specific file
                [table] update the table files of the current table
                [table_html_js] update the html/js table files of the current table
        """
        if mode == "app":
            print("Putting global files")
            localFiles = self._getListOfFiles(self.pathAppFiles)
        elif mode == "file":
            print(f"Putting one file : {self.path}")
            localFiles = [self.path]
        elif (mode == "table") or (mode == "table_html_js"):
            print("Putting table files : {self.tableId}")
            localFiles = self._getListOfFiles(self.pathTableFiles)
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
