from collections import namedtuple
from collections.abc import Sequence as ABCSequence
from typing import Sequence
OdkxServerFile = namedtuple('OdkxServerFile', [
    'filename', 'contentLength', 'contentType', 'md5hash', 'downloadUrl'
])

OdkxServerFile.__new__.__defaults__ = (None, ) * len(OdkxServerFile._fields)


class OdkxServerFileManifest(ABCSequence):
    def __init__(self, datalist: Sequence[OdkxServerFile]):
        self.datalist = datalist
        super().__init__()

    def __getitem__(self, i):
        return self.datalist[i]

    def __len__(self):
        return len(self.datalist)

    def asdict(self) -> dict:
        """json compatible, same as response given from java odk server"""
        return {"files": [dict(zip(file._fields, list(file))) for file in self.datalist]}
