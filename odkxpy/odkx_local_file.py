from collections import namedtuple

OdkxLocalFile = namedtuple('OdkxLocalFile', [
    'filename', 'contentLength', 'contentType', 'md5hash'
])

OdkxLocalFile.__new__.__defaults__ = (None, ) * len(OdkxLocalFile._fields)
