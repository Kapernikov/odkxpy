from collections import namedtuple

OdkxServerFile = namedtuple('OdkxServerFile', [
    'filename', 'contentLength', 'contentType', 'md5hash', 'downloadUrl'
])

OdkxServerFile.__new__.__defaults__ = (None, ) * len(OdkxServerFile._fields)
