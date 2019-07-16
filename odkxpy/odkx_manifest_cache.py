
"""
Cache manifest files
works on https://docs.opendatakit.org/odk-x/odk-2-sync-protocol/#data-grouping-2-rest-synchronization-table-level-files-api
formDef.json not considered ideal, so try not to rely on
"""
from .odkx_connection import OdkxConnection
from .odkx_server_file import OdkxServerFile
from .local_storage_sql import SqlLocalStorage, CacheNotFoundError
from typing import List, TYPE_CHECKING, Any
import sqlalchemy


def formdef_class(base):
    class FormDef(base):
        __tablename__ = "odkxpy_cached_formdef"

        tableId = sqlalchemy.Column(
            sqlalchemy.String, primary_key=True, index=True)
        md5hash = sqlalchemy.Column(sqlalchemy.String)
        form_id = sqlalchemy.Column(sqlalchemy.String)
        xlsx = sqlalchemy.Column(
            sqlalchemy.dialects.postgresql.JSONB(none_as_null=False))
        instance_name = sqlalchemy.Column(sqlalchemy.String)

    return FormDef

class OdkManifestCache:
    def __init__(self, storage: SqlLocalStorage, connection: OdkxConnection):
        self._storage: SqlLocalStorage = storage
        self._connection: OdkxConnection = connection

    def _cache_object(self, manifest:OdkxServerFile, orm_def: Any, key:str, orm_mapper):
        #table could not exist
        orm_def.__table__.create(bind=self._storage.engine, checkfirst=True)

        with self._storage.local_session_scope() as session:
            cached_obj = session.query(orm_def).get(key)
            if cached_obj is None or cached_obj.md5hash != manifest.md5hash:
                # needs update
                data = self._connection.GET("files/2/" + manifest.filename)
                session.merge(orm_mapper(data))

    

class OdkTableManifestCache(OdkManifestCache):
    def __init__(self, tableId: str, storage: SqlLocalStorage, connection: OdkxConnection):
        super().__init__(storage, connection)
        self.tableId: str = tableId

        # orm objects and session should be same for caching purposes
        self.FormDef = formdef_class(storage.declarative_base())
        

    def formDef_mapper(self, manifest_file):
        def mapper(obj):
            for setting in obj["xlsx"]["settings"]:
                if setting["setting_name"] == "form_id":
                    form_id = setting['value']
                if setting["setting_name"] == "instance_name":
                    instance_name = setting["value"]
            FormDef = self.FormDef
            return FormDef(tableId=self.tableId, md5hash=manifest_file.md5hash,
                             form_id=form_id, xlsx=obj["xlsx"], instance_name=instance_name)
        return mapper

    

    def do_sync(self, tableid_manifest_files: List[OdkxServerFile]):
        """
        before a tableid sync
        """
        for manifest_file in tableid_manifest_files:
            if manifest_file.filename.endswith("formDef.json"):
                manifest = manifest_file
                FormDef = self.FormDef
                super()._cache_object( manifest, FormDef, self.tableId, self.formDef_mapper(manifest))
                

def getCachedFormDef(odk: OdkTableManifestCache, session: sqlalchemy.orm.session.Session) -> Any:
    """
    reuse odk and session objects for cache performance
    """
    obj = session.query(odk.FormDef).get({"tableId": odk.tableId})
    if obj is None:
        raise CacheNotFoundError(
            f"FormDef, sync manifest file for {odk.tableId} first")
    return obj
