
"""
Cache manifest files
works on https://docs.opendatakit.org/odk-x/odk-2-sync-protocol/#data-grouping-2-rest-synchronization-table-level-files-api
formDef.json not considered ideal, so try not to rely on
"""
from .odkx_connection import OdkxConnection
from .odkx_server_file import OdkxServerFile
from .local_storage_sql import SqlLocalStorage, CacheNotFoundError
from typing import List, TYPE_CHECKING, Any, Callable, Optional
import sqlalchemy


def formdef_class(base):
    class FormDef(base):
        __tablename__ = "odkxpy_cached_formdef"
        filename = sqlalchemy.Column(sqlalchemy.String, primary_key=True, index=True)
        tableId = sqlalchemy.Column(
            sqlalchemy.String, index=True)
        md5hash = sqlalchemy.Column(sqlalchemy.String)
        form_id = sqlalchemy.Column(sqlalchemy.String)
        xlsx = sqlalchemy.Column(
            sqlalchemy.dialects.postgresql.JSONB(none_as_null=False))
        instance_name = sqlalchemy.Column(sqlalchemy.String)

        def __getitem__(self, key):
            value = getattr(self, key, None)
            if value is None:
                raise KeyError
            return value
    return FormDef

def properties_class(base):
    class TableProperties(base):

        __tablename__ = "odkxpy_cached_tableproperties"

        filename = sqlalchemy.Column(sqlalchemy.String, index=True)
        tableId = sqlalchemy.Column(sqlalchemy.String, index=True, primary_key=True)
        md5hash = sqlalchemy.Column(sqlalchemy.String)
        survey_formId = sqlalchemy.Column(sqlalchemy.String)
        document = sqlalchemy.Column(
            sqlalchemy.dialects.postgresql.JSONB(none_as_null=False))
    return TableProperties


class OdkManifestCache:
    def __init__(self, storage: SqlLocalStorage, connection: OdkxConnection):
        self._storage: SqlLocalStorage = storage
        self._connection: OdkxConnection = connection

    def _cache_object(self, manifest:OdkxServerFile, orm_def: type, orm_mapper:Callable[[Any], sqlalchemy.ext.declarative.api.DeclarativeMeta], connection_url:str):
        #table could not exist
        orm_def.__table__.create(bind=self._storage.engine, checkfirst=True)

        with self._storage.local_session_scope() as session:
            cached_obj = session.query(orm_def).filter_by(filename=manifest.filename).first()
            if cached_obj is None or cached_obj.md5hash != manifest.md5hash:
                # needs update
                
                data = self._connection.GET(connection_url)
                session.merge(orm_mapper(data))

    

class OdkTableManifestCache(OdkManifestCache):
    def __init__(self,session: sqlalchemy.orm.session.Session, storage: SqlLocalStorage, connection: OdkxConnection):
        super().__init__(storage, connection)
        self.session = session
        # orm objects and session should be same for caching purposes
        self.FormDef = formdef_class(storage.declarative_base())
        self.TableProperties = properties_class(storage.declarative_base())
        self.FormDef.__table__.create(bind=self._storage.engine, checkfirst=True)
        self.TableProperties.__table__.create(bind=self._storage.engine, checkfirst=True)

    def _formDef_mapper(self, manifest_file,  tableId):
        def mapper(obj:Any):
            for setting in obj["xlsx"]["settings"]:
                if setting["setting_name"] == "form_id":
                    form_id = setting['value']
                if setting["setting_name"] == "instance_name":
                    instance_name = setting["value"]
            return self.FormDef(filename=manifest_file.filename, tableId=tableId, md5hash=manifest_file.md5hash,
                             form_id=form_id, xlsx=obj["xlsx"], instance_name=instance_name)
        return mapper

    def _properties_mapper(self, manifest_file, tableId):
        def mapper(obj:Any):
            for x in obj:
                if x['key'] == 'SurveyUtil.formId':
                    form_id = x['value']
            return self.TableProperties(filename=manifest_file.filename, tableId=tableId, md5hash=manifest_file.md5hash, survey_formId=form_id, document=obj)
        return mapper

    def do_sync(self,tableId:str, tableid_manifest_files: List[OdkxServerFile]):
        """
        before a tableid sync
        """
        for manifest_file in tableid_manifest_files:
            if manifest_file.filename.endswith("formDef.json"):
                manifest = manifest_file
                FormDef = self.FormDef
                super()._cache_object( manifest, FormDef, self._formDef_mapper(manifest, tableId), connection_url="files/2/" + manifest.filename )
            if manifest_file.filename.endswith("properties.csv"):
                if tableId not in manifest_file.filename:
                    raise ValueError(f"manifest files must belong to {tableId}")
                super()._cache_object(manifest_file, self.TableProperties, self._properties_mapper(manifest_file, tableId),
                 connection_url="tables/" + tableId + "/properties/2")
                

    def getCachedFormDef(self, tableId:str, formId:str) -> Any:


        filename = f"tables/{tableId}/forms/{formId}/formDef.json"
        obj = self.session.query(self.FormDef).get({"filename":filename})
        if obj is None:
            raise CacheNotFoundError(
                f"FormDef, sync manifest files for {tableId} first")
        return obj

    def getCachedSurveyFormId(self, tableId:str) -> str:
        """the one being used by survey app(current)
        """
        formId = self.session.query(self.TableProperties).get({"tableId": tableId})
        if formId is None:
            raise CacheNotFoundError(
                f"FormDef, sync manifest files for {tableId} first")
        return formId.survey_formId