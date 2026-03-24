from .base import *
from .scope_enums import ScopeType
from .schema_scope import SchemaScope

class CatalogScope(SymbolTableScope):
    def spawn_child_scope(self,schema_name = "Undefined_schema") -> SchemaScope:
        result = SchemaScope(self,schema_name,ScopeType.SCHEMA)
        self.children[schema_name] = result

        return result


    def __init__(self,data_base_type:str,name:str = "Undefined",catalog:str="Undefined_db",scope_type=ScopeType.CATALOG):
        """
            name (str, 可选): 当前作用域的名称，默认为 "Undefined"
            catalog (str, 可选): 所属目录的名称，默认为 "Undefined_db"
            scope_type (ScopeType, 可选): 作用域类型，应为 ScopeType.CATALOG，默认为该值
        """
        self.data_base_type = data_base_type
        self._catalog = catalog
        super().__init__(name,parent=None,scope_type = scope_type)
        self.root_dg_node_model = self.init_root_dg_node_model()
        self.create_root_dg_node()

    @property
    def catalog(self):
        return self._catalog

    @property
    def scope_root_key(self) -> tuple:
        result = ((self.scope_type.str(),self.scope_name),)
        return result



    def init_root_dg_node_model(self):
        return CatalogNode(
            node_id = self.scope_key,
            catalog_name = self.catalog,
            scope_name = self.scope_name,
            database_type = self.data_base_type
        )

    def find_schema_scope(self,schema_name:str)->SchemaScope:
        find_schema_scope = self.children.get(schema_name,None)
        if find_schema_scope is None:
            find_schema_scope = self.spawn_child_scope(schema_name)
        return find_schema_scope
