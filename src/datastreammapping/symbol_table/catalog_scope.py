from .base import *
from .scope_enums import ScopeType
from .scheme_scope import SchemaScope

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
        super().__init__(name,parent=None,scope_type = scope_type)
        print(data_base_type.__str__())
        self.root_dg_node_model = CatalogNode(
            node_key = self.scope_key,
            catalog_name = catalog,
            scope_name = name,
            database_type = data_base_type
        )
        self._catalog = catalog

    @property
    def catalog(self):
        return self._catalog

    @property
    def scope_root_key(self) -> tuple:
        result = ((self.scope_type.str(),self.scope_name),)
        return result

    def create_root_dg_node(self):
        self.nodeDgs.add_node(self.scope_root_key, database_object_type=self.scope_type.str(),
                                               database_object_name=self.scope_name, scope_root_temp=self)
