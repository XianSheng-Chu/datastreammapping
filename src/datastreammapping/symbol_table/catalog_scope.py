from .base import SymbolTableScope
from .scope_enums import ScopeType
from .scheme_scope import SchemaScope

class CatalogScope(SymbolTableScope):
    def spawn_child_scope(self,schema_name = "Undefined") -> SchemaScope:
        result = SchemaScope(self,schema_name,ScopeType.SCHEMA)
        self.children[schema_name] = result
        return result


    def __init__(self,name = "Undefined",scope_type=ScopeType.CATALOG):
        super().__init__(name,parent=None,scope_type = scope_type)
