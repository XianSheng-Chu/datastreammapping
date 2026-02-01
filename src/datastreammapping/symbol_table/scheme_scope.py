from .base import SymbolTableScope
from .scope_enums import ScopeType
from .query_scope import QueryScope

class SchemaScope(SymbolTableScope):
    def spawn_child_scope(self,query_name="Undefined_Query") -> QueryScope:
        result = QueryScope(self,query_name,ScopeType.QUERY)
        self.children[query_name] = result
        return result

    def __init__(self,parent,schema_name = None,scope_type=ScopeType.CATALOG):
        super().__init__(scope_name=schema_name,parent=parent, scope_type=scope_type)
