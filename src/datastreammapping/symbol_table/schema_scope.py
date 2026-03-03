from .base import *
from .scope_enums import ScopeType
from .query_scope import QueryScope

class SchemaScope(SymbolTableScope):
    def spawn_child_scope(self,query_name="Undefined_Query") -> QueryScope:
        result = QueryScope(self,query_name,ScopeType.QUERY)
        self.children[query_name] = result
        self.nodeDgs = self.parent.nodeDgs
        return result

    def __init__(self,parent,schema_name = "Undefined_schema",scope_type=ScopeType.SCHEMA):
        self._schema = schema_name
        super().__init__(scope_name=schema_name,parent=parent, scope_type=scope_type)
        self.root_dg_node_model = self.init_root_dg_node_model()
        self.create_root_dg_node()


    @property
    def schema(self)->str:
        return self._schema

    def init_root_dg_node_model(self):
        return SchemaNode(
            node_id = self.scope_key,
            schema_name = self.schema,
            scope_name = self.scope_name
        )

    def find_table(self,table_name:str) -> tuple:

        return None


