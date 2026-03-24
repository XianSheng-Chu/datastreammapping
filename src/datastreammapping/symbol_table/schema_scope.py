
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
        self.create_parent_relationship()


    @property
    def schema(self)->str:
        return self._schema

    def init_root_dg_node_model(self):
        return SchemaNode(
            node_id = self.scope_key,
            schema_name = self.schema,
            scope_name = self.scope_name
        )

    def find_table(self,table_name:str,schema:str=None,catalog:str=None) -> tuple:
        current_schema = self
        if schema != self.schema:
            from .catalog_scope import CatalogScope
            current_catalog: CatalogScope = self.find_parent_scope(ScopeType.CATALOG)
            current_schema = current_catalog.find_schema_scope(schema)

        find_key = current_schema.scope_root_key + ("table",table_name)
        if find_key not in current_schema.nodeDgs.nodes():
            find_key = current_schema.register_table(table_name,TableSourceEnum.SQL_REFERENCED)

        return find_key

    def register_table(
            self,
            table_name: str,
            table_source: TableSourceEnum = TableSourceEnum.PHYSICAL,
            table_comment: Optional[str] = None
    ) -> tuple:
        node_id = self.scope_root_key + (("table",table_name),)
        table_node = TableNode(
            node_id = node_id,
            table_name = table_name,
            table_source = table_source,
            table_comment = table_comment
        )
        self.add_scope_node(table_node)
        edge_model_date = EntitySubordinationEdge(
            source_node_id=node_id,
            target_node_id=self.scope_root_key,
            edge_sub_type=EntitySubordinationEdgeEnum.PARENT_SCOPE
        )
        add_edge_model_to_graph(self.nodeDgs, edge_model_date)
        return node_id





