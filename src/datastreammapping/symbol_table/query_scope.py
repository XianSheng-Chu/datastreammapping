from .base import SymbolTableScope
from .scope_enums import ScopeType
from sqlglot import *

class QueryScope(SymbolTableScope):

    def spawn_child_scope(self, ast_node: expressions=None, scope_type = ScopeType.QUERY) -> 'QueryScope':
        rulest = None
        print(f"spawn_child_scope:line 9:scope_type:{scope_type}")
        if scope_type==ScopeType.SELECT:
            from .runtime_scopes import SelectScope
            rulest = SelectScope(self,scope_type.str(),ast_node)
            self.add_child_scope(rulest)

        if rulest is not None:
            return rulest


    def __init__(self,parent,query_name = None,scope_type=ScopeType.QUERY):
        super().__init__(scope_name=query_name, parent=parent, scope_type=scope_type)
        self.children:list[tuple[ScopeType,QueryScope]] = []
        self.ast_node:expressions = None

    def add_child_scope(self, scope:'QueryScope'):
        self.children.append((scope.scope_type,scope))

    def set_ast_node(self,node):
        if node is None:
            raise ValueError(
                "Invalid expressions node"
            )
        self.ast_node = node
