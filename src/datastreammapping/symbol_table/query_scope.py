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
        self.scope_root:expressions = None

    def add_child_scope(self, scope:'QueryScope'):
        self.children.append((scope.scope_type,scope))

    def set_scope_root(self,node):
        if node is None:
            raise ValueError(
                "Invalid expressions node"
            )
        self.scope_root = node

    def dg_add_node(self, node: Expression):
        dg_key = []
        current_node = node
        if node == self.scope_root:
            pass
        else:
            while True:
                dg_key.append(self.find_parent_key(current_node))
                if current_node.parent is None:
                    break
                current_node = current_node.parent
        dg_key = tuple(dg_key)

        # print(repr(node.root()))
        # print(f"{dg_key}:{node.sql()},{node.key}")

    def find_parent_key(self, node: Expression):
        result: str | tuple
        if node.parent:

            current_parent = node.parent
            for args_key, value in current_parent.args.items():
                if isinstance(value, list):
                    for i in range(len(value)):
                        if value[i] == node:
                            result = (args_key, i)
                            return result
                else:
                    if value == node:
                        result = args_key
                        return result
        else:
            return "query_root"