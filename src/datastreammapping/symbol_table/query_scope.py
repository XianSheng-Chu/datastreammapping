from networkx import MultiDiGraph

from .base import SymbolTableScope
from .scope_enums import ScopeType
from sqlglot import *

class QueryScope(SymbolTableScope):

    def spawn_child_scope(self, ast_node: expressions=None, scope_type = ScopeType.QUERY) -> 'QueryScope':
        rulest = None
        # print(f"spawn_child_scope:line 9:scope_type:{scope_type}")
        if scope_type==ScopeType.SELECT:
            from .runtime_scopes import SelectScope
            rulest = SelectScope(self,scope_type.str(),ast_node)
            self.add_child_scope(rulest)
            # print(ast_node.sql)

        if rulest is not None:
            return rulest


    def __init__(self,parent,query_name = None,scope_type=ScopeType.QUERY):
        super().__init__(scope_name=query_name, parent=parent, scope_type=scope_type)
        self.children:list[tuple[ScopeType,QueryScope]] = []
        self.scope_root:expressions = None
        self.scope_key = None
        if self.scope_type==ScopeType.QUERY:
            self.nodeDgs = MultiDiGraph()
            self.scope_key=self.nodeDgs.add_node(query_name,database_object_type=scope_type.str(),database_object_name = query_name)
            self.query_root_name = query_name
            self.query_count = 0
        else:
            self.nodeDgs = self.parent.nodeDgs
            self.query_root_name = self.parent.query_root_name
            self.query_count = self.parent.query_count
        self.data_node_active = None
        self.current_stage = None


    def add_child_scope(self, scope:'QueryScope'):
        self.children.append((scope.scope_type,scope))

    def set_scope_root(self,node):
        if node is None:
            raise ValueError(
                "Invalid expressions node"
            )
        self.scope_root = node

    def dg_add_node(self, node: Expression):
        dg_key = list()
        current_node = node
        if node == node.root():
            self.query_count += 1
        while True:
            dg_key.append(self.find_parent_key(current_node))
            if current_node.parent is None:
                dg_key.append(self.query_root_name)
                break
            current_node = current_node.parent
        dg_key.reverse()
        dg_key = tuple(dg_key)
        if dg_key not in self.nodeDgs.nodes:
            if self.scope_root == node:
                self.scope_key=dg_key
            self.nodeDgs.add_node(dg_key,exp_key = node.key,exp_node=node,exp_stage=self.current_stage)
            # print(f"{dg_key}:{self.nodeDgs.nodes[dg_key]["exp_node"]}")
        self.data_node_active = dg_key
        return dg_key

        # print(self.nodeDgs.nodes[dg_key])
        # print(repr(node.root()))
        # print(f"{dg_key}:{node.sql()},{node.key}")

    def find_parent_key(self, node: Expression)-> str | tuple:
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
            return self.scope_name,self.query_count

    def set_stage(self,stage_name,ast_node:Expression):
        self.current_stage = stage_name

    def add_node_info(self, node, info:dict):
        for key,value in info.items():
            self.nodeDgs.nodes[self.data_node_active][key] = value