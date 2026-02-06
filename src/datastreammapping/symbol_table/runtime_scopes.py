from .query_scope import *
from .scope_enums import ScopeType
from networkx import MultiDiGraph

class SelectScope(QueryScope):
    def __init__(self, parent:QueryScope,query_name,ast_node,scope_type=ScopeType.QUERY):
        super().__init__(parent,query_name,scope_type)

        self.current_stage = "select"
        self.output_columns:list = []
        self.columns: list = []
        parent.add_child_scope(self)
        self.set_scope_root(ast_node)
        self.fetch_columns(ast_node)
        self.nodeDgs = MultiDiGraph()


    def fetch_columns(self,ast_node:select):
        node = ast_node
        outputs = node.named_selects
        item = node.selects
        for i in  range(0,len(outputs)):
            self.output_columns.append((outputs[i],item[i]))
            if item[i].key=="alias":
                self.columns.append(item[i].this)
            else:
                self.columns.append(item[i])






    def set_stage(self,stage_name,ast_node:Expression):
        self.current_stage = stage_name


    def add_node_info(self, node, info:dict):
        self.dg_add_node(node)
        for key,value in info.items():
            print(f"{key} : {value}")