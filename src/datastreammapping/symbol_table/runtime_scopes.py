from .query_scope import *
from .scope_enums import ScopeType

class SelectScope(QueryScope):
    def __init__(self, parent:QueryScope,query_name,ast_node,scope_type=ScopeType.QUERY):
        super().__init__(parent,query_name)

        self.output_columns:list = []
        self.columns: list = []
        parent.add_child_scope(self)
        self.set_ast_node(ast_node)
        self.fetch_columns(ast_node)
        print('SelectScope:line 12')

    def fetch_columns(self,ast_node:select):
        node = ast_node
        outputs = node.named_selects
        item = node.selects
        for i in  range(0,len(outputs)):
            self.output_columns.append((outputs[i],item[i]))
            if outputs[i].key=="alias":
                self.columns.append(item[i].this)
            else:
                self.columns.append(item[i])

    def add_node(self,ast_node:Expression):
        node = ast_node
