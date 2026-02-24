from .query_scope import *
from .scope_enums import ScopeType

class SelectScope(QueryScope):
    def __init__(self, parent:QueryScope,query_name,ast_node,scope_type=ScopeType.SELECT):
        super().__init__(parent,query_name,scope_type)
        # logical_processing_order用于存储一个定义域下子句在语义中解析的顺序
        self.logical_processing_order = ['with','from','laterals','joins','pivots','sample','prewhere','where',
                                         'match','connect','group','having','windows','qualify','expressions','operation_modifiers',
                                         'distinct','distribute','sort','cluster','order','limit','offset','into','locks','format',
                                         'settings','options',]
        self.current_stage = "select"
        self.scope_root = ast_node
        self.output_columns:list = []
        self.columns: list = []
        parent.add_child_scope(self)
        self.set_scope_root(ast_node)
        self.fetch_columns(ast_node)


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



