from .query_scope import *
from .scope_enums import ScopeType

class SelectScope(QueryScope):
    def __init__(self, parent:QueryScope,query_name,ast_node,scope_type=ScopeType.SELECT):
        super().__init__(parent,query_name,scope_type)
        # logical_processing_order用于存储一个定义域下子句在语义中解析的顺序
        self.logical_processing_order:list = ['with','from','laterals','joins','pivots','sample','prewhere','where',
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

    def scope_logical_order_key(self,dg_key:tuple) ->str:

        result_parent:tuple = self.parent.scope_logical_order_key(self.scope_key)
        if dg_key == self.scope_key:
            # 定义域顶层调用时，无需关注子句的执行顺序
            return result_parent

        result_self = list(dg_key[len(self.scope_key):])
        stage_order_name = result_self[0]

        for i in range(0, len(self.logical_processing_order)):
            if type(stage_order_name) is tuple and self.logical_processing_order[i] == stage_order_name[0] :
                stage_order_name = list(stage_order_name)
                stage_order_name[0] = i + 1000
                stage_order_name[1] = stage_order_name[1] + 1000000
                stage_order_name = tuple(stage_order_name)
            elif type(stage_order_name) is not tuple and self.logical_processing_order[i] == stage_order_name:
                stage_order_name = i + 1000
                stage_order_name = (stage_order_name,0)

        result_self[0] = stage_order_name
        result_self = tuple(result_self)
        result = ''
        for item in list(result_self):
            if type(item) is tuple:
                item = '_'.join(map(str, item))
            result = result + ":" + item

        return result_parent+result

    def add_symbol(self,dg_key:tuple,upper_limit:int,lower_limit:int,symbol_name:str):
        symbol_level = 0
        dg_node = self.nodeDgs.nodes[dg_key]
        if dg_node == "cte":
            symbol_level = 0
            self.current_scope_symbols[symbol_level]={dg_key:symbol_name}
        elif dg_node == "table":
            symbol_level = 1
            self.current_scope_symbols[symbol_level] = {dg_key: symbol_name}
        pass
