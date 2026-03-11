from .query_scope import *
from .scope_enums import ScopeType

class SelectScope(QueryScope):
    def __init__(self, parent:QueryScope,query_name,ast_node,scope_type=ScopeType.SELECT):
        super().__init__(parent,query_name,scope_type)
        # logical_processing_order用于存储一个定义域下子句在语义中解析的顺序
        self.logical_processing_order:list = ['with','with_','from','from_','laterals','joins','pivots','sample','prewhere','where',
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

    def dg_add_node(self, node: Expression):
        super().dg_add_node(node)
        if self.nodeDgs.nodes[self.data_node_active]["exp_stage"]=="expressions":
            for u, v, key in self.nodeDgs.out_edges(self.data_node_active,keys=True):
                if self.nodeDgs.nodes[v]["exp_key"] == "select":
                    self.nodeDgs.nodes[u]["output_flag"] = True

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

    def add_symbol(self,dg_key:tuple,symbol_name:str):
        dg_node = self.nodeDgs.nodes[dg_key]

        #以下分支是为表述select字句中的所有列的名称符号
        dg_key_parent = dg_key[:-1]
        if dg_key_parent not in self.nodeDgs.nodes:
            return
        if dg_node.get("output_flag", False):
            symbol_type = "output"
            if self.current_scope_symbols.get(symbol_type) is None:
                self.current_scope_symbols[symbol_type] = {}
            self.current_scope_symbols[symbol_type][symbol_name] = dg_key
        super().add_symbol(dg_key,symbol_name)

    def symbol_name(self,dg_key:tuple)->str:
        symbol_name = ""
        dg_node = self.nodeDgs.nodes[dg_key]
        if dg_node is None or dg_node.get("exp_key", None) is None:
            return super().symbol_name(dg_key)
        #以下分支是为表述select字句中的所有列的名称符号
        if dg_node.get("output_flag",False):
            if dg_node["exp_key"] in ("alias", "column","star") :
                symbol_name = dg_node["extra_attrs"]["output_name"]
            elif dg_node["exp_key"] == "anonymous":
                symbol_name = dg_node["extra_attrs"]["func_name"]
            elif dg_node["extra_attrs"].get("func_type","")!="":
                symbol_name = dg_node["exp_key"]
            elif dg_node["exp_key"] == "literal":
                symbol_name = "?column?"
            else:
                symbol_name = self.scope_logical_order_key(dg_key)

        if symbol_name == "":
            symbol_name = super().symbol_name(dg_key)

        return symbol_name

    def init_root_dg_node_model(self):
        return SelectScopeNode(
            node_id = self.scope_key,
            exp_key = self.scope_root.key,
            exp_node = self.scope_root
        )
