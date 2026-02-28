from networkx import MultiDiGraph

from .base import *
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
            query_dg_key = query_name
            self.scope_key=self.nodeDgs.add_node(query_dg_key,database_object_type=scope_type.str(),database_object_name = query_name,scope_root_temp = self)
            self.query_root_name = query_dg_key
            self.query_count = 0
        else:
            self.nodeDgs = self.parent.nodeDgs
            self.query_root_name = self.parent.query_root_name
            self.query_count = self.parent.query_count
        self.data_node_active = None
        self.current_stage = None
        self.current_scope_symbols ={}

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
            stage_name = None
            if self.scope_root == node:
                self.scope_key=dg_key
                stage_name = "scope_root"
            elif self.scope_key is not None:

                stage_name = dg_key[len(self.scope_key):][0]
                if type(stage_name) is  tuple:
                    stage_name = stage_name[0]
            self.nodeDgs.add_node(
                                  dg_key,
                                  exp_key = node.key,
                                  exp_node=node,
                                  exp_stage=stage_name,
                                  scope_key = self.scope_key,
                                  scope_root_temp = self     #scope_root_temp属性无需进行持久化
                                  )
        self.data_node_active = dg_key
        return dg_key


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

    def is_descendant(self,ast_node:Expression)->bool:
        """检查 ast_node 是否在 该作用域之中"""
        # find 返回第一个匹配的节点，如果找到则返回节点本身，否则返回 None
        node = ast_node
        if self.scope_root is None:
            return True
        while node != self.scope_root and node is not None:
            node = node.parent
            if node == self.scope_root:
                return True
        return False

    def scope_logical_order_key(self,dg_key:tuple)  ->str:
         # 该定义域内不同的阶段的排序
         result = ''
         for item in list(dg_key):
            if type(item) is tuple:
                item = '_'.join(map(str, item))
            result =result+":"+item
         return result

    def scope_logical_order(self,dg_key:tuple):
        return self.nodeDgs.nodes[dg_key]["scope_root_temp"].scope_logical_order_key(dg_key)

    def apply_logical_order(self):
        """
        对于一个已经完成数据提取的定义域，需要按照语义分析的路径进行排序
        :return:
        """
        dg_node_keys = list(self.nodeDgs.nodes)

        dg_node_keys.sort(key=self.scope_logical_order)

        return dg_node_keys

    def add_symbol(self,dg_key:tuple,upper_limit:int,lower_limit:int,symbol_name:str):

        pass

    def symbol_name(self,dg_key:tuple)->str:
        symbol_name = ""
        dg_node = self.nodeDgs.nodes[dg_key]
        if dg_node is None or dg_node.get("exp_key",None) is None:
            return symbol_name

        # 下列分支是为了表的符号名称
        if dg_node["exp_key"] == "cte":
            return dg_node["alias"]
        elif dg_node["exp_key"] == "table":
            if dg_node["table_alias"] != "":
                return dg_node["table_alias"]

            if dg_node["table_name"] == "":
                dg_key_this = list(dg_key)
                dg_key_this.append("this")
                dg_key_this = tuple(dg_key_this)
                return self.symbol_name(dg_key_this)

            symbol_name = dg_node["table_name"]
            if dg_node["schema"] != "":
                symbol_name = f"{dg_node["schema"]}.{symbol_name}"

            if dg_node["catalog"] != "":
                symbol_name = f"{dg_node["catalog"]}.{symbol_name}"

            return symbol_name

        #以下分支是为表述select字句中的所有列的名称符号
        dg_key_parent = dg_key[:-1]
        if dg_key_parent not in self.nodeDgs.nodes:
            return f"not exists {dg_key_parent}"

        if self.nodeDgs.nodes[dg_key_parent].get("exp_key") in ("select",):
            if dg_node["exp_key"] in ("alias", "column","star") :
                symbol_name = dg_node["output_name"]

            if dg_node["exp_key"] == "anonymous":
                symbol_name = dg_node["func_name"]
            elif dg_node.get("func_type","")!="":
                symbol_name = dg_node["exp_key"]

            if dg_node["exp_key"] == "literal":
                symbol_name = dg_node["?column?"]


        return symbol_name