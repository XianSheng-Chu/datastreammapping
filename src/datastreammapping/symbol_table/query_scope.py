from networkx import MultiDiGraph


from .base import *
from .scope_enums import ScopeType
from sqlglot import *

class QueryScope(SymbolTableScope):

    def spawn_child_scope(self, ast_node: expressions=None, scope_type:ScopeType = ScopeType.QUERY) -> 'QueryScope':
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
        # self.scope_key = None
        if self.scope_type==ScopeType.QUERY:
            self.root_dg_node_model = self.init_root_dg_node_model()
            self.create_root_dg_node()
            self.create_parent_relationship()
            query_dg_key = self.scope_root_key
            self.query_root_name = query_dg_key
            self.query_count = 0
        else:
            self.nodeDgs:MultiDiGraph = self.parent.nodeDgs
            self.query_root_name = self.parent.query_root_name
            self.query_count = self.parent.query_count
        self.data_node_active:tuple
        self.current_stage = None
        self.current_scope_symbols:dict ={}


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
                dg_key.reverse()
                dg_key=list(self.query_root_name)+dg_key
                break
            current_node = current_node.parent

        dg_key = tuple(dg_key)
        if dg_key not in self.nodeDgs.nodes:
            stage_name = None
            if self.scope_root == node:
                self.scope_key=dg_key
                stage_name = "scope_root"
                node_model = self.init_root_dg_node_model()
                self.add_scope_node(node_model)
                if self.parent.scope_type == ScopeType.QUERY:
                    self.create_parent_relationship(EntitySubordinationEdgeEnum.PARENT_QUERY_SCOPE)
            elif self.scope_key is not None:
                stage_name = dg_key[len(self.scope_key):][0]
                if type(stage_name) is  tuple:
                    stage_name = stage_name[0]
                node_model = BaseExpressionsNode(
                    node_id=dg_key,
                    exp_key=node.key,
                    exp_node=node,
                    exp_stage=stage_name,
                    scope_key = self.scope_key
                )
                self.add_scope_node(node_model)

            for i in range(-1,-len(dg_key),-1):
                if dg_key[:i] in self.nodeDgs.nodes:
                    parent_key = dg_key[:i]
                    edge_model_code = CodeStructureEdge(
                        source_node_id=dg_key,
                        target_node_id=parent_key,
                        edge_sub_type=CodeStructureEdgeEnum.SYNTAX_TREE_PARENT
                    )
                    add_edge_model_to_graph(self.nodeDgs,edge_model_code)

                    if self.nodeDgs.nodes[parent_key].get("exp_stage","scope_root") == "scope_root":
                        pass
                    break

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
            self.nodeDgs.nodes[self.data_node_active]["extra_attrs"][key] = value
            # self.nodeDgs.nodes[self.data_node_active][key] = value

    def is_descendant(self,ast_node:Expression)->bool:
        """检查 ast_node 是否在 该作用域之中"""
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

    def add_symbol(self,dg_key:tuple,symbol_name:str):
        dg_node = self.nodeDgs.nodes[dg_key]

        if dg_node.get("exp_stage","scope_root") == "scope_root":
            self.current_scope_symbols.clear()

        if dg_node.get("exp_key") is None:
            return



        if dg_node["exp_key"] in ("cte","table") :
            symbol_type = dg_node["exp_key"]
            if self.current_scope_symbols.get(symbol_type) is None:
                self.current_scope_symbols[symbol_type] = {}
            self.current_scope_symbols[symbol_type][symbol_name] = dg_key
        elif dg_node["exp_key"] in ("subquery",) and dg_node["exp_stage"] in ("from","from_","joins"):
            symbol_type = "table"
            if self.current_scope_symbols.get(symbol_type) is None:
                self.current_scope_symbols[symbol_type] = {}
            self.current_scope_symbols[symbol_type][symbol_name] = dg_key

    def symbol_name(self,dg_key:tuple)->str:
        symbol_name = ""
        dg_node = self.nodeDgs.nodes[dg_key]
        if dg_node is None or dg_node.get("exp_key",None) is None:
            return symbol_name

        # 下列分支是为了表的符号名称
        if dg_node["exp_key"] == "cte":
            return dg_node["extra_attrs"]["alias"]
        elif dg_node["exp_key"] == "table":
            if dg_node["extra_attrs"]["table_alias"] != "":
                return dg_node["extra_attrs"]["table_alias"]
            if dg_node["extra_attrs"]["table_name"] == "":
                dg_key_this = list(dg_key)
                dg_key_this.append("this")
                dg_key_this = tuple(dg_key_this)
                return self.symbol_name(dg_key_this)

            symbol_name = dg_node["extra_attrs"]["table_name"]
            if dg_node["extra_attrs"]["schema"] != "":
                schema_name = dg_node["extra_attrs"]["schema"]
                symbol_name = f"{schema_name}.{symbol_name}"
            if dg_node["extra_attrs"]["catalog"] != "":
                catalog_name = dg_node["extra_attrs"]["catalog"]
                symbol_name = f"{catalog_name}.{symbol_name}"


            return symbol_name
        elif dg_node["exp_key"] == "subquery":
            if dg_node["extra_attrs"].get("alias","") != "":
                symbol_name = dg_node["extra_attrs"]["alias"]
            else:
                symbol_name = self.scope_logical_order_key(dg_key)

        return symbol_name

    def create_table_relationship_map(self):
        scoop_root:QueryScope=self


        while scoop_root.scope_type != ScopeType.QUERY:
            scoop_root = scoop_root.parent
        dg_node_order_keys = scoop_root.apply_logical_order()



        for dg_key in dg_node_order_keys:
            if self.nodeDgs.nodes[dg_key].get("node_name") is None:
                # 补充Expression节点的node_name属性
                self.nodeDgs.nodes[dg_key]["node_name"] = self.get_node_name(dg_key)
            dg_node  = scoop_root.nodeDgs.nodes[dg_key]
            current_scoop:QueryScope = dg_node["scope_root_temp"]
            if not isinstance(current_scoop, QueryScope):
                continue

            if dg_node.get("exp_key", "") in ("subquery","cte"):

                edge_model = DataStreamMappingEdge(
                    source_node_id=dg_key+("this",),
                    target_node_id=dg_key,
                    edge_sub_type=DataStreamMappingEdgeEnum.SUBQUERY_TO_QUERY
                )
                add_edge_model_to_graph(self.nodeDgs, edge_model)
                print(f"{dg_node.get("exp_key", "")}<-{dg_key+("this",)}")
            elif dg_node.get("exp_key", "") in ("tablealias",):
                pass

            if dg_node.get("exp_key","")=="table":
                if dg_node["extra_attrs"].get("table_name")=="":
                    #此处是处理table为一个func的分支
                    pass
                else:
                    symbol_name = dg_node["extra_attrs"]["table_name"]
                    table_name = symbol_name
                    schema_name = self.schema
                    catalog_name = self.catalog

                    if dg_node["extra_attrs"]["schema"] != "":
                        schema_name = dg_node["extra_attrs"]["schema"]
                        symbol_name = f"{schema_name}.{symbol_name}"
                    if dg_node["extra_attrs"]["catalog"] != "":
                        catalog_name = dg_node["extra_attrs"]["catalog"]
                        symbol_name = f"{catalog_name}.{symbol_name}"
                    source_symbol_key = current_scoop.find_mapping_source("table",symbol_name)
                    if source_symbol_key is  None:
                        from .schema_scope import SchemaScope
                        current_schema: SchemaScope = self.find_parent_scope(ScopeType.SCHEMA)
                        source_symbol_key = current_schema.find_table(table_name,schema_name,catalog_name)
                        edge_model = DataStreamMappingEdge(
                            source_node_id=source_symbol_key,
                            target_node_id=dg_key,
                            edge_sub_type=DataStreamMappingEdgeEnum.SQL_REFERENCED_TABLE_FROM_ENTITY
                        )
                    else:
                        edge_model = DataStreamMappingEdge(
                            source_node_id=source_symbol_key,
                            target_node_id=dg_key,
                            edge_sub_type=DataStreamMappingEdgeEnum.TABLE_FROM_QUERY
                        )


                    add_edge_model_to_graph(self.nodeDgs, edge_model)


                    # print(f"{symbol_name}<-{source_symbol_key}")

            current_scoop.add_symbol(dg_key,current_scoop.symbol_name(dg_key))

    def find_mapping_source(self, symbol_type:str, symbol_name:str) -> tuple|list[tuple]:
        current_scoop = self
        result = None
        if symbol_type == "table":
            result = current_scoop.current_scope_symbols.get("cte",{}).get(symbol_name)
            while result is None:
             current_scoop = current_scoop.parent
             if current_scoop.scope_type == ScopeType.QUERY:
                 break
             result = current_scoop.current_scope_symbols.get("cte", {}).get(symbol_name)
             if result is not None:
                 break
        if symbol_type in ("column","star"):

            if symbol_name=="" or symbol_name is None:
                current_tables = current_scoop.current_scope_symbols.get("table", {})
                if len(current_tables) == 1:
                    for table_key in current_tables.values():
                        result = table_key
                if symbol_type == "star":
                    # 只有在select * 的情况下才可能来源于多个表
                    result = list(current_tables.values())

            if result is None:
                # sql中字段可以上溯一个父定义域去获取数据，例如exists语句的条件
                while current_scoop.scope_type != ScopeType.QUERY:
                    result = current_scoop.current_scope_symbols.get("table", {}).get(symbol_name, None)
                    if result is not None:
                        break
                    current_scoop = current_scoop.parent

        return result

    def create_column_relationship_map(self):
        scoop_root: QueryScope = self
        while scoop_root.scope_type != ScopeType.QUERY:
            scoop_root = scoop_root.parent
        dg_node_order_keys = scoop_root.apply_logical_order()


        for dg_key in dg_node_order_keys:
            dg_node  = scoop_root.nodeDgs.nodes[dg_key]
            current_scoop:QueryScope = dg_node["scope_root_temp"]
            if dg_node.get("exp_key", "") in ("column","star") and dg_node["exp_stage"] not in ("order",):
                if dg_node.get("exp_key", "")=="star" and scoop_root.nodeDgs.nodes.get(dg_key[:-1]).get("exp_key", "") == "column":
                    # 如果某个星号已经是一个column的一部分，那么就需要跳过relationship构建过程
                    continue
                symbol_name = ""
                if dg_node["extra_attrs"].get("table","") != "":
                    table_name = dg_node["extra_attrs"].get("table","")
                    symbol_name = table_name
                    if dg_node["extra_attrs"].get("schema","") != "":
                        schema_name = dg_node["extra_attrs"].get("schema","")
                        symbol_name = f"{schema_name}.{symbol_name}"
                        if dg_node["extra_attrs"].get("catalog","") != "":
                            catalog_name = dg_node["extra_attrs"].get("catalog","")
                            symbol_name = f"{catalog_name}.{symbol_name}"

                source_symbol_key = current_scoop.find_mapping_source(dg_node.get("exp_key", ""), symbol_name)

                if source_symbol_key is not None:
                    if  dg_node["extra_attrs"]["output_name"] != "*":
                        edge_sub_type =  DataStreamMappingEdgeEnum.FIELD_TRACES_FROM_TABLE
                    else:
                        edge_sub_type =  DataStreamMappingEdgeEnum.STAR_FIELD_DERIVED_FROM_FIELD
                    if type(source_symbol_key) is tuple:
                        edge_model = DataStreamMappingEdge(
                            source_node_id=source_symbol_key,
                            target_node_id=dg_key,
                            edge_sub_type=edge_sub_type
                        )
                        add_edge_model_to_graph(self.nodeDgs, edge_model)
                    else:
                        # 用于处理 select * from a,b,c 的星号的引用关系
                        for source_symbol in source_symbol_key:
                            edge_model = DataStreamMappingEdge(
                                source_node_id=source_symbol,
                                target_node_id=dg_key,
                                edge_sub_type=edge_sub_type
                            )
                            add_edge_model_to_graph(self.nodeDgs, edge_model)

                print(f"{dg_node["extra_attrs"].get("table","?")}.{dg_node["extra_attrs"]["output_name"]}<-{source_symbol_key}")

    def init_root_dg_node_model(self):
        return QueryNode(
            node_id = self.scope_key,
            scope_name = self.scope_name
        )

    def create_relationship_map(self):
        for children_type,children_scoop in  self.children:
            children_scoop.create_relationship_map()

    def get_node_name(self,dg_key:tuple)->Optional[str]:
        """获取某个Expression节点的node_name"""

        result = None
        dg_node = self.nodeDgs.nodes[dg_key]
        if dg_node["exp_key"] in ("alias", "column", "star"):
            result = dg_node["extra_attrs"]["output_name"]
        elif dg_node["exp_key"] == "anonymous":
            result = dg_node["extra_attrs"]["func_name"]
        elif dg_node["exp_key"] == "literal":
            if dg_node["extra_attrs"]["is_string"]:
                result =  "'"+dg_node["extra_attrs"]["literal_value"]+"'"
            else:
                result = dg_node["extra_attrs"]["literal_value"]
        elif dg_node["extra_attrs"].get("func_type", "") != "":
            result = dg_node["exp_key"]

        if result != "":
            return result

        result = self.symbol_name(dg_key)

        
        return result

    def create_logical_relationship_map(self):
        scope_nodes = self.find_child_nodes(self.scope_key, self.nodeDgs)
        scope_nodes = [item for item in scope_nodes if self.nodeDgs.nodes[item]["scope_root_temp"] == self]
        scope_nodes.sort(key=self.scope_logical_order)
        for dg_key in scope_nodes:
            for i in range(-1,-len(dg_key),-1):
                if dg_key[:i] in self.nodeDgs.nodes:
                    parent_key = dg_key[:i]
                    edge_model_code = CodeStructureEdge(
                        source_node_id=dg_key,
                        target_node_id=parent_key,
                        edge_sub_type=CodeStructureEdgeEnum.SYNTAX_TREE_PARENT
                    )
                    add_edge_model_to_graph(self.nodeDgs,edge_model_code)
                    parent_node = self.nodeDgs.nodes[parent_key]
                    parent_attrs = self.nodeDgs.nodes[parent_key]["extra_attrs"]
                    current_node = self.nodeDgs.nodes[dg_key]
                    edge_type = None
                    if current_node.get("exp_stage","") == "scope_root":
                        edge_type = DataStreamMappingEdgeEnum.DATA_FROM_QUERY
                    elif parent_node.get("exp_key") is not None and (parent_attrs.get("predicate_attrs","consume") != "consume" or
                          (parent_attrs.get("predicate_attrs","") == "consume" and parent_node["exp_key"] != "join" ) or
                          (parent_node["exp_key"] == "join" and self.find_parent_key(current_node["exp_node"])== "on") or
                          parent_node["exp_key"] == "paren" and current_node["extra_attrs"].get("predicate_attrs","") != "" ):
                        edge_type = DataStreamMappingEdgeEnum.LOGICAL_LINK

                    edges = self.nodeDgs.get_edge_data(dg_key,parent_key)
                    if edges is None or EdgeMainTypeEnum.DATA_STREAM_MAPPING not in edges.keys() and edge_type is not None:
                        edge_model_date = DataStreamMappingEdge(
                            source_node_id=dg_key,
                            target_node_id=parent_key,
                            edge_sub_type=edge_type
                        )
                        add_edge_model_to_graph(self.nodeDgs, edge_model_date)
                    break
