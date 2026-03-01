from sqlglot import Expression
from sqlglot.expressions import Table, Column, false
from typing import Callable
from ..graph.run_result_dto  import *
from ..symbol_table import *
class RuleEngine:
    """
    规则引擎，负责将AST节点转换为图元素。

    应用预定义的规则来创建表节点、列节点和它们之间的关系。
    """

    def __init__(self,execute_result:dict[str, list[dict[str,Callable]]]):
        """初始化规则引擎，加载默认规则集。"""
        self.current_scope = None
        self.current_scope:QueryScope
        self.table_rules = []
        self.column_rules = []
        self.relationship_rules = []
        self.nodeList:list[RunResultDTO] = []
        self.relationsList:list[RelationshipDTO] = []
        self.execute_result = execute_result

    def apply_rules(self, ast_node:Expression,query_scope:QueryScope):
        """
        对AST节点应用所有匹配的规则。

        参数:
            ast_node: 要处理的AST节点

        返回:
            List[GraphElement]: 生成的图元素列表（节点和边）
        """
        tree = ast_node.dfs()
        self.current_scope = query_scope
        # import json
        # print(json.dumps(ast_node.dump(), sort_keys=False, indent=4))
        for item in tree:
            pattern_flag = False
            rule_weight = 0
            actions = None
            for file_name in self.execute_result.keys():
                rules_list = self.execute_result[file_name]
                for node_rule in rules_list:
                    for step_name in node_rule.keys():
                        if step_name == "pattern":
                            rule = node_rule[step_name]
                            pattern_flag = rule(item)
                        if step_name == "conditions" and pattern_flag:
                            rule = node_rule[step_name]
                            if rule_weight < rule(item):
                                rule_weight = rule(item)
                                actions = node_rule["actions"]

            if actions is not None:
                while not self.current_scope.is_descendant(item):
                    self.current_scope = self.current_scope.parent
                self.current_scope = actions(item,self.current_scope)

        dg_node_keys = self.current_scope.apply_logical_order()

        # test
        self.current_scope.create_table_relationship_map()
        for node in dg_node_keys:
            if self.current_scope.nodeDgs.nodes[node].get("exp_node") is not None:
                # print(f"{node}:{self.current_scope.nodeDgs.nodes[node]["exp_stage"]}")
                # print(f"{node}:{self.current_scope.nodeDgs.nodes[node].get("output_flag",False)}")
                symbol_name = self.current_scope.nodeDgs.nodes[node]["scope_root_temp"].symbol_name(node)
                self.current_scope.nodeDgs.nodes[node].get("scope_root_temp").add_symbol(node,symbol_name)
                print(f"{node}:{symbol_name}")
                # print(f"{node}:{self.current_scope.nodeDgs.nodes[node]["scope_root_temp"].symbol_name(node)}")

            else:
                print(f"{node}")



    def _create_nodes(self,ast_node:Expression):
        for item in ast_node:
            if item.key=="table":
                self._create_table_node(item)
            elif item.key=="column":
                self._create_column_node(item)

    def _create_table_node(self, table_ast:Table):
        """
        根据表AST节点创建表图节点。

        参数:
            table_ast: 表类型的AST节点

        返回:
            TableNode: 创建的表节点
        """
        self.nodeList.append(create_table_dto(table_ast.name,alias=table_ast.alias,schema=table_ast.catalog))



    def _create_column_node(self, column_ast:Column):
        """
        根据列AST节点创建列图节点。

        参数:
            column_ast: 列类型的AST节点

        返回:
            ColumnNode: 创建的列节点
        """
        self.nodeList.append(create_column_dto(column_ast.name,column_ast.table,alias=column_ast.alias))


    def _create_contains_relationship(self, table_node, column_node):
        """
        创建表包含列的关系边。

        参数:
            table_node: 表节点
            column_node: 列节点

        返回:
            GraphEdge: 表包含列的关系边
        """


        pass

    def _find_matching_rules(self, node_type):
        """
        查找匹配指定节点类型的规则。

        参数:
            node_type: 节点类型名称

        返回:
            List[Rule]: 匹配的规则列表
        """
        pass