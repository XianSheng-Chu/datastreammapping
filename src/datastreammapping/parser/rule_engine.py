from sqlglot import Expression
from sqlglot.expressions import Table, Column

from ..graph.run_result_dto  import *

class RuleEngine:
    """
    规则引擎，负责将AST节点转换为图元素。

    应用预定义的规则来创建表节点、列节点和它们之间的关系。
    """

    def __init__(self):
        """初始化规则引擎，加载默认规则集。"""
        self.table_rules = []
        self.column_rules = []
        self.relationship_rules = []
        self.nodeList:list[RunResultDTO] = []
        self.relationsList:list[RelationshipDTO] = []

    def apply_rules(self, ast_node:Expression):
        """
        对AST节点应用所有匹配的规则。

        参数:
            ast_node: 要处理的AST节点

        返回:
            List[GraphElement]: 生成的图元素列表（节点和边）
        """
        self._create_nodes(ast_node)

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