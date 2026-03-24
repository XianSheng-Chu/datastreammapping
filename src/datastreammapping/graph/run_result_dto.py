"""
运行结果数据传输对象 (DTO) 定义
用于在规则引擎和图构建器之间传递图元素信息
"""

from dataclasses import dataclass
from typing import Dict, Any, List, Optional


@dataclass
class RunResultDTO:
    """
    规则引擎输出的数据传输对象
    描述要创建的图元素，但不包含具体实现
    """
    # 核心标识
    element_id: str  # 元素唯一标识
    element_type: str  # 元素类型：'table', 'column'

    # 属性数据
    properties: Dict[str, Any]  # 元素属性字典

    # 关系信息
    relationships: List['RelationshipDTO']  # 要建立的关系列表

    # 元数据（用于调试和跟踪）
    source_ast_type: Optional[str] = None  # 来源的AST节点类型
    rule_name: Optional[str] = None  # 应用的规则名称

    @classmethod
    def create_table_dto(cls, table_name: str, alias: Optional[str] = None,
                         schema: Optional[str] = None) -> 'RunResultDTO':
        """创建表节点的DTO"""
        properties = {"name": table_name}
        if alias:
            properties["alias"] = alias
        if schema:
            properties["schema"] = schema

        return cls(
            element_id=f"table:{table_name}",
            element_type="table",
            properties=properties,
            relationships=[],
            source_ast_type="Table",
            rule_name="table_mapping_rule"
        )

    @classmethod
    def create_column_dto(cls, column_name: str, table_name: str,
                          alias: Optional[str] = None,
                          data_type: Optional[str] = None) -> 'RunResultDTO':
        """创建列节点的DTO"""
        properties = {
            "name": column_name,
            "table_name": table_name
        }
        if alias:
            properties["alias"] = alias
        if data_type:
            properties["data_type"] = data_type

        return cls(
            element_id=f"column:{table_name}.{column_name}",
            element_type="column",
            properties=properties,
            relationships=[],
            source_ast_type="Column",
            rule_name="column_mapping_rule"
        )


@dataclass
class RelationshipDTO:
    """
    关系描述DTO
    描述图元素之间的关系
    """
    # 关系端点
    source_id: str  # 源元素ID
    target_id: str  # 目标元素ID

    # 关系类型
    relationship_type: str  # 关系类型：'contains', 'references'

    # 关系属性
    properties: Dict[str, Any] = None  # 关系属性字典

    @classmethod
    def create_contains_relationship(cls, table_name: str, column_name: str) -> 'RelationshipDTO':
        """创建表包含列的关系DTO"""
        return cls(
            source_id=f"table:{table_name}",
            target_id=f"column:{table_name}.{column_name}",
            relationship_type="contains",
            properties={}
        )


@dataclass
class GraphElementsDTO:
    """
    图元素批量传输容器
    用于批量传递多个节点和关系
    """
    nodes: List[RunResultDTO]  # 节点DTO列表
    edges: List[RelationshipDTO]  # 边DTO列表

    # 批次信息
    batch_id: Optional[str] = None  # 批次标识
    source_sql: Optional[str] = None  # 来源SQL（用于调试）

    def __init__(self, nodes: List[RunResultDTO] = None, edges: List[RelationshipDTO] = None):
        self.nodes = nodes or []
        self.edges = edges or []

    def add_table(self, table_name: str, alias: Optional[str] = None) -> str:
        """添加表节点并返回节点ID"""
        table_dto = RunResultDTO.create_table_dto(table_name, alias)
        self.nodes.append(table_dto)
        return table_dto.element_id

    def add_column(self, column_name: str, table_name: str,
                   alias: Optional[str] = None) -> str:
        """添加列节点并返回节点ID"""
        column_dto = RunResultDTO.create_column_dto(column_name, table_name, alias)
        self.nodes.append(column_dto)
        return column_dto.element_id

    def add_contains_relationship(self, table_name: str, column_name: str):
        """添加表包含列的关系"""
        relationship = RelationshipDTO.create_contains_relationship(table_name, column_name)
        self.edges.append(relationship)

    def get_table_count(self) -> int:
        """获取表节点数量"""
        return len([node for node in self.nodes if node.element_type == "table"])

    def get_column_count(self) -> int:
        """获取列节点数量"""
        return len([node for node in self.nodes if node.element_type == "column"])

    def get_relationship_count(self) -> int:
        """获取关系数量"""
        return len(self.edges)

    def clear(self):
        """清空所有元素"""
        self.nodes.clear()
        self.edges.clear()

    def is_empty(self) -> bool:
        """检查是否为空"""
        return len(self.nodes) == 0 and len(self.edges) == 0

    def to_summary_dict(self) -> Dict[str, Any]:
        """转换为摘要字典"""
        return {
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "table_count": self.get_table_count(),
            "column_count": self.get_column_count(),
            "relationship_count": self.get_relationship_count(),
            "batch_id": self.batch_id,
            "source_sql": self.source_sql
        }


# 便捷的构造函数
def create_table_dto(table_name: str, **properties) -> RunResultDTO:
    """便捷函数：创建表DTO"""
    return RunResultDTO.create_table_dto(table_name, **properties)


def create_column_dto(column_name: str, table_name: str, **properties) -> RunResultDTO:
    """便捷函数：创建列DTO"""
    return RunResultDTO.create_column_dto(column_name, table_name, **properties)


def create_contains_relationship(table_name: str, column_name: str) -> RelationshipDTO:
    """便捷函数：创建包含关系"""
    return RelationshipDTO.create_contains_relationship(table_name, column_name)


def create_empty_graph_elements() -> GraphElementsDTO:
    """创建空的图元素容器"""
    return GraphElementsDTO()