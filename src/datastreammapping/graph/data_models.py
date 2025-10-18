# graph/data_models.py

from typing import Dict, Any, List, Optional
from enum import Enum
from dataclasses import dataclass
import networkx as nx


class NodeType(Enum):
    """定义mvp阶段需要处理的数据类型"""
    TABLE = "Table"
    COLUMN = "Column"


class RelationshipType(Enum):
    """MVP阶段只定义这一种核心关系"""
    CONTAINS = "CONTAINS"


@dataclass
class SQLKnowledgeGraph:
    """
    MVP阶段的核心图谱容器
    封装networkx，提供业务语义接口
    """
    _graph: nx.MultiDiGraph = None

    def __post_init__(self):
        """初始化图谱实例"""
        pass

    def add_table(self, table_name: str, alias: Optional[str] = None) -> str:
        """
        添加表节点

        Args:
            table_name: 表名称
            alias: 表别名（可选）

        Returns:
            创建的节点ID
        """
        pass

    def add_column(self, column_name: str, table_name: str, alias: Optional[str] = None) -> str:
        """
        添加列节点

        Args:
            column_name: 列名称
            table_name: 所属表名称
            alias: 列别名（可选）

        Returns:
            创建的节点ID
        """
        pass

    def link_table_column(self, table_name: str, column_name: str):
        """
        建立表-列的包含关系

        Args:
            table_name: 表名称
            column_name: 列名称

        Raises:
            ValueError: 当表节点或列节点不存在时
        """
        pass

    def get_tables(self) -> List[Dict[str, Any]]:
        """
        获取所有表节点

        Returns:
            表节点信息列表
        """
        pass

    def get_columns(self, table_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        获取列节点，可筛选指定表的列

        Args:
            table_name: 表名称（可选，用于筛选）

        Returns:
            列节点信息列表
        """
        pass

    def get_relationships(self) -> List[Dict[str, Any]]:
        """
        获取所有关系

        Returns:
            关系信息列表
        """
        pass

    def to_dict(self) -> Dict[str, Any]:
        """
        转换为可序列化的字典格式
        这是MVP最重要的输出方法

        Returns:
            可序列化的图谱数据
        """
        pass

    def visualize(self) -> str:
        """
        简单的文本可视化，用于调试
        MVP阶段避免复杂依赖，用纯文本展示

        Returns:
            格式化的文本输出
        """
        pass

    def get_node_count(self) -> int:
        """
        获取节点总数

        Returns:
            节点数量
        """
        pass

    def get_edge_count(self) -> int:
        """
        获取边总数

        Returns:
            边数量
        """
        pass

    def clear(self):
        """
        清空图谱数据
        """
        pass