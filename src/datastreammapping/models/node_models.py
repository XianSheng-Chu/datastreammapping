
from networkx import MultiDiGraph
from pydantic import BaseModel, Field, model_validator,field_serializer,PrivateAttr
from typing import Literal, Optional, List,Dict
from datetime import datetime
from sqlglot import Dialects

from enum import StrEnum


class BaseNode(BaseModel):
    node_id:tuple = Field(..., description="全局唯一节点ID")
    description:Optional[str] = Field(None, description="节点描述")

    @property
    def node_type(self) -> str:
        """只读属性：获取当前模型的 Neo4j Label"""
        return self._neo4j_label

    created_data: datetime = Field(default_factory=datetime.now, description="创建时间")
    last_updated_data: datetime = Field(default_factory=datetime.now, description="更新时间")

    _neo4j_label: str =PrivateAttr(default="BaseNode")

    @property
    def neo4j_label(self) -> str:
        """只读属性：获取当前模型的 Neo4j Label"""
        return self._neo4j_label



    @model_validator(mode="after")
    def update_updated_at(self) -> "BaseNode":
        """每次修改模型时自动更新 last_updated_data"""
        self.last_updated_data = datetime.now()
        return self

    @field_serializer("created_at", "updated_at",check_fields=False)
    def serialize_datetime(self, value: datetime) -> str:
        """统一时间格式"""
        return value.strftime("%Y-%m-%d %H:%M:%S")

    @classmethod
    def get_neo4j_labels(cls) -> List[str]:
        """
        递归获取继承链上的所有 Label：
        - 子类的 Label + 基类的 Label
        """
        labels = [cls._neo4j_label]

        # 遍历基类，获取它们的 Label（跳过 BaseModel 本身）
        for base in cls.__bases__:
            if hasattr(base, "get_neo4j_labels") and base is not BaseModel:
                labels.extend(base.get_neo4j_labels())

        # 去重并返回
        return list(dict.fromkeys(labels))

class BaseEntityNode(BaseNode):
    """
    所有实体对象的基类：
    - 例子：数据库表、字段、API接口、消息队列Topic
    """
    _neo4j_label: str =PrivateAttr(default="BaseEntity")



class CatalogNode(BaseEntityNode):
    _neo4j_label: str = PrivateAttr(default="Catalog")

    # 核心属性
    catalog_name:str = Field(..., description="数据目录的名称，在关系型数据库中一般指的是DATANAME")
    scope_name:str = Field(..., description="数据定义域的名称,可以写明环境信息等非严格字段,也可以直接书写实例标识")

    database_type: Literal[
        Dialects.DIALECT.value,
        Dialects.ATHENA.value,
        Dialects.BIGQUERY.value,
        Dialects.CLICKHOUSE.value,
        Dialects.DATABRICKS.value,
        Dialects.DORIS.value,
        Dialects.DRILL.value,
        Dialects.DRUID.value,
        Dialects.DUCKDB.value,
        Dialects.HIVE.value,
        Dialects.MATERIALIZE.value,
        Dialects.MYSQL.value,
        Dialects.ORACLE.value,
        Dialects.POSTGRES.value,
        Dialects.PRESTO.value,
        Dialects.PRQL.value,
        Dialects.REDSHIFT.value,
        Dialects.RISINGWAVE.value,
        Dialects.SNOWFLAKE.value,
        Dialects.SPARK.value,
        Dialects.SPARK2.value,
        Dialects.SQLITE.value,
        Dialects.STARROCKS.value,
        Dialects.TABLEAU.value,
        Dialects.TERADATA.value,
        Dialects.TRINO.value,
        Dialects.TSQL.value
    ]   = Field(..., description="数据库类型")

    # 其他属性
    extra_attrs:Optional[Dict[str|int, str|int]]= Field(default_factory=dict)


class SchemaNode(BaseEntityNode):
    _neo4j_label: str = PrivateAttr(default="Catalog")

    # 核心属性
    schema_name:str = Field(..., description="schema名称")
    scope_name:str = Field("auto_schema", description="数据定义域的名称,可以写明环境信息等非严格字段")

    # 其他属性
    extra_attrs: Optional[Dict[str | int, str | int]] = Field(default_factory=dict)


class QueryNode(BaseEntityNode):
    _neo4j_label: str =PrivateAttr(default="QueryNode")
    scope_name:str = Field(..., description="查询的名称，Query一般是作为一整个sql文件存在的")

    # 其他属性
    extra_attrs: Optional[Dict[str | int, str | int]] = Field(default_factory=dict)

class TableSourceEnum(StrEnum):
    PHYSICAL = "physical"  # 物理表：已在元数据中注册，真实挂载在 schema 下
    SQL_REFERENCED = "sql_referenced"  # SQL引用表：仅在SQL中被引用，未进行元数据注册



class TableNode(BaseEntityNode):
    _neo4j_label: str =PrivateAttr(default="TableNode")

    table_name:str = Field(..., description="数据库实体表的表名")
    table_source: TableSourceEnum = Field(..., description="主要用于区分表的类型，分为未注册表，物理表，视图，临时表等")
    table_comment: Optional[str] = Field(None, description="表注释（来自元数据）")

    # 其他属性
    extra_attrs: Optional[Dict[str | int, str | int]] = Field(default_factory=dict)


def add_node_model_to_graph(graph: MultiDiGraph, model: BaseNode, exclude: set = None):
    # 将节点模型写入图中
    exclude = exclude or set()
    # exclude标识不需要写入图的模型属性
    node_attrs = model.model_dump(exclude=exclude)
    graph.add_node(model.node_id, **node_attrs)





