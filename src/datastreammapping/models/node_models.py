import sqlglot
from networkx import MultiDiGraph
from pydantic import BaseModel, Field, model_validator, field_serializer, PrivateAttr, computed_field, SkipValidation
from typing import Literal, Optional, List, Dict, Any, Annotated
from datetime import datetime
from sqlglot import Dialects, Expression

from enum import StrEnum


class BaseNode(BaseModel):
    node_id:tuple = Field(..., description="全局唯一节点ID")
    description:Optional[str] = Field(None, description="节点描述")
    node_name:Optional[str] = Field(None, description="节点名称")

    @computed_field #computed_field注解的字段在调用model_dump()时会被序列化
    @property
    def node_type(self) -> str:
        """只读属性：获取当前模型的 Neo4j Label"""
        return self._neo4j_label

    created_data: datetime = Field(default_factory=datetime.now, description="创建时间")
    last_updated_data: datetime = Field(default_factory=datetime.now, description="更新时间")

    _neo4j_label: str =PrivateAttr(default="BaseNode")

    @computed_field
    @property
    def neo4j_label(self) -> str:
        """只读属性：获取当前模型的 Neo4j Label"""
        return self._neo4j_label



    @model_validator(mode="after")
    def update_updated_at(self) -> "BaseNode":
        """每次修改模型时自动更新 last_updated_data"""
        self.last_updated_data = datetime.now()
        return self

    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
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

    # 其他属性
    extra_attrs:Optional[Dict[str|int, str|int]]= Field(default_factory=dict, description="节点拓展属性")

class BaseEntityNode(BaseNode):
    """
    所有实体对象的基类：
    - 例子：数据库表、字段、API接口、消息队列Topic
    """
    _neo4j_label: str =PrivateAttr(default="BaseEntity")
    is_entity:Literal[True] = Field(True, description="标识节点是否时一个实体节点")



class CatalogNode(BaseEntityNode):
    _neo4j_label: str = PrivateAttr(default="Catalog")

    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
        self.node_name = f"{self.node_type}:{self.catalog_name}"
        return self

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





class SchemaNode(BaseEntityNode):
    _neo4j_label: str = PrivateAttr(default="Schema")

    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
        self.node_name = f"{self.node_type}:{self.schema_name}"
        return self

    # 核心属性
    schema_name:str = Field(..., description="schema名称")
    scope_name:str = Field("auto_schema", description="数据定义域的名称,可以写明环境信息等非严格字段")




class QueryNode(BaseEntityNode):
    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
        self.node_name = f"{self.node_type}:{self.scope_name}"
        return self

    _neo4j_label: str =PrivateAttr(default="Query")
    scope_name:str = Field(..., description="查询的名称，Query一般是作为一整个sql文件存在的")

    # 其他属性
    extra_attrs: Optional[Dict[str | int, str | int]] = Field(default_factory=dict)

class TableSourceEnum(StrEnum):
    PHYSICAL = "physical"  # 物理表：已在元数据中注册，真实挂载在 schema 下
    SQL_REFERENCED = "sql_referenced"  # SQL引用表：仅在SQL中被引用，未进行元数据注册



class TableNode(BaseEntityNode):
    _neo4j_label: str =PrivateAttr(default="Table")

    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
        self.node_name = f"{self.node_type}:{self.table_name}"
        return self

    table_name:str = Field(..., description="数据库实体表的表名")
    table_source: TableSourceEnum = Field(..., description="主要用于区分表的类型，分为未注册表，物理表，视图，临时表等")
    table_comment: Optional[str] = Field(None, description="表注释（来自元数据）")




class BaseExpressionsNode(BaseNode):
    """
    所有sql语句内表达式的父节点
    """
    _neo4j_label: str =PrivateAttr(default="Expressions")
    is_entity:Literal[False] = Field(False, description="标识节点是否时一个实体节点")
    exp_key: str = Field(..., description="Expressions对象的key,通常是小写的类名")
    exp_stage:str = Field(..., description="节点所在编译阶段的名称，比如where字句，group字句等")
    #SkipValidation将跳过所有的字段校验
    exp_node:Annotated[SkipValidation,Expression] = Field(None, description="节点挂载的Expression树的位置")
    output_flag:bool = Field(False, description="标识该节点是否是一个输出字段，能否被父作用域引用，在select中代表该节点是否是一个查询字段")
    scope_key:tuple = Field(..., description="节点所在sql定义域的根节点")


class BaseExpressionsScopeNode(BaseExpressionsNode):
    """
    所有sql语句内单独的符号表定义域的父节点
    """
    _neo4j_label: str =PrivateAttr(default="ExpressionsScope")
    exp_stage:Literal["scope_root"] = Field("scope_root", description="对于一个定义域的顶端节点，阶段名称固定为scope_root")

    @model_validator(mode="after")
    def create_node_name(self) -> "BaseNode":
        """每次修改模型时自动更新 node_name"""
        self.node_name = f"{self.exp_key}"
        return self


class SelectScopeNode(BaseExpressionsScopeNode):
    """
    select语句节点
    """
    _neo4j_label: str =PrivateAttr(default="SelectScope")
    output_names:list[str] = Field(default_factory=list, description="select的输出列的列名列表")

class InsertScopeNode(BaseExpressionsScopeNode):
    """
    select语句节点
    """
    _neo4j_label: str =PrivateAttr(default="InsertScope")
    insert_names :list[str] = Field(default_factory=list, description="insert语句的插入字段")








def add_node_model_to_graph(graph: MultiDiGraph, model: BaseNode, exclude: set = None):
    # 将节点模型写入图中
    exclude = exclude or set()
    # exclude标识不需要写入图的模型属性
    node_attrs = model.model_dump(exclude=exclude)
    graph.add_node(model.node_id, **node_attrs)





