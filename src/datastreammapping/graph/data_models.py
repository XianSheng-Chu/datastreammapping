# graph/data_models.py

from pydantic import BaseModel, Field, model_validator,field_serializer,PrivateAttr
from typing import Literal, Optional, List,Dict
from datetime import datetime
from sqlglot import Dialects


class BaseNode(BaseModel):
    node_key:tuple = Field(..., description="全局唯一节点ID")


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

class BaseEntityNode(BaseModel):
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






