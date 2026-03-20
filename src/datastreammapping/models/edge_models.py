
from pydantic import BaseModel, Field, model_validator,field_serializer,PrivateAttr
from typing import Literal, Optional, Any,Dict
from datetime import datetime
from networkx import MultiDiGraph

from .enums import (
    EdgeMainTypeEnum,
    EntitySubordinationEdgeEnum,
    DataStreamMappingEdgeEnum,
    CodeStructureEdgeEnum,
)




class BaseEdge(BaseModel):
    edge_id:str  = Field(None, description="关系唯一ID")
    source_node_id: tuple = Field(..., description="起点节点ID")
    target_node_id: tuple = Field(..., description="终点节点ID")

    edge_main_type: EdgeMainTypeEnum = Field(..., description="关系主类型")
    edge_sub_type: str = Field(..., description="关系细分类型")

    created_at: datetime = Field(default_factory=datetime.now, description="关系创建时间")
    updated_at: datetime = Field(default_factory=datetime.now, description="关系更新时间")
    extra_attrs: Dict[str, Any] = Field(default_factory=dict, description="动态扩展属性")

    @field_serializer("created_at", "updated_at",check_fields=False)
    def serialize_datetime(self, value: datetime) -> str:
        """统一时间格式"""
        return value.strftime("%Y-%m-%d %H:%M:%S")


    # 自动生成 edge_id
    @model_validator(mode="after")
    def generate_edge_id(self) -> "BaseEdge":
        if not self.edge_id:
            timestamp = int(self.created_at.timestamp())
            # 规则：{主类型}:{子类型}:{源节点}:{目标节点}:{时间戳}
            self.edge_id = (
                f"{self.edge_main_type.value}:{self.edge_sub_type}:"
                f"{self.source_node_id}:{self.target_node_id}:{timestamp}"
            )
        return self

class EntitySubordinationEdge(BaseEdge):
    edge_main_type: Literal[EdgeMainTypeEnum.ENTITY_SUBORDINATION] = Field(EdgeMainTypeEnum.ENTITY_SUBORDINATION, description="实体关系类型")
    edge_sub_type:EntitySubordinationEdgeEnum  = Field(..., description="关系细分类型")

class DataStreamMappingEdge(BaseEdge):
    edge_main_type: Literal[EdgeMainTypeEnum.DATA_STREAM_MAPPING] = Field(EdgeMainTypeEnum.DATA_STREAM_MAPPING, description="实体关系类型")
    edge_sub_type:DataStreamMappingEdgeEnum  = Field(..., description="关系细分类型")

class CodeStructureEdge(BaseEdge):
    edge_main_type: Literal[EdgeMainTypeEnum.CODE_STRUCTURE] = Field(EdgeMainTypeEnum.CODE_STRUCTURE, description="实体关系类型")
    edge_sub_type:CodeStructureEdgeEnum  = Field(..., description="关系细分类型")




def add_edge_model_to_graph(
    graph:MultiDiGraph,
    edge_model: BaseEdge,
    exclude_attrs: Optional[set[str]] = None):
    """
       将边模型对象添加到 MultiDiGraph 中

       Args:
           graph: 目标 MultiDiGraph 对象
           edge_model: 边模型对象（BaseEdge 或其子类）
           exclude_attrs: 不需要存储到图中的模型属性（默认排除 source_node_id/target_node_id）
    """

    if exclude_attrs is None:
        exclude_attrs = {"source_node_id", "target_node_id"}

    source = edge_model.source_node_id
    target = edge_model.target_node_id
    edge_key = edge_model.edge_main_type

    if source not in graph.nodes:
        raise ValueError("node_key '{}' is not in MultiDiGraph object nodes".format(source))

    if target not in graph.nodes:
        raise ValueError("node_key '{}' is not in MultiDiGraph object nodes".format(target))

    #将模型转换为字典（排除不需要的属性）

    edge_attrs = edge_model.model_dump(exclude=exclude_attrs)
    # 4. 调用 add_edge 原地添加边
    graph.add_edge(
        source,
        target,
        key=edge_key,
        **edge_attrs
    )