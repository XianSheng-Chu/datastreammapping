from abc import ABC, abstractmethod

from .scope_enums import ScopeType
from ..models.node_models import *
from ..models.edge_models import *


class SymbolTableScope(ABC):
    def __init__(self,scope_name, parent:'SymbolTableScope'=None,scope_type:ScopeType=None):
        self.parent = parent  # 父作用域引用
        self.scope_type = scope_type  # 作用域类型
        self.symbols = {}  # 当前作用域符号表
        self.children = {}  # 子作用域（可选，用于构建完整树）
        self.scope_name = scope_name
        if self.parent is not None:
            self.nodeDgs: MultiDiGraph = self.parent.nodeDgs
        else:
            self.nodeDgs: MultiDiGraph = MultiDiGraph()
        self.scope_key = self.scope_root_key






    def create_root_dg_node(self):
        self.add_scope_node(self.root_dg_node_model)

    def create_parent_relationship(self,edge_sub_type = EntitySubordinationEdgeEnum.PARENT_SCOPE):
        if self.parent is None:
            return
        edge_model = EntitySubordinationEdge(
            source_node_id=self.scope_key,
            target_node_id=self.parent.scope_key,
            edge_sub_type=edge_sub_type
        )
        add_edge_model_to_graph(self.nodeDgs, edge_model)




    def find_parent_scope(self,scope_type:ScopeType) -> 'SymbolTableScope':
        """
        寻找第一个目标类型的父作用域对象
        :return:SymbolTableScope
        """
        current = self.parent
        if current is None:
            return self
        while current.scope_type != scope_type:
            current = current.parent
        return current

    @abstractmethod
    def spawn_child_scope(self)->'SymbolTableScope':
        """
        创建一个子作用域
        :return:
        """

    @property
    def catalog(self) -> str:
        return self.parent.catalog

    @property
    def schema(self)->str:
        return self.parent.schema

    @property
    def scope_root_key(self) -> tuple:
        current = self
        result = None
        if current.parent is not None:
            result =  current.parent.scope_root_key + ((self.scope_type.str(),self.scope_name),)
        return result

    def init_root_dg_node_model(self)->BaseNode:
        model:BaseNode = None
        if self.root_dg_node_model is not None:
            model = self.root_dg_node_model
        return model

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

    def add_scope_node(self,model: BaseNode, exclude: set = None):
        add_node_model_to_graph(self.nodeDgs, model, exclude)
        self.nodeDgs.nodes[model.node_id]["scope_root_temp"] = self


