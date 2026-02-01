from abc import ABC, abstractmethod


class SymbolTableScope(ABC):
    def __init__(self,scope_name, parent:'SymbolTableScope'=None, scope_type=None):
        self.parent = parent  # 父作用域引用
        self.scope_type = scope_type  # 作用域类型
        self.symbols = {}  # 当前作用域符号表
        self.children = {}  # 子作用域（可选，用于构建完整树）
        self.scope_name = scope_name

    @abstractmethod
    def spawn_child_scope(self)->'SymbolTableScope':
        """
        创建一个子作用域
        :return:
        """



