from enum import Enum

class ScopeCategory(Enum):
    """作用域类别"""
    DATABASE = 1     # 数据库级别
    QUERY = 5       # 查询级别


class ScopeType(Enum):
    """
    作用域类型枚举 - 带元数据
    每个枚举值包含：显示名称、类别、是否可嵌套
    """

    # === 数据库级别 ===
    CATALOG = ("catalog", ScopeCategory.DATABASE, False)  # 全局作用域
    SCHEMA = ("schema", ScopeCategory.DATABASE, True)  # 模式作用域

    # === 查询级别 ===
    QUERY = ("query", ScopeCategory.QUERY, True)  # 主查询
    SELECT = ("select", ScopeCategory.QUERY, True)  # 主查询
    SUBQUERY = ("subquery", ScopeCategory.QUERY, True)  # 普通子查询
    CORRELATED_SUBQUERY = ("correlated_subquery", ScopeCategory.QUERY, True)
    CTE = ("cte", ScopeCategory.QUERY, True)  # CTE
    RECURSIVE_CTE = ("recursive_cte", ScopeCategory.QUERY, True)

    @classmethod
    def from_string(cls, name: str):

        # 显示名称查找
        for member in cls:
            if member.value[0] == name:  # value[0] 是显示名称
                return member

        raise ValueError(
            f"Invalid scope type: '{name}'"
        )


    def str(self):
        # 显示名称查找
        return self.value[0]

