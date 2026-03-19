# 第一层：所有关系的主分类
from enum import StrEnum


class EdgeMainTypeEnum(StrEnum):
    ENTITY_SUBORDINATION = "entity_subordination"
    DATA_STREAM_MAPPING = "data_stream_mapping"
    CODE_STRUCTURE = "code_structure"



# 子枚举1：实体从属关系
class EntitySubordinationEdgeEnum(StrEnum):
    # 指向父作用域的关系，比如schema与catalog之间的关系
    PARENT_SCOPE = "parent_scope"
    # 指向QUERY_SCOPE的关系，即一个sql文本中多个sql语句都指向sql文本名称的关系
    PARENT_QUERY_SCOPE = "parent_query_scope"

# 子枚举2：数据流映射
class DataStreamMappingEdgeEnum(StrEnum):


    # 存储的是直接引用实体表的数据流
    SQL_REFERENCED_TABLE_FROM_ENTITY = "sql_referenced_table_from_entity"

    # 存储的是字段引用表名的关系
    FIELD_TRACES_FROM_TABLE = "field_traces_from_table"

    # 存储select字句中表达式到字段别名的关系
    ALIAS_FROM_EXPRESSION = "alias_from_expression"

    # 存储来自于CTE表达式的信息
    TABLE_FROM_QUERY = "table_from_query"

    # 存储某个被括号的subquery到subquery或者cte的关系,cte的subquer映射到from字句中的table的关系
    SUBQUERY_TO_QUERY = "subquery_to_query"

    # 表别名
    TABLE_ALIAS_FROM_QUERY = "table_alias_from_query"

    # select字句流向外部定义域的数据流
    EXPRESSION_TO_QUERY = "expression_to_query"

    # 存储一个query字句所输出的数据信息，例如 exists (select 1 from tmp) 这样的关系
    DATA_FROM_QUERY = "data_from_query"

    # 存储的是来自于表或者子查询的 * 号引用
    STAR_FIELD_DERIVED_FROM_FIELD = "star_derived_from_field"

    # 以下均为非幂等映射，即数据可能已经经历了转换操作
    # 逻辑链路,所有的涉及到逻辑关系的判断所包含的关系
    LOGICAL_LINK = "logical_link"

    # 二元比较符的左值链路
    LOGICAL_LINK_BINARY_LEFT = "logical_link_binary_left"

    # 二元比较符的左值链路
    LOGICAL_LINK_BINARY_RIGHT = "binary_logical_link_right"

    # 数据转换流，例如在select子句中对输出字段所做的转换操作，以及某些位置使用的各种函数
    TRANSFORM_DATE = "transform_date"


    # 用于指向执行顺序的流程，例如order中的字段列表，表的join顺序等
    EXECUTION_SEQUENTIAL = "execution_sequential"

    # 兜底
    DATA_STREAM_OTHER = "data_stream_other"



# 子枚举3：代码结构
class CodeStructureEdgeEnum(StrEnum):
    # sql的语法树结构，关系中存储着两个节点的依赖关系，例如where节点指向select节点
    SYNTAX_TREE_PARENT = "syntax_tree_parent"
