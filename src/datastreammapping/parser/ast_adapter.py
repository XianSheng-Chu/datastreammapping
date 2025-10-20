import sqlglot
from sqlglot import Expression
from sqlglot import Dialect
import sqlglot
import sqlglot.errors as errors
import typing as t
from ..utils import exceptions as dsmExceptions
class ASTAdapter:
    """
    SQL抽象语法树适配器。

    负责使用sqlglot解析SQL并转换为统一的内部AST格式。
    """
    dialect:t.Union[str, Dialect, t.Type[Dialect], None]
    sql_string:str
    def __init__(self,sql_string:str,dialect=None):
        self.dialect = dialect
        self.sql_string = sql_string


    def parse_sql(self, sql_string,read=None) -> Expression:
        """
        解析SQL字符串为抽象语法树(AST)。

        参数:
            sql_string: 要解析的SQL语句

        返回:
            ASTNode: 统一的AST根节点

        异常:
            SQLParseError: 当SQL无法解析时抛出
        """
        try:
            exp = sqlglot.parse_one(sql_string,reat=read)
        except errors.ParseError as e:
            raise dsmExceptions.SQLParseError("sql无法转换为AST,请校验是否是合法语句") from e
        return exp

    def parse_sql(self) -> Expression:
        return self.parse_sql(self.sql_string, self.dialect)

    def traverse_ast(self, ast_node, visitor):
        """
        遍历AST树并应用访问者模式。

        参数:
            ast_node: 要遍历的AST根节点
            visitor: 实现访问者接口的对象，处理各类型节点
        """
        pass

    def _convert_to_unified_format(self, sqlglot_ast):
        """
        将sqlglot的AST转换为统一的内部格式。

        参数:
            sqlglot_ast: sqlglot库生成的原始AST

        返回:
            ASTNode: 统一格式的AST根节点
        """
        pass

    def _extract_table_references(self, ast_node):
        """
        从AST中提取所有表引用信息。

        参数:
            ast_node: AST节点

        返回:
            List[TableInfo]: 表引用信息列表
        """
        pass

    def _extract_column_references(self, ast_node):
        """
        从AST中提取所有列引用信息。

        参数:
            ast_node: AST节点

        返回:
            List[ColumnInfo]: 列引用信息列表
        """
        pass