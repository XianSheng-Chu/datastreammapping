import pytest
from unittest.mock import Mock, patch, MagicMock
import datastreammapping.core as dsmCore

class TestCompiler:
    """编译器功能的测试用例集合。"""

    def setup_method(self):
        self.compiler = dsmCore.SQLToGraphCompiler()

    def test_compiler_initialization(self):
        """测试编译器实例能否正确初始化。"""
        self.compiler = dsmCore.SQLToGraphCompiler()
        assert self.compiler is not None


    def test_compile_simple_sql(self):
        """测试编译器能够处理简单的SELECT * FROM table语句。"""
        self.compiler.compile_sql("""
        
        select sysdate as last_update_date,emp_id,nvl2(a.emp_name,'default',a.emp_name),user1.func1(a.emp_id,a.emp_name) as user_id from emp a
        
        ""","oracle")

    def test_compile_simple_subquery(self):
        """测试编译器能够处理简单的SELECT * FROM table语句。"""
        self.compiler.compile_sql("""

        select false,Last_Day(now()),a.* from (select user_id,user_name from  emp ) a

        """)

    def test_compile_simple_case(self):
        """测试编译器能够处理简单的SELECT * FROM table语句。"""
        self.compiler.compile_sql("""

        select (case when a.emp_name is null 
                    then 'isnull'
                    when a.emp_name != (1,2,5,6) and 1=1 or 1=0
                    then a.emp_name
                else 'user_name' end) as emp_name
          from  emp a

        """)

    def test_compile_with_columns(self):
        """测试编译器能够正确处理包含具体列名的SELECT语句。"""
        self.compiler.compile_sql(
            """
            with a as (select * from user) select a.name,a.id from emp a  join dept b on a.dept_id =b.dept_id where a.id=2
            group by a.name order by b.dept_id;
            """)

    def test_compile_with_table_alias(self):
        """测试编译器能够正确处理使用表别名的SQL语句。"""
        pass

    def test_compile_empty_sql_raises_error(self):
        """测试传入空SQL语句时编译器抛出适当的错误。"""
        pass

    def test_compile_invalid_sql_raises_parse_error(self):
        """测试传入无效SQL时编译器抛出解析错误。"""
        pass