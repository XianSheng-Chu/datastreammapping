import pytest
from unittest.mock import Mock, patch, MagicMock
import datastreammapping.core as dsmCore

class TestCompiler:
    """编译器功能的测试用例集合。"""

    def setup_method(self):
        self.compiler = dsmCore.SQLToGraphCompiler("postgres")

    def test_compiler_initialization(self):
        """测试编译器实例能否正确初始化。"""
        self.compiler = dsmCore.SQLToGraphCompiler("postgres")
        assert self.compiler is not None


    def test_compile_simple_sql(self):
        """测试编译器能够处理简单的SELECT * FROM table语句。"""
        self.compiler.compile_sql("""
        
        select sysdate as last_update_date,emp_id,nvl2(a.emp_name,'default',a.emp_name),user1.func1(a.emp_id,a.emp_name) as user_id from emp a
        
        ""","oracle")

    def test_compile_simple_subquery(self):
        """测试编译器能够处理简单的SELECT * FROM table语句。"""
        self.compiler.compile_sql("""

        -- 电商用户复购行为深度分析（表与子查询多次出现）
SELECT 
    -- 主用户信息
    main_u.user_id,
    main_u.user_name,
    main_u.register_time,
    
    -- 首次订单信息（第1次使用 fact_order）
    first_o.order_id AS first_order_id,
    first_o.order_create_time AS first_order_time,
    first_o_total.amount AS first_order_amount,
    
    -- 最近订单信息（第2次使用 fact_order）
    last_o.order_id AS last_order_id,
    last_o.order_create_time AS last_order_time,
    last_o_total.amount AS last_order_amount,
    
    -- 复购订单数（第3次使用 fact_order，关联子查询）
    (SELECT COUNT(*) 
     FROM fact_order repurchase_o  -- 子查询中再次使用 fact_order
     WHERE repurchase_o.user_id = main_u.user_id
       AND repurchase_o.order_create_time > first_o.order_create_time
       AND repurchase_o.order_status IN ('paid', 'completed')
    ) AS repurchase_order_count,
    
    -- 复购订单总金额（第4次使用 fact_order，相似子查询再次出现）
    (SELECT SUM(repurchase_detail.price * repurchase_detail.quantity)
     FROM fact_order repurchase_o2  -- 又一次使用 fact_order
     INNER JOIN fact_order_detail repurchase_detail
         ON repurchase_o2.order_id = repurchase_detail.order_id
     WHERE repurchase_o2.user_id = main_u.user_id
       AND repurchase_o2.order_create_time > first_o.order_create_time
       AND repurchase_o2.order_status IN ('paid', 'completed')
    ) AS repurchase_total_amount,
    
    -- 用户平均订单金额（第5次使用 fact_order，子查询中多次关联 dim_user）
    (SELECT AVG(order_avg.amount)
     FROM (
         -- 子查询的子查询，再次使用 fact_order 和 dim_user
         SELECT 
             o_inner.user_id,
             SUM(od_inner.price * od_inner.quantity) AS amount
         FROM fact_order o_inner  -- 再次使用 fact_order
         INNER JOIN fact_order_detail od_inner
             ON o_inner.order_id = od_inner.order_id
         INNER JOIN dim_user u_inner  -- 再次使用 dim_user
             ON o_inner.user_id = u_inner.user_id
         WHERE u_inner.user_id = main_u.user_id
           AND o_inner.order_status IN ('paid', 'completed')
         GROUP BY o_inner.user_id
     ) AS order_avg
    ) AS user_avg_order_amount,
    
    -- 同注册月份用户的平均复购次数（子查询中同时使用 dim_user 和 fact_order 多次）
    (SELECT AVG(repurchase_count)
     FROM (
         SELECT 
             u_compare.user_id,
             COUNT(*) AS repurchase_count
         FROM dim_user u_compare  -- 再次使用 dim_user
         INNER JOIN fact_order o_compare  -- 再次使用 fact_order
             ON u_compare.user_id = o_compare.user_id
         WHERE 
             DATE_TRUNC('month', u_compare.register_time) = DATE_TRUNC('month', main_u.register_time)
             AND o_compare.order_create_time > (
                 -- 子查询的子查询的子查询，fact_order 又出现了
                 SELECT MIN(first_compare.order_create_time)
                 FROM fact_order first_compare
                 WHERE first_compare.user_id = u_compare.user_id
             )
         GROUP BY u_compare.user_id
     ) AS compare_repurchase
    ) AS cohort_avg_repurchase_count

FROM 
    dim_user main_u  -- 主表 dim_user
INNER JOIN (
    -- 首次订单子查询（第1次嵌套使用 fact_order）
    SELECT 
        fo.user_id,
        fo.order_id,
        fo.order_create_time
    FROM fact_order fo
    INNER JOIN (
        SELECT user_id, MIN(order_create_time) AS first_time
        FROM fact_order  -- 子查询中再次使用 fact_order
        GROUP BY user_id
    ) AS first_time_o
        ON fo.user_id = first_time_o.user_id
        AND fo.order_create_time = first_time_o.first_time
) AS first_o
    ON main_u.user_id = first_o.user_id
INNER JOIN (
    -- 最近订单子查询（第2次嵌套使用 fact_order，结构与上面相似）
    SELECT 
        lo.user_id,
        lo.order_id,
        lo.order_create_time
    FROM fact_order lo
    INNER JOIN (
        SELECT user_id, MAX(order_create_time) AS last_time
        FROM fact_order  -- 又一次使用 fact_order
        GROUP BY user_id
    ) AS last_time_o
        ON lo.user_id = last_time_o.user_id
        AND lo.order_create_time = last_time_o.last_time
) AS last_o
    ON main_u.user_id = last_o.user_id
INNER JOIN (
    -- 首次订单金额子查询（第3次嵌套使用 fact_order）
    SELECT 
        fo_total.order_id,
        SUM(od_total.price * od_total.quantity) AS amount
    FROM fact_order fo_total
    INNER JOIN fact_order_detail od_total
        ON fo_total.order_id = od_total.order_id
    GROUP BY fo_total.order_id
) AS first_o_total
    ON first_o.order_id = first_o_total.order_id
INNER JOIN (
    -- 最近订单金额子查询（第4次嵌套使用 fact_order，结构与上面相似）
    SELECT 
        lo_total.order_id,
        SUM(od_total2.price * od_total2.quantity) AS amount
    FROM fact_order lo_total
    INNER JOIN fact_order_detail od_total2
        ON lo_total.order_id = od_total2.order_id
    GROUP BY lo_total.order_id
) AS last_o_total
    ON last_o.order_id = last_o_total.order_id

WHERE 
    main_u.is_deleted = 0
    AND main_u.user_status = 'active'
    AND first_o.order_create_time != last_o.order_create_time  -- 排除只有一次订单的用户
    AND (
        -- WHERE 子句中也使用子查询，再次关联 fact_order
        EXISTS (
            SELECT 1
            FROM fact_order high_value_o
            WHERE high_value_o.user_id = main_u.user_id
              AND high_value_o.order_create_time > first_o.order_create_time
              AND (
                  SELECT SUM(price * quantity)
                  FROM fact_order_detail
                  WHERE order_id = high_value_o.order_id
              ) > 1000
        )
        OR main_u.register_time < '2024-06-01'
    )

ORDER BY 
    repurchase_order_count DESC,
    user_avg_order_amount DESC
LIMIT 50 OFFSET 0;
        

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
            WITH company_avg AS (
                SELECT AVG(salary) AS avg_salary FROM employees
            ),
            high_earners AS (
                SELECT e.id, e.name, e.salary, e.department_id
                FROM employees e, company_avg
                WHERE e.salary > company_avg.avg_salary
            )
            SELECT 
                d.department_name,
                COUNT(h.id) AS high_earner_count,
                1,d.*
            FROM (select * from fin_date.departments) d
            LEFT JOIN high_earners h ON d.id = h.department_id
            GROUP BY d.department_name
            ORDER BY high_earner_count DESC;
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