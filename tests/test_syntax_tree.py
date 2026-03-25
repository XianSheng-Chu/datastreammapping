import json

import sqlglot

from tests.test_SqlScriptMapping import write_string_to_file

def test_syntax_tree():
    sql = """
        -- 示例：将每个部门中工资高于该部门平均工资的员工插入到 high_salary_employees 表中
WITH dept_avg AS (
    SELECT department_id, AVG(salary) AS avg_salary
    FROM employees
    GROUP BY department_id
)
INSERT INTO ctr.tr.high_salary_employees (employee_id, name, department_id, salary, insert_time)
SELECT e.employee_id, e.name, e.department_id, e.salary, NOW()
FROM employees e
JOIN dept_avg d ON e.department_id = d.department_id
WHERE e.salary > d.avg_salary;
        """
    parsed = sqlglot.parse_one(sql, read="postgres")
    write_string_to_file("../temp/语法树JSON.json", json.dumps(parsed.dump(), sort_keys=False, indent=4))

