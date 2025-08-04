import json

import sqlglot
from sqlglot.expressions import Expression, Select
import sqlglot.dialects

from datastreammapping.sqlscriptmapping import SqlScriptMapping


def write_string_to_file(file_path, content):
    """
    清空文件并将字符串写入文件

    参数:
        file_path (str): 目标文件路径
        content (str): 要写入的字符串内容
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)

# 解析 INSERT 语句
sql = """
WITH DepartmentSummary AS (
    SELECT 
        d.department_id AS dep_id,
        d.department_name AS dep_name,
        COUNT(e.employee_id) OVER (PARTITION BY d.department_id) AS emp_count,
        AVG(e.salary) OVER (PARTITION BY d.department_id) AS avg_sal
    FROM departments d
    LEFT JOIN employees e ON d.department_id = e.department_id
),
EmployeeRanking AS (
    SELECT 
        e.employee_id AS emp_id,
        e.employee_name AS emp_name,
        e.salary AS emp_salary,
        RANK() OVER (partition by e.employee_name ORDER BY e.salary DESC) AS sal_rank,
        e.department_id AS dep_id,
        'Active' AS status_flag
    FROM employees e
    WHERE e.hire_date > DATE '2020-01-01'
)
SELECT 
    er.emp_id AS employee_identifier,
    er.emp_name AS employee_name,
    er.emp_salary AS annual_salary,
    er.sal_rank AS salary_rank,
    ds.dep_name AS department_name,
    ds.avg_sal AS department_avg_salary,
    er.status_flag AS status_indicator,
    'Direct Report' AS employee_type
FROM EmployeeRanking er
JOIN DepartmentSummary ds ON er.dep_id = ds.dep_id

UNION ALL

SELECT 
    NULL AS employee_identifier,
    'TOTAL' AS employee_name,
    SUM(e.salary) AS annual_salary,
    NULL AS salary_rank,
    'ALL DEPARTMENTS' AS department_name,
    AVG(e.salary)OVER (ORDER BY e.salary DESC) AS department_avg_salary,
    'System' AS status_indicator,
    'Aggregate' AS employee_type
FROM employees e
WHERE EXISTS (
    SELECT 1 
    FROM departments d 
    WHERE d.department_id = e.department_id
);
"""
parsed = sqlglot.parse_one(sql,read="postgres")

# 提取插入的目标表和列
insert_node = parsed.find(sqlglot.exp.Select)

select_node: Select = insert_node.find(sqlglot.exp.Select).copy()


# 解析 SQL 生成 AST
ast = sqlglot.parse_one(sql)


dict = {}
def myTraverse(tree,temp = ""):
    """递归遍历语法树并打印节点信息"""
    node = tree.copy().bfs()

    for item in node:
        if item.depth==0:
            continue

        if item.depth>1:
            break

        if  None==item.parent_select:
            selete_path = -1
        else:
            pass
            selete_path = item.parent_select.depth

        if item.key == "identifier":
            pass
            #item.
        if item.key == "alias":
            #print(item.depth*" "+f"{item.alias}")
            temp += f"({tree.depth}){item.alias}({type(item).__name__}) <-"
            myTraverse(item, temp)

            pass
        if item.key == "tablealias":
            #print(item.depth*" "+f"{item}")
            pass
        if item.key == "column":
            temp += f"({tree.depth}){item.this}"
            temp += f"<-({tree.depth}){item.table}"
            #print(f"{temp} : {item} ({type(item).__name__})")
            pass
        if item.key == "from":
            temp += f"({tree.depth}){item.alias_or_name}({type(item).__name__}) <-"
            myTraverse(item, temp)

        if item.key == "join":
            temp += f"({tree.depth}){item.alias_or_name}({type(item).__name__}) <-"
            myTraverse(item, temp)
            #print(item.alias_or_name)
        if item.key == "table":
            temp += f"({tree.depth}){item.this}({type(item).__name__}) <-"
            #print(temp)
        if item.key == "subquery":
            myTraverse(item, temp)
            #print(json.dumps(item.dump(), sort_keys=False, indent=4))
        if item.key == "select":
             myTraverse(item,temp)
        if item.key == "where":
             temp += f"({tree.depth}){item.this}({type(item).__name__}) <-"
             #myTraverse(item,temp)
        if item.key == "and":
             temp += f"({tree.depth}){item.this}({type(item).__name__}) <-"
             #myTraverse(item,temp)
        if item.key == "eq":
             temp += f"({tree.depth}){item.this}({type(item).__name__}) <-"
             #myTraverse(item,temp)

        if item.key == "exists":
             temp += f"({tree.depth}){item.this}({type(item).__name__}) <-"
             #myTraverse(item,temp)

        temp = ""
        print(item.depth * "\t"+f"{item.depth}|query:({selete_path})\t{(item)}\t{type(item).__name__}")


#myTraverse(select_node)
#print(select_node.parent_select)
var = SqlScriptMapping(parsed,"postgres")
for item in var.nodeMap.items():
    #print(f"{item[0]}:{type(item[1]).__name__}\t:{type(item[1].parent).__name__}\t:{item[1]}")

    if item[1].key=="column":
        #print(f"{item[0]}:{type(item[1]).__name__}\t:{type(item[1].parent).__name__}\t:{item[1]}")
        #print(item[1].text("table") =="")
        pass
    if item[1].key== "table":
        #print(item[1].named_selects)
        pass
    if item[1].key== "alias":
        #print(item[1].named_selects)
        #print(item[1].args["alias"])
        pass
    if(item[1].key=="concat"):
        pass
        #print(item[1])
        #print(item[1].sql_name())
    if(item[1].key=="star"):
        pass
        #print(item[1])
        #print(item[1].args)
    if(item[1].key=="tablealias"):
        print(item[1])
        print(item[1].args)




for key,value in  var.logicMap[(0,0)]["TableAlias"].items():
    for item in var.expressionsMapTest(key,value):
        print(item)
        pass
    #print("\n")
    #print(var.expressionsMapTest(key,value))
for key,value in var.logicMap.items():
    print(str(key)+":")
    for k1,v1 in value.items():
        print(f"\t{k1}:{v1}")
        pass

for key,value in var.nodeMap.items():
    if value.key=="cte":
        print(f"{key}:{value}")
#print(var.nodeMap[(4,35)].key)
#var.popNode((2, 36))
print(var.root.sql())

#print(json.dumps(parsed.dump(), sort_keys=False, indent=4))

write_string_to_file("temp/语法树JSON.json",json.dumps(parsed.dump(), sort_keys=False, indent=4))
tree = select_node.bfs()
nodeDpath = 0
depthSeq = 0
for item in tree:
    depthSeq += 1
    if nodeDpath != item.depth:
        nodeDpath = item.depth
    if item.key == "cte":
        nodeKey = (nodeDpath, depthSeq)
        print(f"{item.alias}:{nodeKey}")


for node, attrs in var.nodeDg.nodes(data=True):
    if attrs.get("visibilityFlag") :
        attrs["expObject"] = None
        #print(f"{node}: {attrs}")

for u,v,k,d in var.nodeDg.edges(keys=True, data=True):
    if k=="logicalMapping" :
        source = lambda u,x:x.get("note") if u[0]+u[1]<0 else x.get("expNode").key
        print(f"{u}:{source(u,var.nodeDg.nodes[u])}->{v}:{var.nodeDg.nodes[v].get("objName")}:({source(v,var.nodeDg.nodes[v])})[key:{k}]:{d}")


print('alias' in ( "tablealias"))