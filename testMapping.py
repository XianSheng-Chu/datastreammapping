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
--INSERT INTO db1.arget_t (id, name,emp_id)
with source_t as (select * from source_ti a),
source_user1 as (select * from source_user),
source_emp_t as (select * from source_emp_ti),
source_user as (select user_id,user_name from source_t group by user_id,user_name,decode(user_type_code,'admin',1,0) having count(*) > 1 and max(user_type)<>min(user_type))
SELECT distinct
    a.user_id AS id,
    b.user_id,
    CONCAT(a.first_name, ' ', a.last_name) AS source_names,
    case when b.emp_id = '01101'
    then '22331' else a.emp_id end  as user_emp_id,
    b.emp_id as new_emp_id,
    b.emp_name,
    max(b.emp_id)over(partition by a.user_id),
    row_number()over(partition by dept.dept_id order by user_id desc,a.emp_id ) as rn,
    1 as bvz_id,
    '压测区' as bvz_name,
    'testVale',
    now() as last_update_date,
    fun1('a') as user_name, 
    fun2(b.emp_id) as emp_user_name, 
    dim.contract_number,
    dim.*,*,a.status,
    count(*)over(partition by a.user_id,1),
    ? as test_value,
    $P_START_DATE as start_date
FROM (
with source_t_cte as (select * from source_t_cte1)

select user_id,first_name,status,emp_id from user1.source_t a left join source_t_cte b on a.user_id = b.cust_id) a join source_emp_t b
on a.emp_id = b.emp_id,
user1.dim_contract dim,ctr.user2.dept
 join test3 on a.user_id= test3.test3_id
left join (select t4.test4_id from test4 t4) on a.user_id= test4_id

WHERE a.status = 'active'
and exists (select 1 from user2.contract_t c where a.contract_id = c.contract_id)
and (a.user_id = user2.dim.user_id1(+)
and b.dept_id = ctr.user2.dept.dept_id2)
order by source_names desc,id desc,emp_name,2 desc
union all
with table_union_cte as (select *from table_union2)
select id,user_id,source_names,user_emp_id,new_emp_id,emp_name,rn1,'1','testVale',last_update_date,user_name,table_union.emp1_user_name,contract_number,*
from table_union_cte t2
union all
select id,user_id,source_names,user_emp_id,new_emp_id,emp_name,rn1,'1','testVale',last_update_date,user_name,emp3_user_name,contract_number,*
from (select *from table_union1) where last_update_date > '2025-04-27'::date
;
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


for node, attrs in var.nodeDgs.nodes(data=True):
    if attrs.get("visibilityFlag") :
        #attrs["expObject"] = None
        print(f"{node}: {attrs}")

for u,v,k,d in var.nodeDgs.edges(keys=True, data=True):
    if k=="logicalMapping" :
        source = lambda u,x:x.get("note") if u[0]+u[1]<0 else var.nodeMap[u].key
        print(f"{u}:{source(u, var.nodeDgs.nodes[u])}->{v}:{var.nodeDgs.nodes[v].get("objName")}:({source(v, var.nodeDgs.nodes[v])})[key:{k}]:{d}")

from pyvis.network import Network
import networkx as nx

# 创建多重有向图
G = nx.MultiDiGraph()
edges = [
    ('A', 'B', {'key': 'edge1', 'weight': 4, 'label': 'Edge 1'}),
    ('A', 'B', {'key': 'edge2', 'weight': 7, 'label': 'Edge 2'}),
    ('B', 'C', {'key': 'edge3', 'weight': 5, 'label': 'Edge 3'}),
    ('C', 'A', {'key': 'edge4', 'weight': 6, 'label': 'Edge 4'})
]
G.add_edges_from(edges)

G = var.nodeDgs.copy()
G = nx.relabel_nodes(G, lambda x: str(x))

# 批量设置所有边的 'weight' 属性为 1
for u, v, key in G.edges(keys=True):
    G[u][v][key]['parentNode'] = None
    G[u][v][key]['locigType'] = str(G[u][v][key].get('locigType'))



nx.set_node_attributes(G,  None,'expNode')
nx.set_node_attributes(G,  None,'locigType')
for node, data in G.nodes(data=True):
    # 设置 PyVis 将使用的 label 属性
    if 'objName' in data:
        data['label'] = data['objName']

# 验证结果
#print(list(G.edges()))

# 转换为 PyVis 网络
net = Network(notebook=True, directed=True, height="1000px", width="100%")
net.from_nx(G)

# 配置选项
net.set_options("""
{
  "physics": {
    "enabled": true,
    "stabilization": {
      "iterations": 100
    }
  },
  "edges": {
    "smooth": {
      "type": "horizontal",
      "roundness": 0.2
    },
    "arrows": {
      "to": {
        "enabled": true,
        "scaleFactor": 1.5
      }
    }
  },
  "interaction": {
    "hover": true,
    "tooltipDelay": 200
  }
}
""")

# 添加自定义边标签
for edge in net.edges:
    # 从原始图获取键和权重
    original_data = G.get_edge_data(edge['from'], edge['to'], edge['note'])
    if original_data:
        edge['title'] = f"Key: {edge['key']}\nWeight: {original_data['weight']}"
        edge['label'] = f"{edge['key']}:{original_data['weight']}"
# 保存或显示
net.show("multi_digraph.html")

import neo4jInstall as ni
# 执行导入
# 配置 Neo4j 连接
uri = "bolt://localhost:7687"
user = "neo4j"
password = "19990602"
if 1==0:
    importer = ni.Neo4jImporter(uri, user, password)
    importer.import_graph(var.nodeDgs)
    importer.close()
#MATCH (n) DETACH DELETE n  --删除所有数据
#match (n) WITH n CALL apoc.create.addLabels(n, [n.lable]) YIELD node AS labeled  RETURN count(labeled) --增加节点标签
#MATCH (n)-[r]->(m) where m.id = [0,0] return n