import json

from sqlglot.expressions import Select
import sqlglot.dialects
import  neo4jInstall as ni
from datastreammapping.models import EdgeMainTypeEnum, DataStreamMappingEdgeEnum
from datastreammapping.sqlscriptmapping import SqlScriptMapping

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

def write_string_to_file(file_path, content):
    """
    清空文件并将字符串写入文件

    参数:
        file_path (str): 目标文件路径
        content (str): 要写入的字符串内容
    """
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(content)
from sqlglot import generator
def test_sqlmapping():
    # 解析 INSERT 语句
    sql = """
    -- 电商用户复购行为深度分析（表与子查询多次出现）
SELECT 
    -- 主用户信息
    main_u.user_id,
    main_u.user_name,
    main_u.register_time,
    
    -- 首次订单信息（第1次使用 fact_order）
    first_o.order_id AS "first_order_id",
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
        OR main_u."register_time" < '2024-06-01'
    )

ORDER BY 
    repurchase_order_count DESC,
    user_avg_order_amount DESC
LIMIT 50 OFFSET 101;
    """
    parsed = sqlglot.parse_one(sql,read="postgres")
    # write_string_to_file("../temp/语法树JSON.json", json.dumps(parsed.dump(), sort_keys=False, indent=4))
    sqlglot.parser.Parser
    # 提取插入的目标表和列
    insert_node = parsed.find(sqlglot.exp.Select)

    select_node: Select = insert_node.find(sqlglot.exp.Select).copy()


    # 解析 SQL 生成 AST
    ast = sqlglot.parse_one(sql)


    dict = {}



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

    tree = select_node.bfs()
    nodeDpath = 0
    depthSeq = 0
    for item in tree:
        depthSeq += 1
        if nodeDpath != item.depth:
            nodeDpath = item.depth
        if item.key == "cte":
            nodeKey = (nodeDpath, depthSeq)
            #print(f"{item.alias}:{nodeKey}")


    for node, attrs in var.nodeDgs.nodes(data=True):
        if attrs.get("visibilityFlag") :
            #attrs["expObject"] = None
            #print(f"{node}: {attrs}")
            pass

    for u,v,k,d in var.nodeDgs.edges(keys=True, data=True):
        if k=="logicalMapping" :
            source = lambda u,x:x.get("note") if u[0]+u[1]<0 else var.nodeMap[u].key
            #print(f"{u}:{source(u, var.nodeDgs.nodes[u])}->{v}:{var.nodeDgs.nodes[v].get('objName')}:({source(v, var.nodeDgs.nodes[v])})[key:{k}]:{d}")

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
    import datastreammapping.core as dsmCore
    compiler = dsmCore.SQLToGraphCompiler("postgres")
    compiler.compile_sql(
        """
        -- 电商用户复购行为深度分析（表与子查询多次出现）
               -- 示例：将每个部门中工资高于该部门平均工资的员工插入到 high_salary_employees 表中
            WITH dept_avg AS (
                SELECT department_id, AVG(salary) AS avg_salary
                FROM employees
                GROUP BY department_id
            )
            INSERT INTO high_salary_employees (employee_id, name, department_id, salary, insert_time)
            SELECT e.employee_id, e.name, e.department_id, e.salary, NOW()
            FROM high_salary_employees e
            JOIN dept_avg d ON e.department_id = d.department_id
            WHERE e.salary > d.avg_salary;
        """)
    nodeDg = compiler.current_query.nodeDgs.copy()
    # to_remove = [node for node, attrs in nodeDg.nodes(data=True) if attrs.get('exp_stage',"scope_root") not in ("scope_root", 'from', 'from_', 'expressions')]
    to_remove = [node for node, attrs in nodeDg.nodes(data=True) if attrs.get('exp_key',"root")  in
                 ("scope_root",'with','with_','from','from_','laterals','join','pivots','sample','prewhere','where',
                                         'match','connect','group','having','windows','qualify','','operation_modifiers',
                                         'distinct','distribute','sort','cluster','order','limit','offset','into','locks','format',
                                         'settings','options',)]

    # nodeDg.remove_nodes_from(to_remove)

    mapping = {node: str(node) for node in nodeDg.nodes}
    nodeDgs = nx.relabel_nodes(nodeDg, mapping, copy=True)
    for node, data in nodeDgs.nodes(data=True):
        label = data["extra_attrs"].get('output',None)

        if label is None:
            label =  data.get("node_name","")

        if label == "":
            label =  data.get("exp_key","")
        if label == "":
            label =  str(node)
        data["label"] = label
        data["node_id"] = str(node)

        attrs_to_remove = ["exp_node","scope_root_temp","created_data","last_updated_data","scope_key","node_id"]
        for attr in attrs_to_remove:
            data.pop(attr, None)
        nodeDgs.nodes[node]["title"] = f"节点key：{node}\n类型：{data.get('exp_key', data.get("node_name"))}\n{data.get('exp_stage', '')}"
        if type(node) is tuple:
            print(data)


            # 转换为 PyVis 网络
    net = Network(notebook=True, directed=True, height="1000px", width="100%")

    #移除不需要的边
    nodeDgs.remove_edges_from([(u, v, k) for (u, v, k) in nodeDgs.edges(keys=True) if k == EdgeMainTypeEnum.CODE_STRUCTURE ])


    net.from_nx(nodeDgs)
    net.hierarchical = True  # 开启层次布局
    # 配置选项
    net.set_options("""
    {
  "layout": {
    "hierarchical": {
      "enabled": true,
      "direction": "LR",        
      "levelSeparation": 250,   
      "nodeSpacing": 180,       
      "sortMethod": "directed"  
    }
  },
    "interaction": {
    "hover": true,       
    "tooltipDelay": 100  
  },
  "physics": {
    "enabled": false  
  },
  "edges": {
    "arrows": { "to": { "enabled": true, "scaleFactor": 1.2 } },
    "color": { "color": "#ccc", "highlight": "#4285f4" },
    "width": 2
  },
  "interaction": { "hover": true, "zoomView": true }
}
    """)

    # 添加自定义边标签

    for edge in net.edges:
        # 从原始图获取键和权重
        original_data = nodeDgs.get_edge_data(edge['from'], edge['to'])
        if original_data:
            edge['label'] = edge['edge_sub_type']
            # edge['title'] = f"Key: {edge['key']}\nWeight: {original_data['weight']}"
            # edge['label'] = f"{edge['key']}:{original_data['weight']}"
    # 保存或显示
    net.show("fixtures/multi_digraph.html")

    # 执行导入
    # 配置 Neo4j 连接
    uri = "bolt://localhost:7687"
    user = "neo4j"
    if 1==0:
        importer = ni.Neo4jImporter(uri, user, password)
        importer.import_graph(var.nodeDgs)
        importer.close()
    #MATCH (n) DETACH DELETE n  --删除所有数据
    #match (n) WITH n CALL apoc.create.addLabels(n, [n.lable]) YIELD node AS labeled  RETURN count(labeled) --增加节点标签
    #MATCH (n)-[r]->(m) where m.id = [0,0] return n