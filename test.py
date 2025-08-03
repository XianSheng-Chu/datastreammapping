import networkx as nx
from sipbuild.generator.outputs import output_pyi

# 创建空有向图
dg = nx.DiGraph()

# 从边列表创建
edges = [(1, 2), (2, 3), (3, 1)]  # 形成环
dg = nx.DiGraph(edges)

# 从邻接字典创建
adj_dict = {
    "A": {"B": {"weight": 4}, "C": {"weight": 2}},
    "B": {"C": {"weight": 1}},
    "C": {}  # 无出边
}
dg = nx.DiGraph(adj_dict)


# 添加节点
dg.add_node("D")
dg.add_node("E", type="endpoint", color="red")

# 批量添加节点
dg.add_nodes_from(["F", "G"], category="processing")

# 获取节点信息
print("所有节点:", list(dg.nodes))
print("带属性节点:", dg.nodes.data())

# 删除节点（同时删除关联的边）
dg.remove_node("F")


# 节点数量
num_nodes = dg.number_of_nodes()

# 边数量
num_edges = dg.number_of_edges()

# 检查图是否有方向
print("是否为有向图:", dg.is_directed())

# 节点出度（发出的边）
print("节点D的出度:", dg.out_degree("D"))

# 节点入度（指向的边）
print("节点C的入度:", dg.in_degree("C"))

# 带权出度
print("节点D的带权出度:", dg.out_degree("D", weight="weight"))

# 所有节点度数字典
out_degrees = dict(dg.out_degree())
in_degrees = dict(dg.in_degree())

myDg = nx.DiGraph()

myDg.add_node((0,0),type="Select",outputList=['id', 'name', 'source_code', 'contract_number', 'delete_flag'])
myDg.add_node((1, 0),type="Columns",name="id",outputFlag=True,outputName="id")
myDg.add_node((1, 1),type="Columns",name="name",outputFlag=True,outputName="name")
myDg.add_node((1, 2),type="Columns",name="source_code",outputFlag=True,outputName="source_code")
myDg.add_node((1, 3),type="Columns",name="contract_number",outputFlag=True,outputName="contract_number")
myDg.add_node((1, 4),type="Columns",name="delete_flag",outputFlag=True,outputName="delete_flag")
myDg.add_node((3,0),type="Select",outputList=['id', 'name', 'source_code', 'contract_number', 'delete_flag'])





