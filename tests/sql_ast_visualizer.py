import sqlglot
import graphviz
from sqlglot import exp


def visualize_sql_ast(sql, dialect="mysql", output_file="sql_ast"):
    # 解析 SQL 生成 AST
    parsed = sqlglot.parse_one(sql, read=dialect)

    # 创建 Graphviz 图形对象
    dot = graphviz.Digraph(comment='SQL AST', format='png')
    dot.attr(rankdir='TB')  # 树的方向：TB（上到下）, LR（左到右）

    # 递归遍历 AST 节点
    def add_node(node, parent_id=None):
        node_id = str(id(node))

        # 生成节点标签
        node_type = node.__class__.__name__
        label = f"{node_type}"

        # 为特定节点类型添加详细信息
        if isinstance(node, exp.Column):
            label += f"\nColumn: {node.name}\nTable: {node.table}"
        elif isinstance(node, exp.Table):
            label += f"\nTable: {node.name}\nAlias: {node.alias}"
        elif isinstance(node, exp.Select):
            label += "\nSELECT"
        elif isinstance(node, exp.Join):
            label += f"\nJOIN Type: {node.side}"

        # 添加节点到图
        dot.node(node_id, label, shape='box', style='filled', fillcolor='#e0f0ff')

        # 连接父节点
        if parent_id:
            dot.edge(parent_id, node_id)

        # 递归处理子节点
        for k, v in node.args.items():
            if isinstance(v, list):
                for child in v:
                    if isinstance(child, exp.Expression):
                        child_id = add_node(child, node_id)
            elif isinstance(v, exp.Expression):
                child_id = add_node(v, node_id)

        return node_id

    # 从根节点开始遍历
    add_node(parsed)

    # 保存并渲染图形
    dot.render(output_file, view=True)
    return dot


if __name__ == "__main__":
    # 示例 SQL
    sample_sql = """
    SELECT 
        a.id AS user_id,
        b.name 
    FROM 
        table_a AS a
    JOIN 
        table_b b ON a.id = b.user_id
    WHERE 
        a.age > 18
    """

    # 生成 AST 可视化
    visualize_sql_ast(sample_sql, dialect="mysql")