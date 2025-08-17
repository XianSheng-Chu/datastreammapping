from neo4j import GraphDatabase
import json
import re


def clean_value(value):
    """将复杂对象转换为 Neo4j 支持的简单类型"""
    # 基本类型直接返回
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value

    # 列表/元组类型递归处理每个元素
    if isinstance(value, (list, tuple)):
        return [clean_value(v) for v in value]

    # 字典类型转换为 JSON 字符串
    if isinstance(value, dict):
        return json.dumps(value, ensure_ascii=False)

    # 其他类型转换为字符串
    try:
        # 尝试 JSON 序列化
        return json.dumps(value, default=str, ensure_ascii=False)
    except:
        return str(value)


def clean_properties(properties):
    """清理属性字典中的所有值"""
    cleaned = {}
    for k, v in properties.items():
        # 跳过特定键（如 type 用于关系类型）
        if k == "type":
            continue

        # 清理值
        cleaned[k] = clean_value(v)
    return cleaned


def sanitize_key(key):
    """确保关系类型是有效的 Cypher 标识符"""
    # 移除特殊字符（保留字母、数字、下划线）
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '_', key)
    # 确保不以数字开头
    if sanitized and sanitized[0].isdigit():
        sanitized = '_' + sanitized
    return sanitized


class Neo4jImporter:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))

    def close(self):
        self.driver.close()

    def import_graph(self, graph):
        with self.driver.session() as session:
            # 批量导入节点
            node_batch = []
            for node, data in graph.nodes(data=True):
                cleaned_data = clean_properties(data)
                cleaned_data["id"] = node  # 确保id在属性中
                node_batch.append(cleaned_data)

            session.execute_write(self._create_nodes_batch, node_batch)

            # 批量导入关系
            rel_batch = []
            for u, v, key, data in graph.edges(keys=True, data=True):
                # 确保关系类型存在
                rel_type = data.get("key", "RELATED")
                rel_type = sanitize_key(rel_type)

                rel_data = {
                    "src_id": u,
                    "tgt_id": v,
                    "nx_key": str(key),
                    "rel_type": rel_type,  # 单独存储关系类型
                    **clean_properties(data)  # 其他属性
                }
                rel_batch.append(rel_data)

            session.execute_write(self._create_relationships_batch, rel_batch)

    @staticmethod
    def _create_nodes_batch(tx, nodes):
        query = (
            "UNWIND $nodes AS node "
            "MERGE (n:Expression {id: node.id}) "  # 基础节点类型
            "SET n = node "  # 设置所有属性（覆盖id）
        )
        tx.run(query, nodes=nodes)

    @staticmethod
    def _create_relationships_batch(tx, relationships):
        query = (
            "UNWIND $rels AS rel "
            "MATCH (a {id: rel.src_id}), (b {id: rel.tgt_id}) "
            # 动态创建关系（使用 rel_type 属性）
            "CREATE (a)-[r:RELATION]->(b) "
            "SET r = rel, "  # 设置所有属性
            "r.type = rel.rel_type"  # 单独存储关系类型
        )
        tx.run(query, rels=relationships)
