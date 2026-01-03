from typing import Dict, Any, Callable


class PatternMatcher:
    """模式匹配器 - 匹配AST节点与配置规则"""

    def __init__(self, compiled_patterns: Dict[str, Callable]):
        """初始化模式匹配器"""

    def match_node(self, ast_node: Any, pattern_type: str) -> bool:
        """检查AST节点是否匹配指定模式"""

    def extract_node_properties(self, ast_node: Any,
                                property_template: Dict[str, Any]) -> Dict[str, Any]:
        """从AST节点提取属性"""