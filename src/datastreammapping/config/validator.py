from typing import Dict, Any, List, Optional
class ConfigValidator:
    """配置验证器 - 验证配置文件的完整性和正确性"""

    def validate_rule_config(self, config: Dict[str, Any]) -> List[str]:
        """验证规则配置"""

    def _validate_required_fields(self, config: Dict[str, Any]) -> List[str]:
        """验证必需字段"""

    def _validate_rules_structure(self, rules: Dict[str, Any]) -> List[str]:
        """验证规则结构"""

    def _validate_single_rule(self, rule_name: str, rule_config: Dict[str, Any]) -> List[str]:
        """验证单个规则"""

    def validate_and_raise(self, config: Dict[str, Any]):
        """验证配置并在有错误时抛出异常"""