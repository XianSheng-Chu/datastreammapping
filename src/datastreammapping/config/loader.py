import os
from ruamel.yaml import YAML
from pathlib import Path
from typing import Dict, Any, List, Optional


class ConfigLoader:
    """配置加载器 - 负责加载和解析YAML配置文件"""

    def __init__(self, config_dir: Optional[str] = None):
        """初始化配置加载器"""

    def _resolve_config_dir(self, config_dir: Optional[str]) -> Path:
        """解析配置目录路径"""

    def load_rule_file(self, rule_file: str) -> Dict[str, Any]:
        """加载单个规则文件"""

    def load_default_config(self) -> Dict[str, Any]:
        """加载默认配置文件"""

    def load_all_rules(self) -> Dict[str, Any]:
        """加载所有规则配置"""

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """加载并解析YAML文件"""

    def _merge_configs(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """深度合并两个配置字典"""

    def clear_cache(self):
        """清空配置缓存"""