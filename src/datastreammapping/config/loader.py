import os
from ruamel.yaml import YAML
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..utils import *

class ConfigLoader:
    """配置加载器 - 负责加载和解析YAML配置文件"""

    def __init__(self, config_dir: Optional[str] = None):
        """初始化配置加载器"""
        """
        初始化配置加载器

        Args:
            config_dir: 可选的配置目录路径。如果为None，则自动查找默认配置目录
                       Optional configuration directory path. If None, automatically 
                       finds the default config directory

        Raises:
            ConfigLoadError: 当配置目录不存在时抛出
                           Raised when the configuration directory does not exist
        """
        # 解析配置目录路径
        self.config_dir = self._resolve_config_dir(config_dir)

        # 初始化配置缓存字典
        # Key: 文件路径, Value: 解析后的配置字典
        self._cache: Dict[str, Any] = {}

        # 跟踪文件修改时间，用于缓存失效
        # Key: 文件路径, Value: 最后修改时间戳
        self._file_mtimes: Dict[str, float] = {}


    def _resolve_config_dir(self, config_dir: Optional[str]) -> Path:
        """解析配置目录路径"""
        if config_dir:
            user_path = Path(config_dir)
            if user_path.exists() and user_path.is_dir():
                return user_path.resolve()
            else:
                raise ConfigLoadError(f"Provided config directory does not exist/配置目录不存在: {user_path.absolute()}")
        current_file_dir = Path(__file__).parent

        package_configs_dir = current_file_dir.parent / "configs"

        if package_configs_dir.exists() and package_configs_dir.is_dir():
            return package_configs_dir.resolve()

    def load_rule_file(self, rule_file: str) -> Dict[str, Any]:
        """加载单个规则文件"""
        yaml = YAML(typ='rt')
        with open(rule_file, 'r', encoding='utf-8') as file:
            data = yaml.load(file)
        return data

    def load_default_config(self) -> Dict[str, Any]:
        """加载默认配置文件"""
        defalultPuth = self._resolve_config_dir((Path(__file__).resolve().parent.parent/"configs").__str__())
        return self._load_yaml_file(defalultPuth/"default.yaml")


    def load_all_rules(self) -> Dict[str, Any]:
        """加载所有规则配置"""

    def _load_yaml_file(self, file_path: Path) -> Dict[str, Any]:
        """加载并解析YAML文件"""

        cache_key = str(file_path)

        # 检查缓存
        current_mtime = file_path.stat().st_mtime
        if (cache_key in self._cache and
                self._file_mtimes.get(cache_key) == current_mtime):
            return self._cache[cache_key]


        #预加载与其他情况
        try:
            yaml = YAML(typ='rt')
            with open(file_path, 'r' ,encoding='utf-8') as file:
                data = yaml.load(file)


            if "rules" in data:
                rules = data.pop("rules")
                imports = rules.pop("imports")
                base_config = data.copy()

                for import_path in imports:
                    rules_path = file_path.parent / import_path
                    imported_config = self.load_rule_file(file_path.parent/import_path)
                    #base_config = self._merge_configs(base_config, imported_config)
                    self._cache[rules_path] = imported_config
                    self._file_mtimes[rules_path] = current_mtime
                config = base_config

            # 更新缓存
                self._cache[cache_key] = base_config
                self._file_mtimes[cache_key] = current_mtime
                return config
        except Exception as e:
            raise ConfigLoadError(f"Provided config directory does not exist/配置目录不存在: {file_path}: {e}")


    def _merge_configs(self, base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """深度合并两个配置字典"""

    def clear_cache(self):
        """清空配置缓存"""
        self._cache.clear()
        self._file_mtimes.clear()

    @property
    def rule_files(self)->Dict[str, Any]:
        return self._cache