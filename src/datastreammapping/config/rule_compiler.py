from typing import Dict,Any,Callable,List

from .rule_compile_functions import *

class RuleCompiler:
    """规则编译器 - 将配置规则编译为可执行代码"""

    def __init__(self, compiled_rules_cache: Dict[str, Any] = None):
        """初始化规则编译器"""
        self.compiled_rules_cache = compiled_rules_cache
        self.execute_result = self.compile_rules(self.compiled_rules_cache)


    def compile_rules(self, rules_config: Dict[str, Any]) -> dict[str, list[dict[str,Callable]]]:
        """编译所有规则为可执行函数"""
        result = dict[str, list[dict[str, Callable]]]()

        for file_name in rules_config.keys():
            result[file_name] = list[dict[str,Callable]]()
            file = rules_config[file_name]
            rule_callables = []
            for rule_name in file.keys():

                rule = file[rule_name]
                # if str(file_name) == r"D:\Software\Domes\datastreammapping\src\datastreammapping\configs\rules\base_rules.yaml":
                if file.get("basic_rule") is not None and rule_name!= "basic_rule":
                    rule_node = dict()
                    step_name = "pattern"
                    rule_node[step_name] = self._compile_rule_pattern(rule[step_name])
                    rule_callables.append(rule_node)
                    step_name = "conditions"
                    rule_node[step_name] = self._compile_rule_conditions(rule[step_name])
                    rule_callables.append(rule_node)
                    step_name = "actions"
                    rule_node[step_name] = self._compile_rule_action(rule[step_name])
                    rule_callables.append(rule_node)
            result[file_name] = rule_callables
        return  result

    def _compile_rule_pattern(self, pattern: Any) -> Callable:
        """编译规则模式匹配函数"""
        if pattern["type"]=="node":
            return PatternNode(pattern["class_name"],pattern["level"]).apply
    def _compile_rule_conditions(self, conditions: List) -> Callable:
        """编译规则条件检查函数"""
        return Conditions(conditions).apply

    def _compile_rule_action(self, actions: list) -> Callable:
        """编译规则动作执行函数"""
        return Action(actions).apply

    def _create_rule_function(self, pattern_matcher: Callable,
                              condition_checkers: List[Callable],
                              action_executor: Callable) -> Callable:
        """创建完整的规则执行函数"""