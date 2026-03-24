from sqlglot import *
from ..symbol_table import *
from ..symbol_table import QueryScope


class PatternNode:
    def __init__(self,className:str|list[str],model:str="current"):
        self.className = className
        self.model=model

    def apply(self,node:Expression)->bool:
        if self.model=="current":
            return node.key == self.className.lower()
        if self.model == "super":
            result = True
            for super_class_name in self.className:
                parent_class = getattr(expressions, super_class_name)
                result = result and isinstance(node, parent_class)
            return result
        return False



class Conditions:
    def __init__(self, conditions: list[dict[dict]]):
        self.conditions = conditions



    def condition_split(self,condition:dict, node: Expression)->int:
        for condition_name in condition.keys():
            if str.lower(condition_name) == "and":
                return self.condition_and(condition[condition_name],node)

            if str.lower(condition_name) == "or":
                return self.condition_or(condition[condition_name], node)

            if str.lower(condition_name) == "condition_true":
                return self.condition_true(condition[condition_name])

            if str.lower(condition_name) == "parent_class":
                return self.condition_parent_class(condition[condition_name], node)
            if str.lower(condition_name) == "condition_false":
                return 0
            if str.lower(condition_name) == "in":
                return self.condition_in(condition[condition_name], node)
        return 0

    def condition_or(self,conditions:list, node: Expression)->int:
        return_value = 0
        for condition in conditions:
            condition_value = self.condition_split(condition,node)
            if condition_value>return_value:
                return_value = condition_value
        return return_value

    def condition_and(self,conditions:list, node: Expression)->int:
        return_value = 0
        for condition in conditions:
            condition_value = self.condition_split(condition, node)
            if condition_value == 0:
                return 0
            if condition_value>return_value:
                return_value = condition_value
        return return_value

    def condition_true(self,conditions:dict)->int:
        return conditions["weight"]

    def condition_parent_class(self, condition:dict, node: Expression):

        #获取一个类对象
        parent_class = getattr(expressions, condition["class_name"])
        # isinstance(node, parent_class)函数用来判断对象是否为某个类对象的子类
        if isinstance(node, parent_class):
            return condition["weight"]
        return 0

    def condition_in(self, condition:dict, node: Expression):
        return_value = 0
        if condition["match_value"] == "class_name":
            class_list = [s.lower() for s in condition["value_list"]]
            if node.key in class_list:
                return_value = condition["weight"]
        return return_value

    def apply(self, node: Expression) -> int:
        return self.condition_or(self.conditions,node)




class Action:
    current_scope: QueryScope|SelectScope
    def __init__(self,actions:list):
        self.current_scope:QueryScope = None
        self.actions = actions

    def actions_list(self,actions_list:list,node: Expression,scope)->QueryScope:

        for actions in actions_list:
            for action_name in actions.keys():
                if action_name == "actions":
                    self.actions_list(actions[action_name],node,self.current_scope)
                if action_name == "add_node":
                    self.dg_add_node(node, self.current_scope)
                if action_name == "scopes":
                    self.current_scope = self.create_scopes(actions[action_name],node,self.current_scope)
                if action_name == "set_stage":
                    self.set_stage(actions[action_name],node,self.current_scope)
                if action_name == "property_values":
                    self.property_values(actions[action_name], node, self.current_scope)
                if action_name == "args_values":
                    self.args_values(actions[action_name], node, self.current_scope)
                if action_name == "add_custom_values":
                    self.add_custom_values(actions[action_name], node, self.current_scope)
        return self.current_scope

    def create_scopes(self,scopes_name,node: Expression,scope:QueryScope) -> QueryScope:
        scopes_type = ScopeType.from_string(scopes_name)
        current_scope = scope.spawn_child_scope(node,scopes_type)
        self.current_scope = current_scope
        return current_scope

    def set_stage(self,stage_name,node: Expression,scope):
        scope.set_stage(stage_name,node)

    def apply(self, node: Expression,scope:QueryScope):
        self.current_scope = scope
        return self.actions_list(self.actions, node, scope)

    def dg_add_node(self,node, scope:SelectScope|QueryScope):
        scope.dg_add_node(node)

    def property_values(self, propertys:dict, node, scope:QueryScope):
        rulest = {}
        for property in propertys:
            for property_name, info in property.items():
                rulest[info] = getattr(node, property_name)
        scope.add_node_info(node,rulest)

    def args_values(self, args_list:dict, node, scope:QueryScope):
        rulest = {}
        for node_args in args_list:
            for node_arg, info in node_args.items():
                if node_arg not in set(node.arg_types.keys()):
                    raise ValueError("Argument '{}' is not a valid {} node arg_types key".format(node_arg,node.__class__))
                if info["node_key"] in node.args:

                    if isinstance(node.args.get(node_arg), Expression):
                        rulest[info["node_key"]] = node.args.get(node_arg).name
                    else:
                        rulest[info["node_key"]] = node.args.get(node_arg)
                elif info["node_key"] not in node.args and info["keep_missing"]:
                    rulest[info["node_key"]] = info.get("default_value")
                    # print(f"Action:line:129:node_arg:{node_arg}:{node.method}")
                    # print(repr(node))
        scope.add_node_info(node,rulest)

    def add_custom_values(self, param, node, scope):
        rulest = {param["key_name"]:param["value"]}
        scope.add_node_info(node, rulest)









