from sqlglot import *
from ..symbol_table import *



class PatternNode:
    def __init__(self,className:str,model:str="current"):
        self.className = className
        self.model=model

    def apply(self,node:Expression)->bool:
        if self.model=="current":
            return node.key == self.className
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

    def apply(self, node: Expression) -> int:
        return self.condition_or(self.conditions,node)




class Action:
    def __init__(self,actions:list):
        self.actions = actions

    def actions_list(self,actions_list:list,node: Expression,scope:QueryScope):
        for actions in actions_list:
            for action_name in actions.keys():
                if action_name == "actions":
                    self.actions_list(actions[action_name],node,scope)
                if action_name == "scopes":
                    self.create_scopes(actions[action_name],node,scope)


    def create_scopes(self,scopes_name,node: Expression,scope:QueryScope) -> QueryScope:
        scopes_type = ScopeType.from_string(scopes_name)
        current_scope = scope.spawn_child_scope(node,scopes_type)
        print(f"Action:line 93:scopes_name:{scopes_type}:scopes_name:{scopes_name}")
        return current_scope

    def apply(self, node: Expression,scope:QueryScope):
        return self.actions_list(self.actions, node, scope)








