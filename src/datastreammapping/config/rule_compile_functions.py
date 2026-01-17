from nest_asyncio import apply
from sqlglot import Expression

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
        self.condition_or(self.conditions)


    def condition_split(self,condition:dict, node: Expression)->int:
        for condition_name in condition.keys():
            if condition_name == "And":
                return self.condition_and(condition[condition_name],node)
        return 0

    def condition_or(self,conditions:list, node: Expression)->int:
        return_value = 0
        for condition in conditions:
            condition_value = self.condition_split(condition,node)
            if condition_value>0:
                return_value = condition_value
        return return_value

    def condition_and(self,conditions:list, node: Expression)->int:
        for condition in conditions:
            condition_value = self.condition_split(condition, node)
            if condition_value == 0:
                return 0

    def apply(self, node: Expression) -> int:
        return 0





