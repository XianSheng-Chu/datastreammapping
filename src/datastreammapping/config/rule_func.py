from sqlglot import Expression

class PatternNode:
    def __init__(self,node:Expression,className:str,model:str="current"):
        self.node = node
        self.className = className
        self.model=model





