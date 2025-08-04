"""
## SqlScriptMapping

提供基于AST解析获取的sql脚本的字段级映射的方法

"""
import copy

from openpyxl.styles.builtins import output
from sqlglot import expressions
from sqlglot.dialects.dialect import DialectType
from sqlglot.expressions import *
from copy import deepcopy
import networkx as nx

class SqlScriptMapping():
    root:expressions
    """
    root代表着对象需要挂载在哪个语法树之中
    """
    dialect:DialectType
    nodeMap:dict[tuple[int]:expressions]
    nodeMapInverse:dict[int:tuple[int]]
    logicMap:dict[tuple[int]:dict[str:dict]]
    logicMapInverse: dict[tuple[int]:list[str]]
    logicMapModel={"SetOperation":{"validFlag":False,"distinct":False,"isSetOperation":False,"parentOperation":(),"thisNode":()},#sql的交集并集与差集
                   "Table": {},"Alias":{},"TableAlias":{},"Columns":{},"DerivedTable":{},"output":{},"aliasSource":{},
                   "columnsSource":{},"tableAliasSource":{},"cteSource":{},"tableSource":{},"Where":{},"Join":{},"Group":{},"outputList":[],
                   "Order":{},
                   "Func":{},
                   #"Condition":{} #布尔表达式基类，包含所有布尔类型操作，比如Predicate(>,<,=),Connector(and,or)
                   "Binary":{},#二元表达式
                   "Unary":{}, #一元表达式
                   "Predicate":{},#谓词
                   "Subquery":{},#子查询
                   "Window":{}
                   }
    nodeDg = nx.MultiDiGraph
    def __init__(self,root:expressions,dialect:DialectType=None):
        self.root=root.copy()
        self.dialect = dialect
        self.nodeMapInverse = {}
        self.nodeMap = {}
        self.logicMap = {}
        self.logicMapInverse = {}
        self.nodeDg = nx.MultiDiGraph()
        self.__logicMapInit()

    def __noneMapInit(self):
        self.nodeMap.clear()
        self.nodeDg.clear()
        tree = self.root.bfs()
        nodeDpath = 0
        depthSeq = 0
        for item in tree:
            if nodeDpath!=item.depth:
                nodeDpath = item.depth
                depthSeq = 0
            nodeKey = (nodeDpath,depthSeq)
            self.nodeMap[nodeKey] = item
            self.nodeMapInverse[id(item)] = nodeKey
            self.nodeDg.add_node(nodeKey,expNode=item,visibilityFlag=False)
            if item.parent is not None:
                self.nodeDg.add_edge(nodeKey,self.__nodeBfsKey(item.parent),key="parentNode",note="父节点")
            depthSeq+=1

        self.nodeDg.add_nodes_from(self.LOGICALEND)




    def __logicMapInit(self):
        self.__noneMapInit()
        self.logicMap.clear()
        for (key,value) in self.nodeMap.items():
            self.__nodeToLogic(key,value)
        ctes = {}
        for key,value in self.logicMap.items():
            if key!=(0,0):
                node = self.nodeMap[key]

                if  node.parent.key=="union":
                    parentNode = node.parent
                    #self.logicMap[key]["SetOperation"] = self.__nodeBfsKey(parentNode)

            for k,v in value["DerivedTable"].items():
                ctes[v] = k
        for selectNode,nodeDict in self.logicMap.items():
            if self.logicMap[selectNode]["SetOperation"]["isSetOperation"]:
                outputKey = self.logicMap[selectNode]["SetOperation"]["thisNode"]
                self.logicMap[selectNode]["output"]=copy.deepcopy( self.logicMap[outputKey]["output"])
                self.logicMap[selectNode]["outputList"]=copy.deepcopy( self.logicMap[outputKey]["outputList"])

            for nodeKey,nodeId in nodeDict["Columns"].items():
                for columnId in nodeId:
                    if True and columnId == (4, 0):
                        columnId = columnId
                        pass
                    tokenGraph = self._expressionsMap(nodeKey,self.nodeMap[columnId])
                    for (tokenTarget,tokenSource) in tokenGraph:
                        self.logicMap[selectNode]["columnsSource"][tokenTarget]=tokenSource

                    #self.nodeDg.add_edge(tokenSource.values(), nodeKey, key="logicalMapping", note="逻辑映射")

            for (nodeKey,nodeId) in nodeDict["Alias"].items():
                tokenGraph = self._expressionsMap(nodeKey,self.nodeMap[nodeId])
                tempDict:dict={}
                self.logicMap[selectNode]["aliasSource"][nodeKey] = {}
                listNum = 0
                for (tokenTarget,tokenSource) in tokenGraph:
                    #此处可能出现输出的别名与列明相同的情况
                    if tokenTarget in self.logicMap[selectNode]["Columns"].keys() and listNum!=0:
                        self.logicMap[selectNode]["aliasSource"][nodeKey][tokenTarget] = self.logicMap[selectNode]["Columns"][tokenTarget]
                        continue
                    listNum+=1
                    tempDict[tokenTarget] = tokenSource
                    for k in tokenSource.keys():
                        tempDict[k] = tokenSource[k]
                for k1,v1 in tempDict.items():
                    if type(v1) is tuple and k1!=nodeKey:
                        self.logicMap[selectNode]["aliasSource"][nodeKey][k1] = v1
            for (nodeKey, nodeId) in nodeDict["TableAlias"].items():
                self.logicMap[selectNode]["tableAliasSource"][nodeKey] = self._expressionsMap(nodeKey,self.nodeMap[nodeId])[0][1]
            for (nodeKey, nodeId) in nodeDict["DerivedTable"].items():
                self._expressionsMap(nodeKey, self.nodeMap[nodeId])
            for (nodeKey, nodeId) in nodeDict["Window"].items():
                self._expressionsMap(nodeKey, self.nodeMap[nodeId])
            for (nodeKey, nodeId) in nodeDict["Table"].items():
                self._expressionsMap(nodeKey, self.nodeMap[nodeId])
                value = None
                if nodeKey in  nodeDict["DerivedTable"].keys() and  self.nodeMap[nodeId].db=="":
                    value = ("DerivedTable", nodeDict["DerivedTable"][nodeKey])
                for k,v  in ctes.items():
                    if nodeKey == v and self.__parentSelect(self.nodeMap[nodeId])>k and self.nodeMap[nodeId].db=="":
                        value = ("DerivedTable", k)
                operationParentId = selectNode
                while self.logicMap[operationParentId]["SetOperation"]["parentOperation"]!=():
                    operationParentId = self.logicMap[operationParentId]["SetOperation"]["parentOperation"]
                    unionParent = self.logicMap[operationParentId]
                    if nodeKey in unionParent["DerivedTable"].keys():
                        value = ("DerivedTable",unionParent["DerivedTable"][nodeKey])

                if value==None:
                    value = ("Table", (-1, -8))
                self.logicMap[selectNode]["tableSource"][nodeKey] = value


    def __nodeToLogic(self,bfsKey:Tuple,node:expressions):
        """
        :param bfsKey: BFS树中的具体数据位置，如果其是一个select或union节点，那么会在self.logicMap中创建一个键值对

        :param node:
        :return:

        comment:logicMap的key值应该遵循如下规则
            1.拥有对应的Expressions类的节点，首字母应该大写，其他的信息，首字母应该小写
        """
        locigType=set()
        if node.key =="select":
            self.logicMap[bfsKey] = deepcopy(self.logicMapModel)
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag":False,"className":node.key,"objName":self.expressionsName(node)})
            locigType.add(node.key)
            for item in node.expressions:
                name = self.expressionsName(item)
                self.logicMap[self.__parentSelect(item)]["output"][name] = self.__nodeBfsKey(item)
                self.logicMap[self.__parentSelect(item)]["outputList"].append((name,self.__nodeBfsKey(item)))
                self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": item.key, "isOutput": True})
                locigType.add("output")
            if node.parent is not None and isinstance(node.parent,SetOperation):
                self.logicMap[bfsKey]["SetOperation"]["parentOperation"] = self.__nodeBfsKey(node.parent)
            locigType.add("select")
        elif isinstance(node,SetOperation):
            self.logicMap[bfsKey] = deepcopy(self.logicMapModel)
            self.logicMap[bfsKey]["SetOperation"]["validFlag"] = True
            self.logicMap[bfsKey]["SetOperation"]["isSetOperation"] = True
            self.logicMap[bfsKey]["SetOperation"]["distinct"] = node.args.get("distinct",False)
            self.logicMap[bfsKey]["SetOperation"]["thisNode"] = self.__nodeBfsKey(node.this)
            if node.parent is not None:
                self.logicMap[bfsKey]["SetOperation"]["parentOperation"] = self.__nodeBfsKey(node.parent)
            locigType.add(node.key)
        # elif node.key!="union" and  node!=node.root() and node.parent.key == "select" and len(node.output_name)!=0:
        #     name = node.output_name
        #     if  node.key in ("column","star") and node.output_name == "*":
        #         name = self.expressionsName(node)
        #     self.logicMap[self.__parentSelect(node)]["output"][name] = bfsKey
        #     self.logicMap[self.__parentSelect(node)]["outputList"].append(name)
        #     self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True,"className":node.key,"isOutput":True})
        #     locigType.add("output")
        if node.key=="window":
            self.logicMap[self.__parentSelect(node)]["Window"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
            locigType.add(node.key)
        if node.key=="alias":
            self.logicMap[self.__parentSelect(node)]["Alias"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
            locigType.add("Alias")
        if node.key =="column":
            if node.sql() not in self.logicMap[self.__parentSelect(node)]["Columns"].keys():
                self.logicMap[self.__parentSelect(node)]["Columns"][self.expressionsName(node)]=[]
            self.logicMap[self.__parentSelect(node)]["Columns"][self.expressionsName(node)].append(bfsKey)
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
            locigType.add("Columns")
        if node.key =="star" and node.parent.key not in ('column','count'):
            if node.sql() not in self.logicMap[self.__parentSelect(node)]["Columns"].keys():
                self.logicMap[self.__parentSelect(node)]["Columns"][self.expressionsName(node)] = []
            self.logicMap[self.__parentSelect(node)]["Columns"][self.expressionsName(node)].append(bfsKey)
            self.nodeDg.nodes[bfsKey].update(
                {"visibilityFlag": True, "className": node.key, "objName": self.expressionsName(node)})
            locigType.add("Columns")
        if node.key=="table":
            self.logicMap[self.__parentSelect(node)]["Table"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
            #self.logicMap[self.__parentSelect(node)]["TableAlias"][node.name] = bfsKey

            locigType.add("Table")
        if  isinstance(node,DerivedTable):
            if self.__parentSelect(node) == (0, 0) and (0, 0) not in self.logicMap.keys():
                self.logicMap[(0, 0)] = deepcopy(self.logicMapModel)
            if node.key=='cte':
                self.logicMap[self.__parentSelect(node)]["DerivedTable"][self.expressionsName(node)] = bfsKey
                self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
                locigType.add("DerivedTable")
            elif node.key == 'subquery':
                self.logicMap[self.__parentSelect(node)]["Subquery"][self.expressionsName(node)] = bfsKey
                self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
                if node.parent.key in ("select","join"):
                    self.logicMap[self.__parentSelect(node)]["Table"][self.expressionsName(node)] = bfsKey
                    locigType.add("Table")
                locigType.add("Subquery")
        elif node.key=="tablealias" and node.parent.key!="cte":
            self.logicMap[self.__parentSelect(node)]["TableAlias"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node)})
            locigType.add("TableAlias")
        if  node!=node.root() and node.parent.key == "group":
            self.logicMap[self.__parentSelect(node)]["Group"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True,"isGroup":True})
            locigType.add("Group")
        if  node!=node.root() and node.parent.key == "join":
            self.logicMap[self.__parentSelect(node)]["Join"][self.expressionsName(node)] = bfsKey

        if node.key=="where":
            self.logicMap[self.__parentSelect(node)]["Where"][self.expressionsName(node)] = bfsKey

        if isinstance(node,Func):
            funcDict = self.logicMap[self.__parentSelect(node)]["Func"]
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True,"className": node.key,"objName":self.expressionsName(node),"isFunc": True,"funcName":self.expressionsName(node),"arg_types":str(node.arg_types)})
            locigType.add("Func")
            if self.expressionsName(node)  in funcDict.keys():
                funcDict[self.expressionsName(node)][bfsKey]=node.sql()
            else:
                funcDict[self.expressionsName(node)]={bfsKey:node.sql()}

        if  node.key=="literal":
            type = lambda x: "String" if x else "noString"
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key,"objName":self.expressionsName(node),"literalValue":{"value":node.sql(),"type":type(node.is_string)  }})
        if node.key == "null":
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True,"className": node.key,"objName":self.expressionsName(node),"isFunc": True,"funcName":self.expressionsName(node),"arg_types":str(node.arg_types)})

        if node.key == "order":
            list=[]
            for ordered in node.expressions:
                orderedKey = self.__nodeBfsKey(ordered)
                list.append((orderedKey,ordered.args.get("desc",False)))
                self.nodeDg.nodes[orderedKey].update({"visibilityFlag": True, "className": ordered.key,"objName": self.expressionsName(ordered)})
            temp = self.logicMap[self.__parentSelect(node)]["Order"].get(self.expressionsName(node),{})
            temp[bfsKey]=list
            self.logicMap[self.__parentSelect(node)]["Order"][self.expressionsName(node)] = temp

        if isinstance(node,Binary):
            self.logicMap[self.__parentSelect(node)]["Binary"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key, "objName": self.expressionsName(node)})
            locigType.add("Binary")
        if isinstance(node,Unary):
            self.logicMap[self.__parentSelect(node)]["Unary"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update({"visibilityFlag": True, "className": node.key, "objName": self.expressionsName(node)})
            locigType.add("Unary")
        if isinstance(node, Predicate):
            self.logicMap[self.__parentSelect(node)]["Predicate"][self.expressionsName(node)] = bfsKey
            self.nodeDg.nodes[bfsKey].update(
                {"visibilityFlag": True, "className": node.key, "objName": self.expressionsName(node)})
            locigType.add("Predicate")

        if len(locigType)>0:
            self.nodeDg.nodes[bfsKey].update({"locigType":locigType})


    def __nodeBfsKey(self,node:expressions) ->tuple[int]:
        return self.nodeMapInverse.get(id(node))


    def __parentSelect(self,node:expressions)->tuple:
        if node ==None:
            return 0,0
        if node.key in("select","union"):
            return self.__nodeBfsKey(node)
        elif node==node.root():
            return self.__nodeBfsKey(node)
        else:
            return self.__parentSelect(node.parent)

    def expressionsMapTest(self,name:str = "id",node=(1,4)):
        return self._expressionsMap(self.expressionsName(self.nodeMap[node]),self.nodeMap[node])

    def _expressionsMap(self,node: expressions) -> list[tuple[str:dict]]:
        return self._expressionsMap(self.expressionsName(node),node)

    LOGICALEND = [
        ((-1, -2), {"visibilityFlag": "False", "note": "字段，未指定表名"}),
        ((-1, -3), {"visibilityFlag": "False", "note": "常量"}),
        ((-1, -4), {"visibilityFlag": "False", "note": "参数为0的函数，比如now()"}),
        ((-1, -5), {"visibilityFlag": "False", "note": "以”*“列出的所有字段"}),
        ((-1, -6), {"visibilityFlag": "False", "note": "拥有别名的子查询"}),
        ((-1, -7), {"visibilityFlag": "False", "note": "with语句的别名"}),
        ((-1, -8), {"visibilityFlag": "False", "note": "实体数据源，比如表名，视图名等"}),
        ((-1, -9), {"visibilityFlag": "False", "note": "Null值"})
    ]


    def _expressionsMap(self,name:str,node:expressions)->list[tuple[str:dict]]:
        """
        用于解析在sql代码中一个完整并且逻辑映射连续的字符序列,例如:
            select emp_id as id from emp e;
            emp_id as id 会被解析成为 emp_id->id
            emp e 会被解析成为 emp->e
        :param name:
        :param node:
        :return:
        返回值节点的元组映射关系如下
        (-1,-2):字段，未指定表名
        (-1,-3):常量
        (-1,-4):参数为0的函数，比如now()
        (-1,-5):以”*“列出的所有字段
        (-1,-6):拥有别名的子查询
        (-1,-7):with语句的别名
        *(-1,-8):实体数据源，比如表名，视图名等
        (-1,-9):Null值
        """
        tokenGraph = []
        if node.key == "column":
            tupleTemp = (lambda node: self.__nodeBfsKey(node) if node.text("table") != "" else (-1, -2))(node)
            tupleTemp = self.logicMap[self.__parentSelect(node)]["DerivedTable"].get(node.table,(-1, -2))
            tupleTemp = self.logicMap[self.__parentSelect(node)]["TableAlias"].get(node.table,tupleTemp)
            if node.table == "":
                tableName = "--"
            elif node.db=="":
                tableName = node.table
            elif node.catalog=="":
                tableName = f"{node.db}.{node.table}"
            else:
                tableName = f"{node.catalog}.{node.db}.{node.table}"
            tupleTemp = self.logicMap[self.__parentSelect(node)]["Table"].get(tableName,tupleTemp)
            if tupleTemp==(-1,-2):
                tupleTemp = self.logicMap[self.__parentSelect(node.parent_select.parent)]["TableAlias"].get(tableName, tupleTemp)
                tupleTemp = self.logicMap[self.__parentSelect(node.parent_select.parent)]["Table"].get(tableName, tupleTemp)
            temp = {node.table:tupleTemp}
            self.nodeDg.add_edge(tupleTemp, self.__nodeBfsKey(node), key="logicalMapping",note="逻辑映射")
            if node.sql()=="test3.test3_id":
                print(f"---------{tableName}------------")
                print(tupleTemp)
                print(self.logicMap[self.__parentSelect(node)]["Table"])
            tokenGraph.append((name,temp))
        elif node.key == "alias":
            self.nodeDg.add_edge(self.__nodeBfsKey(node.this), self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")

        elif node.key == "literal":
            temp = {name:(-1, -3)}
            tokenGraph.append((name, temp))
            self.nodeDg.add_edge((-1, -3), self.__nodeBfsKey(node), key="logicalMapping",note="逻辑映射")
        elif node.key == "null":
            temp = {name:(-1, -9)}
            tokenGraph.append((name, temp))
            self.nodeDg.add_edge((-1, -9), self.__nodeBfsKey(node), key="logicalMapping",note="逻辑映射")
        elif node.key == "star" and node.parent.key not in ('column','count'):
            temp = {name:(-1, -5)}
            tokenGraph.append((name, temp))

            for k,v in self.logicMap[self.__parentSelect(node)]["TableAlias"].items():
                self.nodeDg.add_edge(v, self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")
            for k, v in self.logicMap[self.__parentSelect(node)]["Table"].items():
                if self.nodeMap[v].alias == "":
                    self.nodeDg.add_edge(v, self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")
        elif node.key=="window":
            self.nodeDg.add_edge(self.__nodeBfsKey(node.this), self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")
            for item in  node.args.get("partition_by"):
                self.nodeDg.add_edge(self.__nodeBfsKey(item), self.__nodeBfsKey(node), key="logicalMapping",
                                     note="逻辑映射-order")
            if node.arg_types["order"]:
                self.nodeDg.add_edge(self.__nodeBfsKey(node.arg_types["order"]), self.__nodeBfsKey(node), key="logicalMapping",
                                     note="逻辑映射-partition_by")
        elif node.key in ("alias","concat","max","window","order","ordered") or isinstance(node,Func) or isinstance(node,Predicate):
            temp = {}
            templist = []
            loopDpath=node.depth+1
            for item in node.bfs():
                if item == node :continue
                if loopDpath!=item.depth:break
                if name == str(item):continue
                templist.extend(self._expressionsMap(self.expressionsName(item),item))
                temp[str(item)] = self.__nodeBfsKey(item)
                loopDpath = item.depth
            if temp == {}:
                temp = {name:(-1,-4)}
            tokenGraph.append((name,temp))
            tokenGraph.extend(templist)
        elif node.key == "tablealias":
            if node.parent.key=='table':
                tokenGraph.append((self.expressionsName(node), {self.expressionsName(node.parent):self.__nodeBfsKey(node.parent)}))
                self.nodeDg.add_edge(self.logicMap[self.__parentSelect(node)]["Table"][self.expressionsName(node.parent)], self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")
            else:
                temp = self.__nodeBfsKey(node)
                if  node.parent.key=='subquery':
                    tokenGraph.append((self.expressionsName(node), {self.__nodeBfsKey(node.parent.this):(-1,-6)}))
                    self.nodeDg.add_edge(
                        self.__nodeBfsKey(node.parent.this),
                        temp, key="logicalMapping", note="subquery逻辑映射")

                elif    node.parent.key == 'cte':
                    tokenGraph.append((self.expressionsName(node), {(temp[0], temp[1]-1): (-1, -7)}))
                    self.nodeDg.add_edge(
                        self.__nodeBfsKey(node.parent.this),
                        temp, key="logicalMapping", note="subquery逻辑映射")
                elif    node.parent.key == 'udtf':
                    pass

        elif node.key =="table":
            if node.db=="":
                tempNode = node
                tablename = self.expressionsName(node)
                nodeBfsKey = self.__parentSelect(tempNode)
                if tablename == "table_union_cte":
                    tablename = "table_union_cte"
                while 1:

                    tempNode = self.nodeMap[nodeBfsKey]
                    if self.logicMap[nodeBfsKey]["DerivedTable"].get(tablename) is not None:

                        tempvar = self.logicMap[nodeBfsKey]["DerivedTable"].get(tablename)
                        print(f"{tempNode.key}.{tempvar}->{tablename}")
                        self.nodeDg.add_edge(
                            tempvar,
                            self.__nodeBfsKey(node), key="logicalMapping", note="逻辑映射")
                        break
                    if nodeBfsKey == (0,0):
                        break
                    else:
                        nodeBfsKey = self.__parentSelect(tempNode.parent)


        elif isinstance(node, DerivedTable):
            if node.key=="cte":
                tokenGraph.append((self.expressionsName(node), self.__nodeBfsKey(node.this)))
                self.nodeDg.add_edge(
                    self.__nodeBfsKey(node.this),
                    self.__nodeBfsKey(node), key="logicalMapping", note="subquery逻辑映射")
                #tokenGraph.append((self.expressionsName(node), {self.expressionsName(node.parent): self.__nodeBfsKey(node.parent)}))




        return tokenGraph

    def expressionsName(self,node:expressions)->str:
        name:str=""
        if node.key in ("column", "ordered", "order", "tablealias", "star", "literal","null"):
            name = node.sql()
        elif node.key=="window":
            name = f"window-{self.expressionsName(node.this)}-{self.__nodeBfsKey(node)}"
        elif node.key == "anonymous":
            name = node.name
        elif isinstance(node, Func):
            name = f"{node.sql_name()}"
        elif isinstance(node, Binary) or isinstance(node, Unary):
            name = node.sql()
        elif isinstance(node,Predicate):
            name = str(node)

        elif node.key =="alias":
            name = str(node.args.get("alias"))
        elif node.key =="star":
            name = str(node)
        elif node.key =="table":
            if node.db=="":
                name = node.name
            elif node.catalog=="":
                name = f"{node.db}.{node.name}"
            else:
                name = f"{node.catalog}.{node.db}.{node.name}"
        elif isinstance(node,DerivedTable):
            if node.key == "cte":
                name=node.alias
            if node.key == "subquery":
                if node.alias == "":
                    name="subquery"+str(self.__nodeBfsKey(node))
                else:
                    name=node.alias
        elif node.key in ("select","union"):
            name=f"{node.key}-{self.__nodeBfsKey(node)}"
        return name

    @property
    def inputTable(self)->list[str]:
        tableList = list()
        for key,value in self.logicMap.items():
            var = list(map(lambda x:x[0],list(filter(lambda x:x[1][0]=="Table",value["tableSource"].items()))))
            tableList+=var
            pass

        return list(set(tableList))

    @property
    def outputColumns(self)->list[str]:
        return list(self.logicMap[(0,0)]["output"].keys())

    def popNode(self,nodeId:tuple)->expressions:
        """
        从self.root中删除一个节点
        :param nodeId:
        :return: 返回被删除的节点
        """
        return self.nodeMap[nodeId].pop()

