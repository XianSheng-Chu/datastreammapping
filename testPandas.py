import pandas as pd

pd.options.display.max_rows = 10  # 设置自由列表输出最多10行
print(pd.__version__)
df1 = pd.DataFrame({
    'var1': 1.0,
    'var2': [1, 2, 3, 4],
    'var3': ["test", "train", "test", "train"],
    'var4': 'cons'}
)

s1 = pd.Series(["test", "train", "test", "train"])
s1

s1 = pd.Series(data = ["test", "train", "test", "train"],name = "var3")
s1

df2 = pd.read_csv(
    filepath_or_buffer="D:\\Files\\jupyterRootPath\\dataSource\\测试数据.csv",#文件路径
    sep=",",#分隔符
    header="infer",#指定第几行作为变量名
    names=None,#自定义变量名列表
    index_col=None,#将会被用作索引的列名，多列时只能使用序号列表
    usecols=None,#指定只读入某些列,使用列索引或者列名称均可
    encoding=None,#读入文件编码方式
    na_values=None#指定作为缺失值的字段列表
)

#print(df2)

df3 = pd.read_excel(
io="D:\\Files\\jupyterRootPath\\dataSource\\测试数据.xlsx",
sheet_name = 0
)

print(df3)

import pandas as pd
import scipy.stats as ss
import matplotlib

matplotlib.rcParams['font.sans-serif'] = ['SimHei']
ccss = pd.read_excel(
io="D:\\Files\\jupyterRootPath\\dataSource\\测试数据.xlsx",
sheet_name = 0
)

ccss.groupby('rule_name')['row_count','connect_id'].mean()