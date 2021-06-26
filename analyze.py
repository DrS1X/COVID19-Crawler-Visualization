pyspark --master local[4] --py-files ~/works/table.py

# table.py
from pyspark import SparkConf,SparkContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from datetime import datetime
import pyspark.sql.functions as func

 
def getTable():
    
    def toDate(inputStr):
        newStr = ""
        s1 = inputStr[0:4]
        s2 = inputStr[4:6]
        s3 = inputStr[6:]
        newStr = s1+"-"+s2+"-"+s3
        date = datetime.strptime(newStr, "%Y-%m-%d")
        return date
    #主程序:

    spark = SparkSession.builder.config(conf = SparkConf()).getOrCreate()

    fields = [StructField("confirmedCount", IntegerType(),False),
              StructField("confirmedIncr", IntegerType(),False),
              StructField("curedCount", IntegerType(),False),
              StructField("curedIncr", IntegerType(),False),
              StructField("currentConfirmedCount", IntegerType(),False),
              StructField("currentConfirmedIncr", IntegerType(),False),
              StructField("dateId", DateType(),False),
              StructField("deadCount", IntegerType(),False),
              StructField("deadIncr", IntegerType(),False),
              StructField("highDangerCount", IntegerType(),False),
              StructField("midDangerCount", IntegerType(),False),
              StructField("suspectedCount", IntegerType(),False),
              StructField("suspectedCountIncr", IntegerType(),False),
              StructField("province", StringType(),False),]
    schema = StructType(fields)


    #rdd0 = spark.sparkContext.textFile("/user/hadoop/us-counties.txt")
    # 链接hadoop数据库
    rdd0 = spark.sparkContext.textFile("hdfs://localhost:9000/user/hadoop/works/china.txt")
    #rdd0 = sc.textFile("hdfs://localhost:9000/user/hadoop/works/china.txt")
    rdd1 = rdd0.map(lambda x:x.split("\t")).map(
        lambda p: Row(
            int(p[0]),int(p[1]),int(p[2]),int(p[3]),int(p[4]),int(p[5]),toDate(p[6]),int(p[7]),int(p[8]),int(p[9]),int(p[10]),int(p[11]),int(p[12]),p[13]
        )
    )
    shemaUsInfo = spark.createDataFrame(rdd1,schema)
    shemaUsInfo.createOrReplaceTempView("covid_inf")
    return shemaUsInfo
# end table.py

# 创建临时表
import table
df = table.getTable()

# before.py 预处理成txt格式
import pandas as pd

#.csv->.txt
data = pd.read_csv('/home/hadoop/works/china.csv')
with open('/home/hadoop/works/china.txt','a+',encoding='utf-8') as f:
    for line in data.values:
        f.write((str(line[0])+'\t'+str(line[1])+'\t'+str(line[2])+'\t'+str(line[3])+'\t'+str(line[4])+'\t'+str(line[5])+'\t'+str(line[6])+'\t'+str(line[7])+'\t'+str(line[8])+'\t'+str(line[9])+'\t'+str(line[10])+'\t'+str(line[11])+'\t'+str(line[12])+'\t'+str(line[13])+'\n'))
# end before.py


# 特征分析
# 全国数据
df = spark.sql('SELECT dateId,SUM(confirmedCount) as confirmedCount,sum(deadCount) as deadCount ,SUM(deadIncr) as deadIncr,SUM(curedCount) as curedCount,SUM(curedIncr) as curedIncr,sum(confirmedIncr) as confirmedIncr,SUM(currentConfirmedCount) as currentConfirmedCount,SUM(suspectedCount) as suspectedCount,sum(suspectedCountIncr) as suspectedCountIncr from covid_inf group by dateId ORDER BY dateId')

# 注册为临时表全国疫情数据供下一步使用
df.createOrReplaceTempView("covid_info")
# 1计算每日全国的治愈率和死亡率：
# 每日治愈率 = 每日治愈人数 / 当前患病人数
df1 = spark.sql('select dateId,curedIncr / currentConfirmedCount * 100 as curedRate,deadIncr / currentConfirmedCount * 100 as deadRate,confirmedIncr,deadIncr from covid_info')
df1.repartition(1).write.json("result1")
# 2统计我国截止每日的累计确诊人数和累计死亡人数，每日的疑似病例。
#all
df2 = spark.sql('select dateId,confirmedIncr,deadIncr,suspectedCountIncr from covid_info')
df2.repartition(1).write.json("result2")
#-4.30
df2 = spark.sql("select dateId,confirmedIncr,deadIncr,suspectedCountIncr from covid_info where dateId <= '2020-04-30'")
df2.repartition(1).write.json("result11")
#6.13-9.19
df2 = spark.sql("select dateId,confirmedIncr,deadIncr,suspectedCountIncr from covid_info where dateId >= '2020-06-13' and dateId <= '2020-09-19'")
df2.repartition(1).write.json("result12")
#5.1-6.2
df2 = spark.sql("select dateId,confirmedIncr,deadIncr,suspectedCountIncr from covid_info where dateId >= '2021-05-01'")
df2.repartition(1).write.json("result13")
# 截止6.2，我国各省累计确诊、死亡人数和病死率，治愈率—>表格
df3 = spark.sql('select province,sum(confirmedIncr) as confirmedCount,sum(deadIncr) as deadCount,sum(deadIncr)/sum(confirmedIncr) *100 as deadRate,sum(curedIncr)/sum(confirmedIncr) * 100 as curedRate from covid_inf  group by province')
df3.repartition(1).write.json("result3")
# 4 累计死亡人数最多的10个省—>漏斗图
df4 = spark.sql('select province,sum(deadIncr) as deadCount from covid_inf  group by province order by sum(deadIncr) desc limit 10 ')
df4.repartition(1).write.json("result4")
# 5 累计确诊人数最多的10个州—>词云图
df5 = spark.sql('select province,sum(confirmedIncr) as confirmedIncr from covid_inf  group by province order by sum(confirmedIncr) desc limit 10 ')
df5.repartition(1).write.json("result5")
# 6 统计2020年每个季度全国的的新增确诊—>饼状图 96762
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId >= '2020-01-01' and dateId <= '2020-03-31' ")82631
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-03-31' and dateId <= '2020-06-30' ")2601
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-06-30' and dateId <= '2020-09-30' ")5809
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-09-30' and dateId <= '2020-12-31' ")5721


# 3 全国确诊人数排名30天：
df3 = spark.sql('select dateId,confirmedIncr from covid_info ')
df3.repartition(1).write.json("result3")
# 4 全国治愈人数排名30天：
df4 = spark.sql('select dateId,curedIncr from covid_info ')
df4.repartition(1).write.json("result4")
# 5 截至6.2，全国各地区最终确诊病：
df5 = spark.sql('select province,sum(confirmedIncr) as confirmedCount from covid_inf group by province order by sum(confirmedIncr) desc')
df5.repartition(1).write.json("result5")

# 6 全国每日的疑似病例：
df6 = spark.sql('select dateId,suspectedCountIncr from covid_info  order by suspectedCountIncr desc limit 30')
df6.repartition(1).write.json("result6")

# 8 统计每个季度全国的的新增确诊

# 9 统计每日新增确诊病例增长比例


# 画图
from pyecharts import options as opts
from pyecharts.charts import Bar
from pyecharts.charts import Line
from pyecharts.components import Table
from pyecharts.charts import WordCloud
from pyecharts.charts import Pie
from pyecharts.charts import Funnel
from pyecharts.charts import Scatter
from pyecharts.charts import PictorialBar
from pyecharts.options import ComponentTitleOpts
from pyecharts.globals import SymbolType
import json


#1.画出每日的累计确诊病例数和死亡数和疑似数——>双柱状图
def drawChart_1():
    root = "/home/hadoop/works/result1/part13.json"
    print(root)
    #confirmedIncr,deadIncr,suspectedCountIncr
    date = []
    cases = []
    deaths = []
    suspected = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            date.append(str(js['dateId']))
            suspected.append(int(js['suspectedCountIncr']))
            cases.append(int(js['confirmedIncr']))
            deaths.append(int(js['deadIncr']))
    d = (
    Bar()
    .add_xaxis(date)
    .add_yaxis("确诊人数", cases, stack="stack1")
    .add_yaxis("死亡人数", deaths, stack="stack1")
    .add_yaxis("疑似人数", suspected, stack="stack1")
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(title_opts=opts.TitleOpts(title="我国每日累计确诊、死亡人数和疑似人数"))
    .render("/home/hadoop/works/result/result13.html")
    )
    
    
    
#2.画出截止6.2，我国各省累计确诊、死亡人数和病死率，治愈率—>表格
def drawChart_2():
  root = "/home/hadoop/works/result1/part3.json"
  allState = []
  with open(root, 'r') as f:
      while True:
          line = f.readline()
          if not line:                            # 到 EOF，返回空字符串，则终止循环
              break
          js = json.loads(line)
          row = []
          row.append(str(js['province']))
          row.append(int(js['confirmedCount']))
          row.append(int(js['deadCount']))
          row.append(float(js['deadRate']))
          row.append(float(js['curedRate']))
          allState.append(row)
  table = Table()
  headers = ["省份", "累计确诊", "死亡人数", "病死率","治愈率"]
  rows = allState
  table.add(headers, rows)
  table.set_global_opts(
      title_opts=ComponentTitleOpts(title="我国各省疫情一览", subtitle="")
  )
  table.render("/home/hadoop/works/result/result2.html")
  
  
  
  
#3.画出每日的新增确诊病例数和死亡数——>折线图
def drawChart_3():
    root = "/home/hadoop/works/result1/part2.json" 
    date = []
    cases = []
    deaths = []
    suspected = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            date.append(str(js['dateId']))
            suspected.append(int(js['suspectedCountIncr']))
            cases.append(int(js['confirmedIncr']))
            deaths.append(int(js['deadIncr']))
    (
    Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
    .add_xaxis(xaxis_data=date)
    .add_yaxis(
        series_name="新增确诊",
        y_axis=cases,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="最大值")
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[opts.MarkLineItem(type_="average", name="平均值")]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="我国每日新增确诊折线图", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    .render("/home/hadoop/works/result/result3.html")
    )
    (
    Line(init_opts=opts.InitOpts(width="1600px", height="800px"))
    .add_xaxis(xaxis_data=date)
    .add_yaxis(
        series_name="新增死亡",
        y_axis=deaths,
        markpoint_opts=opts.MarkPointOpts(
            data=[opts.MarkPointItem(type_="max", name="最大值")]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="average", name="平均值"),
                opts.MarkLineItem(symbol="none", x="90%", y="max"),
                opts.MarkLineItem(symbol="circle", type_="max", name="最高点"),
            ]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="我国每日新增死亡折线图", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    .render("/home/hadoop/works/result/result4.html")
    )
    
#4.画出我国死亡人数最多的10个省——>词云图    
def drawChart_4():
    root = "/home/hadoop/works/result1/part4.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            row=(str(js['province']),int(js['deadCount']))
            data.append(row)

    c = (
    WordCloud()
    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="我国死亡人数最多省份Top10"))
    .render("/home/hadoop/works/result/result5.html")
    )
    
#5.画出我国确诊最多的10个省——>词云图    
def drawChart_5():
    root = "/home/hadoop/works/result1/part5.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            row=(str(js['province']),int(js['confirmedIncr']))
            data.append(row)

    c = (
    WordCloud()
    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="我国确诊最多省份Top10"))
    .render("/home/hadoop/works/result/result6.html")
    )
    
#6.画出我国死亡最多的10个省——>象柱状图  
def drawChart_6():
    root = "/home/hadoop/works/result1/part4.json" 
    state = []
    totalDeath = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            state.insert(0,str(js['province']))
            totalDeath.insert(0,int(js['deadCount']))

    c = (
    PictorialBar()
    .add_xaxis(state)
    .add_yaxis(
        "",
        totalDeath,
        label_opts=opts.LabelOpts(is_show=False),
        symbol_size=18,
        symbol_repeat="fixed",
        symbol_offset=[0, 0],
        is_symbol_clip=True,
        symbol=SymbolType.ROUND_RECT,
    )
    .reversal_axis()
    .set_global_opts(
        title_opts=opts.TitleOpts(title="PictorialBar-我国各省死亡人数Top10"),
        xaxis_opts=opts.AxisOpts(is_show=False),
        yaxis_opts=opts.AxisOpts(
            axistick_opts=opts.AxisTickOpts(is_show=False),
            axisline_opts=opts.AxisLineOpts(
                linestyle_opts=opts.LineStyleOpts(opacity=0)
            ),
        ),
    )
    .render("/home/hadoop/works/result/result7.html")
    )   
    
#7.画出我国确诊最多的10个省——>象柱状图  
def drawChart_7():
    root = "/home/hadoop/works/result1/part5.json" 
    state = []
    totalDeath = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            state.insert(0,str(js['province']))
            totalDeath.insert(0,int(js['confirmedIncr']))

    c = (
    PictorialBar()
    .add_xaxis(state)
    .add_yaxis(
        "",
        totalDeath,
        label_opts=opts.LabelOpts(is_show=False),
        symbol_size=18,
        symbol_repeat="fixed",
        symbol_offset=[0, 0],
        is_symbol_clip=True,
        symbol=SymbolType.ROUND_RECT,
    )
    .reversal_axis()
    .set_global_opts(
        title_opts=opts.TitleOpts(title="PictorialBar-我国各省确诊人数Top10"),
        xaxis_opts=opts.AxisOpts(is_show=False),
        yaxis_opts=opts.AxisOpts(
            axistick_opts=opts.AxisTickOpts(is_show=False),
            axisline_opts=opts.AxisLineOpts(
                linestyle_opts=opts.LineStyleOpts(opacity=0)
            ),
        ),
    )
    .render("/home/hadoop/works/result/result8.html")
    )   
#7.找出确诊最多的10个省——>漏斗图
def drawChart_8():
    root = "/home/hadoop/works/result1/part5.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # 到 EOF，返回空字符串，则终止循环
                break
            js = json.loads(line)
            data.insert(0,[str(js['province']),int(js['confirmedIncr'])])
    
    c = (
    Funnel()
    .add(
        "province",
        data,
        sort_="ascending",
        label_opts=opts.LabelOpts(position="inside"),
    )
    .set_global_opts(title_opts=opts.TitleOpts(title=""))
    .render("/home/hadoop/works/result/result9.html")
    )   
#8.美国的病死率--->饼状图   
def drawChart_9():
    values = []
    values.append(["第一季度(%)",round(float(82631/96762)*100,2)])
    values.append(["第二季度(%)",round(float(2601/96762)*100,2)])
    values.append(["第三季度(%)",round(float(5809/96762)*100,2)])
    values.append(["第四季度(%)",round(float(5721/96762)*100,2)])
    c = (
    Pie()
    .add("", values)
    .set_colors(["red","orange","black","blue"])
    .set_global_opts(title_opts=opts.TitleOpts(title="2020年各季度确诊人数占比"))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    .render("/home/hadoop/works/result/result10.html")
    )    
   #可视化主程序：
index = 1
while index<=9:
    funcStr = "drawChart_" + str(index)
    eval(funcStr)()
    index+=1 
