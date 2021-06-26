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
    #������:

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
    # ����hadoop���ݿ�
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

# ������ʱ��
import table
df = table.getTable()

# before.py Ԥ�����txt��ʽ
import pandas as pd

#.csv->.txt
data = pd.read_csv('/home/hadoop/works/china.csv')
with open('/home/hadoop/works/china.txt','a+',encoding='utf-8') as f:
    for line in data.values:
        f.write((str(line[0])+'\t'+str(line[1])+'\t'+str(line[2])+'\t'+str(line[3])+'\t'+str(line[4])+'\t'+str(line[5])+'\t'+str(line[6])+'\t'+str(line[7])+'\t'+str(line[8])+'\t'+str(line[9])+'\t'+str(line[10])+'\t'+str(line[11])+'\t'+str(line[12])+'\t'+str(line[13])+'\n'))
# end before.py


# ��������
# ȫ������
df = spark.sql('SELECT dateId,SUM(confirmedCount) as confirmedCount,sum(deadCount) as deadCount ,SUM(deadIncr) as deadIncr,SUM(curedCount) as curedCount,SUM(curedIncr) as curedIncr,sum(confirmedIncr) as confirmedIncr,SUM(currentConfirmedCount) as currentConfirmedCount,SUM(suspectedCount) as suspectedCount,sum(suspectedCountIncr) as suspectedCountIncr from covid_inf group by dateId ORDER BY dateId')

# ע��Ϊ��ʱ��ȫ���������ݹ���һ��ʹ��
df.createOrReplaceTempView("covid_info")
# 1����ÿ��ȫ���������ʺ������ʣ�
# ÿ�������� = ÿ���������� / ��ǰ��������
df1 = spark.sql('select dateId,curedIncr / currentConfirmedCount * 100 as curedRate,deadIncr / currentConfirmedCount * 100 as deadRate,confirmedIncr,deadIncr from covid_info')
df1.repartition(1).write.json("result1")
# 2ͳ���ҹ���ֹÿ�յ��ۼ�ȷ���������ۼ�����������ÿ�յ����Ʋ�����
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
# ��ֹ6.2���ҹ���ʡ�ۼ�ȷ����������Ͳ����ʣ������ʡ�>���
df3 = spark.sql('select province,sum(confirmedIncr) as confirmedCount,sum(deadIncr) as deadCount,sum(deadIncr)/sum(confirmedIncr) *100 as deadRate,sum(curedIncr)/sum(confirmedIncr) * 100 as curedRate from covid_inf  group by province')
df3.repartition(1).write.json("result3")
# 4 �ۼ�������������10��ʡ��>©��ͼ
df4 = spark.sql('select province,sum(deadIncr) as deadCount from covid_inf  group by province order by sum(deadIncr) desc limit 10 ')
df4.repartition(1).write.json("result4")
# 5 �ۼ�ȷ����������10���ݡ�>����ͼ
df5 = spark.sql('select province,sum(confirmedIncr) as confirmedIncr from covid_inf  group by province order by sum(confirmedIncr) desc limit 10 ')
df5.repartition(1).write.json("result5")
# 6 ͳ��2020��ÿ������ȫ���ĵ�����ȷ�>��״ͼ 96762
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId >= '2020-01-01' and dateId <= '2020-03-31' ")82631
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-03-31' and dateId <= '2020-06-30' ")2601
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-06-30' and dateId <= '2020-09-30' ")5809
df6 = spark.sql("select sum(confirmedIncr) from covid_info where  dateId > '2020-09-30' and dateId <= '2020-12-31' ")5721


# 3 ȫ��ȷ����������30�죺
df3 = spark.sql('select dateId,confirmedIncr from covid_info ')
df3.repartition(1).write.json("result3")
# 4 ȫ��������������30�죺
df4 = spark.sql('select dateId,curedIncr from covid_info ')
df4.repartition(1).write.json("result4")
# 5 ����6.2��ȫ������������ȷ�ﲡ��
df5 = spark.sql('select province,sum(confirmedIncr) as confirmedCount from covid_inf group by province order by sum(confirmedIncr) desc')
df5.repartition(1).write.json("result5")

# 6 ȫ��ÿ�յ����Ʋ�����
df6 = spark.sql('select dateId,suspectedCountIncr from covid_info  order by suspectedCountIncr desc limit 30')
df6.repartition(1).write.json("result6")

# 8 ͳ��ÿ������ȫ���ĵ�����ȷ��

# 9 ͳ��ÿ������ȷ�ﲡ����������


# ��ͼ
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


#1.����ÿ�յ��ۼ�ȷ�ﲡ������������������������>˫��״ͼ
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
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
                break
            js = json.loads(line)
            date.append(str(js['dateId']))
            suspected.append(int(js['suspectedCountIncr']))
            cases.append(int(js['confirmedIncr']))
            deaths.append(int(js['deadIncr']))
    d = (
    Bar()
    .add_xaxis(date)
    .add_yaxis("ȷ������", cases, stack="stack1")
    .add_yaxis("��������", deaths, stack="stack1")
    .add_yaxis("��������", suspected, stack="stack1")
    .set_series_opts(label_opts=opts.LabelOpts(is_show=False))
    .set_global_opts(title_opts=opts.TitleOpts(title="�ҹ�ÿ���ۼ�ȷ�������������������"))
    .render("/home/hadoop/works/result/result13.html")
    )
    
    
    
#2.������ֹ6.2���ҹ���ʡ�ۼ�ȷ����������Ͳ����ʣ������ʡ�>���
def drawChart_2():
  root = "/home/hadoop/works/result1/part3.json"
  allState = []
  with open(root, 'r') as f:
      while True:
          line = f.readline()
          if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
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
  headers = ["ʡ��", "�ۼ�ȷ��", "��������", "������","������"]
  rows = allState
  table.add(headers, rows)
  table.set_global_opts(
      title_opts=ComponentTitleOpts(title="�ҹ���ʡ����һ��", subtitle="")
  )
  table.render("/home/hadoop/works/result/result2.html")
  
  
  
  
#3.����ÿ�յ�����ȷ�ﲡ����������������>����ͼ
def drawChart_3():
    root = "/home/hadoop/works/result1/part2.json" 
    date = []
    cases = []
    deaths = []
    suspected = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
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
        series_name="����ȷ��",
        y_axis=cases,
        markpoint_opts=opts.MarkPointOpts(
            data=[
                opts.MarkPointItem(type_="max", name="���ֵ")
            ]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[opts.MarkLineItem(type_="average", name="ƽ��ֵ")]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="�ҹ�ÿ������ȷ������ͼ", subtitle=""),
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
        series_name="��������",
        y_axis=deaths,
        markpoint_opts=opts.MarkPointOpts(
            data=[opts.MarkPointItem(type_="max", name="���ֵ")]
        ),
        markline_opts=opts.MarkLineOpts(
            data=[
                opts.MarkLineItem(type_="average", name="ƽ��ֵ"),
                opts.MarkLineItem(symbol="none", x="90%", y="max"),
                opts.MarkLineItem(symbol="circle", type_="max", name="��ߵ�"),
            ]
        ),
    )
    .set_global_opts(
        title_opts=opts.TitleOpts(title="�ҹ�ÿ��������������ͼ", subtitle=""),
        tooltip_opts=opts.TooltipOpts(trigger="axis"),
        toolbox_opts=opts.ToolboxOpts(is_show=True),
        xaxis_opts=opts.AxisOpts(type_="category", boundary_gap=False),
    )
    .render("/home/hadoop/works/result/result4.html")
    )
    
#4.�����ҹ�������������10��ʡ����>����ͼ    
def drawChart_4():
    root = "/home/hadoop/works/result1/part4.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
                break
            js = json.loads(line)
            row=(str(js['province']),int(js['deadCount']))
            data.append(row)

    c = (
    WordCloud()
    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="�ҹ������������ʡ��Top10"))
    .render("/home/hadoop/works/result/result5.html")
    )
    
#5.�����ҹ�ȷ������10��ʡ����>����ͼ    
def drawChart_5():
    root = "/home/hadoop/works/result1/part5.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
                break
            js = json.loads(line)
            row=(str(js['province']),int(js['confirmedIncr']))
            data.append(row)

    c = (
    WordCloud()
    .add("", data, word_size_range=[20, 100], shape=SymbolType.DIAMOND)
    .set_global_opts(title_opts=opts.TitleOpts(title="�ҹ�ȷ�����ʡ��Top10"))
    .render("/home/hadoop/works/result/result6.html")
    )
    
#6.�����ҹ���������10��ʡ����>����״ͼ  
def drawChart_6():
    root = "/home/hadoop/works/result1/part4.json" 
    state = []
    totalDeath = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
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
        title_opts=opts.TitleOpts(title="PictorialBar-�ҹ���ʡ��������Top10"),
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
    
#7.�����ҹ�ȷ������10��ʡ����>����״ͼ  
def drawChart_7():
    root = "/home/hadoop/works/result1/part5.json" 
    state = []
    totalDeath = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
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
        title_opts=opts.TitleOpts(title="PictorialBar-�ҹ���ʡȷ������Top10"),
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
#7.�ҳ�ȷ������10��ʡ����>©��ͼ
def drawChart_8():
    root = "/home/hadoop/works/result1/part5.json" 
    data = []
    with open(root, 'r') as f:
        while True:
            line = f.readline()
            if not line:                            # �� EOF�����ؿ��ַ���������ֹѭ��
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
#8.�����Ĳ�����--->��״ͼ   
def drawChart_9():
    values = []
    values.append(["��һ����(%)",round(float(82631/96762)*100,2)])
    values.append(["�ڶ�����(%)",round(float(2601/96762)*100,2)])
    values.append(["��������(%)",round(float(5809/96762)*100,2)])
    values.append(["���ļ���(%)",round(float(5721/96762)*100,2)])
    c = (
    Pie()
    .add("", values)
    .set_colors(["red","orange","black","blue"])
    .set_global_opts(title_opts=opts.TitleOpts(title="2020�������ȷ������ռ��"))
    .set_series_opts(label_opts=opts.LabelOpts(formatter="{b}: {c}"))
    .render("/home/hadoop/works/result/result10.html")
    )    
   #���ӻ�������
index = 1
while index<=9:
    funcStr = "drawChart_" + str(index)
    eval(funcStr)()
    index+=1 
