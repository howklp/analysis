#-*- coding:utf-8 -*-
"""
author:howklp@163.com
"""

# 执行预处理后总体销量和店铺销量的时间分布统计
# 

import pandas as pd
import os,time
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

engine = create_engine('mysql://root:Mysql-pwd-1234@127.0.0.1/test?charset=utf8')

def totalStat():
    # 读取数据文件day_stat.csv
    data = pd.read_csv('temp/day_stat.csv')     
    
    # 预处理采用分块处理，这里需要先执行groupby
    data = data.groupby('date').sum()
    # 执行排序比较稳妥
    data = data.sort_values(['date'],ascending=True)
    """
    # 绘制订单量/销售额在时间（day）上的分布
    data['count'].plot()
    plt.show()

    data['volume'].plot()
    plt.show()
    #"""

    # 如果需要计算周/月/季等，以周为例
    # 实际工程中，也可能需要选定时间范围，实时计算
    # 添加一个weekday子段
    # 将数据按照weekday分组
    data['weekday'] = pd.to_datetime(data.index).strftime("%W")#.weekday
    #
    week = data.groupby('weekday').aggregate(sum)
    #week['count'].plot(kind='bar')
    #plt.show()

    #
    # 为了便于业务接口的方便调用
    # 将统计数据写入mysql
    # 最好手动创建数据表
    # 否则会遇到创建索引出错
    pd.io.sql.to_sql(week,'week_stat',
                     engine,
                     schema='test',
                     if_exists='append'
                     )

def shopsStat():
    # 不作测试计算了，
    # 可以将所有数据导入到内存（文件不大 (178M)，无需分块）
    # 按照shop/date，执行groupby，再进行后续计算即可
    pass

if __name__ == '__main__':
    totalStat()
