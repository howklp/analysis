#-*- coding:utf-8 -*-
# 读取并处理大文件orders.csv，分析交易记录
# 数据中由于生成程序的问题，有数据错误，，数据生成时间太长（2小时30分左右），姑且跳过处理，
# 但如果实际遇到这种情况，会对交易额度的计算有影响，此处只做pandas的数据分析学习，不影响
# 统计如下几项数据：
#    1、总的交易订单数
#    2、总的交易额度
#    3、交易订单数在时间上的分布
#    4、交易额度在时间上的分布
#    
#    5、添加一列，记录每个订单的交易额度

import pandas as pd
import os,time


# 全局变量定义
# 交易总订单
global total_count 
total_count = 0

# 交易总额度
global total_volume
total_volume = 0

# ==============================================================================
# 读取数据文件
def start():
    f = 'orders.csv'
    t0 = time.time()
    # 分块读取，块大小为 10**7
    chunksize = 10 ** 7
    batch = 1
    for chunk in pd.read_csv(f, chunksize=chunksize,error_bad_lines=False):
	t1 = time.time()
	analysis(chunk)
        print 'Analysis Chunk [%s]' % batch
        print 'timeit = [%s]' % (time.time() - t1)
	print '*' * 100        
        #if batch >= 2:
	#    break
	batch += 1
    t2 = time.time()
    print 'Taotal Timeit = [%s]' % (t2 - t0)

def analysis(chunk):
    tcount = totalCount(chunk)
    # 订单额度在生成数据时没有计算，故需要在chunk（DataFrame）中添加这一列
    chunk['volume'] = chunk['quantity'] * chunk['price']
    #print chunk

    # 总交易额度
    totalVolume(chunk)
    
    # 每天的交易订单量
    statDay(chunk)

    # 店铺日交易统计
    shopStat(chunk)
    

# ==========================================================================
# 计算总体
# 交易总订单计算
def totalCount(chunk):
    global total_count
    # 交易总订单
    total_count += chunk.count()[0]
    print total_count

# 交易总额度
def totalVolume(chunk):
    global total_volume
    total_volume += chunk['volume'].sum()
    #print total_volume

# 计算销量（订单量）在时间维度（日）上的分布
# 采用groupby方法
def statDay(chunk):
    #print chunk
    count = chunk.groupby('date').count()
    # 由于生成的数据不存在Nan的情况，计算count，随便取一个字段的值均可代替
    count = count['volume'].to_frame()
    count.rename(columns={'volume':'count'},inplace=True)
   
    # 日销售额度
    volume = chunk.groupby('date')['volume'].sum().to_frame()
    
    # 按照时间合并两个结果集（DataFrame）
    day = count.join(volume,how='inner')
    
    # 存储中间计算结果
    day.to_csv('temp/day_stat.csv',mode='a',header=0)    
# ==========================================================================
# 计算具体的店铺数据
def shopStat(chunk): 
    # 将店铺的日订单数，日销售额等数据写入临时文件
    count = chunk.groupby(['shop','date']).count()
    count = count['volume'].to_frame()
    count.rename(columns={'volume':'count'},inplace=True)
    
    volume = chunk.groupby(['shop','date'])['volume'].sum().to_frame()
    
    shop_day = count.join(volume,how='inner')

    shop_day.to_csv('temp/shop_day_stat.csv',mode='a',header=0)

 
# ==========================================================================
def init():
    # 初始化
    # 删除上次计算的临时文件     
    if not os.path.exists('temp/'):
	os.mkdir('temp/')
    if os.path.exists('temp/day_stat.csv'):
	os.remove('temp/day_stat.csv')
    with open('temp/day_stat.csv','w') as f:
	f.write('date,count,volume\n')

    if os.path.exists('temp/shop_day_stat.csv'):
	os.remove('temp/shop_day_stat.csv')
    with open('temp/shop_day_stat.csv','w') as f:
	f.write('shop,date,count,volume\n')

if __name__ == '__main__':
    init()
    start()
        
