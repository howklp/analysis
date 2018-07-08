# -*- coding:utf-8 -*-
#
# 所有交易记录均生成csv文件，共pandas读取

import os,random,time
import pandas as pd
from datetime import datetime

def create_shops():
    # 创建商铺，假定有1W家商铺
    shops = ['s%s' % i for i in range(10000,20000)]
    #with open('shops.csv','w') as f:
    #    f.write('\n'.join(shops))
    return shops
    
def create_products():
    # 定义5000种商品，每种基准价格处于20元到1000元之间    
    ret = []    
    for i in range(1000,6000):
        name = 'p%s' % i
        price = random.randint(20,1000)
        product = '%s,%s' % (name,price)
        ret.append(product)
    return ret

def shop_products(shops,products):
    # 每个店铺商品（类别）数量在20-150之间     
    # 每个商品的库存在5-999之间    
    results = []
    for s in shops:
        # 随机商品数量
        pcount = random.randint(20,150)
        # 获取随机商品
        shop_products = random.sample(products,pcount)
        #print shop_products        
             
	results += ['%s,%s' % (s,p) for p in shop_products]
    #print len(results)
    return results 


# 假定2017年有1000W用户购买商品
# 每类商品每个买家购买类型量在1-5
# 假定每天总的交易用户在40 - 60W之间
def records(products):
    # 交易记录
    #"""
    t1 = time.time()
    users = ['u%s' % i for i in range(10000000,20000000)]
    with open('user.data','w') as f:
        f.write(str(users))
    #"""
    dates = pd.date_range('20170101',periods=365)
    
    with open('orders.csv','w') as f:
        f.write('user,shop,product,price,quantity,date\n')
    for date in dates:	
	t1 = time.time()
	# 随机购买用户名单
    	user_sample = random.sample(users,random.randint(400000,600000))        
	results = []
        for user in user_sample:
            # 随机取1-10件商品
            # 购买数量，随机取1-3
	    product_sample = random.sample(products,random.randint(1,10))
            # 购买数量
            quantity = random.randint(1,4)
            
            # 订单
            # user,shopname,product,price,quantity,date
	    results += ['%s,%s,%s,%s' % (user,product,quantity,date) for product in product_sample]
        
        with open('orders.csv','a+') as f:
	    f.write('\n'.join(results))
	    f.write('\n') # 第一批数据中缺失这个换行，导致数据出错
	t2 = time.time()
        print datetime.now(),date,t2 - t1
 
shops = create_shops()
products = create_products()
products = shop_products(shops,products)
records(products)
