#!/usr/bin/env python
# coding: utf-8

import os
import json
import pandas as pd
#import matplotlib.pyplot as plt
from statistics import mean 
from tslearn.metrics import dtw
import numpy as np
#import multiprocessing as mp
from multiprocessing import Pool
#from multiprocessing import Process

# print(sidlist)
import pymysql, sqlalchemy
from sqlalchemy import create_engine
from datetime import datetime
from time import time
#from threading import Thread


#engine = create_engine("mysql+pymysql://teb101Club:teb101Club@localhost/twstock??charset=utf8mb4", max_overflow=5)

#engine = create_engine("mysql+pymysql://teb101Club:teb101Club@localhost/twstock??charset=utf8mb4", pool_recycle=3600)

# # 取某檔股票，半年股價作分析
# # 比對所有dataset

#def DB_pattern_Sweep(sid, sl):
def DT_pattern_Sweep(inputs):
    sid=inputs[0]
    df_DP=inputs[1]
    sl = inputs[2]

    start=time()

    e = create_engine("mysql+pymysql://teb101Club:teb101Club@localhost/twstock??charset=utf8mb4")
    c = e.connect()

    query="select date, open, close, high, low from twstock.transactions where sid={} and\
     (date between '2020-01-31' and '2020-07-31');".format(sid)
    #print(query)
    #price=list(engine.execute(query))
    price = list(c.execute(query))
    c.close()
    #print(price)
    df_test=pd.DataFrame(price, columns=['date', 'open', 'close', 'high', 'low'])
    #print(df.shape)    
#    scoreList_ALL=[]
#    dateList_ALL=[]  
#    dateDict={}
    print('processing {} for Double Top Pattern.....'.format(sid))
    #k=0
    
    DP_dict={}
    DPLIST=[]
    for dbl in df_DP['length']:
        for i in range(df_test.shape[0]-dbl):
            datelist=list(df_test['date'][df_test.shape[0]-i-dbl:df_test.shape[0]-i])
            test_list_open=df_test['open'][df_test.shape[0]-i-dbl:df_test.shape[0]-i].transform(lambda x: x/x.mean())
            test_list_close=df_test['close'][df_test.shape[0]-i-dbl:df_test.shape[0]-i].transform(lambda x: x/x.mean())
            test_list_high=df_test['high'][df_test.shape[0]-i-dbl:df_test.shape[0]-i].transform(lambda x: x/x.mean())
            test_list_low=df_test['low'][df_test.shape[0]-i-dbl:df_test.shape[0]-i].transform(lambda x: x/x.mean())

            for j in range(df_DP.shape[0]):

                if list(test_list_open)[-1]==min(list(test_list_open)): #W底最右邊須突破最大值
                    #print('processing {} by DB{}'.format(sid, j))
                    a = dtw(df_DP['open'][j],test_list_open)
                    b = dtw(df_DP['close'][j],test_list_close)
                    c = dtw(df_DP['high'][j],test_list_high)
                    d = dtw(df_DP['low'][j],test_list_low)
                    
                    score=float(mean([a,b,c,d]))
                    
                    if score<sl:
                        print('DoubleTop', sid, score, [datelist[0].isoformat(), datelist[-1].isoformat()])
                        DPLIST.append({'score':score, 'TI': [datelist[0].isoformat(), datelist[-1].isoformat()]})


    if DPLIST!=[]:
        DP_dict[sid]=DPLIST
        
    end=time()
    print('cost time {} seconds for {}'.format(end-start, sid))
    
    return DP_dict
    
if __name__ == '__main__':
    #print(datetime.now())

    jsonpath = 'dataset'
    i = 0
    list_dataset = []
    for dirPath, dirNames, fileNames in os.walk(jsonpath):
        # print (dirPath)
        for f in fileNames:
            # print (os.path.join(dirPath, f))
            df = pd.read_json(os.path.join(dirPath, f))
            # print(df.ctype[0])
            df[['open', 'close', 'low', 'high']] = df.KBar.apply(pd.Series)

            # print(df.ctype[0])
            # df_dataset[['open', 'close', 'low', 'high']]=df[['open', 'close', 'low', 'high']].transform(lambda x: x/x.mean())

            df[['open', 'close', 'low', 'high']] = df[['open', 'close', 'low', 'high']].transform(
                lambda x: x / x.mean())

            list_dataset.append([df.ctype[0], list(df['open']), list(df['close']), list(df['low']), list(df['high'])])

            # df.dataset=pd.DataFrame(list_dataset)
    np_dataset = np.array(list_dataset)
    df_dataset = pd.DataFrame(np_dataset, columns=['ctype', 'open', 'close', 'low', 'high'])
    # df_dataset

    df_dataset['length'] = df_dataset['open'].apply(lambda x: len(x))
    df_DB = df_dataset[df_dataset['ctype'] == 'DoubleBot'].reset_index(drop=True)
    # df_DT=df_dataset[df_dataset['ctype']=='DoubleTop'].reset_index(drop=True)
    df_DB['length'] = df_DB['open'].apply(lambda x: len(x))


    def inversex(listx):
        y = [2 * mean(listx) - x for x in listx]
        return y
    df_DT=df_DB
    df_DT['open'] = df_DB['open'].apply(inversex)
    df_DT['close'] = df_DB['close'].apply(inversex)
    df_DT['low'] = df_DB['low'].apply(inversex)
    df_DT['high'] = df_DB['high'].apply(inversex)



    engine = create_engine("mysql+pymysql://teb101Club:teb101Club@localhost/twstock??charset=utf8mb4", max_overflow=5)
    c=engine.connect()
    query="select distinct sid from twstock.transactions;"
    #sidlist=engine.execute(query)
    sidlist = c.execute(query)
    c.close()


    inputs=[]

    for _ in sidlist:
        inputs.append([_.sid, df_DT , 0.02])
        inputs.append([_.sid, df_DT, 0.03])
        # _DB_dict=DB_pattern_Sweep(sid, 0.05)
        # if bool(_DB_dict):
        #     DB_dict.update(_DB_dict)

    # print(inputs)
    #運行多處理程序

    pool = Pool(4)
    data = pool.map(DT_pattern_Sweep, inputs)
    pool.close()

    DB_dict={}
    for _data in data:
        if bool(_data):
            DB_dict.update(_data)

    json.dump(DB_dict, open('DTinfo_2.json', "wt"))




