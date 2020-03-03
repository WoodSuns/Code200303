# -*- coding: utf-8 -*-
import os
import datetime
import time
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
#mpl.use('TkAgg')
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['font.serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
import matplotlib.pyplot as plt
from strategy.strategy_mgr import StrategyManager

#择时策略二：成分股策略，给定参数全样本表现
begindate=datetime.datetime(2004,1,1)
enddate=datetime.datetime(2019,7,8)
path='E:\\MarketTiming\\output\\Constituent\\'
if not os.path.exists(path):
    os.makedirs(path)

strategyList=['strategy_CMTM','strategy_CMA','strategy_Cv1','strategy_Cv2']
SmoothLenList={'strategy_CMTM':140,'strategy_CMA':150,'strategy_Cv1':150,'strategy_Cv2':150}
indexList=[u'上证50', u'沪深300',u'中证500',u'中证800']
columnsList=[u'年化',u'最大回撤',u'最大回撤开始',u'最大回撤结束',u'总交易次数',u'每年交易次数',u'夏普',u'胜率',u'盈亏比',u'月度胜率',u'周度胜率',u'持仓周期',u'空仓周期']


manager=StrategyManager(strategyList)
result=pd.DataFrame(columns=['strategyName','index']+columnsList)
for strategyName in strategyList:
    fig = plt.figure(figsize=(15, 15))
    i=1
    for index in indexList:
        beg = time.time()

        strategy=manager.getStrategys(strategyName)(index=index,begindate=begindate,enddate=enddate)
        strategy.calculatePosition(SmoothLen=SmoothLenList[strategyName])
        strategy.calculateNetvalue()
        strategy.riskAnalysis()

        ax = fig.add_subplot(2,2,i)
        f=strategy.netvalue.plot(ax=ax,title=index)
        f.axes.title.set_size(15)
        ax2 = ax.twinx()
        strategy.sign.plot(ax=ax2,color='k',alpha=0.5)

        i=i+1
        result=result.append(strategy.res,ignore_index=True,sort=False)
        result.loc[result.index[-1],['strategyName','index']]=[strategyName,index]
        end = time.time()
        print strategyName+index+":{:.2f}s".format(end-beg)
        plt.savefig(path + strategyName + '.png')
result.to_excel(path+'result.xlsx', engine='xlsxwriter')
result_mean=result.groupby(['strategyName'])[ u'夏普',u'年化', u'最大回撤',u'胜率', u'每年交易次数',u'持仓周期', u'空仓周期',u'盈亏比'].mean()
result_mean.to_excel(path+'resultmean.xlsx', engine='xlsxwriter')
