# -*- coding: utf-8 -*-
import os
import copy
import time
import datetime
import pandas as pd
import matplotlib as mpl
mpl.use('Agg')
#mpl.use('TkAgg')
mpl.rcParams['font.sans-serif'] = ['SimHei']
mpl.rcParams['font.serif'] = ['SimHei']
mpl.rcParams['axes.unicode_minus'] = False
import matplotlib.pyplot as plt
from strategy.strategy_mgr import StrategyManager

#择时策略一：量价共振策略，给定参数样本外表现
begindate=datetime.datetime(2004,1,1)
enddate=datetime.datetime(2019,7,8)

path='E:\\MarketTiming\\output\\Timing1\\'
if not os.path.exists(path):
    os.makedirs(path)


indexList=[u'上证综指', u'上证380', u'上证180', u'上证50', u'沪深300', u'中证1000', u'中证100',
       u'中证500', u'中证800', u'深证成指', u'深证100R', u'中小板指', u'创业板指', u'创业300',
       u'中小板综', u'创业板综', u'深证综指', u'万得全A']
columnsList=[u'年化',u'最大回撤',u'最大回撤开始',u'最大回撤结束',u'总交易次数',u'每年交易次数',u'夏普',u'胜率',u'盈亏比',u'月度胜率',u'周度胜率',u'持仓周期',u'空仓周期']

def outsample(begindate,enddate,p1,p2,p3,path):
    strategyList=['strategy_v1','strategy_v2','strategy_v3','strategy_p2','strategy_vp']
    parmList={'strategy_v1':p1,'strategy_v2':p2,'strategy_v3':p2,'strategy_p2':p3,'strategy_vp':p2}
    manager = StrategyManager(strategyList)
    result = pd.DataFrame(columns=['strategyName', 'index'] + columnsList)
    for strategyName in strategyList:
        fig = plt.figure(figsize=(25, 25))
        i = 1

        for index in indexList:
            strategy = manager.getStrategys(strategyName)(index=index, begindate=begindate, enddate=enddate)
            strategy.calculatePosition(**parmList[strategyName])
            strategy.calculateNetvalue()
            strategy.riskAnalysis()
            result = result.append(strategy.res, ignore_index=True, sort=False)
            result.loc[result.index[-1], ['strategyName', 'index']] = [strategyName, index]

            ax = fig.add_subplot(6, 3, i)
            f = strategy.netvalue.plot(ax=ax, title=index)
            f.axes.title.set_size(15)
            i = i + 1
            print strategyName + index

        plt.savefig(path + strategyName + '.png')

    result.to_excel(path + 'result.xlsx', engine='xlsxwriter')
    result_mean = result.groupby('strategyName')[
        u'夏普',u'年化', u'最大回撤',u'胜率', u'每年交易次数',u'持仓周期', u'空仓周期',u'盈亏比'].mean()
    result_mean.to_excel(path + 'resultmean.xlsx', engine='xlsxwriter')


p1={'Threshold':1.150}
p2={'Threshold1':1.150,'Threshold2':1.175}
p3={'N_short':10,'N_long':26,'compare':12}
outsample(begindate,enddate,p1,p2,path)



