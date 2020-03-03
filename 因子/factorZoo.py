# -*- coding: utf-8 -*-
# Author: sss
# Create Date: 2019/11/7

import time
import pickle
import datetime
import pandas as pd
import numpy as np
import statsmodels.api as sm
from scipy.stats import kurtosis
from scipy.stats import norm
from portmgr_Q.factor import HTradeFactorDemo


path='D:\\Data'


   
cdfdir={}     
class H_RVdir3(HTradeFactorDemo):	 
    #成交量交易方向判定：批量方向判别法-将一段区间内的成交量按照一定比例（标准化）分配给买入和卖出
    #参数取值：1（买入成交量和与总成交量和的比值）2（买入卖出成交量和之差的绝对值与总成交量和的比值,即知情交易概率）
    #         3 (买入成交量波动率与买入卖出成交量波动率和的比值)4(买入卖出成交量波动率差与买入卖出成交量波动率和的比值)
    def __init__(self,parm=1,standard=False,how='mean',frequency=60,lagTradeDays=20,path=path, validTradingDayRatio=0.7,
                 items=['close', 'preClose','volume'], factorSymbol=None,dailyFactorSymbol=None):
        if factorSymbol is None:
            factorSymbol = self.__class__.__name__+'_'+str(parm)
        if dailyFactorSymbol is None:
            dailyFactorSymbol = self.__class__.__name__
        HTradeFactorDemo.__init__(self,path=path,standard=standard,how=how,
                                 lagTradeDays=lagTradeDays,
                                 frequency=frequency,
                                 validTradingDayRatio=validTradingDayRatio,
                                 items=items,
                                 factorSymbol=factorSymbol,dailyFactorSymbol=dailyFactorSymbol) 
        self.parm=parm
    
    def _getcdf(self,x):
        if x>4:
            return 1
        if x<-4:
            return 0
        x = round(x, 2)
        if x not in cdfdir.keys():
            cdfdir[x]=norm.cdf(x)
        return cdfdir[x]
            
    def getFactor(self):
        data=self.dailydata.reset_index()
        data['date']=data['dateTime'].apply(lambda x:x.date())
        data['ret']=data['close']/data['preClose']-1
        mean,std=data['ret'].mean(),data['ret'].std()
        data['ret_standard']=(data['ret']-mean)/std
        data['upvol']=data['ret_standard'].apply(self._getcdf)*data['volume']
        data['downvol']=data['volume']-data['upvol']
        buy_sum=data.groupby(['securityId','date'])['upvol'].sum()
        sale_sum=data.groupby(['securityId','date'])['downvol'].sum()
        buy_std=data.groupby(['securityId','date'])['upvol'].std()
        sale_std=data.groupby(['securityId','date'])['downvol'].std()

        self.dailyfactor=pd.DataFrame()
        self.dailyfactor[1]=buy_sum/(buy_sum+sale_sum)
        self.dailyfactor[2]=(buy_sum-sale_sum).abs()/(buy_sum+sale_sum)
        self.dailyfactor[3]=buy_std/(buy_std+sale_std)
        self.dailyfactor[4]=(buy_std-sale_std).abs()/(buy_std+sale_std)
        self.dailyfactor.replace([np.inf,-np.inf],np.nan,inplace=True)
        
    def getDailyFactor(self,dateTime):
        HTradeFactorDemo.getDailyFactor(self,dateTime)
        #加入这一句代码：
        self.dailyfactor=self.dailyfactor[self.parm] 
        

 
        
if __name__ == '__main__':
    
    dateTime = datetime.datetime(2014, 4, 30)
    f =H_RVdir3()
    
    clock0 = time.clock()
    time0 = time.time()
    
    data = f.calculateFactorValue(dateTime)
    
    clock1 = time.clock()
    time1 = time.time()
    
    print('clock',clock1-clock0)
    print('time',time1-time0)
    
    
    
    

    
