# -*- coding: utf-8 -*-
import pickle
import numpy as np
import pandas as pd
from strategy import strategyDemo

class strategy_obv(strategyDemo):
    def getOBV(self):
        d = self.data.copy()
        d['ret'] = d['close'].pct_change()
        d['f1'] = (d['ret'] > 0).astype(np.int)
        d['f2'] = (d['ret'] < 0).astype(np.int)
        d['obv'] = d['volume'] * d['f1'] - d['volume'] * d['f2']
        d['OBV'] = d['obv'].cumsum()
        return d['OBV']
    def calculatePosition(self,n_short=5,n_long=20):
        d = self.data.copy()
        d['OBV'] = self.getOBV()
        d['short'] = d['OBV'].rolling(n_short).mean()
        d['long'] = d['OBV'].rolling(n_long).mean()
        d['value'] = d['short'] / d['long']
        d['res']=d.apply(lambda x:1.0 if x['value']>1 else 0,axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']


class strategy_vol(strategyDemo):
    def calculatePosition(self,n_short=5,n_long=20):
        d = self.data.copy()
        d['short'] = d['volume'].rolling(n_short).mean()
        d['long'] = d['volume'].rolling(n_long).mean()
        d['value'] = d['short'] / d['long']
        d['res'] = d.apply(lambda x: 1 if x['value'] > 1 else 0, axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']

class strategy_volEWM(strategyDemo):
    def calculatePosition(self,n_short=5,n_long=90):
        d = self.data.copy()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['value'] = d['short'] / d['long']
        d['res'] = d.apply(lambda x: 1 if x['value'] > 1 else 0, axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']

class strategy_v1(strategyDemo):
    def calculatePosition(self,L=50,N=3,Threshold=1.15,n_short=5,n_long=90):
        d = self.data.copy()
        d['price'] = d['close'].ewm(span=L).mean()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['value'] = (d['price'] / d['price'].shift(N)) * (d['short'] / d['long'])
        d['res'] = d.apply(lambda x: 1 if x['value'] > Threshold else 0, axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']


class strategy_v2(strategyDemo):
    def calculatePosition(self,L=50,N=3,Threshold1=1.15,Threshold2=1.30,n_short=5,n_long=90):
        d = self.data.copy()
        d['price'] = d['close'].ewm(span=L).mean()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['value'] = (d['price'] / d['price'].shift(N)) * (d['short'] / d['long'])

        d['p5MA']=d['close'].rolling(5).mean()
        d['p90MA'] = d['close'].rolling(90).mean()
        d['Threshold']=d.apply(lambda x:Threshold1 if x['p5MA']>x['p90MA'] else Threshold2,axis=1)
        d['res'] = d.apply(lambda x: 1 if x['value'] > x['Threshold'] else 0, axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']

class strategy_v3(strategyDemo):
    def calculatePosition(self,L=50,N=3,Threshold1=1.15,Threshold2=1.30,n_short=5,n_long=90):
        d = self.data.copy()
        d['price'] = d['close'].ewm(span=L).mean()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['value'] = (d['price'] / d['price'].shift(N)) * (d['short'] / d['long'])

        d['p5MA']=d['close'].rolling(5).mean()
        d['p90MA'] = d['close'].rolling(90).mean()
        d['Threshold']=d.apply(lambda x:Threshold1 if x['p5MA']>x['p90MA'] else Threshold2,axis=1)
        d['res'] = d.apply(lambda x: 1 if x['value'] > x['Threshold'] else 0, axis=1)

        d['p']=d['close'].ewm(span=4).mean()
        d['p_ret']=d['p'].diff().abs()
        d['p_ret10'] = d['p'].diff(10).abs()
        d['p1']=d['p_ret10']/d['p_ret'].rolling(9).sum()*100.0
        d['p2'] =d['p'].pct_change(10)
        d['down']=d.apply(lambda x:1 if x['p1']>=50 and x['p2']<0 else 0,axis=1)
        d['res'] = d.apply(lambda x: 0 if x['down'] == 1 else x['res'],axis=1)

        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']


class strategy_p1(strategyDemo):
    def calculatePosition(self,N_short=10,N_long=26,compare=12):
        d = self.data.copy()
        d['short'] = d['close'].ewm(span=N_short).mean()
        d['long'] = d['close'].ewm(span=N_long).mean()
        d['TrendInd'] = d['short'] / d['long']
        d['AcceleratorInd'] = d['TrendInd'] /d['TrendInd'].ewm(span=compare).mean()
        d['res'] = d.apply(lambda x: 1 if x['TrendInd'] >= 1 and x['AcceleratorInd'] >= 1 else 0,axis=1)

        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']


class strategy_p2(strategyDemo):
    def calculatePosition(self, N_short=10, N_long=26, compare=12):
        d = self.data.copy()
        d['short'] = d['close'].ewm(span=N_short).mean()
        d['long'] = d['close'].ewm(span=N_long).mean()
        d['TrendInd'] = d['short'] / d['long']
        d['AcceleratorInd'] = d['TrendInd'] / d['TrendInd'].ewm(span=compare).mean()
        d['res'] = d.apply(lambda x: 1 if x['TrendInd'] >= 1 and x['AcceleratorInd'] >= 1 else 0,axis=1)

        d['p5MA']=d['close'].rolling(5).mean()
        d['p90MA'] = d['close'].rolling(90).mean()
        d['p'] = d['close'].ewm(span=4).mean()
        d['p2'] = d['p'].pct_change(10)
        d['down'] = d.apply(lambda x: 1 if x['p5MA']<=x['p90MA'] and x['p2'] < 0 else 0,axis=1)
        d['res'] = d.apply(lambda x: 0 if x['down'] == 1 else x['res'],axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']


class strategy_vp(strategyDemo):
    def calculatePosition(self,L=50,N=3,Threshold1=1.15,Threshold2=1.30,n_short=5,n_long=90, N_short=10, N_long=26, compare=12):
        v3 = strategy_v3(self.index, self.begindate, self.enddate)
        v3.calculatePosition(L=L,N=N,Threshold1=Threshold1,Threshold2=Threshold2,n_short=n_short,n_long=n_long)
        p_v3 = v3.position

        p2 = strategy_p2(self.index, self.begindate, self.enddate)
        p2.calculatePosition(N_short=N_short,N_long= N_long,compare= compare)
        p_p2 = p2.position

        d = self.data.copy()
        d['position_v3']=p_v3
        d['position_p2']=p_p2
        d['p5MA']=d['close'].rolling(5).mean()
        d['p90MA'] = d['close'].rolling(90).mean()


        d['down'] = d.apply(lambda x: 1 if x['p5MA'] <= x['p90MA'] else 0,axis=1)
        for i in d.index:
            if d.loc[i, 'down'] == 0:
                if d.loc[i, 'position_v3'] >0 or d.loc[i, 'position_p2'] >0 :
                    d.loc[i, 'res'] = 1.0
                else:
                    d.loc[i, 'res'] = 0
            else:
                d.loc[i, 'res'] = d.loc[i, 'position_v3']
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']

down_path='E:\\MarketTiming\\data\\'
class strategy_CMTM(strategyDemo):
    #使用成分股数据得到持仓空仓变量(动量)，按流动市值加权，并两次平滑
    #成分股数据应包括所有出现的股票在该区间的数据，避免新加入的股票没有历史数据
    def getdata(self,opens,closes,volume):
        strategyDemo.getdata(self,opens,closes,volume)
        dIndex={u'上证50':'000016.SH',u'沪深300':'000300.SH',u'中证500':'000905.SH', u'中证800':'000906.SH'}
        if self.index in dIndex.keys():
            self.indexId=dIndex[self.index]

        path0 = down_path + self.indexId + 'stockList.pkl'
        path1 = down_path + self.indexId + 'stockData.pkl'
        with open(path0, 'rb') as f:
            self.stockList=pickle.load(f)
        with open(path1, 'rb') as f:
            self.stockData=pickle.load(f)
            self.stockData=self.stockData.sort_values(by=['securityId','dateTime'])

    def stockFun(self,data,stockFunParm={'MTMLen':90}):
        #对成分股x['securityId']在时间点x['dateTime']，使用成分股数据stockData得到持仓空仓变量（动量）
        if stockFunParm==None:
            stockFunParm ={'MTMLen':90}
        data['close_dif']=data['close'].diff(stockFunParm['MTMLen'])
        data['stockValue']=(data['close_dif']>0).astype(float)
        return data
    def _wavg(self,group,avg_name,weight_name):
        #加权平均数
        d=group[avg_name]
        w=group[weight_name]
        return (d*w).sum()/w.sum()
    def calculatePosition(self,stockFunParm=None,SmoothLen=130,Len=20):
        # 使用成分股数据得到持仓空仓变量，按流动市值加权，并两次平滑
        d=pd.DataFrame()
        stockData=self.stockData.groupby('securityId').apply(self.stockFun,stockFunParm)
        stockList=pd.merge(self.stockList,stockData[['securityId','dateTime','stockValue']],how='left',on=['securityId','dateTime'])
        d['value']=stockList.groupby('dateTime').apply(self._wavg,'stockValue','marketValue')*100
        d['valueSmooth']=d['value'].ewm(span=SmoothLen).mean()
        d['valueSmooth2'] = d['valueSmooth'].ewm(span=Len).mean()
        d['diff']=d['valueSmooth2'].diff()
        d['res'] = d.apply(lambda x: 1 if x['diff'] >0 else 0,axis=1)
        d = d.loc[self.begindate:self.enddate].dropna()
        self.position = d['res']
        self.sign=d['valueSmooth2']

class strategy_CMA(strategy_CMTM):
    def stockFun(self,data,stockFunParm={'MALen':150}):
        #对成分股x['securityId']在时间点x['dateTime']，使用成分股数据stockData得到持仓空仓变量（动量）
        if stockFunParm==None:
            stockFunParm ={'MALen':150}
        data['close_MA']=data['close'].rolling(stockFunParm['MALen']).mean()
        data['stockValue']=(data['close']>data['close_MA']).astype(float)
        return data
class strategy_Cv1(strategy_CMTM):
    def stockFun(self,d,stockFunParm={'L':50,'N':3,'Threshold':0.8,'n_short':5,'n_long':90}):
        #对成分股x['securityId']在时间点x['dateTime']，使用成分股数据stockData得到持仓空仓变量（动量）
        if stockFunParm == None:
            stockFunParm = {'L':50,'N':3,'Threshold':0.95,'n_short':5,'n_long':90}
        L,N,Threshold,n_short,n_long=stockFunParm['L'],stockFunParm['N'],stockFunParm['Threshold'],stockFunParm['n_short'],stockFunParm['n_long']
        d['price'] = d['close'].ewm(span=L).mean()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['v'] = (d['price'] / d['price'].shift(N)) * (d['short'] / d['long'])
        d['stockValue']=(d['v']>Threshold).astype(float)
        return d
class strategy_Cv2(strategy_CMTM):
    def stockFun(self,d,stockFunParm={'L':50,'N':3,'Threshold1':0.85,'Threshold2':1.15,'n_short':5,'n_long':90}):
        #对成分股x['securityId']在时间点x['dateTime']，使用成分股数据stockData得到持仓空仓变量（动量）
        if stockFunParm == None:
            stockFunParm = {'L':50,'N':3,'Threshold1':0.95,'Threshold2':1.15,'n_short':5,'n_long':90}
        L, N, Threshold1, Threshold2, n_short, n_long = stockFunParm['L'], stockFunParm['N'], stockFunParm[
            'Threshold1'], stockFunParm['Threshold2'], stockFunParm['n_short'], stockFunParm['n_long']
        d['price'] = d['close'].ewm(span=L).mean()
        d['short'] = d['volume'].ewm(span=n_short).mean()
        d['long'] = d['volume'].ewm(span=n_long).mean()
        d['value'] = (d['price'] / d['price'].shift(N)) * (d['short'] / d['long'])

        d['p5MA']=d['close'].rolling(5).mean()
        d['p90MA'] = d['close'].rolling(90).mean()
        d['Threshold']=d.apply(lambda x:Threshold1 if x['p5MA']>x['p90MA'] else Threshold2,axis=1)
        d['stockValue'] = d.apply(lambda x: 1.0 if x['value'] > x['Threshold'] else 0.0, axis=1)
        return d
