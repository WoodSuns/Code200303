# -*- coding: utf-8 -*-
import datetime
import numpy as np
import pandas as pd

begindate=datetime.datetime(2004,1,1)
enddate=datetime.datetime(2019,1,25)

opens=pd.read_excel('E:\\MarketTiming\\data\\open.xlsx',index_col=u'时间').replace(0,np.nan)
closes=pd.read_excel('E:\\MarketTiming\\data\\close.xlsx',index_col=u'时间').replace(0,np.nan)
volume=pd.read_excel('E:\\MarketTiming\\data\\volume.xlsx',index_col=u'时间').replace(0,np.nan)

class strategyDemo:
    def __init__(self,index,begindate=begindate,enddate=enddate,opens=opens,closes=closes,volume=volume):
        self.index=index
        self.begindate=begindate
        self.enddate=enddate
        self.position=pd.DataFrame()    #仓位数据
        self.netvalue=pd.DataFrame()    #策略和指数净值数据
        self.value=pd.Series()          #策略净值
        self.res=pd.Series()            #策略评价指标
        self.getdata(opens=opens, closes=closes, volume=volume)
    def getdata(self,opens,closes,volume):
        index=self.index
        data = pd.DataFrame()
        data['open'] = opens[index]
        data['close'] = closes[index]
        data['volume'] = volume[index]
        data = data.dropna()
        self.data = data.sort_index()  # 指数数据

    def calculateNetvalue(self):
        #根据策略生成的仓位数据，计算策略净值和指数净值
        position=self.position
        data=self.data
        netvalue = pd.DataFrame(columns=['indexvalue','netvalue'])
        netvalue.loc[position.index[0], 'netvalue'] = 1
        for i in range(1, len(position)):
            if position.loc[position.index[i - 1]] == 1:
                if i - 1 == 0 or position.loc[position.index[i - 2]] == 0:
                    # 多头开仓：以开盘价买入
                    netvalue.loc[position.index[i], 'netvalue'] = netvalue.loc[position.index[i - 1], 'netvalue'] \
                                                                  * data.loc[position.index[i], 'close'] \
                                                                  / data.loc[position.index[i], 'open']
                else:
                    # 多头持仓
                    netvalue.loc[position.index[i], 'netvalue'] = netvalue.loc[position.index[i - 1], 'netvalue'] \
                                                                  * data.loc[position.index[i], 'close'] \
                                                                  / data.loc[position.index[i - 1], 'close']
            else:
                if i - 1 > 0 and position.loc[position.index[i - 2]] == 1:
                    # 平仓：以开盘价卖出
                    netvalue.loc[position.index[i], 'netvalue'] = netvalue.loc[position.index[i - 1], 'netvalue'] \
                                                                  * data.loc[position.index[i], 'open'] \
                                                                  / data.loc[position.index[i - 1], 'close']
                else:
                    # 空仓
                    netvalue.loc[position.index[i], 'netvalue'] = netvalue.loc[position.index[i - 1], 'netvalue']

        netvalue['indexvalue'] = data.loc[netvalue.index, 'close'] / data.loc[netvalue.index[0], 'close']
        self.netvalue=netvalue
        self.value = netvalue['netvalue']

    def riskAnalysis(self):
        #根据策略净值计算风险收益指标
        value=self.value
        position=self.position
        rate = value.pct_change()
        Rate = value.iloc[-1] ** (242.0 / len(value)) - 1               # 年化收益
        Sigma = rate.std() * 242 ** 0.5                                 # 年化波动率
        Sharpe = (Rate - 0.03) / Sigma                                  # 夏普比率
        tem = (value.cummax() - value) / value.cummax()
        max_down = tem.max()                                            # 最大回撤
        max_down_end = tem.astype(float).idxmax()
        max_down_begin = value[:max_down_end].astype(float).idxmax()

        transaction = pd.DataFrame(columns=['begin', 'end', 'earn'])
        for i in range(len(position)):
            if position.loc[position.index[i]] == 1 and (i == 0 or position.loc[position.index[i - 1]] == 0):
                # 交易开始，即开仓前净值
                transaction.loc[len(transaction), 'begin'] = value.loc[position.index[i]]
            elif position.loc[position.index[i]] == 0 and i > 0 and position.loc[
                position.index[i - 1]] == 1 and i < len(position) - 1:
                # 交易结束，即平仓后净值
                transaction.loc[len(transaction) - 1, 'end'] = value.loc[position.index[i + 1]]
        if np.isnan(transaction.loc[len(transaction) - 1, 'end']):
            transaction.loc[len(transaction) - 1, 'end'] = value.loc[position.index[-1]]
        transaction['earn'] = transaction['end'] - transaction['begin']
        earn = transaction['earn']
        winProba = (earn > 0).mean()                                    # 胜率
        winLossRatio = -earn[earn > 0].sum()/earn[earn< 0].sum()        # 盈亏比
        num = len(earn)                                                 # 交易次数
        numPerYear = num / (len(value) / 242.0)                         # 每年交易次数
        days1 = position.sum()*1.0/ num                                 # 持仓周期
        days0 = (position.count() - position.sum())*1.0/ num            # 空仓周期


        rateMonth = value.resample('1M').last().pct_change()
        rateWeek = value.resample('1W').last().pct_change()
        winProbaMonth = (rateMonth > 0).mean()                          # 月度胜率
        winProbaWeek = (rateWeek > 0).mean()                            # 周度胜率

        name = [u'年化',u'最大回撤',u'最大回撤开始',u'最大回撤结束',u'总交易次数',u'每年交易次数',u'夏普',u'胜率',u'盈亏比',u'月度胜率',u'周度胜率',u'持仓周期',u'空仓周期']
        self.res=pd.DataFrame([[Rate,max_down,max_down_begin,max_down_end,num,numPerYear,Sharpe,winProba,winLossRatio,winProbaMonth,winProbaWeek,days1,days0]],columns=name)