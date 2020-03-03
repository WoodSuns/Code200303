# -*- coding: utf-8 -*-
import os
import pickle
import datetime
import pandas as pd
from datafeeds import DataFeeds
dataSource = DataFeeds()
AIndexConstituent= dataSource.getDataFeed('AIndexConstituent')
AShareQuotation = dataSource.getDataFeed('AShareQuotation')
AShareVars = dataSource.getDataFeed("AShareVars")

indexIds=['000016.SH','000300.SH','000905.SH','000906.SH']#上证50、沪深300、中证500、中证800:
begindate=datetime.datetime(2003,1,1)
enddate=datetime.datetime(2019,11,30)
down_path='E:\\MarketTiming\\data\\'


datelist = AShareQuotation.getAShareQuotation(securityIds=['000001.SH'],
                                              items=['close'],
                                              beginDateTime=begindate,
                                              endDateTime=enddate,
                                              frequency=86400)['dateTime']
tradeDates = [dt.to_pydatetime() for dt in datelist]

for index in indexIds:
        path0 = down_path + index + 'stockList.pkl'
        path1 = down_path + index + 'stockData.pkl'
        all_stock_list = AIndexConstituent.getAIndexConstituent(indexIds=[index], beginDateTime=begindate,
                                                             endDateTime=enddate)
        stockData=AShareQuotation.getAShareQuotation(securityIds=all_stock_list['securityId'].tolist(),
                                              items=['close','volume'],
                                              beginDateTime=begindate,
                                              endDateTime=enddate,
                                              frequency=86400,
                                              adjusted=1,
                                              adjustedDate=datetime.datetime(1970, 1, 1))       #所有出现过的成分股历史数据

        #删除指数出现前的日期
        dates=tradeDates[:]
        dates_remove=[]
        for dt in dates:
            l=AIndexConstituent.getAIndexConstituent(indexIds=[index], beginDateTime=dt,endDateTime=dt)
            if len(l)>0:
                break
            else:
                dates_remove.append(dt)
        for dt in dates_remove:
            dates.remove(dt)
        stockMarketValue=AShareVars.getAShareDayVars(securityIds=all_stock_list['securityId'].tolist(),
                                              items=['marketValue'],dateTimeList=dates)
        stockList=AIndexConstituent.getBatchAIndexConstituent(dateTimeList=dates,indexIds=[index])

        stockList =pd.merge(stockList,stockMarketValue,how='left',on=['securityId','dateTime']) #每天成分股及其流通市值

        print(index)
        with open(path0, 'wb') as f:
                pickle.dump(stockList, f)
        with open(path1, 'wb') as f:
            pickle.dump(stockData, f)
