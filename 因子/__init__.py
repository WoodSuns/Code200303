#!/usr/bin/python  
# -*- coding: utf-8 -*-  
#coding=utf-8
"""
#------------------------------------------------------------------------------
#----Python File Instruction---------------------------------------------------
#------------------------------------------------------------------------------
#
"""
import abc
import types
import datetime
import pandas as pd
import numpy as np
from sqlalchemy.types import CHAR
from datafeeds.utils import DateTimeForm, DBType
from datafeeds.utils.financeutil import SecurityIdForm
from datafeeds.utils.relationaldb import ConnectDB
from datafeeds.utils.nosqldb import ConnectNoSQLDB
from portmgr_Q.factor import missingvalue, outliers, standardization
from datafeeds import DataFeeds
import time
import os

class BaseFactor(object):    
    """
    #--------------------------------------------------------------------------
    #----Clsass Instruction----------------------------------------------------
    #--------------------------------------------------------------------------
    # This class is a base class uesed to caculate and get factor value.
    # This class should not be used directly.
    # 
    # The type of 'dateTime' can only be python DateTime.
    #
    # This class supports all kinds of factor, such as stock factor, fund factor,   
    # bond factor, future factor, option factor and so on. However, the factor 
    # must have 4 characteristics: 
    # 1) factorSymbol,
    # 2) dateTime,
    # 3) securityId,
    # 4) factorValue
    # And with 'factorSymbol', 'dateTime' and 'securityId', 'factorValue' can be
    # distinctly identified.
    #
    # This class is an abstract class and doesn't use any data source.
    """    
    __metaclass__ = abc.ABCMeta
    def __init__(self, factorSymbol, factorDirection=1, factorParameters={}):
        """
        #----------------------------------------------------------------------
        #----Attibutes related to factor itself--------------------------------
        #----------------------------------------------------------------------
        """
        # Set factorSymbol
        self.setFactorSymbol(factorSymbol)
        
        # Set factorParameters
        self.setFactorParameters(factorParameters)   
        #----------------------------------------------------------------------
        # type: Int;  meaning: -1 the smaller the better, 1 the larger the better
        if factorDirection  in [1, -1]:  
            self.__factorDirection = factorDirection
        else: 
            raise BaseException("Not support factorDirection:%s" % factorDirection) 
    
    def calculateFactorValue(self, dateTime):
        """
        #--------------------------------------------------------------------------
        #----Method Instruction----------------------------------------------------
        #--------------------------------------------------------------------------
        # This is the key method which needs to be rewritten to calculate factor value.
        # @param dateTime: type：python DateTime       
        # @return  DataFrame ['securityId','factorValue']
        """
        return pd.DataFrame()
        
    """
    #--------------------------------------------------------------------------
    #----Methods to set attributes---------------------------------------------
    #--------------------------------------------------------------------------
    """
    def setFactorSymbol(self, factorSymbol):
        #----------------------------------------------------------------------
        # factorSymbol: type: Str or Unicode
        if factorSymbol == None:
            raise BaseException(" [BaseFactor]'factorSymbol' can not be None, please set one") 
        elif type(factorSymbol) not in [types.StringType, types.UnicodeType]:
            raise BaseException(" [BaseFactor]'factorSymbol' must be string.") 
        elif len(factorSymbol) == 0:
            raise BaseException(" [BaseFactor]The lenght of 'factorSymbol' can not be 0") 
        else:
             self.__factorSymbol = factorSymbol
             
    def setFactorDirection(self, factorDirection):
        #----------------------------------------------------------------------
        # factorDirection: type: Int can only be 1 or -1
        if factorDirection  in [1, -1]:  
            self.__factorDirection = factorDirection
        else: 
            raise BaseException("Not support factorDirection:%s" % factorDirection)
            
    def setFactorParameters (self, factorParameters):
        #----------------------------------------------------------------------
        # factorParameters: Dict{key:value} = {paraname(Str):paravalue }
        if type(factorParameters) != types.DictType:
            raise BaseException(" [BaseFactor]'factorParameters' must be dict.")
        else:
            self.__factorParameters = factorParameters 
            
    """    
    #--------------------------------------------------------------------------
    #----Methods to get attributes---------------------------------------------
    #--------------------------------------------------------------------------
    """
    def getFactorSymbol(self):
        return self.__factorSymbol
    
    def getFactorDirection(self):
        return self.__factorDirection
        
    def getFactorParameters(self,item=None):
        if item == None:
            return self.__factorParameters   
        else:
            if item not in self.__factorParameters.keys():
                raise BaseException(" [BaseFactor]'__factorParameters' doesn't support item: %s."%item)
            else:
                return self.__factorParameters[item]  
            
        
class BaseFactorWithDB(BaseFactor):
    """
    #--------------------------------------------------------------------------
    #----Clsass Instruction----------------------------------------------------
    #--------------------------------------------------------------------------
    # This class inherits class 'BaseFactor', adding the methods to get factor 
    # value from database and update factor value to database.
    #
    # When it comes to storing data, database is important, this class supports
    # all kinds of database which uses 'SQL Clause' to get or delete data. It 
    # also suppots noSQL database such as MongoDB and InfluxDB.
    #
    # This class supports all kinds of factor, such as stock factor, fund factor,   
    # bond factor, future factor, option factor and so on. However, the factor 
    # must have 4 characteristics:
    # 1) factorSymbol,
    # 2) dateTime,
    # 3) securityId,
    # 4) factorValue
    # And with 'factorSymbol', 'dateTime' and 'securityId', 
    # 'factorValue' can be distinctly identified.
    #
    # This class doesn't use any data source.
    """    
    def __init__(self, factorSymbol=None, factorDirection=1, factorParameters={}):
        """
        #----------------------------------------------------------------------
        #----Initialization Instruction----------------------------------------
        #----------------------------------------------------------------------
        """  
        # Initialize super class. 
        super(BaseFactorWithDB,self).__init__(factorSymbol=factorSymbol, factorDirection=factorDirection, factorParameters=factorParameters)
             
        """
        #----------------------------------------------------------------------
        #----Attibutes related to database used to store factor value----------
        #----------------------------------------------------------------------
        """
        # Set factorStoreDB, __factorStoreDB is an instance
        self.__factorStoreDB = None
        # Set tableNameInDB
        self.__tableNameInDB = self.getFactorSymbol().lower()
        # Set tableVariableName
        self.__tableVariableName = {'dateTime':'tdate',
                                'securityId':'security_code',
                                'factorSymbol':'factor_symbol',
                                'factorValue':'factor_value',
                                'updataDateTime':'operation_date'}
        self.__dateTimeFormInDB = DateTimeForm.strDate
        self.__securityIdFormInDB = SecurityIdForm.defaultId
        self.__securityIdLengthInDB = 20
        """
        #----------------------------------------------------------------------
        #----Attibutes related to missing value, outliers and standardization--
        #----------------------------------------------------------------------
        """
        #----------------------------------------------------------------------
        # type: BaseMissingValue(a specific ont)
        # meaning: an initialized Class to process missing value
        self.__processMissingValue = missingvalue.DeleteMissingValue()
        #----------------------------------------------------------------------
        # type: BaseOutliers(a specific ont)
        # meaning: an initialized Class to process outliers
        self.__processOutliers = outliers.SigmaMethod()
        #----------------------------------------------------------------------
        # type: BaseStandardization(a specific ont)
        # meaning: an initialized Class to standardize data
        self.__standardization = standardization.ZScore()
    """   
    #--------------------------------------------------------------------------
    #----Methods related to get or calulate factor value ---------------------
    #--------------------------------------------------------------------------
    # The type of return data form these methods is 
    # data = {'dateTime':strdate or strdatetime,
    #         'securityId':List[instrument],
    #         'factorValue':List[number]}
    # note: This Dictionary type is easy to be transformed to DataFrame 
    # If there is no data, then return 'None'.
    """     
    def getFactorValueFromSQLDB(self, dateTimeList):
        
        # Step1 Check if the type of input parameter(s) is right.
        if type(dateTimeList) != types.ListType:
            raise BaseException(" [BaseFactorWithDB]'dateTimeList' must be list." )
        else:
            for dateTime in dateTimeList:
                if type(dateTime) != datetime.datetime:
                    raise BaseException(" [BaseFactorWithDB] item in 'dateTimeList' must be datetime.datetime." )
        dateTimeList = list(set(dateTimeList))
        # Step2 Get some vaariables about factor value in database.
        securityIdName = self.getTableVariableName('securityId') 
        dateTimeName = self.getTableVariableName('dateTime') 
        factorValueName = self.getTableVariableName('factorValue') 
        factorSymbolName = self.getTableVariableName('factorSymbol') 
        talbeInDB = self.getTableNameInDB() 
        factorSymbol = "'"+self.getFactorSymbol()+"'" 
        
        # Step3 Generate sql code.
        dbDateTimeList = ""
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            for dateTime in dateTimeList:
                if dateTime == dateTimeList[-1]:
                    dbDateTimeList += "'" + dateTime.strftime('%Y-%m-%d') + "'"
                else:
                    dbDateTimeList += "'" + dateTime.strftime('%Y-%m-%d') + "', "
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            for dateTime in dateTimeList:
                if dateTime == dateTimeList[-1]:
                    dbDateTimeList += "'" + dateTime.strftime('%Y-%m-%d %H:%M:%S') + "'"
                else:
                    dbDateTimeList += "'" + dateTime.strftime('%Y-%m-%d %H:%M:%S') + "', "
        elif self.getDateTimeFormInDB() == DateTimeForm.intDate:
            for dateTime in dateTimeList:
                if dateTime == dateTimeList[-1]:
                    dbDateTimeList += dateTime.strftime('%Y%m%d')
                else:
                    dbDateTimeList += dateTime.strftime('%Y%m%d') + ", "
        elif self.getDateTimeFormInDB() == DateTimeForm.strIntDate:
            for dateTime in dateTimeList:
                if dateTime == dateTimeList[-1]:
                    dbDateTimeList += "'" + dateTime.strftime('%Y%m%d') + "'"
                else:
                    dbDateTimeList += "'" + dateTime.strftime('%Y%m%d') + "', "        
        else:
            raise BaseException("[BaseFactorWithDB] Not support dateTimeForm in database when getting data from database:%s" % self.getDateTimeFormInDB())
            
        sqlCause = ("select " + dateTimeName + " as datetime, " + securityIdName + " as securityid, " + factorValueName + " as factorvalue from "
                + talbeInDB +" where " + dateTimeName + " in (" + dbDateTimeList +") and " + factorSymbolName + " = "+factorSymbol)
#        print sqlCause
        # Step4 Get date from database.
        data1 = self.getFactorStoreDB().getDataWithSqlClause(sqlCause)
        if len(data1) == 0:
            return pd.DataFrame()
        data1.rename(columns={"datetime":"dateTime", "securityid":"securityId", "factorvalue":"factorValue"}, inplace=True)
        data1['securityId'] = data1['securityId'].str.strip() 
        if self.getDateTimeFormInDB() in [DateTimeForm.strDate, DateTimeForm.strDateTime, DateTimeForm.strIntDate]:
            data1['dateTime'] = data1['dateTime'].astype(np.str)
            data1['dateTime'] = data1['dateTime'].str.strip()
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y-%m-%d"))
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y-%m-%d %H:%M:%S"))
        elif self.getDateTimeFormInDB() in [DateTimeForm.intDate , DateTimeForm.strIntDate]:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y%m%d"))
        
        # Step5 Check if get factor value of all dateTime in dateTimeList
        inputDateTime = set(dateTimeList)
        outputDateTime = set(data1.loc[:,'dateTime'])
        noValueDateTime = list(inputDateTime-outputDateTime)
        if len(noValueDateTime) != 0 :
            print("[BaseFactorWithDB] Can not get factor value of factor: %s in datetime: %s"%(self.getFactorSymbol(), str(noValueDateTime)))
        data = data1
        return data
        
    def getFactorValueFromMongoDB(self, dateTimeList):
        
        # Step1 Check if the type of input parameter(s) is right.
        if type(dateTimeList) != types.ListType:
            raise BaseException(" [BaseFactorWithDB]'dateTimeList' must be list." )
        else:
            for dateTime in dateTimeList:
                if type(dateTime) != datetime.datetime:
                    raise BaseException(" [BaseFactorWithDB] item in 'dateTimeList' must be datetime.datetime." )
        dateTimeList = list(set(dateTimeList))
        # Step2 Get some vaariables about factor value in database.
        tableVariableName = self.getTableVariableName()
        securityIdName = tableVariableName['securityId']       
        dateTimeName = tableVariableName['dateTime']
        factorValueName = tableVariableName['factorValue']
        factorSymbolName = tableVariableName['factorSymbol']
        talbeInDB = self.getTableNameInDB() 
        factorSymbol = self.getFactorSymbol()  
        
        # Step3 Generate dbDateTimeList.
        dbDateTimeList = []
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            for dateTime in dateTimeList:
                dbDateTimeList.append(dateTime.strftime('%Y-%m-%d'))
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            for dateTime in dateTimeList:
                dbDateTimeList.append(dateTime.strftime('%Y-%m-%d %H:%M:%S'))
        elif self.getDateTimeFormInDB() == DateTimeForm.intDate:
            for dateTime in dateTimeList:
                dbDateTimeList.append(int(dateTime.strftime('%Y%m%d')))
        elif self.getDateTimeFormInDB() == DateTimeForm.strIntDate:
            for dateTime in dateTimeList:
                dbDateTimeList.append(dateTime.strftime('%Y%m%d'))
        else:
            raise BaseException("[BaseFactorWithDB] Not support dateTimeForm in database when getting data from database:%s" % self.getDateTimeFormInDB())
        # Step4 Get date from database.
        db = self.getFactorStoreDB().connectDB()
        collection = db[talbeInDB]
        data0 = []
        for doc in collection.find({dateTimeName:{"$in":dbDateTimeList}, factorSymbolName:factorSymbol}):
            data0.append(doc)
        if len(data0) == 0:
            return pd.DataFrame()
        data1 = pd.DataFrame(data0).loc[:,[securityIdName, dateTimeName, factorValueName]]  
        data1.rename(columns={dateTimeName:"dateTime", securityIdName:"securityId", factorValueName:"factorValue"}, inplace=True)
        data1['securityId'] = data1['securityId'].str.strip() 
        if self.getDateTimeFormInDB() in [DateTimeForm.strDate, DateTimeForm.strDateTime, DateTimeForm.strIntDate]:
            data1['dateTime'] = data1['dateTime'].str.strip()
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y-%m-%d"))
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y-%m-%d %H:%M:%S"))
        elif self.getDateTimeFormInDB() in [DateTimeForm.intDate , DateTimeForm.strIntDate]:
            data1.loc[:,'dateTime'] = data1.loc[:,'dateTime'].apply(lambda x: datetime.datetime.strptime(str(x).strip(),"%Y%m%d"))
        
        # Step5 Check if get factor value of all dateTime in dateTimeList
        inputDateTime = set(dateTimeList)
        outputDateTime = set(data1.loc[:,'dateTime'])
        noValueDateTime = list(inputDateTime-outputDateTime)
        if len(noValueDateTime) != 0 :
            print("[BaseFactorWithDB] Can not get factor value of factor: %s in datetime: %s"%(self.getFactorSymbol(), str(noValueDateTime)))
        data = data1
        return data
    
    
    def getFactorValueFromDB(self, dateTimeList):

        if self.getFactorStoreDB()== None:
#            print "__________________________________11111111111111___________"
            return pd.DataFrame()
        if self.getFactorStoreDB().getDBType() in [DBType.sqlServer, DBType.oracle, DBType.postGreSql]:
            data = self.getFactorValueFromSQLDB(dateTimeList)
        elif self.getFactorStoreDB().getDBType() == DBType.mongoDB:
            data = self.getFactorValueFromMongoDB(dateTimeList)
        else:
            raise BaseException("[BaseFactorWithDB] Not support factorStoreDB in database when getting data from database:%s" % self.getFactorStoreDB())
        return data
        
    def getOrCalculateFactorValue(self,dateTime):
        """
        #----------------------------------------------------------------------
        # Since the factor value may be stored in database, so this method is uest to: 
        # 1) get factor value from database, 
        # 2) or caculate factor value.
        # @param dateTime: type：python DateTime       
        # @return  DataFrame ['dateTime','securityId','factorValue']
        """
        
        # step0 Check if the type of input parameter(s) is right.
        if dateTime == None:
            raise BaseException("Need to set a dateTime." )
        # step1 Secondly, get data form database. If has then return, ontherwise run step2
        data1 = self.getFactorValueFromDB(dateTimeList=[dateTime])
        if len(data1) != 0:
            return data1
        # step2 Finally, caculate factor value.
        data2 = self.calculateFactorValue(dateTime)
        if len(data2) == 0 :
            return pd.DataFrame()
        else:
            data2.loc[:,"dateTime"] = dateTime
            return data2          
    
    def getStandardizedFactorValue(self, dateTime):
        # step1 Get and transform factor value to be the correct type.
        data1 =  self.getOrCalculateFactorValue(dateTime)
        if len(data1) == 0:
            return pd.DataFrame()
        data2 = dict(zip(list(data1.loc[:,'securityId']),list(data1.loc[:,'factorValue'])))
        # step2 Process missing value
        data3 = self.getProcessMissingValue().process(data2)
        # step3 Process outliers.
        data4 = self.getProcessOutliers().process(data3)
        
        # steep4 Standardize data.
        data5 = self.getStandardization().process(data4, self.getFactorDirection())
        if len(data5) == 0 :
            return pd.DataFrame()
        else:
            data = pd.DataFrame({'dateTime':dateTime,'securityId':data5.keys(), 'factorValue':data5.values()}) 
            return data 
    
    """
    #--------------------------------------------------------------------------
    #----Methods related to update factor table to database--------------------
    #--------------------------------------------------------------------------
    """
    def deleteFactorValueInSQLDB(self, dateTime):
        
        # step1 Check if database and table exist. If not return None.
        if self.getFactorStoreDB() == None or self.getFactorStoreDB().hasTable(self.__tableNameInDB) == False:
            raise BaseException("[BaseFactorWithDB]No database or table." )
        # step2 Check if the type of input parameter(s) is right.
        if dateTime == None:
            raise BaseException("[BaseFactorWithDB]Need to set a dateTime." )
        elif type(dateTime) != datetime.datetime:
            raise BaseException("[BaseFactorWithDB]Not support dateTime:%s" % dateTime)
        strDate = dateTime.strftime('%Y-%m-%d')
        strDateTime = dateTime.strftime('%Y-%m-%d %H:%M:%S')
        # step3 Transform the form of dateTime.
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            dbDateTime = "'"+strDate+"'"
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            dbDateTime = "'"+strDateTime+"'"
        elif self.getDateTimeFormInDB() == DateTimeForm.intDate:
            dbDateTime = strDate[0:4]+strDate[5:7]+strDate[8:10]
        elif self.getDateTimeFormInDB() == DateTimeForm.strIntDate:
            dbDateTime = "'"+strDate[0:4]+strDate[5:7]+strDate[8:10]+"'"
        else:
            raise BaseException("[BaseFactorWithDB]Not support dateTimeForm in database when deleting data from database:%s" % self.getDateTimeFormInDB())
        # step4 Delete date in database.
        dateTimeName = self.getTableVariableName()['dateTime']
        factorSymbolName = self.getTableVariableName() ['factorSymbol'] 
        talbeInDB = self.getTableNameInDB()
        factorSymbol = "'"+self.getFactorSymbol()+"'"
        # note； Variable names in sql clause(oracle) may not case-sensative, 
        # so all variable names in sql clause are low_case, such as 'securityid' not 'securityId'. 
        sqlCause = ("delete from " + talbeInDB +" where " + dateTimeName + " = " + dbDateTime +" and " + factorSymbolName + " = "+factorSymbol)
        self.getFactorStoreDB().deleteDataWithSqlClause(sqlCause)
    
    def deleteFactorValueInMongoDB(self, dateTime):
        # step1 Check if database and table exist. If not return None.
        if self.getFactorStoreDB() == None:
            raise BaseException("[BaseFactorWithDB]No database or table." )
        # step2 Check if the type of input parameter(s) is right.
        if dateTime == None:
            raise BaseException("[BaseFactorWithDB]Need to set a dateTime." )
        elif type(dateTime) != datetime.datetime:
            raise BaseException("[BaseFactorWithDB]Not support dateTime:%s" % dateTime)
        strDate = dateTime.strftime('%Y-%m-%d')
        strDateTime = dateTime.strftime('%Y-%m-%d %H:%M:%S')
        # step3 Transform the form of dateTime.
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            dbDateTime = strDate
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            dbDateTime = strDateTime
        elif self.getDateTimeFormInDB() == DateTimeForm.strIntDate:
            dbDateTime = strDate[0:4]+strDate[5:7]+strDate[8:10]
        elif self.getDateTimeFormInDB() == DateTimeForm.intDate:
            dbDateTime = int(strDate[0:4]+strDate[5:7]+strDate[8:10])
        else:
            raise BaseException("[BaseFactorWithDB]Not support dateTimeForm in database when deleting data from database:%s" % self.getDateTimeFormInDB())    
        # step4 Delete date in database.
        dateTimeName = self.getTableVariableName()['dateTime']
        factorSymbolName = self.getTableVariableName() ['factorSymbol'] 
        talbeInDB = self.getTableNameInDB()
        factorSymbol = self.getFactorSymbol() 
        db = self.getFactorStoreDB().connectDB()
        collection = db[talbeInDB]
        collection.delete_many({dateTimeName:dbDateTime, factorSymbolName:factorSymbol})
        
    def deleteFactorValueInDB(self, dateTime): 
        if self.getFactorStoreDB().getDBType() in [DBType.sqlServer, DBType.oracle, DBType.postGreSql]:
            self.deleteFactorValueInSQLDB(dateTime)
        elif self.getFactorStoreDB().getDBType() == DBType.mongoDB:
            self.deleteFactorValueInMongoDB(dateTime)
        else:
            raise BaseException("[BaseFactorWithDB] Not support factorStoreDB in database when deleting data from database:%s" % self.getFactorStoreDB())
        
        
    def updateFactorTableToDB(self, dateTime, dataNumber=None, maxNullRatio=1.0, reUpdate = False):
        """ 
        #----------------------------------------------------------------------
        #----Parameters:-------------------------------------------------------
        # dateTime: type: strdatetime or strdate(Str)
        # dataNumber: type: Int; meaning: the number of instrment in a specific dateTime
        # maxNullRatio: type: float; 
        #               meaning: the maximun ratio of the number of null factor value divdided by dataNumber
        # reUpdate: type: Bool; 
        #           meaning: if true, delete old data and update new data even the data in dateTime has been updated
        #
        #---- Table structure in database:-------------------------------------
        # 1 Default variable names in '__tableNameInDB'
        # tableVariableName = {'dateTime':'tdate',
        #                     'securityId':'security_code',
        #                     'securityName':None,
        #                     'factorSymbol':'factor_symbol',
        #                     'factorValue':'factor_value',
        #                     'updataDateTime':'operation_date'}
        # In default, there is no variable securityName.
        #
        # 2  Default type of variables in table 
        # dataTime: type: strdate, strdatetime or strintdate(Str or Unicode)  or intdate
        # securityId: type: instrument(Str or Unicode)
        # factorSymbol: type: Str or Unicode
        # factorValue: type: Number
        # operationDateTime: type: strdatetime(Str or Unicode)
        # note: only the type of dataTime can be changed. and the type of dataTime must be
        # DateTimeForm.strdate, DateTimeForm.strdatetime, DateTimeForm.intdate or DateTimeForm.strintdate.
        # It is recommended to use DateTimeForm.strdate or DateTimeForm.strdatetime
        #
        # 3 Default table example
        # tdate       security_code  factor_symbol  factor_value  operation_date
        # 2015-12-11  SH600000       EP             0.075         2016-10-14 19:30:24
        #
        """
        # step0 Initialize the logs.
        logs = {'datetime':dateTime.strftime('%Y-%m-%d %H:%M:%S'), 
                'factor_symbol':self.getFactorSymbol(), 
                'update_log': [],
                'update_status' : [], 
                'operation_time': []}
        # step1 Check if the type of input parameter(s) is right.
        # Check the type of dateTime
        if dateTime == None:
            logs['update_log'].append("Need to set a dateTime.")
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        if type(dateTime) != datetime.datetime:
            logs['update_log'].append("Not support dateTime:%s" % dateTime)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs             
        # check the type of dataNumber
        if dataNumber != None and type(dataNumber) != types.IntType:
            logs['update_log'].append("Not support dataNumber:%s" % dataNumber)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        elif dataNumber != None and dataNumber <= 0:
            logs['update_log'].append("Not support dataNumber:%s" % dataNumber)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        # check the type of  maxNullRatio
        if maxNullRatio != 0 and maxNullRatio != 1 and  type(maxNullRatio) != types.FloatType:  
            logs['update_log'].append("Not support maxNullRatio:%s" % maxNullRatio)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        elif maxNullRatio < 0 or maxNullRatio > 1:
            logs['update_log'].append("Not support maxNullRatio:%s" % maxNullRatio)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        # check the type of  reUpdate    
        if type(reUpdate) != types.BooleanType:  
            logs['update_log'].append("Not support reUpdate:%s" % reUpdate)
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs    
        logs['update_log'].append("Start running updating program.")
        logs['update_status'].append("Start Updating")
        logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # step2 Check the database used to store factor value, if there is no database, then return, otherwise, continue.
        if self.getFactorStoreDB() == None:
            logs['update_log'].append("There is no database used to store factor value. Please set it with method setFactorStoreDB.")
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        # step3 Transform the form of dateTime.
        strDate = dateTime.strftime('%Y-%m-%d')
        strDateTime = dateTime.strftime('%Y-%m-%d %H:%M:%S')   
        if self.getDateTimeFormInDB() == DateTimeForm.strDate:
            dbDateTime = strDate
        elif self.getDateTimeFormInDB() == DateTimeForm.strDateTime:
            dbDateTime = strDateTime
        elif self.getDateTimeFormInDB() == DateTimeForm.strIntDate:
            dbDateTime = strDate[0:4]+strDate[5:7]+strDate[8:10]
        elif self.getDateTimeFormInDB() == DateTimeForm.intDate:
            dbDateTime = int(strDate[0:4]+strDate[5:7]+strDate[8:10])
        else:
            logs['update_log'].append("Not supported dbDateTimeFrom in database when updating data to database: %s" %self.getDateTimeFormInDB() )
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        # step4 Check if the database has table '__tableNameInDB'.
        if self.getFactorStoreDB().getDBType() in [DBType.sqlServer, DBType.oracle, DBType.postGreSql]:
            hasTable = self.getFactorStoreDB().hasTable(self.getTableNameInDB())
        elif self.getFactorStoreDB().getDBType() == DBType.mongoDB:
            hasTable = True
        else:
            raise BaseException("[BaseFactorWithDB] Not support factorStoreDB in database when updating data to database:%s" % self.getFactorStoreDB())
        if hasTable == True:
        # step5 Check if the data in dateTime has been updated, if has, check if delete and reupdate. 
            data1 = self.getFactorValueFromDB([dateTime])
            # case: table has data of dateTime
            if len(data1) != 0: 
                # case: table has data but reupdating is forced to run.
                if reUpdate == True:
                    logs['update_log'].append("The data of %s has been updated, but reupdating is forced to run.\
                                              Delete the data and reupdate ." % dbDateTime)
                    logs['update_status'].append("Reupdating...")
                    logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    self.deleteFactorValueInDB(dateTime)
                # case: table has data but reupdating is not forced to run, and the dataNumber is not None and
                #  dataNumber is not the length of data1['factorValue']. 
                elif reUpdate == False and (dataNumber != None and dataNumber != len(data1['factorValue'])) :
                    logs['update_log'].append("The data of %s has been updated, but the number of data is\
                                               not right. Delete the data and reupdate." % dbDateTime)
                    logs['update_status'].append("Reupdating...")
                    logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    self.deleteFactorValueInDB(dateTime)
                # case: table has data but reupdating is not forced to run, and the dataNumber is None or
                #  dataNumber is the length of data1['factorValue']
                elif reUpdate == False and (dataNumber == None or dataNumber == len(data1['factorValue'])) :
                    logs['update_log'].append("The data of %s has been updated." % dbDateTime)
                    logs['update_status'].append("Updated")
                    logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
                    return logs
            # case has table but no data
            else:
                logs['update_log'].append("The data of %s has not been updated(no data), update it now." % dbDateTime)
                logs['update_status'].append("Updating...")
                logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # case: no table 
        else:
            logs['update_log'].append("The data of %s has not been updated(no table), update it now." % dbDateTime)
            logs['update_status'].append("Updating...")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))

        # step6 Caculate factorvalue
        data2 = self.calculateFactorValue(dateTime)
        if len(data2) == 0:
            logs['update_log'].append("Caculating factor value fails." )
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        elif len(data2) != 0:
            logs['update_log'].append("Caculating factor value succeeds.Check the factor before update to database." )
            logs['update_status'].append("Updating...")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        # step7 Check the the number of data value if needed.
        if dataNumber !=None and dataNumber != len(data2):
            logs['update_log'].append("The number of data is " + str(len(data2)) + ", not the set dataNumber " + str(dataNumber) + "."  )
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
        # step8 Check the null ratio of factor value
        nullFactorVluae = len(data2[pd.isnull(data2["factorValue"])])
        if nullFactorVluae >= len(data2)*maxNullRatio:
            logs['update_log'].append("The null factor value is too much. The number of data is " + str(len(data2.keys())) 
                                        + ", null factor value is " + str(nullFactorVluae) + "."  )
            logs['update_status'].append("Not Updated")
            logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            return logs
            
        # step9 Form the input table    
        data3 = data2
        tableVariableName = self.getTableVariableName()
        securityIdName = tableVariableName['securityId']       
        dateTimeName = tableVariableName['dateTime']
        factorValueName = tableVariableName['factorValue']
        factorSymbolName = tableVariableName['factorSymbol']
        updataDateTimeName = tableVariableName['updataDateTime']
        
        factorSymbol = self.getFactorSymbol()  
        data3.rename(columns={"factorValue":factorValueName, "securityId":securityIdName}, inplace=True)
        data3.loc[:,dateTimeName] = dbDateTime
        data3.loc[:,factorSymbolName] = factorSymbol
        data3.loc[:,updataDateTimeName] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        logs['update_log'].append("Everything is OK, update factor value to database."  )
        logs['update_status'].append("Updating...")
        logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            
        # step10 Update factor value to database.
        data = data3
        talbeInDB = self.getTableNameInDB() 
        if self.getFactorStoreDB().getDBType() in [DBType.sqlServer, DBType.oracle, DBType.postGreSql]:
            if self.getDateTimeFormInDB() == DateTimeForm.intDate:
                dtype=({securityIdName:CHAR(self.getSecurityIdLengthInDB()),
                        factorSymbolName:CHAR(len(self.getFactorSymbol())), updataDateTimeName:CHAR(20)})
            else:
                dtype=({dateTimeName:CHAR(20), securityIdName:CHAR(self.getSecurityIdLengthInDB()),
                        factorSymbolName:CHAR(len(self.getFactorSymbol())), updataDateTimeName:CHAR(20)})
            self.getFactorStoreDB().updateTableToDB(tableName=data, tableNameInDB=talbeInDB, dtype=dtype)
        elif self.getFactorStoreDB().getDBType() == DBType.mongoDB:
            self.getFactorStoreDB().updateTableToDB(tableName=data, tableNameInDB=talbeInDB)        
        logs['update_log'].append("The data of %s is updated normally."  %dbDateTime )
        logs['update_status'].append("Updated")
        logs['operation_time'].append(datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
        return logs
    """
    #--------------------------------------------------------------------------
    #----Methods to set attributes---------------------------------------------
    #--------------------------------------------------------------------------
    """    
    def setFactorStoreDB(self, factorStoreDB):
        #----------------------------------------------------------------------
        # factorStoreDB: type: ConnectDB(a specific one)

        if isinstance(factorStoreDB, ConnectDB) or factorStoreDB == None or isinstance(factorStoreDB, ConnectNoSQLDB):
#            print "__________iiii______"
            self.__factorStoreDB = factorStoreDB
        else:
            raise BaseException("[BaseFactorWithDB]'__factorStoreDB' doesn't support factorStoreDB:%s" % factorStoreDB) 
            
    def setTableNameInDB(self, tableNameInDB=None):
        #----------------------------------------------------------------------
        # setTableNameInDB: type: Str or Unicode
        if tableNameInDB == None:
            self.__tableNameInDB = self.__factorSymbol.lower()
        elif  type(tableNameInDB) in [types.StringType, types.UnicodeType]:
            self.__tableNameInDB = tableNameInDB.lower()
        else:
            raise BaseException(" [BaseFactorWithDB]'__tableNameInDB' doesn't support tableNameInDB:%s" % tableNameInDB) 
            
    def setTableVariableName(self, dateTimeName=None, securityIdName=None,  
                             factorSymbolName=None, factorValueName=None, updataDateTimeName=None):
        """
        #----------------------------------------------------------------------
        # 
        # The type of all input parameters is Str or Unicode or None.
        # Default value:
        #     {'dateTime':'tdate',
        #      'securityId':'security_code',
        #      'factorSymbol':'factor_symbol',
        #      'factorValue':'factor_value',
        #      'updataDateTime':'operation_date'}    
        # type of the Default value: Dict{key:value} = {Str or Unicode : Str Or Unicode or None }
        #
        # Since some databases doesn't support Upercase name of variable,
        # all values in '__tableVariableName' are set to be lowercase.
        # 
        # Usually, we use the default __tableVariableName and don't need to reset.
        """
        typeAccepted = [types.NoneType, types.StringType, types.UnicodeType]
        if type(dateTimeName) not in typeAccepted:
            raise BaseException("[BaseFactorWithDB]'dateTimeName' must be string.")
        elif type(securityIdName) not in typeAccepted:
            raise BaseException("[BaseFactorWithDB]'securityIdName' must be string.")
        elif type(factorSymbolName) not in typeAccepted:
            raise BaseException("[BaseFactorWithDB]'factorSymbolName' must be string.")
        elif type(factorValueName) not in typeAccepted:
            raise BaseException("[BaseFactorWithDB]'factorValueName' must be string.")
        elif type(updataDateTimeName) not in typeAccepted:
            raise BaseException("[BaseFactorWithDB]'updataDateTimeName' must be string.") 
        
        if dateTimeName != None:
            self.__tableVariableName['dateTime'] = dateTimeName.lower()
        if securityIdName != None:
            self.__tableVariableName['securityId'] = securityIdName.lower()
        if factorSymbolName != None:
            self.__tableVariableName['factorSymbol'] = factorSymbolName.lower()
        if factorValueName != None:
            self.__tableVariableName['factorValue'] = factorValueName.lower()
        if updataDateTimeName != None:
            self.__tableVariableName['updataDateTime'] = updataDateTimeName.lower()
            
            
    def setDateTimeFormInDB(self,dateTimeForm):
        #----------------------------------------------------------------------
        # dateTimeForm: type: Int 
        # Note: It is recommended to ues the globle variable in Class DateTimeForm 
        #       to set '__dateTimeFormInDB'.
        if dateTimeForm not in [DateTimeForm.strDate, DateTimeForm.strDateTime, DateTimeForm.intDate, DateTimeForm.strIntDate]:
            raise BaseException("[BaseFactorWithDB] '__dateTimeFormInDB' doesn't support %s" % dateTimeForm)
        else:
            self.__dateTimeFormInDB = dateTimeForm 
            
    def setSecurityIdFormInDB(self,securityIdForm):
        #----------------------------------------------------------------------
        # dateTimeForm: type: Int 
        # Note: It is recommended to ues the globle variable in Class SecurityIdForm 
        #       to set '__dateTimeFormInDB'.
        if securityIdForm not in SecurityIdForm.securityIdFormScope:
            raise BaseException("[BaseFactorWithDB] '__securityIdFormInDB' doesn't support %s"% securityIdForm)
        else:
            self.__securityIdFormInDB = securityIdForm         
            
            
    def setSecurityIdLengthInDB(self,securityIdLength):
        #----------------------------------------------------------------------
        # securityIdLength: type: Int (greater than 0)
        if type(securityIdLength) != types.IntType and securityIdLength > 0:
            self.__securityIdLengthInDB = securityIdLength
        else:
            raise BaseException("[BaseFactorWithDB] '__securityIdLengthInDB' doesn't support %s"% securityIdLength)   
    
    def setProcessMissingValue(self, missingValueInstance):
        #----------------------------------------------------------------------
        # missingValueInstance: type: missingvalue.BaseMissingValue(a specific one)
        if isinstance(missingValueInstance, missingvalue.BaseMissingValue):
            self.__processMissingValue = missingValueInstance
        else:
            raise BaseException("Not support missingValueInstance:%s" % missingValueInstance)
    def setProcessOutliers(self, outliersInstacne):
        #----------------------------------------------------------------------
        # outliersInstacne: type: outliers.BaseOutliers(a specific one)
        if isinstance(outliersInstacne, outliers.BaseOutliers):
            self.__processOutliers = outliersInstacne
        else:
            raise BaseException("Not support outliersInstacne:%s" % outliersInstacne)
    def setStandardization(self, standardizationInstance):
        #----------------------------------------------------------------------
        # standardizationInstance: type: standardization.BaseStandardization(a specific one)
        if isinstance(standardizationInstance, standardization.BaseStandardization):
            self.__standardization = standardizationInstance
        else:
            raise BaseException("Not support standardizationInstance:%s" % standardizationInstance)
    """
    #--------------------------------------------------------------------------
    #----Methods to get attributes---------------------------------------------
    #--------------------------------------------------------------------------
    """        
    def getFactorStoreDB(self):
        return self.__factorStoreDB
        
    def getTableNameInDB(self):
        return self.__tableNameInDB
        
    def getTableVariableName(self,item=None):
        if item == None:
            return self.__tableVariableName
        else:
            if item not in self.__tableVariableName.keys():
                raise BaseException("[BaseFactorWithDB] '__tableVariableName' doesn't support item: %s"% item)
            else:
                return self.__tableVariableName[item]
    
    def getDateTimeFormInDB(self):
        return self.__dateTimeFormInDB
    
    def getSecurityIdFormInDB(self):
        return self.__securityIdFormInDB
        
    def getSecurityIdLengthInDB(self):
        return self.__securityIdLengthInDB     
        
    def getProcessMissingValue(self):
        return self.__processMissingValue
    
    def getProcessOutliers(self):
        return self.__processOutliers
        
    def getStandardization(self):
        return self.__standardization   


class TradeFactorDemo(BaseFactorWithDB):
    # initial class
    """
    #--------------------------------------------------------------------------
    #----Class Instruction----------------------------------------------------
    #--------------------------------------------------------------------------
    """    
    def __init__(self, lagTradeDays=None,frequency=86400,validTradingDayRatio=0.7,items=None,varitems=None,
                 factorSymbol=None,  factorDirection=1, fetchDataOnOld=True):
        BaseFactorWithDB.__init__(self, factorSymbol=factorSymbol, factorDirection=factorDirection)
        self.items = items
        self.varsitems = varitems
        self.FREQUENCY = frequency
        self.stocklist = None
        self.stockdata = pd.DataFrame()
        self.stockvars = pd.DataFrame()
        self.offset1 = lagTradeDays
        self.validTradingDayRatio = validTradingDayRatio
        if self.offset1 is None:
            self.maxoffset = None
        else:
            self.maxoffset = self.offset1*max(14400/self.FREQUENCY, 1)
        self.maxoffset_day = self.offset1
        self.fetchDataOnOld = fetchDataOnOld
        self.__database = DataFeeds()
        self.__tcalendar = self.__database.getDataFeed("AShareCalendar")
        self.__stockcode = self.__database.getDataFeed("AShareCodes")
        self.__stockdata = self.__database.getDataFeed("AShareQuotation")
        self.__stockvars = self.__database.getDataFeed("AShareVars")
        
    def getDataSource(self):
        return self.__database

    def _getStockCode(self, beginDateTime=None, endDateTime=None):
        return self.__stockcode.getAShareCodes(beginDateTime, endDateTime)

    def _getStockData(self, securityIds, items, beginDateTime=None, endDatetime=None, frequency= None, adjusted=1,
                      adjustedDate=datetime.datetime(1970, 1, 1)):
        endDatetime = endDatetime.replace(hour = 15)
        frequency = self.FREQUENCY
        return self.__stockdata.getAShareQuotation(securityIds, items, frequency, beginDateTime, endDatetime, adjusted,
                                                   adjustedDate)
        
    def _getStockVars(self,securityIds, dateTimeList, items = []):
        if items == []:
            return pd.DataFrame()
        return self.__stockvars.getAShareDayVars(dateTimeList, securityIds, items)

    def _getTradeDate(self, beginDateTime=None, endDateTime=None):
        endDateTime = endDateTime.replace(hour = 15)
        return self.__stockdata.getAShareQuotation(['000001.SH'], ['close'], self.FREQUENCY, 
                                                   beginDateTime, endDateTime, 1, datetime.datetime(1970, 1, 1)
                                                   )['dateTime'].to_frame()
        
    def _getVarsDate(self, beginDateTime=None, endDateTime=None):
        endDateTime = endDateTime.replace(hour = 15)
        return self.__stockdata.getAShareQuotation(['000001.SH'], ['close'], 86400, 
                                                   beginDateTime, endDateTime, 1, datetime.datetime(1970, 1, 1)
                                                   )['dateTime'].to_frame()

    def getData(self, dateTime=None):
        if not dateTime:
            dateTime = datetime.datetime.now()
        if not isinstance(dateTime, datetime.datetime):
            raise BaseException("[getData] 'dateTime'must be datetime.datetime")
        if self.items is None or self.maxoffset is None:
            return
        try:
            timedelta = datetime.timedelta(days=self.offset1 *2 +20)
            tradedates = self._getTradeDate(dateTime - timedelta, dateTime)
            self.beginDateTime = tradedates.iloc[-self.maxoffset, 0].to_pydatetime()
            self.endDateTime = tradedates.iloc[-1, 0].to_pydatetime()
            self.stocklist = self._getStockCode(self.endDateTime, self.endDateTime)
            if self.fetchDataOnOld and not self.stockdata.empty:
                oldstockset = pd.Index(self.stockdata.index.get_level_values("securityId").unique())
                newstockset = self.stocklist.set_index("securityId").index
                # 去除新代码列表中没有的代码
                stock_drop = oldstockset.difference(newstockset)

                if not pd.Series(stock_drop).empty:
                    self.stockdata.drop(labels=stock_drop, level="securityId", inplace=True)
                # 取旧代码新数据，
                stock_intersection = newstockset.intersection(oldstockset)
                if not pd.Series(stock_intersection).empty:
                    olddateset = pd.Index(self.stockdata.index.get_level_values("dateTime").unique())
                    newdateset = tradedates.iloc[-self.maxoffset:].set_index("dateTime").index
                    date_drop = olddateset.difference(newdateset)
                    
                    # 去除新日期中没有的部分
                    if not pd.Series(date_drop).empty:
                        self.stockdata.drop(labels=date_drop, level="dateTime", inplace=True)
                    # 更新旧日期中没有的部分
                    date_new = newdateset.difference(olddateset)
                    if not pd.Series(date_new).empty:
                        tmp_begin = date_new[0].to_pydatetime()
                        tmp_end = date_new[-1].to_pydatetime()
                        update_data = self._getStockData(stock_intersection.tolist(), self.items, tmp_begin, tmp_end)
                        update_data.set_index(['dateTime', 'securityId'], inplace=True)
                        self.stockdata = self.stockdata.append(update_data)
                # 取新增代码数据
                stock_new = newstockset.difference(oldstockset)
                if not pd.Series(stock_new).empty:
                    new_stock_data = self._getStockData(stock_new.tolist(), self.items, self.beginDateTime,
                                                        self.endDateTime)
                    new_stock_data.set_index(['dateTime', 'securityId'], inplace=True)
                    self.stockdata = self.stockdata.append(new_stock_data)
                self.stockdata.sort_index(inplace=True)
            else:
                self.stockdata = self._getStockData(self.stocklist['securityId'].tolist(), self.items,
                                                    self.beginDateTime, self.endDateTime)
                self.stockdata.set_index(['dateTime', 'securityId'], inplace=True)
        except Exception as err:
            print(err)
            raise
            
    def getVars(self, dateTime=None):
        if not dateTime:
            dateTime = datetime.datetime.now()
        if not isinstance(dateTime, datetime.datetime):
            raise BaseException("[getData] 'dateTime'must be datetime.datetime")
        if self.varsitems is None or self.maxoffset_day is None:
            return
        try:
            timedelta = datetime.timedelta(days=self.offset1 *2 +20)
            tradedates = self._getVarsDate(dateTime - timedelta, dateTime)
            self.beginDateTime = tradedates.iloc[-self.maxoffset_day, 0].to_pydatetime()
            self.endDateTime = tradedates.iloc[-1, 0].to_pydatetime()
            tradedates2 = tradedates.loc[tradedates['dateTime']>=self.beginDateTime]
            tradedates2 = [date.to_pydatetime() for date in tradedates2['dateTime']]
            self.stocklist = self._getStockCode(self.endDateTime, self.endDateTime)
            if self.fetchDataOnOld and not self.stockvars.empty:
                oldstockset = pd.Index(self.stockvars.index.get_level_values("securityId").unique())
                newstockset = self.stocklist.set_index("securityId").index
                # 去除新代码列表中没有的代码
                stock_drop = oldstockset.difference(newstockset)
                if not pd.Series(stock_drop).empty:
                    self.stockvars.drop(labels=stock_drop, level="securityId", inplace=True)
                # 取旧代码新数据，
                stock_intersection = newstockset.intersection(oldstockset)
                if not pd.Series(stock_intersection).empty:
                    olddateset = pd.Index(self.stockvars.index.get_level_values("dateTime").unique())
                    newdateset = tradedates.iloc[-self.maxoffset_day:].set_index("dateTime").index
                    date_drop = olddateset.difference(newdateset)
                    
                    # 去除新日期中没有的部分
                    if not pd.Series(date_drop).empty:
                        self.stockvars.drop(labels=date_drop, level="dateTime", inplace=True)
                    # 更新旧日期中没有的部分
                    date_new = newdateset.difference(olddateset)
                    if not pd.Series(date_new).empty:
                        date_new = [date.to_pydatetime() for date in date_new]
                        update_data = self._getStockVars(stock_intersection.tolist(), date_new, self.varsitems)
                        update_data.set_index(['dateTime', 'securityId'], inplace=True)
                        self.stockvars = self.stockvars.append(update_data)
                # 取新增代码数据
                stock_new = newstockset.difference(oldstockset)
                if not pd.Series(stock_new).empty:
                    new_stock_data = self._getStockVars(stock_new.tolist(), tradedates2, self.varsitems)
                    new_stock_data.set_index(['dateTime', 'securityId'], inplace=True)
                    self.stockvars = self.stockvars.append(new_stock_data)
                self.stockvars.sort_index(inplace=True)
            else:
                self.stockvars = self._getStockVars(self.stocklist['securityId'].tolist(), tradedates2, self.varsitems)
                self.stockvars.set_index(['dateTime', 'securityId'], inplace=True)
        except Exception as err:
            print(err)
            raise  
            

class HTradeFactorDemo(TradeFactorDemo):
    # initial class
    """
    #--------------------------------------------------------------------------
    #----Class Instruction----------------------------------------------------
    #--------------------------------------------------------------------------
    高频因子的类:
        高频因子一般不会用到跨日的数据
        故首先算出每日的日度因子，再算月度因子（也可以是周频或者带衰减的日度因子）
        1，日度因子：
            根据频率导入该天的本地数据（dailyData）或者线上数据
            计算日度因子
            保存日度因子（dailyFactor）
        2，月度因子：
            导入lagdays区间的日度因子
            统计方法：平均值、标准差、平均值/标准差、累乘
        
        其中:dailydata是DataFrame，索引为'dateTime', 'securityId'；列为items。
             dailyfactor是Series,索引为'date', 'securityId'。
    """    
    def __init__(self,path,standard=False,how='mean',lagTradeDays=None,frequency=60,
                 validTradingDayRatio=0.7,items=None,varitems=None,factorSymbol=None,  factorDirection=1, 
                 fetchDataOnOld=False,dailyFactorSymbol=None):
        s_str='_s' if standard else '' #是否对日度因子横截面标准化
        factorSymbol=factorSymbol+'_'+str(lagTradeDays)+how+s_str
        TradeFactorDemo.__init__(self, factorSymbol=factorSymbol, factorDirection=factorDirection,
                                  lagTradeDays=lagTradeDays,frequency=frequency,validTradingDayRatio=validTradingDayRatio,
                                  items=items,varitems=varitems,fetchDataOnOld=fetchDataOnOld)
        self.standard=standard
        self.how=how
        #dailyData路径
        self.dailyData_path=path+'\\_dailyData\\'+str(frequency)
        #dailyFactor路径
        if type(dailyFactorSymbol) not in [types.StringType, types.UnicodeType]:
            raise BaseException("dailyFactorSymbol must be string.")  
        else:
            self.dailyFactor_path=path+'\\_dailyFactor\\'+dailyFactorSymbol
            if not os.path.exists(self.dailyFactor_path):
                os.makedirs(self.dailyFactor_path)
        
        self.dailydata=pd.DataFrame()
        self.dailyfactor=pd.DataFrame()
        
    def getDailyData(self,dateTime):
        #导入dateTime当天的数据
        if not isinstance(dateTime, datetime.datetime):
            raise BaseException("[getData] 'dateTime'must be datetime.datetime")
        if self.items is None or self.maxoffset is None:
            return
        
        if os.path.exists(self.dailyData_path):
            #如果本地数据文件存在，则导入本地数据:包括量价base、委托仓位position、衍生derived三部分
            
            items0=list(set(self.items).intersection(['close', 'preClose','volume']))
            data0=pd.DataFrame()
            if len(items0)>0:
                #如果要提取的指标在base包括的三个指标中
                file0=self.dailyData_path+'\\base\\'+dateTime.strftime('%Y-%m-%d')+'.pkl'
                data0=pd.read_pickle(file0)
                data0=data0[items0]
            
            items1=list(set(self.items).intersection(['bc1','buy1','sc1','sale1','bc2','buy2',
                        'sc2','sale2','bc3','buy3','sc3','sale3','bc4','buy4','sc4','sale4','bc5','buy5','sc5','sale5']))
            data1=pd.DataFrame()
            if len(items1)>0:
                #如果要提取的指标在position包括的三个指标中
                file1=self.dailyData_path+'\\position\\'+dateTime.strftime('%Y-%m-%d')+'.pkl'
                data1=pd.read_pickle(file1)
                data1=data1[items1]
            
            items2=list(set(self.items).intersection(['spread']))
            data2=pd.DataFrame()
            if len(items2)>0:
                #如果要提取的指标在derived包括的三个指标中
                file2=self.dailyData_path+'\\derived\\'+dateTime.strftime('%Y-%m-%d')+'.pkl'
                data2=pd.read_pickle(file2)
                data2=data2[items2]
            
            self.dailydata=pd.concat([data0,data1,data2],axis=1)
        else:
            #如果本地数据文件不存在，则导入线上数据
            self.dailydata = self._getStockData(self.stocklist['securityId'].tolist(), self.items,
                                                    dateTime,dateTime)
            self.dailydata.set_index(['dateTime', 'securityId'], inplace=True)


    def getDailyFactor(self,dateTime):
        #如果存在日度因子数据，则导入；否则首先导入日度数据，然后计算日度因子并保存在本地
        #其中,self.getFactor()使用self.dailydata计算self.dailyfactor
        if not isinstance(dateTime, datetime.datetime):
            raise BaseException("[getData] 'dateTime'must be datetime.datetime")
        
        file0=self.dailyFactor_path+'\\'+dateTime.strftime('%Y-%m-%d')+'.pkl'
        if os.path.exists(file0):
            #如果存在日度因子数据，则导入
            self.dailyfactor=pd.read_pickle(file0)
        else:
            #如果不存在，则首先导入日度数据，然后计算日度因子并保存在本地
            self.getDailyData(dateTime)
            self.getFactor()
            self.dailyfactor.replace([np.inf,-np.inf],np.nan,inplace=True)
            self.dailyfactor.to_pickle(file0)
        if self.standard:
            self.dailyfactor=(self.dailyfactor-self.dailyfactor.mean())/self.dailyfactor.std()


    def _wavg(self,group,avg_name,weight_name):
        d=group[avg_name]
        w=group[weight_name]
        return (d*w).sum()/w.sum()
    def transformToId(self,data):
        #将日度因子值转化成月度因子值，其中['dailyfactor']为日度因子值
        if self.how=='mean':
            #平均数
            data=data.groupby('securityId')['dailyfactor'].mean()
        elif self.how=='wmean':
            #线性加权平均数
            data=data.sort_values( 'date', ascending=1)#越远的在前面
            data['weight']=1.0
            data['weight']=data.groupby('securityId')['weight'].cumsum()
            data=data.groupby('securityId').apply(self._wavg,'dailyfactor','weight')
        elif self.how=='ewmean':
            #指数加权平均数
            a=2.0/(1+self.offset1)
            data=data.sort_values('date', ascending=0)#越近的在前面
            data['weight']=1
            data['weight']=data.groupby('securityId')['weight'].cumsum()
            data['weight']=data['weight'].apply(lambda x:(1-a)**(x-1))
            data=data.groupby('securityId').apply(self._wavg,'dailyfactor','weight')
        elif self.how=='median':
            #中位数
            data=data.groupby('securityId')['dailyfactor'].median()
        elif self.how=='std':
            #标准差
            data=data.groupby('securityId')['dailyfactor'].std()
        elif self.how=='cv':
            #变异系数
            data=data.groupby('securityId')['dailyfactor'].mean()/data.groupby('securityId')['dailyfactor'].std()
            data.replace([np.inf,-np.inf],np.nan,inplace=True)
        elif self.how=='prod':
            #乘积
            data=data.groupby('securityId')['dailyfactor'].prod()
        return data    
        
    def calculateFactorValue(self, dateTime):
        if not isinstance(dateTime, datetime.datetime):
            raise BaseException("[getData] 'dateTime'must be datetime.datetime")
            

        timedelta = datetime.timedelta(days=self.offset1 *2 +20)
        tradedates = self._getTradeDate(dateTime - timedelta, dateTime)         #频率为类频率
        self.beginDateTime = tradedates.iloc[-self.maxoffset, 0].to_pydatetime()
        self.endDateTime = tradedates.iloc[-1, 0].to_pydatetime()
        self.tradeDateList =self._getVarsDate(self.beginDateTime, self.endDateTime)   #频率为天
        self.stocklist = self._getStockCode(self.endDateTime, self.endDateTime)
        
        dailyfactor = pd.DataFrame()
        tradeDateList1 = [dt.to_pydatetime() for dt in self.tradeDateList['dateTime']]
        for dt in tradeDateList1:
            self.getDailyFactor(dt)
            dailyfactor=dailyfactor.append(self.dailyfactor.to_frame('dailyfactor').reset_index())
            dailyfactor['date']=dailyfactor['date'].astype('datetime64[ns]')

        
        #保留有效的股票列表
        count=dailyfactor.groupby('securityId').size()
        stocklist=count[count>=self.offset1 * self.validTradingDayRatio]
        dailyfactor=pd.merge(dailyfactor,stocklist.reset_index()[['securityId']],on='securityId',how='inner')
        
        factor=self.transformToId(dailyfactor).to_frame('factorValue')
        return  self.stocklist.merge(factor, left_on='securityId', right_index=True, how='left')
    
    


if __name__=='__main__':
    #from pyalgotrade.multifactor.connectdb import ConnectSqlServer, ConnectOracle
    from datafeeds.utils.relationaldb import ConnectSqlServer, ConnectOracle
    from datafeeds.utils.nosqldb import ConnectMongoDB
    import numpy as np
    class Test(BaseFactorWithDB):
        def __init__(self, factorSymbol='Test', factorParameters={}):
            # Initialize super class. 
            super(Test,self).__init__(factorSymbol=factorSymbol, factorParameters=factorParameters)
        
        def calculateFactorValue(self, dateTime):
            return pd.DataFrame({"securityId":['000001.SZ','000002.SZ','600000.SH'],"factorValue":[2.2, 4.4, 3.3]})
    test = Test()
        
    
    sqlDB = ConnectSqlServer(dbServer='10.38.200.33', dbPort='1433', dbDateBase='fangdw', 
                      dbUserId='sa', dbPassWord='fdw900705')
    oracleDB = ConnectOracle(dbServer='10.38.200.211', dbPort='1521', dbDateBase='axzqalpha', 
                       dbUserId='SYSTEM', dbPassWord='axzq123')
    mongoDB = ConnectMongoDB(dbServer='10.38.200.48', dbPort=27017, dbDateBase='factor')         
    
    test.setFactorStoreDB(mongoDB)
    test.setDateTimeFormInDB(DateTimeForm.intDate)
    test.setProcessOutliers(outliers.KeepOutliers())
    
    dateTimeList = [datetime.datetime(2017,4,30),datetime.datetime(2017,5,18),datetime.datetime(2017,5,19),datetime.datetime(2017,6,1)]
    logs = pd.DataFrame() 
    """
    for dateTime in dateTimeList:
        log = test.updateFactorTableToDB(dateTime,reUpdate=False)
        logs = logs.append(pd.DataFrame(log))
        print pd.DataFrame(log)
    """
    a= test.getFactorValueFromMongoDB([datetime.datetime(2017,5,28)])
    b = test.getStandardizedFactorValue(datetime.datetime(2017,5,28))
    #c = test.getOrCaculateFactorValue(datetime.datetime(2017,5,19))

      
      
  
