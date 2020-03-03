# -*- coding: utf-8 -*-

from strategy.strategyZoo import *


class StrategyManager(object):
    def __init__(self,NameList):
        self.strategys={}
        for name in NameList:
            self.strategys[name] = globals()[name]
    def getStrategys(self,name):
        return self.strategys[name]




