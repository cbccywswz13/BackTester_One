import datetime
import pandas as pd
from math import isnan

from abc import ABCMeta,abstractmethod

from Event_engine import SignalEvent



class Strategy(object):
    """description of class"""
    __metaclass__==ABCMeta

    @abstractmethod
    def calculate_signals(self):
        raise NotImplementedError('should implement calculate_signals()')



class BuyAndHoldStrtegy(Strategy):
    def __init__(self,bars,events):
        self.bars=bars
        self.symbol_list=self.bars.symbol_list
        self.events=events

        self.bought=self._calculate_initial_bought()

    def _calculate_initial_bought(self):
        bought={}
        for s in self.symbol_list:
            bought[s]=False
        return bought

    def calculate_signals(self):
        for s in self.symbol_list:
            bars=self.bars.get_latest_bars(s,N=1)
            if bars is not None and bars !=[]:
                if self.bought[s]==False:
                    signal=SignalEvent(s,bars.index,'Long')
                    self.events.put(signal)
                    self.bought[s]=True
            