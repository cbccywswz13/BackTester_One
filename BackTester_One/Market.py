import datetime
import os,os.path
import pymongo
import pandas as pd

from abc import ABCMeta
from abc import abstractclassmethod
from Event_engine import MarketEvent

class DataHandler(object):
    """description of class"""
    __metaclass__=ABCMeta


    @abstractmethod
    def get_latest_bars(self, symbol, N=1):
        """
        Returns the last N bars from the latest_symbol list,
        or fewer if less bars are available.
        """
        raise NotImplementedError("Should implement get_latest_bars()")

    @abstractmethod
    def update_bars(self):
        """
        Pushes the latest bar to the latest symbol structure
        for all symbols in the symbol list.
        """
        raise NotImplementedError("Should implement update_bars()")

class HistoricCSVDataHandler(DataHandler):
    def __init__(self,events,csv_dir,symbol_list):
        self.events=events
        self.csv_dir=csv_dir
        self.symbol_list=symbol_list

        self.symbol_data={}
        self.continue_backtest=True
        self._open_convert_csv_files()

    def _open_convert_csv_files():
        comb_index=None
        for s in self.symbol_list:
            self.symbol_data[s]=pd.read_csv(os.path.join(self.csv_dir,'%s.csv'%s),\
                header=0,index_col=0,names=['datetime','open','low','high','close','volume','oi'])
        if comb_index is None:
            comb_index=self.symbol_data[s].index
        else:
            comb_index=comb_index.union(self.symbol_data[s].index)

        #set the latest sybol_data to None
        self.latest_symbol_data[s]=[]
        
        for s in self.symbol_list:
            self.symbol_data[s]=self.symbol_data[s].reindex(
                index=comb_index,method='pad')#pad=ffill

    def _get_new_bar(self,symbol):
        for b in self.symbol_data[symbol].iterrows():
            yield b

    def get_latest_bars(self, symbol, N = 1):
        try:
            bar_list=self.latest_symbol_data[symbol]
        except KeyError:
            print('the symbol is not in the historical data set')
        else:
            return bar_list[-N:]



    def update_bars(self):
        for s in self.symbol_list:
            try:
                bar=self._get_new_bar(s).next()
            except StopIteration:
                self.continue_backtest=False
            else:
                if bar is not None:
                    self.latest_symbol_data[s].append(bar)
        
        self.events.put(MarketEvent())