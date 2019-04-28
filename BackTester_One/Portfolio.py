import datetime
import numpy as np
import pandas as pd
import queue

from abc import ABCMeta,abstractmethod
from math import floor
from Event_engine import FillEvent,OrderEvent


class Portfolio(object):
    """description of class"""

    __metaclass__=ABCMeta

    @abstractmethod
    def update_signal(self,event):
        raise NotImplementedError('should implement update_signal')
    @abstractmethod
    def update_fill(self,event):
        raise NotImplementedError('should implement update_fill')


class NaivePortfolio(Portfolio):
    def __init__(self,bars,events,start_date,initial_capital=100000):
        self.bars=bars
        self.symbol_list=self.bars.symbol_list
        self.events=events
        self.start_date=start_date
        self.initial_capital=initial_capital

        self.all_positions = self.construct_all_positions()
        self.current_positions = dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        self.all_holdings = self.construct_all_holdings()
        self.current_holdings = self.construct_current_holdings()

    def construct_all_positions(self):
        d=dict((k,v) for k,v in [(s,0) for s in self.symbol_list])
        d['datetime']=self.start_date
        return [d]

    def construct_all_holdings(self):
        d = dict( (k,v) for k, v in [(s, 0.0) for s in self.symbol_list] )
        d['datetime'] = self.start_date
        d['cash'] = self.initial_capital
        d['commission'] = 0.0
        d['total'] = self.initial_capital
        return [d]

    def construct_current_holdings(self):
        d=dict((k,v) for k,v in[(s,0.0) for s in self.symbol_list])
        d['cash']=self.initial_capital
        d['commission']=0.0
        d['total']=self.initial_capital
        return d


    def update_timeindex(self,event):
        bars={}
        for sym  in self.symbol_list:
            bars[sym]=self.bars.get_latest_bars(sym,N=1)
        #update position
        dp=dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )

        dp['datetime'] = bars[self.symbol_list[0]][0][1]
        for s in self.symbol_list:
            dp[s]=self.current_positions[s]
            
        self.all_positions.append(dp)

        dh=dict( (k,v) for k, v in [(s, 0) for s in self.symbol_list] )
        dh['datetime']=bars[self.symbol_list[0]][0][1]
        dh['cash']=self.current_holdings['cash']
        dh['commission']=self.current_holdings['commission']
        dh['total']=self.current_holdings['cash']

        for s in self.symbol_list:
            market_value=self.current_positions[s]*bars[s][0][5]
            dh[s]=market_value
            dh['total']+=market_value#add cash first,then add the position value

        self.all_holdings.append(dh)

    def update_positions_from_fill(self,fill):
        fill_dir=0
        if fill.direction=="BUY":
            fill_dir=1
        if fill_direction=="SELL":
            fill_dir=-1

        self.current_positions[fill.symbol] += fill_dir*fill.quantity


    def update_holdings_from_fill(self,fill):
        fill_dir=0
        if fill.direction=="BUY":
            fill_dir=1
        if fill.direction=="SELL":
            fill_dir=-1

        fill_cost=self.bars.get_latest_bars(fill.symbol,N=1)[0][5]#close price

        cost=fill_dir*fill_cost*fill.quantity
        self.current_holdings[fill.symbol]+=cost
        self.current_holdings['commission'] += fill.commission
        self.current_holdings['cash'] -= (cost + fill.commission)
        self.current_holdings['total'] -= (cost + fill.commission)

    def update_fill(self, event):

        if event.type=="FILL":
            self.update_positions_from_fill(event)
            self.update_holdings_from_fill(event)

    def generate_naive_order(self,signal):
        order=None
        symbol=signal.symbol
        direction=signal.signal_type
        strength=signal.strength
        mkt_quantity = floor(100 * strength)
        cur_quantity = self.current_positions[symbol]
        order_type = 'MKT'
        if direction == 'LONG' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'BUY')
        if direction == 'SHORT' and cur_quantity == 0:
            order = OrderEvent(symbol, order_type, mkt_quantity, 'SELL')   
    
        if direction == 'EXIT' and cur_quantity > 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'SELL')
        if direction == 'EXIT' and cur_quantity < 0:
            order = OrderEvent(symbol, order_type, abs(cur_quantity), 'BUY')
        return order


    def update_signal(self, event):
        if event.type=='SIGNAL':
            order_event=self.generate_naive_order(event)
            self.events.put(order_event)

    def create_equity_curve_dataframe(self):
        curve=pd.DataFrame(self.all_holdings)
        curve.set_index('datetime',inplace=True)
        curve['returns']=curve['total'].pct_change()
        curve['equity_curve']=(1.0+curve['returns']).cumprod()
        self.create_equity_curve_dataframe=curve