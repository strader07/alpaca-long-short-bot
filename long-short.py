import datetime
import threading
import time
# import pandas as pd
# import sys
# from datetime import timedelta

import alpaca_trade_api as tradeapi
# import pytz
from alpaca_trade_api.rest import TimeFrame
from config import *


class LongShort:
    def __init__(self):
        self.alpaca = tradeapi.REST(API_KEY, API_SECRET, APCA_API_BASE_URL, 'v2')

        stockUniverse = ['DOMO', 'TLRY', 'SQ', 'MRO', 'AAPL', 'GM', 'SNAP', 'SHOP',
                         'SPLK', 'BA', 'AMZN', 'SUI', 'SUN', 'TSLA', 'CGC', 'SPWR',
                         'NIO', 'CAT', 'MSFT', 'PANW', 'OKTA', 'TWTR', 'TM', 'RTN',
                         'ATVI', 'GS', 'BAC', 'MS', 'TWLO', 'QCOM', ]
        # Format the allStocks variable for use in the class.
        self.allStocks = []
        for stock in stockUniverse:
            self.allStocks.append([stock, 0])

        self.long = []
        self.short = []
        self.qShort = None
        self.qLong = None
        self.adjustedQLong = None
        self.adjustedQShort = None
        self.blacklist = set()
        self.longAmount = 0
        self.shortAmount = 0
        self.timeToClose = None
        self.equity = 0

    def run(self):
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)

        # Wait for market to open.
        print("Waiting for market to open...")
        tAMO = threading.Thread(target=self.awaitMarketOpen)
        tAMO.start()
        tAMO.join()
        print("Market opened.")

        # Rebalance the portfolio every minute, making necessary trades.
        while True:

            # Figure out when the market will close so we can prepare to sell beforehand.
            clock = self.alpaca.get_clock()
            closingTime = clock.next_close.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            self.timeToClose = closingTime - currTime

            if self.timeToClose < (60 * 15):
                # Close all positions when 15 minutes til market close.
                print("Market closing soon.  Closing positions.")

                positions = self.alpaca.list_positions()
                for position in positions:
                    if position.side == 'long':
                        orderSide = 'sell'
                    else:
                        orderSide = 'buy'
                    qty = abs(int(float(position.qty)))
                    respSO = []
                    tSubmitOrder = threading.Thread(target=self.submitOrder(qty, position.symbol, orderSide, respSO))
                    tSubmitOrder.start()
                    tSubmitOrder.join()

                # Run script again after market close for next trading day.
                print("Sleeping until market close (15 minutes).")
                time.sleep(60 * 15)
            else:
                # Rebalance the portfolio.
                tRebalance = threading.Thread(target=self.rebalance)
                tRebalance.start()
                tRebalance.join()
                time.sleep(60)

    # Wait for market to open.
    def awaitMarketOpen(self):
        isOpen = self.alpaca.get_clock().is_open
        while not isOpen:
            clock = self.alpaca.get_clock()
            openingTime = clock.next_open.replace(tzinfo=datetime.timezone.utc).timestamp()
            currTime = clock.timestamp.replace(tzinfo=datetime.timezone.utc).timestamp()
            timeToOpen = int((openingTime - currTime) / 60)
            print(str(timeToOpen) + " minutes til market open.")
            time.sleep(60)
            isOpen = self.alpaca.get_clock().is_open

    def rebalance(self):
        tRerank = threading.Thread(target=self.rerank)
        tRerank.start()
        tRerank.join()

        # Clear existing orders again.
        orders = self.alpaca.list_orders(status="open")
        for order in orders:
            self.alpaca.cancel_order(order.id)

        print("We are taking a long position in: " + str(self.long))
        print("We are taking a short position in: " + str(self.short))

        # Remove positions that are no longer in the short or long list, and make a list of positions that do not
        # need to change.  Adjust position quantities if needed.
        executed = [[], []]
        long_symbols = [pos[0] for pos in self.long]
        short_symbols = [pos[0] for pos in self.short]
        positions = self.alpaca.list_positions()
        self.blacklist.clear()
        for position in positions:
            # if self.long.count(position.symbol) == 0:
            if position.symbol not in long_symbols:
                # Position is not in long list.
                # if self.short.count(position.symbol) == 0:
                if position.symbol not in short_symbols:
                    # Position not in short list either.  Clear position.
                    if position.side == "long":
                        side = "sell"
                    else:
                        side = "buy"
                    respSO = []
                    tSO = threading.Thread(target=self.submitOrder,
                                           args=[abs(int(float(position.qty))), position.symbol, side, respSO])
                    tSO.start()
                    tSO.join()
                else:
                    # Position in short list.
                    if position.side == "long":
                        # Position changed from long to short.  Clear long position to prepare for short position.
                        side = "sell"
                        respSO = []
                        tSO = threading.Thread(target=self.submitOrder,
                                               args=[int(float(position.qty)), position.symbol, side, respSO])
                        tSO.start()
                        tSO.join()
                    else:
                        new_qty = self.short[short_symbols.index(position.symbol)][1]
                        if abs(int(float(position.qty))) == new_qty:
                            # Position is where we want it.  Pass for now.
                            pass
                        else:
                            # Need to adjust position amount
                            diff = abs(int(float(position.qty))) - new_qty
                            if diff > 0:
                                # Too many short positions.  Buy some back to rebalance.
                                side = "buy"
                            else:
                                # Too little short positions.  Sell some more.
                                side = "sell"
                            respSO = []
                            tSO = threading.Thread(target=self.submitOrder,
                                                   args=[abs(diff), position.symbol, side, respSO])
                            tSO.start()
                            tSO.join()
                        executed[1].append(position.symbol)
                        self.blacklist.add(position.symbol)
            else:
                # Position in long list.
                if position.side == "short":
                    # Position changed from short to long.  Clear short position to prepare for long position.
                    respSO = []
                    tSO = threading.Thread(target=self.submitOrder,
                                           args=[abs(int(float(position.qty))), position.symbol, "buy", respSO])
                    tSO.start()
                    tSO.join()
                else:
                    new_qty = self.long[long_symbols.index(position.symbol)][1]
                    if int(float(position.qty)) == new_qty:
                        # Position is where we want it.  Pass for now.
                        pass
                    else:
                        # Need to adjust position amount.
                        diff = abs(int(float(position.qty))) - new_qty
                        if diff > 0:
                            # Too many long positions.  Sell some to rebalance.
                            side = "sell"
                        else:
                            # Too little long positions.  Buy some more.
                            side = "buy"
                        respSO = []
                        tSO = threading.Thread(target=self.submitOrder, args=[abs(diff), position.symbol, side, respSO])
                        tSO.start()
                        tSO.join()
                    executed[0].append(position.symbol)
                    self.blacklist.add(position.symbol)

        # Send orders to all remaining stocks in the long and short list.
        respSendBOLong = []
        tSendBOLong = threading.Thread(target=self.sendBatchOrder, args=[self.long, "buy", respSendBOLong])
        tSendBOLong.start()
        tSendBOLong.join()
        respSendBOLong[0][0] += executed[0]

        respSendBOShort = []
        tSendBOShort = threading.Thread(target=self.sendBatchOrder, args=[self.short, "sell", respSendBOShort])
        tSendBOShort.start()
        tSendBOShort.join()
        respSendBOShort[0][0] += executed[1]

    # Re-rank all stocks to adjust longs and shorts.
    def rerank(self):
        tRank = threading.Thread(target=self.rank)
        tRank.start()
        tRank.join()

        # Grabs the top and bottom quarter of the sorted stock list to get the long and short lists.
        longShortAmount = len(self.allStocks) // 4
        self.long = []
        self.short = []
        for i, stockField in enumerate(self.allStocks):
            if i < longShortAmount:
                self.short.append([stockField[0], 0])
            elif i > (len(self.allStocks) - 1 - longShortAmount):
                self.long.append([stockField[0], 0])
            else:
                continue

        # Determine amount to long/short based on total stock price of each bucket.
        self.set_position_size()

    def set_position_size(self):
        equity = int(float(self.alpaca.get_account().equity))
        trade_amount = equity * TRADE_EQUITY_PERCENT / 100
        self.shortAmount = trade_amount * LONG_PERCENT / 100
        self.longAmount = trade_amount * SHORT_PERCENT / 100

        long_amount = self.longAmount / len(self.long)
        short_amount = self.shortAmount / len(self.short)
        long_positions = self.long.copy()
        short_positions = self.short.copy()

        _end = str(datetime.datetime.now().isoformat('T')).split(".")[0]+"Z"
        _start = str(datetime.datetime.now().date())
        long_temp_amount = 0
        short_temp_amount = 0
        for i, position in enumerate(long_positions):
            symbol = position[0]
            print(self.alpaca.get_bars(symbol, TimeFrame.Minute, _start, _end, limit=1, adjustment='raw'))
            last_price = self.alpaca.get_bars(symbol, TimeFrame.Minute, _start, _end, limit=1, adjustment='raw')[0].c
            qty = int(round(long_amount / last_price))
            if qty == 0:
                qty = 1
            long_temp_amount += qty * last_price
            self.long[i][1] = qty
        for i, position in enumerate(short_positions):
            symbol = position[0]
            last_price = self.alpaca.get_bars(symbol, TimeFrame.Minute, _start, _end, limit=1, adjustment='raw')[0].c
            qty = int(round(short_amount / last_price))
            if qty == 0:
                qty = 1
            short_temp_amount += qty * last_price
            self.short[i][1] = qty

        self.longAmount = long_temp_amount
        self.shortAmount = short_temp_amount

        print(self.long)
        print(self.short)
        print(self.longAmount, self.shortAmount)

    # Get the total price of the array of input stocks.
    def getTotalPrice(self, stocks, resp):
        totalPrice = 0
        _end = str(datetime.datetime.now().isoformat("T"))+"+00:00"
        _start = str(datetime.datetime.now().date())
        for stock in stocks:
            bars = self.alpaca.get_bars(stock, TimeFrame.Minute, _start, _end, limit=1, adjustment='raw')
            totalPrice += bars[0].c
        resp.append(totalPrice)

    # Submit a batch order that returns completed and uncompleted orders.
    def sendBatchOrder(self, positions, side, resp):
        executed = []
        incomplete = []
        stocks = [pos[0] for pos in positions]
        for i, stock in enumerate(stocks):
            if self.blacklist.isdisjoint({stock}):
                respSO = []
                qty = positions[i][1]
                tSubmitOrder = threading.Thread(target=self.submitOrder, args=[qty, stock, side, respSO])
                tSubmitOrder.start()
                tSubmitOrder.join()
                if not respSO[0]:
                    # Stock order did not go through, add it to incomplete.
                    incomplete.append(stock)
                else:
                    executed.append(stock)
                respSO.clear()
        resp.append([executed, incomplete])

    # Submit an order if quantity is above 0.
    def submitOrder(self, qty, stock, side, resp):
        if qty > 0:
            try:
                self.alpaca.submit_order(stock, qty, side, "market", "day")
                print("Market order of | " + str(qty) + " " + stock + " " + side + " | completed.")
                resp.append(True)
            except Exception as e:
                print(e)
                print("Order of | " + str(qty) + " " + stock + " " + side + " | did not go through.")
                resp.append(False)
        else:
            print("Quantity is 0, order of | " + str(qty) + " " + stock + " " + side + " | not completed.")
            resp.append(True)

    # Get percent changes of the stock prices over the past 10 minutes.
    def getPercentChanges(self):
        length = 10
        _end = str(datetime.datetime.now().isoformat("T"))+"+00:00"
        _start = str(datetime.datetime.now().date())

        for i, stock in enumerate(self.allStocks):
            bars = self.alpaca.get_bars(stock[0], TimeFrame.Minute, _start, _end, limit=length, adjustment='raw')
            print(bars)
            self.allStocks[i][1] = (bars[-1].c - bars[0].o) / bars[0].o

    # Mechanism used to rank the stocks, the basis of the Long-Short Equity Strategy.
    def rank(self):
        # Ranks all stocks by percent change over the past 10 minutes (higher is better).
        tGetPC = threading.Thread(target=self.getPercentChanges)
        tGetPC.start()
        tGetPC.join()

        # Sort the stocks in place by the percent change field (marked by pc).
        self.allStocks.sort(key=lambda x: x[1])


# Run the LongShort class
ls = LongShort()
ls.run()
