from bs4 import BeautifulSoup
from pprint import pformat
from lxml import html
import sys
import json
import urllib.parse
import requests
import logging
import datetime


logging.basicConfig(filename='app.log', filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.DEBUG)
NIFTY_PERFORMANCE = None


class MARKET_URL(object):
    __NSE_HOME_PAGE = 'https://www.nseindia.com/live_market/dynaContent/live_watch'
    __EQUITIES = '/equities_stock_watch.htm'
    __SECTOR_INFO = '/stock_watch/<sector-id>StockWatch.json'
    __STOCK_HISTORY = '/get_quote/getHistoricalData.jsp?'
    __OPTION_CHAIN = '/option_chain/optionKeys.jsp?'
    __FO_STOCKS = '/dynaContent/live_watch/stock_watch/foSecStockWatch.json'
    NSE_TRADER = 'https://www.niftytrader.in/technical-school/nse-fo-lot-size/'
    NIFTY_STOCK_WATCH = __NSE_HOME_PAGE + '/stock_watch/niftyStockWatch.json'

    @classmethod
    def get_fo_stocks(cls):
        logging.info(cls.__NSE_HOME_PAGE + cls.__FO_STOCKS)
        return cls.__NSE_HOME_PAGE + cls.__FO_STOCKS

    @classmethod
    def get_historical_data_url(cls, symbol, period='week'):
        query = {'symbol': symbol, 'series': 'EQ', 'fromDate': 'undefined', 'toDate': 'undefined', 'datePeriod': period}
        return cls.__NSE_HOME_PAGE + cls.__STOCK_HISTORY + urllib.parse.urlencode(query)

    @classmethod
    def get_option_chain_url(cls, symbol):
        query = {"symbolCode": "1098",
                 "symbol": symbol,
                 "instrument": "-",
                 "date": "-",
                 "segmentLink": "17",
                 "symbolCount": "2"}
        return cls.__NSE_HOME_PAGE + cls.__OPTION_CHAIN + urllib.parse.urlencode(query)


class LiveMarket(object):

    def __init__(self):
        self.response = requests.get(MARKET_URL.NIFTY_STOCK_WATCH)
        self.nse_trader_response = requests.get(MARKET_URL.NSE_TRADER)
        self._fetch_nifty_performance()
        self.top_losers = []
        self.top_gainers = []

    def _fetch_options(self):
        losing_stocks = self._filter_losing_stocks_in_last_one_week()
        option_chains = {}
        for stock in losing_stocks:
            try:
                options = self._get_option_chains_for_stock(stock.get('symbol'))
                if options:
                    option_chains[stock.get('symbol')] = options
                else:
                    logging.warning('No options for %s' % stock.get('symbol'))
            except Exception as error:
                logging.error(error)
        logging.info(pformat(option_chains))
        return option_chains

    def _fetch_nifty_performance(self):
        global NIFTY_PERFORMANCE
        stock_watch = json.loads(self.response.text)
        nifty50 = stock_watch.get('latestData')[0]
        NIFTY_PERFORMANCE = float(nifty50.get('mCls'))
        logging.info("NIFTY_PERFORMANCE: %s", NIFTY_PERFORMANCE)

    def _get_option_chains_for_stock(self, symbol, option_type='PUT'):
        logging.info("Getting option chain URL for %s", symbol)
        response = self._get_option_chain_data(symbol)
        try:
            stock_ltp = self._get_last_traded_price(response, symbol)
            logging.info("Last traded price of %s is %s", pformat(symbol), pformat(stock_ltp))
            return self._fetch_options_for_stock(symbol, response, stock_ltp, option_type)
        except Exception as error:
            raise error

    def _fetch_options_for_stock(self, symbol, data, stock_ltp, option_type='PUT'):
        valid_options = []
        options_table = data.xpath("//table[@id='octable']")[0]
        options = options_table.xpath('tr')
        appropriate_strike_price = self._get_approximate_strike_price_based_on_volatility_range(options, stock_ltp)
        logging.info("Appropriate strike price for %s is %s", symbol, appropriate_strike_price)
        filtered_options = []
        lot_size = self._get_lot_size_of_option(symbol)
        strike_difference = sys.maxsize
        for option_data in options:
            try:
                option_ltp_price = self._get_ltp_price_of_option(option_data, option_type)
                option_strike_price = self._get_strike_price_of_option(option_data)
                is_enough_volume = self._is_there_enough_volume(option_data)
                if is_enough_volume and strike_difference > abs(option_strike_price - appropriate_strike_price):
                    strike_difference = abs(option_strike_price - appropriate_strike_price)
                    logging.info("Consider strike %s at %s with lot size %s", option_strike_price, option_ltp_price, lot_size)
                    filtered_options.append((strike_difference, option_strike_price, option_ltp_price, lot_size, int(option_ltp_price * lot_size)))
            except Exception as error:
                logging.error('_fetch_options_for_stock %s', pformat(error))
        filtered_options = sorted(filtered_options, key=lambda x: x[0])
        if filtered_options:
            valid_options.append(filtered_options[0][1:])
        return valid_options

    def _get_lot_size_of_option(self, symbol):
        logging.info("Getting lot size for %s" % symbol)
        nse_trader_response = html.fromstring(self.nse_trader_response.text)
        table_headers = nse_trader_response.xpath("//table[@id='tablepress-24']//th")
        month_year = self._get_month_and_year()
        column = 0
        for column, header in enumerate(table_headers):
            if header.text.strip() == month_year:
                break
        lot_size = nse_trader_response.xpath("//table[@id='tablepress-24']/tbody//td[text()='%s']/../td[%s]" % (symbol, column + 1))
        return int(lot_size[0].text.strip())

    def _get_month_and_year(self):
        return datetime.datetime.now().strftime('%b').upper() + '-' + str(datetime.datetime.now().year % 100)

    def _get_strike_price_of_option(self, option_data):
        strike_price_xpath = 'td[@class="grybg"]/a/b'
        try:
            return float(option_data.xpath(strike_price_xpath)[0].text)
        except Exception as error:
            logging.error('_get_strike_price_of_option %s', pformat(error))

    def _get_ltp_price_of_option(self, option_data, option_type):
        if option_type == 'CALL':
            option_ltp_xpath = 'td[@class="grybg"]/preceding-sibling::td[@class="nobg"]/a'
        else:
            option_ltp_xpath = 'td[@class="grybg"]/following-sibling::td[@class="nobg"]/a'
        try:
            return float(option_data.xpath(option_ltp_xpath)[0].text)
        except Exception as error:
            logging.error('_get_ltp_price_of_option %s', pformat(error))

    def _is_there_enough_volume(self, option_data, option_type='PUT'):
        volume = 0
        if option_type == 'CALL':
            volume_xpath = 'td[@class="grybg"][a]/preceding-sibling::td[@class="nobg"][8]'
        else:
            volume_xpath = 'td[@class="grybg"][a]/following-sibling::td[@class="nobg"][8]'
        try:
            volume = int(option_data.xpath(volume_xpath)[0].text.strip().replace(',', ''))
        except Exception as error:
            logging.error('_is_there_enough_volume %s', pformat(error))
        return volume > 100

    def _get_approximate_strike_price_based_on_volatility_range(self, options, stock_ltp):
        min_volatility, max_volatility = self._get_implied_volatility_range_for_options(options)
        percentage = 0
        variance = self._get_percentage_variance()
        if min_volatility > 50:
            percentage = 10 - variance
        elif min_volatility > 40:
            percentage = 8 - variance
        elif min_volatility > 30:
            percentage = 5 - variance
        else:
            logging.exception("Volatility range is %s-%s which is not in 30-50" % (min_volatility, max_volatility))
            raise Exception("Volatility range is %s-%s which is not in 30-50" % (min_volatility, max_volatility))
        return stock_ltp - stock_ltp * percentage / 100

    def _get_percentage_variance(self):
        week =(datetime.datetime.now().day - 1) //7 + 1
        if week == 4:
            return 3
        elif week == 3 or week == 2:
            return 2
        else:
            return 0

    def _get_option_chain_data(self, symbol):
        response = requests.get(MARKET_URL.get_option_chain_url(symbol))
        return html.fromstring(response.text)

    def _get_implied_volatility_range_for_options(self, options, option_type='PUT'):
        implied_volatility_range = []
        for option_data in options:
            try:
                implied_volatility_range.append(float(self._get_implied_volatility(option_data, option_type)))
            except Exception as error:
                logging.error('_get_implied_volatility_range_for_options %s', pformat(error))
        logging.info("Implied volatility range %s", pformat(implied_volatility_range))
        return implied_volatility_range[-1], implied_volatility_range[0]

    def _get_implied_volatility(self, option_data, option_type='PUT'):
        if option_type == 'CALL':
            iv_index = 'td[@class="grybg"][a]/preceding-sibling::td[@class="nobg"][7]'
        else:
            iv_index = 'td[@class="grybg"][a]/following-sibling::td[@class="nobg"][7]'
        try:
            return float(option_data.xpath(iv_index)[0].text.strip().replace(',', ''))
        except Exception as error:
            error = Exception('_get_implied_volatility %s' % error)
            raise error

    def _get_last_traded_price(self, data, symbol):
        ltp = data.xpath("//b[contains(text(), '%s')]" % symbol)[0]
        ltp = float(ltp.text.strip().split()[-1])
        return ltp

    def _get_nifty50_losing_stocks(self):
        losing_stocks = []
        stock_watch = json.loads(self.response.text)
        stocks = stock_watch.get('data')
        for stock in stocks:
            if float(stock.get('mPC')) < NIFTY_PERFORMANCE:
                stock['rating'] = abs(float(stock.get('mPC')) - NIFTY_PERFORMANCE)
                losing_stocks.append(stock)
        losing_stocks = sorted(losing_stocks, key=lambda stock: stock['rating'], reverse=True)
        message = pformat([stock.get('symbol') for stock in losing_stocks])
        logging.info('Losing stocks on NIFTY 50: %s', pformat(message))
        return losing_stocks

    def _filter_losing_stocks_in_last_one_week(self):
        qualified_stocks = []
        losing_stocks = self._get_nifty50_losing_stocks()
        for stock in losing_stocks:
            logging.info('Checking if %s qualified one week downtrend', pformat(stock.get('symbol')))
            if self._is_falling(stock.get('symbol')):
                qualified_stocks.append(stock)
                logging.info('%s passed in one week downtrend check', pformat(stock.get('symbol')))
            else:
                logging.info('%s failed in one week downtrend check', pformat(stock.get('symbol')))
        return qualified_stocks

    def _get_historical_data_for_stock(self, symbol, duration='week'):
        historical_ltp_prices = []
        url = MARKET_URL.get_historical_data_url(symbol, duration)
        response = requests.get(url)
        response = BeautifulSoup(response.text, 'lxml')
        rows = response.findAll('tr')[1:]
        for row in rows:
            colums = row.findAll('td')
            historical_ltp_prices.append(float(colums[-4].get_text()))
        logging.info('Historical data of %s for %s: %s', pformat(symbol), pformat(duration), historical_ltp_prices)
        return historical_ltp_prices

    def _is_falling(self, symbol, duration='week'):
        historical_ltp_prices = self._get_historical_data_for_stock(symbol, duration)
        return historical_ltp_prices[0] < historical_ltp_prices[-1]

    # def get_nifty50_gaining_stocks(self):
    #     gaining_stocks = []
    #     stock_watch = json.loads(self.response.text)
    #     stocks = stock_watch.get('data')
    #     for stock in stocks:
    #         if float(stock.get('mPC')) > 0 and float(stock.get('mPC')) > NIFTY_PERFORMANCE:
    #             stock['rating'] = float(stock.get('mPC')) + NIFTY_PERFORMANCE
    #             gaining_stocks.append(stock)
    #     gaining_stocks = sorted(gaining_stocks, key=lambda stock: stock['rating'], reverse=True)
    #     for stock in gaining_stocks:
    #         print(stock.get('symbol'))


if __name__ == '__main__':
    o = LiveMarket()
    print(pformat(o._fetch_options()))
    # print(o._get_option_chains_for_stock('COALINDIA'))
