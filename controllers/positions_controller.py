import json
from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from broker_libs.broker_methods import *
from controllers.logs_controller import LogsController
from controllers.mqtt_publisher import MqttPublisher
from database_config import db_config


def get_current_price(instrument, broker_id, broker):
    get_ltp_methods = {
        1: get_ltp_zerodha,
        2: get_ltp_angel,
        3: get_ltp_shoonya
    }
    method = get_ltp_methods.get(broker_id)
    ltp = method(broker, instrument)
    return ltp


class PositionsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_fut_positions_table()
        self.create_opt_positions_table()
        self.logs_controller = LogsController()

    def create_fut_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS fut_positions (
                                position_id INT AUTO_INCREMENT PRIMARY KEY,
                                observable_instrument_id INT,
                                zerodha_instrument_token INT,
                                zerodha_trading_symbol VARCHAR(255),
                                zerodha_name VARCHAR(255),
                                zerodha_exchange VARCHAR(255),
                                angel_token INT,
                                angel_symbol VARCHAR(255),
                                angel_name VARCHAR(255),
                                angel_exchange VARCHAR(255),
                                shoonya_token INT,
                                shoonya_trading_symbol VARCHAR(255),
                                shoonya_name VARCHAR(255),
                                shoonya_exchange VARCHAR(255),
                                alice_token VARCHAR(255),
                                alice_trading_symbol VARCHAR(255),
                                alice_name VARCHAR(255),
                                alice_exchange VARCHAR(255),
                                instrument_position_type INT COMMENT 
                                '1 = FUT BUY\r\n2 = FUT SELL',
                                position_type INT COMMENT 
                                '1 = LONG\r\n2 = SHORT',
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_exit_price FLOAT,
                                profit FLOAT,
                                lot_size INT,
                                position_qty INT,
                                time_frame VARCHAR(255)  
                            )
                        ''')
            self.conn.commit()

    def create_opt_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS opt_positions (
                                opt_position_id INT AUTO_INCREMENT PRIMARY KEY,
                                associated_fut_id INT,
                                
                                option_instrument LONGTEXT,
                                
                                instrument_position_type INT COMMENT 
                                '1 = OPT BUY\r\n2 = OPT SELL',
                                position_entry_time DATETIME,
                                position_entry_price FLOAT,
                                position_exit_time DATETIME,
                                position_exit_price FLOAT,
                                profit FLOAT,
                                position_qty INT,
                                time_frame VARCHAR(255)  
                            )
                        ''')
            self.conn.commit()

    def add_fut_to_positions(self, instrument, interval, position_type, instrument_position_type, broker_id, broker):
        current_price = get_current_price(instrument, broker_id, broker),
        with closing(self.conn.cursor()) as cursor:
            sql = '''
                INSERT INTO fut_positions (
                    observable_instrument_id,
                    zerodha_instrument_token,
                    zerodha_trading_symbol,
                    zerodha_name,
                    zerodha_exchange,
                    angel_token,
                    angel_symbol,
                    angel_name,
                    angel_exchange,
                    shoonya_token,
                    shoonya_trading_symbol,
                    shoonya_name,
                    shoonya_exchange,
                    alice_token,
                    alice_trading_symbol,
                    alice_name,
                    alice_exchange,
                    instrument_position_type,
                    position_type,
                    position_entry_time,
                    position_entry_price,
                    lot_size,
                    position_qty,
                    time_frame
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,NOW(), %s, %s, %s, %s)
            '''
            cursor.execute(sql, (
                instrument['o_id'],
                instrument['zerodha_instrument_token'],
                instrument['zerodha_trading_symbol'],
                instrument['zerodha_name'],
                instrument['zerodha_exchange'],
                instrument['angel_token'],
                instrument['angel_symbol'],
                instrument['angel_name'],
                instrument['angel_exchange_segment'],
                instrument['shoonya_token'],
                instrument['shoonya_trading_symbol'],
                instrument['shoonya_name'],
                instrument['shoonya_exchange'],
                instrument['alice_token'],
                instrument['alice_trading_symbol'],
                instrument['alice_symbol'],
                instrument['alice_exchange'],
                instrument_position_type,
                position_type,
                current_price,
                instrument['shoonya_lot_size'],
                1,
                interval
            ))
            self.conn.commit()
            return cursor.lastrowid, current_price

    def check_for_existing_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM fut_positions WHERE zerodha_instrument_token = %s AND angel_token = %s AND '
                'shoonya_token = %s AND alice_token = %s AND position_exit_time IS NULL',
                (instrument['zerodha_instrument_token'], instrument['angel_token'], instrument['shoonya_token'],
                 instrument['alice_token']))
            active_trade = cursor.fetchone()
        return active_trade

    def exit_existing_position(self, existing_position, broker_id, broker, interval):
        try:
            exit_price = get_current_price(existing_position, broker_id, broker)
            entry_price = float(existing_position['position_entry_price'])
            position_type = existing_position['instrument_position_type']
            profit = (exit_price - entry_price) if position_type == 1 else (entry_price - exit_price)

            with self.conn.cursor() as cursor:
                # Update the future position
                cursor.execute('''
                    UPDATE fut_positions
                    SET position_exit_price = %s, position_exit_time = NOW(), profit = %s
                    WHERE position_id = %s
                ''', (exit_price, profit, existing_position['position_id']))

                # Retrieve and update option positions
                cursor.execute('SELECT * FROM opt_positions WHERE associated_fut_id = %s',
                               existing_position['position_id'])
                opt_positions = cursor.fetchall()

                buy_option_data, sell_option_data = None, None
                buy_option_ltp, sell_option_ltp = None, None

                instrument_from_broker = {
                    1: 'zerodha_option',
                    2: 'angel_option',
                    3: 'shoonya_option',
                    4: 'alice_option'
                }
                broker_map = instrument_from_broker.get(broker_id, 'Unknown')

                for position in opt_positions:
                    ins = json.loads(position['option_instrument'])[broker_map]
                    option_ltp = get_current_price(ins, broker_id, broker)

                    if position['instrument_position_type'] == 1:
                        buy_option_data = position
                        buy_option_ltp = option_ltp
                    elif position['instrument_position_type'] == 2:
                        sell_option_data = position
                        sell_option_ltp = option_ltp

                    option_profit = (option_ltp - float(position['position_entry_price'])) if position[
                                                                                                  'instrument_position_type'] == 1 \
                        else (float(position['position_entry_price']) - option_ltp)

                    cursor.execute('''
                        UPDATE opt_positions
                        SET position_exit_price = %s, position_exit_time = NOW(), profit = %s
                        WHERE opt_position_id = %s
                    ''', (option_ltp, option_profit, position['opt_position_id']))

                self.conn.commit()

            payload = {
                "trade_type": "exit",
                "position_type": position_type,
                "fut_position_id": existing_position['position_id'],
                "fut_trade": {
                    "instrument": existing_position,
                    "interval": interval,
                },
                "opt_buy": {
                    "buy_option_data": buy_option_data,
                    "fut_position_id": existing_position['position_id'],
                    "buy_option_current_price": buy_option_ltp
                },
                "opt_sell": {
                    "sell_option_data": sell_option_data,
                    "fut_position_id": existing_position['position_id'],
                    "sell_option_current_price": sell_option_ltp
                }
            }

            return True, 'Success', payload
        except Exception as e:
            self.conn.rollback()  # Roll back in case of any error
            return False, str(e), None

    def get_broker_by_id(self, broker_id):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT * FROM brokers WHERE broker_id = %s', broker_id)
            broker_details = cursor.fetchone()
        return broker_details

    def analyze_to_take_position(self, applied_df, instrument, interval, broker_id, broker):
        mqtt_publisher = MqttPublisher()
        if broker_id in (1, 2):
            current_candle_index = -2
            previous_candle_index = -3
        else:
            current_candle_index = -1
            previous_candle_index = -2

        # Extract current and previous candles
        current_candle = applied_df.iloc[current_candle_index]
        previous_candle = applied_df.iloc[previous_candle_index]
        # Check for existing position
        existing_position = self.check_for_existing_position(instrument)

        # Define the position entry logic
        def create_position(position_type):
            fut_position_id, fut_current_price = self.add_fut_to_positions(
                instrument=instrument, interval=interval, position_type=position_type,
                instrument_position_type=position_type, broker_id=broker_id, broker=broker
            )
            buy_option_data = self.get_option_for_buying(
                instrument=instrument, position_type=position_type, fut_current_price=fut_current_price
            )
            sell_option_data = self.get_option_for_selling(
                instrument=instrument, position_type=position_type, fut_current_price=fut_current_price
            )
            buy_option_current_price, sell_option_current_price = self.add_opt_to_positions(
                buy_option_data=buy_option_data, sell_option_data=sell_option_data,
                interval=interval, broker_id=broker_id,
                broker=broker, fut_position_id=fut_position_id
            )
            return {
                "trade_type": "entry",
                "position_type": position_type,
                "fut_trade": {
                    "instrument": instrument,
                    "interval": interval,
                },
                "opt_buy": {
                    "buy_option_data": buy_option_data,
                    "fut_position_id": fut_position_id,
                    "buy_option_current_price": buy_option_current_price
                },
                "opt_sell": {
                    "sell_option_data": sell_option_data,
                    "fut_position_id": fut_position_id,
                    "sell_option_current_price": sell_option_current_price
                }
            }

        # Handle long position
        if current_candle.pos == 1 and previous_candle.pos != 1:
            if existing_position is None:
                payload = create_position(1)
                mqtt_publisher.publish_payload(payload)
            elif existing_position['instrument_position_type'] != 1:
                status, message, exit_payload = self.exit_existing_position(existing_position, broker_id, broker, interval)
                mqtt_publisher.publish_payload(exit_payload)
                if status:
                    payload = create_position(1)
                    mqtt_publisher.publish_payload(payload)
                else:
                    self.logs_controller.add_log(message)
            else:
                log_msg = f"Long Position for instrument already exists for long OID: {existing_position['observable_instrument_id']}"
                self.logs_controller.add_log(log_msg)

        # Handle short position
        elif current_candle.pos == -1 and previous_candle.pos != -1:
            if existing_position is None:
                payload = create_position(2)
                mqtt_publisher.publish_payload(payload)
            elif existing_position['instrument_position_type'] != 2:
                status, message = self.exit_existing_position(existing_position, broker_id, broker, interval)
                if status:
                    payload = create_position(2)
                    mqtt_publisher.publish_payload(payload)
                else:
                    self.logs_controller.add_log(message)
            else:
                log_msg = f"Short Position for instrument already exists for short OID: {existing_position['observable_instrument_id']}"
                self.logs_controller.add_log(log_msg)

    def get_option_for_buying(self, instrument, position_type, fut_current_price):
        instrument_types = {
            1: 'CE',
            2: 'PE'
        }
        instrument_type = instrument_types.get(position_type, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, zerodha_strike ASC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike DESC LIMIT 1; """,
            "angel_long_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike > %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike ASC LIMIT 1;""",
            "angel_short_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike < %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike DESC LIMIT 1;""",
            "shoonya_long_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price > %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price ASC LIMIT 1;""",
            "shoonya_short_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price < %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price DESC LIMIT 1;""",
            "alice_long_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price > %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price ASC LIMIT 1;""",
            "alice_short_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price < %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price DESC LIMIT 1;"""
        }
        zerodha_query = queries.get('zerodha_long_query' if position_type == 1 else 'zerodha_short_query', 'Unknown')
        angel_query = queries.get('angel_long_query' if position_type == 1 else 'angel_short_query', 'Unknown')
        shoonaya_query = queries.get('shoonya_long_query' if position_type == 1 else 'shoonya_short_query', 'Unknown')
        alice_query = queries.get('alice_long_query' if position_type == 1 else 'alice_short_query', 'Unknown')

        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query,
                           (instrument['zerodha_name'], instrument_type, fut_current_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(angel_query,
                           (instrument['angel_name'], str(fut_current_price) + "00", "%" + instrument_type))
            angel_option = cursor.fetchone()

            cursor.execute(shoonaya_query, (instrument['shoonya_name'], str(fut_current_price), instrument_type))
            shoonya_option = cursor.fetchone()

            cursor.execute(alice_query, (instrument['alice_symbol'], str(fut_current_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option, "angel_option": angel_option, "shoonya_option": shoonya_option,
                "alice_option": alice_option
            }

    def get_option_for_selling(self, instrument, position_type, fut_current_price):
        instrument_types = {
            1: 'PE',
            2: 'CE'
        }
        instrument_type = instrument_types.get(position_type, 'Unknown')

        queries = {
            "zerodha_long_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike > %s ORDER BY zerodha_expiry ASC, zerodha_strike ASC LIMIT 1; """,
            "zerodha_short_query": """ SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-OPT', 'BFO-OPT') AND zerodha_name = %s AND zerodha_instrument_type = %s AND zerodha_expiry >= CURDATE() AND zerodha_strike < %s ORDER BY zerodha_expiry ASC, zerodha_strike DESC LIMIT 1; """,
            "angel_long_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike > %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike ASC LIMIT 1;""",
            "angel_short_query": """SELECT * FROM angel_instruments WHERE angel_instrument_type = 'OPTIDX' AND angel_name = %s AND angel_expiry >= CURDATE() AND angel_strike < %s AND angel_symbol LIKE %s ORDER BY angel_expiry ASC, angel_strike DESC LIMIT 1;""",
            "shoonya_long_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price > %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price ASC LIMIT 1;""",
            "shoonya_short_query": """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type = 'OPTIDX' AND shoonya_symbol = %s AND shoonya_expiry >= CURDATE() AND shoonya_strike_price < %s AND shoonya_option_type = %s ORDER BY shoonya_expiry ASC, shoonya_strike_price DESC LIMIT 1;""",
            "alice_long_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price > %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price ASC LIMIT 1;""",
            "alice_short_query": """SELECT * FROM alice_blue_instruments WHERE alice_instrument_type = 'OPTIDX' AND alice_symbol = %s AND alice_expiry_date >= CURDATE() AND alice_strike_price < %s AND alice_option_type = %s ORDER BY alice_expiry_date ASC, alice_strike_price DESC LIMIT 1;"""
        }

        zerodha_query = queries.get('zerodha_long_query' if position_type == 2 else 'zerodha_short_query', 'Unknown')
        angel_query = queries.get('angel_long_query' if position_type == 2 else 'angel_short_query', 'Unknown')
        shoonaya_query = queries.get('shoonya_long_query' if position_type == 2 else 'shoonya_short_query', 'Unknown')
        alice_query = queries.get('alice_long_query' if position_type == 2 else 'alice_short_query', 'Unknown')

        with closing(self.conn.cursor()) as cursor:
            cursor.execute(zerodha_query,
                           (instrument['zerodha_name'], instrument_type, fut_current_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(angel_query,
                           (instrument['angel_name'], str(fut_current_price) + "00", "%" + instrument_type))
            angel_option = cursor.fetchone()

            cursor.execute(shoonaya_query, (instrument['shoonya_name'], str(fut_current_price), instrument_type))
            shoonya_option = cursor.fetchone()

            cursor.execute(alice_query, (instrument['alice_symbol'], str(fut_current_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option, "angel_option": angel_option, "shoonya_option": shoonya_option,
                "alice_option": alice_option
            }

    def add_opt_to_positions(self, buy_option_data, sell_option_data, interval, broker_id, broker,
                             fut_position_id):
        instrument_from_broker = {
            1: 'zerodha_option',
            2: 'angel_option',
            3: 'shoonya_option',
            4: 'alice_option'
        }
        broker_map = instrument_from_broker.get(broker_id, 'Unknown')
        buy_option_current_price = get_current_price(buy_option_data[broker_map], broker_id, broker)
        sell_option_current_price = get_current_price(sell_option_data[broker_map], broker_id, broker)
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "INSERT INTO opt_positions (associated_fut_id, option_instrument, instrument_position_type, position_entry_time, position_entry_price, position_qty, time_frame) VALUES (%s,%s,%s,NOW(),%s,%s,%s)",
                (fut_position_id, json.dumps(buy_option_data, default=str), 1,
                 buy_option_current_price, 1, interval))
            cursor.execute(
                "INSERT INTO opt_positions (associated_fut_id, option_instrument, instrument_position_type, position_entry_time, position_entry_price, position_qty, time_frame) VALUES (%s,%s,%s,NOW(),%s,%s,%s)",
                (fut_position_id, json.dumps(sell_option_data, default=str), 2,
                 sell_option_current_price, 1, interval))
        self.conn.commit()
        return buy_option_current_price, sell_option_current_price

    def get_all_active_fut_positions(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT * FROM fut_positions WHERE position_exit_time IS NULL')
            active_trades = cursor.fetchall()
        return active_trades

    def get_force_exit_payload(self, position, broker_id, broker, interval):
        exit_price = get_current_price(position, broker_id, broker)
        position_type = position['instrument_position_type']
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT * FROM opt_positions WHERE associated_fut_id = %s', position['position_id'])
            opt_positions = cursor.fetchall()
            buy_option_data, sell_option_data = None, None
            buy_option_ltp, sell_option_ltp = None, None

            instrument_from_broker = {
                1: 'zerodha_option',
                2: 'angel_option',
                3: 'shoonya_option',
                4: 'alice_option'
            }
            broker_map = instrument_from_broker.get(broker_id, 'Unknown')

            for opt_position in opt_positions:
                ins = json.loads(opt_position['option_instrument'])[broker_map]
                option_ltp = get_current_price(ins, broker_id, broker)

                if opt_position['instrument_position_type'] == 1:
                    buy_option_data = opt_position
                    buy_option_ltp = option_ltp
                elif opt_position['instrument_position_type'] == 2:
                    sell_option_data = opt_position
                    sell_option_ltp = option_ltp

            payload = {
                "trade_type": "intraday_exit",
                "position_type": position_type,
                "fut_position_id": position['position_id'],
                "fut_trade": {
                    "instrument": position,
                    "interval": interval,
                    "exit_price": exit_price,
                },
                "opt_buy": {
                    "buy_option_data": buy_option_data,
                    "fut_position_id": position['position_id'],
                    "exit_price": buy_option_ltp
                },
                "opt_sell": {
                    "sell_option_data": sell_option_data,
                    "fut_position_id": position['position_id'],
                    "exit_price": sell_option_ltp
                }
            }
        return payload
