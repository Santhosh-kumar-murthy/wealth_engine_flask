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
        self.create_positions_table()
        self.logs_controller = LogsController()

    def create_positions_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS positions (
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
                                '1 = FUT BUY\r\n2 = FUT SELL\r\n3 = OPT BUY\r\n4 = OPT SELL',
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

    def get_nearby_fut(self, instrument):
        future_instrument_list = {}
        with closing(self.conn.cursor()) as cursor:
            query = """
                SELECT * 
                FROM alice_blue_instruments 
                WHERE (alice_instrument_type = 'FUTIDX' OR alice_instrument_type = 'IF') AND alice_symbol = %s
                AND DATE(alice_expiry_date) >= curdate() ORDER BY alice_expiry_date ASC LIMIT 1;
                """
            cursor.execute(query, instrument['search_key'])
            # Fetching all results
            results = cursor.fetchone()
            future_instrument_list['alice_blue'] = results

            query = """
                SELECT * FROM zerodha_instruments WHERE zerodha_segment IN ('NFO-FUT', 'BFO-FUT') AND zerodha_name = %s
                AND DATE(zerodha_expiry) >= curdate() ORDER BY zerodha_expiry ASC LIMIT 1; """
            cursor.execute(query, instrument['search_key'])
            # Fetching all results
            results = cursor.fetchone()
            future_instrument_list['zerodha'] = results

            query = """SELECT * FROM angel_instruments WHERE angel_instrument_type='FUTIDX' AND angel_name = %s
                            AND DATE(angel_expiry) >= curdate() ORDER BY angel_expiry ASC LIMIT 1; """
            cursor.execute(query, instrument['search_key'])
            # Fetching all results
            results = cursor.fetchone()
            future_instrument_list['angel_one'] = results

            query = """SELECT * FROM shoonya_instruments WHERE shoonya_instrument_type='FUTIDX' AND shoonya_symbol = %s
                            AND DATE(shoonya_expiry) >= curdate() ORDER BY shoonya_expiry ASC LIMIT 1; """
            cursor.execute(query, instrument['search_key'])
            # Fetching all results
            results = cursor.fetchone()
            future_instrument_list['shoonya'] = results

        return future_instrument_list

    def add_fut_to_positions(self, instrument, interval, position_type, instrument_position_type, broker_id, broker):
        fut_obj = self.get_nearby_fut(instrument)
        fut_inst = {}
        if broker_id == 1:
            fut_inst = fut_obj['zerodha']
        elif broker_id == 2:
            fut_inst = fut_obj['angel_one']
        elif broker_id == 3:
            fut_inst = fut_obj['shoonya']
        elif broker_id == 4:
            fut_inst = fut_obj['alice_blue']

        current_price = get_current_price(fut_inst, broker_id, broker)

        with closing(self.conn.cursor()) as cursor:
            sql = '''
                INSERT INTO positions (
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
                fut_obj['zerodha']['zerodha_instrument_token'],
                fut_obj['zerodha']['zerodha_trading_symbol'],
                fut_obj['zerodha']['zerodha_name'],
                fut_obj['zerodha']['zerodha_exchange'],
                fut_obj['angel_one']['angel_token'],
                fut_obj['angel_one']['angel_symbol'],
                fut_obj['angel_one']['angel_name'],
                fut_obj['angel_one']['angel_exchange_segment'],
                fut_obj['shoonya']['shoonya_token'],
                fut_obj['shoonya']['shoonya_trading_symbol'],
                fut_obj['shoonya']['shoonya_symbol'],
                fut_obj['shoonya']['shoonya_exchange'],
                fut_obj['alice_blue']['alice_token'],
                fut_obj['alice_blue']['alice_trading_symbol'],
                fut_obj['alice_blue']['alice_symbol'],
                fut_obj['alice_blue']['alice_exchange'],
                instrument_position_type,
                position_type,
                current_price,
                fut_obj['shoonya']['shoonya_lot_size'],
                1,
                interval
            ))
            self.conn.commit()
            return fut_obj, current_price

    def check_for_existing_position(self, instrument):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                'SELECT * FROM positions WHERE observable_instrument_id = %s AND position_exit_time IS NULL',
                instrument['o_id'])
            active_trades = cursor.fetchall()
        return active_trades

    def exit_existing_position(self, existing_position, broker_id, broker):
        try:
            exit_price = float(get_current_price(existing_position, broker_id, broker))
            entry_price = float(existing_position['position_entry_price'])
            position_type = existing_position['position_type']
            profit = (exit_price - entry_price) if position_type == 1 else (entry_price - exit_price)
            with self.conn.cursor() as cursor:
                # Update the future position
                cursor.execute('''
                    UPDATE positions
                    SET position_exit_price = %s, position_exit_time = NOW(), profit = %s
                    WHERE position_id = %s
                ''', (exit_price, profit, existing_position['position_id']))
                self.conn.commit()
            return True, 'Success', {
                "existing_position": existing_position,
                "exit_price": exit_price,
                "position_type": position_type
            }
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
        existing_positions = self.check_for_existing_position(instrument)

        # Define the position entry logic
        def create_position(position_type):
            fut_obj, fut_current_price = self.add_fut_to_positions(
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
                broker=broker,
                instrument=instrument,
                position_type=position_type,
            )
            return {
                "trade_type": "entry",
                "position_type": position_type,
                "interval": interval,
                "fut_trade": {
                    "instrument": fut_obj,
                    "fut_current_price": fut_current_price
                },
                "opt_buy": {
                    "buy_option_data": buy_option_data,
                    "buy_option_current_price": buy_option_current_price
                },
                "opt_sell": {
                    "sell_option_data": sell_option_data,
                    "sell_option_current_price": sell_option_current_price
                }
            }

        # # simulation
        # current_candle.pos = -1
        # previous_candle.pos = 1

        # Handle long position
        if current_candle.pos == 1 and previous_candle.pos != 1:
            if not existing_positions:
                payload = create_position(1)
                mqtt_publisher.publish_payload(payload)
            else:
                take_position_flag = False
                for existing_position in existing_positions:
                    if existing_position['position_type'] != 1:
                        take_position_flag = True
                        status, message, exit_payload = self.exit_existing_position(existing_position, broker_id,
                                                                                    broker)
                        if status:
                            mqtt_publisher.publish_payload(exit_payload)
                        else:
                            self.logs_controller.add_log(message)
                    else:
                        log_msg = f"Long Position for instrument already exists for long OID: {existing_position['observable_instrument_id']}"
                        self.logs_controller.add_log(log_msg)
                if take_position_flag:
                    payload = create_position(1)
                    mqtt_publisher.publish_payload(payload)

        # Handle short position
        elif current_candle.pos == -1 and previous_candle.pos != -1:
            if not existing_positions:
                payload = create_position(2)
                mqtt_publisher.publish_payload(payload)
            else:
                take_position_flag = False
                for existing_position in existing_positions:
                    if existing_position['position_type'] != 2:
                        take_position_flag = True
                        status, message, exit_payload = self.exit_existing_position(existing_position, broker_id,
                                                                                    broker)
                        if status:
                            mqtt_publisher.publish_payload(exit_payload)
                        else:
                            self.logs_controller.add_log(message)
                    else:
                        log_msg = f"Short Position for instrument already exists for short OID: {existing_position['observable_instrument_id']}"
                        self.logs_controller.add_log(log_msg)
                if take_position_flag:
                    payload = create_position(2)
                    mqtt_publisher.publish_payload(payload)

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
                           (instrument['search_key'], instrument_type, fut_current_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(angel_query,
                           (instrument['search_key'], str(int(float(fut_current_price))) + "00", "%" + instrument_type))
            angel_option = cursor.fetchone()

            cursor.execute(shoonaya_query, (instrument['search_key'], str(fut_current_price), instrument_type))
            shoonya_option = cursor.fetchone()

            cursor.execute(alice_query, (instrument['search_key'], str(fut_current_price), instrument_type))
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
                           (instrument['search_key'], instrument_type, fut_current_price))
            zerodha_option = cursor.fetchone()

            cursor.execute(angel_query,
                           (instrument['search_key'], str(int(float(fut_current_price))) + "00", "%" + instrument_type))
            angel_option = cursor.fetchone()

            cursor.execute(shoonaya_query, (instrument['search_key'], str(fut_current_price), instrument_type))
            shoonya_option = cursor.fetchone()

            cursor.execute(alice_query, (instrument['search_key'], str(fut_current_price), instrument_type))
            alice_option = cursor.fetchone()
            return {
                "zerodha_option": zerodha_option, "angel_option": angel_option, "shoonya_option": shoonya_option,
                "alice_option": alice_option
            }

    def add_opt_to_positions(self, buy_option_data, sell_option_data, interval, broker_id, broker,
                             instrument, position_type):
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
                "INSERT INTO positions (observable_instrument_id, zerodha_instrument_token, zerodha_trading_symbol, zerodha_name, zerodha_exchange, "
                "angel_token, angel_symbol, angel_name, angel_exchange, "
                "shoonya_token, shoonya_trading_symbol, shoonya_name, shoonya_exchange, "
                "alice_token, alice_trading_symbol, alice_name, alice_exchange, "
                "instrument_position_type,position_type, position_entry_time, position_entry_price, "
                "lot_size,position_qty, time_frame) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s)",
                (instrument['o_id'], buy_option_data['zerodha_option']['zerodha_instrument_token'],
                 buy_option_data['zerodha_option']['zerodha_trading_symbol'],
                 buy_option_data['zerodha_option']['zerodha_name'],
                 buy_option_data['zerodha_option']['zerodha_exchange'],
                 buy_option_data['angel_option']['angel_token'], buy_option_data['angel_option']['angel_symbol'],
                 buy_option_data['angel_option']['angel_name'],
                 buy_option_data['angel_option']['angel_exchange_segment'],
                 buy_option_data['shoonya_option']['shoonya_token'],
                 buy_option_data['shoonya_option']['shoonya_trading_symbol'],
                 buy_option_data['shoonya_option']['shoonya_symbol'],
                 buy_option_data['shoonya_option']['shoonya_exchange'],
                 buy_option_data['alice_option']['alice_token'],
                 buy_option_data['alice_option']['alice_trading_symbol'],
                 buy_option_data['alice_option']['alice_symbol'], buy_option_data['alice_option']['alice_exchange'],
                 3,
                 position_type,
                 buy_option_current_price, buy_option_data['zerodha_option']['zerodha_lot_size'], 1, interval))
            cursor.execute(
                "INSERT INTO positions (observable_instrument_id, zerodha_instrument_token, zerodha_trading_symbol, zerodha_name, zerodha_exchange, "
                "angel_token, angel_symbol, angel_name, angel_exchange, "
                "shoonya_token, shoonya_trading_symbol, shoonya_name, shoonya_exchange, "
                "alice_token, alice_trading_symbol, alice_name, alice_exchange, "
                "instrument_position_type,position_type, position_entry_time, position_entry_price, lot_size,position_qty, time_frame) VALUES "
                "(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s)",
                (instrument['o_id'], buy_option_data['zerodha_option']['zerodha_instrument_token'],
                 sell_option_data['zerodha_option']['zerodha_trading_symbol'],
                 sell_option_data['zerodha_option']['zerodha_name'],
                 sell_option_data['zerodha_option']['zerodha_exchange'],
                 sell_option_data['angel_option']['angel_token'], sell_option_data['angel_option']['angel_symbol'],
                 sell_option_data['angel_option']['angel_name'],
                 sell_option_data['angel_option']['angel_exchange_segment'],
                 sell_option_data['shoonya_option']['shoonya_token'],
                 sell_option_data['shoonya_option']['shoonya_trading_symbol'],
                 sell_option_data['shoonya_option']['shoonya_symbol'],
                 sell_option_data['shoonya_option']['shoonya_exchange'],
                 sell_option_data['alice_option']['alice_token'],
                 sell_option_data['alice_option']['alice_trading_symbol'],
                 sell_option_data['alice_option']['alice_symbol'], sell_option_data['alice_option']['alice_exchange'],
                 4, position_type,
                 sell_option_current_price, sell_option_data['zerodha_option']['zerodha_lot_size'], 1, interval))
        self.conn.commit()
        return buy_option_current_price, sell_option_current_price
