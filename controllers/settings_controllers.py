from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class SettingsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def get_active_broker(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''SELECT * FROM brokers WHERE broker_system_use_status = 1''')
            all_active_brokers = cursor.fetchone()
        return all_active_brokers

    def get_time_frame_settings(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('SELECT setting_value as active_time_frame FROM settings WHERE setting_name = %s',
                           'active_time_frame')
            time_frame = cursor.fetchone()
        return time_frame
