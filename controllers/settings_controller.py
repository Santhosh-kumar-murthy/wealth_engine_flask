import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class MqttSettingsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_settings_table()

    def create_settings_table(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''
                               CREATE TABLE IF NOT EXISTS wealthi_settings (
                                   mqtt_host VARCHAR(255),
                                   mqtt_port VARCHAR(255),
                                   mqtt_topic VARCHAR(255)
                               )
                           ''')
            self.conn.commit()

    def get_settings(self):
        with self.conn.cursor() as cursor:
            cursor.execute('''SELECT * FROM wealthi_settings''')
            settings = cursor.fetchone()
        return settings
