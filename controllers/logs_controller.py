import datetime
from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class LogsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)
        self.create_logs_table()

    def create_logs_table(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
                            CREATE TABLE IF NOT EXISTS system_logs (
                                log_id INT AUTO_INCREMENT PRIMARY KEY,
                                log_content LONGTEXT,
                                log_date_time DATETIME
                            )
                        ''')
            self.conn.commit()

    def add_log(self, log_content):
        print(log_content)
        with closing(self.conn.cursor()) as cursor:
            cursor.execute('''
            INSERT INTO system_logs (log_content,log_date_time)
            VALUES (%s,%s)
            ''', (log_content, datetime.datetime.now()))
        self.conn.commit()
