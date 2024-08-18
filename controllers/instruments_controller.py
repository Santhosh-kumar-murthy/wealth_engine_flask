from contextlib import closing

import pymysql
from pymysql.cursors import DictCursor

from database_config import db_config


class InstrumentsController:
    def __init__(self):
        self.conn = pymysql.connect(**db_config, cursorclass=DictCursor)

    def get_observable_instruments(self):
        with closing(self.conn.cursor()) as cursor:
            cursor.execute(
                "SELECT * FROM observable_instruments")
            observable_instruments = cursor.fetchall()
        return observable_instruments
