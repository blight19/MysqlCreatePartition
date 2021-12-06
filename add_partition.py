#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/11/29
from logger import Logger
import pymysql


class PartitionAdder:
    logger = Logger(stream=True).get_logger()

    def __init__(self, host, username, password, table, db, port=3306,
                 bak_table=None, is_test=False, extend_partition=24):
        self.host = host
        self.port = port
        self.db = db
        self.user = username
        self.passwd = password
        self.table = table
        self.bak_table = bak_table  # 已有备份表情况下备份表名
        self.is_test = is_test  # 是否为测试模式
        self.extend_partition = extend_partition  # 扩展多少个分区

    def __enter__(self):
        self._conn = pymysql.connect(host=self.host, port=self.port, autocommit=True, database=self.db,
                                     user=self.user, passwd=self.passwd, connect_timeout=10)
        self._cursor = self._conn.cursor()
        self._dict_cursor = self._conn.cursor(pymysql.cursors.DictCursor)

        self.logger.info(f"Connect to {self.host} success")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def get_create_table(self, table):
        sql = f"SHOW CREATE TABLE {table}"
        self._dict_cursor.execute(sql)
        res = self._dict_cursor.fetchone()
        return res.get("Create Table")

    def run(self):
        print(self.get_create_table(self.table))



if __name__ == '__main__':
    with PartitionAdder(host="10.5.37.22", username="dbamanager", password="123Gwmfc56", db="atc",
                        table="ep_psbc_order_number", extend_partition=24,
                        bak_table="ep_psbc_order_number_htr",
                        is_test=False,
                        ) as c:
        c.run()
