#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/11/4
import pymysql
import socket

class PartitionMover:
    def __init__(self, host, user, passwd, db, port=3306):
        self.host = host
        self.user = user
        self.passwd = passwd
        self.db = db
        self.port = port

    def __enter__(self):
        self._conn = pymysql.connect(host=self.host, port=self.port, autocommit=True, database=self.db,
                                     user=self.user, passwd=self.passwd, connect_timeout=10)
        self._cursor = self._conn.cursor()
        self._dict_cursor = self._conn.cursor(pymysql.cursors.DictCursor)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()
