#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/11/12
from creator import Creator

if __name__ == '__main__':

    with Creator(host="127.0.0.1", username="user", password="pwd", db="xxx",
                 table="xxx",extend_partition=24,

                 bak_table="xxx",
                 is_test=False, mul_thread=False, hot_table_month=3,part_col="new_create_time"
                 ) as c:
        c.run()
