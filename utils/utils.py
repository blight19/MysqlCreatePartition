#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/10/22

import datetime
import math
import re


def sql_index_processing(sql: str,part_col:str):
    def re_add_ctime(matched):
        m = matched.group(1) + f',`{part_col}`'
        return f"PRIMARY KEY ({m})"

    a = re.sub(r"PRIMARY\s+KEY\s+\((.*?)\)", re_add_ctime, sql, flags=re.IGNORECASE)
    return a


def sql_name_processing(sql: str, name: str):
    def re_name(matched):
        m = matched.group(1)

        return f"CREATE TABLE `{name}`"

    a = re.sub(r"CREATE\s+TABLE\s+`(.*?)`", re_name, sql, flags=re.IGNORECASE)
    return a


def add_month(base: datetime.datetime, month: int) -> datetime.datetime:
    current_month = base.month
    new_month = current_month + month
    current_year = base.year
    y = math.floor(new_month / 12)
    m = new_month % 12
    if m == 0:
        m = 12
        y = y - 1
    return datetime.datetime(year=current_year + y, month=m, day=1)


def sub_month(base: datetime.datetime, month: int) -> datetime.datetime:
    current_month = base.month
    current_year = base.year
    calc_month = current_month - month  # calc_month
    if calc_month <= 0:
        y = math.floor(calc_month / 12) * -1
        m = calc_month + 12 * (y)
        if m == 0:
            return datetime.datetime(year=current_year - y - 1, month=12, day=1)
        return datetime.datetime(year=current_year - y, month=m, day=1)
    else:
        return datetime.datetime(year=current_year, month=calc_month, day=1)


class MonthCreator:
    def __init__(self, start: datetime.datetime, end: datetime.datetime, extend: int):
        self.start = start
        self.end = end
        self.extend = extend

    def __len__(self):
        if self.end < self.start:
            raise Exception(f"End must latter than start!end:{self.end},start:{self.start}")
        return (self.end.year - self.start.year) * 12 + (self.end.month - self.start.month) + self.extend

    def __iter__(self):
        for item in range(len(self)):
            yield add_month(self.start, item)

    def __getitem__(self, item):
        if item > (len(self) - 1):
            raise IndexError
        if item < 0:
            item = item + len(self)
        return add_month(self.start, item)

    def __repr__(self):
        return f"<MonthCreator start:{self.start} end:{self.end} extend:{self.extend} final:{self[-1]}>"


if __name__ == '__main__':
    m = MonthCreator(
        start=datetime.datetime(year=2000, month=1, day=5),
        end=datetime.datetime(year=2001, month=3, day=4),
        extend=3
    )
    print("m in MonthCreator:")
    for i in m:
        print(i)

    print("The length of MonthCreator",len(m))
    print(m)
    # for i in range(100):
    #     print(i, " ", sub_month(datetime.datetime.now(), i))
    # print(sub_month(datetime.datetime.now(), 1))
