#!/usr/bin/env python
# -*- coding:utf-8 -*-
# author:yanshushuang
# datetime:2021/12/2
import pymysql

conn = pymysql.connect(host="10.5.37.22", user="dbamanager", password="123Gwmfc56", db="atc")

cursor = conn.cursor()
cursor.execute("select id from ep_package_order_batch")
ids1 = set([x[0] for x in cursor.fetchall()])

cursor.execute("select id from ep_package_order_batch_htr")
ids2 = set([x[0] for x in cursor.fetchall()])

cursor.execute("select id from ep_package_order_batch_old")
ids3 = set([x[0] for x in cursor.fetchall()])

full_ids = ids2 ^ ids1 & ids3
print(len(full_ids))
batch =10000
i=0
full_ids = list(full_ids)
while True:
    print(i)
    full_ids_str = ','.join(full_ids[i:i+batch])

    sql1 = f'insert into ep_package_order_batch_old select * from ep_package_order_batch where id in ({full_ids_str})'
    cursor.execute(sql1)
    conn.commit()
    sql1 = f'insert into ep_package_order_batch_old select * from ep_package_order_batch_htr where id in ({full_ids_str})'
    cursor.execute(sql1)
    conn.commit()
    i = i + batch
