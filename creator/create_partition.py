import datetime
import time

import pymysql
from .errors import NoCreateTimeError
from utils import MonthCreator, sql_index_processing, sub_month, sql_name_processing, add_month
from logger import Logger
from concurrent.futures import ThreadPoolExecutor, wait, ALL_COMPLETED
from prettytable import PrettyTable
from dbutils.pooled_db import PooledDB

'''
用于创建分区表的脚本
功能：
    1.检测数据库中是否有part_col列，并判断是否为TIMESTAMP，否则退出
    3.将旧表重命名
    2.根据part_col列查找当前表中的最早的数据，最晚的数据，并获取其中的年和月
    
    4.获取给定表的建表语句
    5.根据建表语句和数据中的最早日期，最晚日期，创建分区表，替代原表
    6.将原表数据重新插入到新表中
'''


class Creator:
    logger = Logger(stream=True,file=True).get_logger()

    def __init__(self, host, username, password, table, db, part_col, port=3306, hot_table_month=2,
                 bak_table=None, new_bak_table_tail="_htr", is_test=False, mul_thread=True, extend_partition=24):
        self.host = host
        self.port = port
        self.db = db
        self.user = username
        self.passwd = password
        self.table = table
        self.part_col = part_col
        self.bak_table = bak_table  # 已有备份表情况下备份表名
        self.new_name = None  # 热表重命名后的新名
        self.new_bak_name = None  # 已有备份表情况下，重命名后的新名
        self.version = None
        self.new_bak_table_tail = new_bak_table_tail  # 新冷表的重命名在最后添加的别名
        self.hot_table_month = hot_table_month  # 新的热表保留几个月数据
        self.data_count_before = 0  # 处理之前统计数据条目
        self.data_count_after = 0  # 处理之后统计数据条目
        self.is_test = is_test  # 是否为测试模式
        self.conn_pool = None  # 多线程的时候使用连接池
        self.start_time = None  # 所有数据中的最早时间
        self.mul_thread = mul_thread  # 是否开启多线程
        self.extend_partition = extend_partition  # 扩展多少个分区

    def __enter__(self):
        self._conn = pymysql.connect(host=self.host, port=self.port, autocommit=True, database=self.db,
                                     user=self.user, passwd=self.passwd, connect_timeout=10)
        self._cursor = self._conn.cursor()
        self._dict_cursor = self._conn.cursor(pymysql.cursors.DictCursor)
        # self.check_version()
        self.logger.info(f"Connect to {self.host} success")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._conn.close()

    def check_version(self):
        self._dict_cursor.execute("select version();")
        self.version = self._dict_cursor.fetchone().get("version()").split(".")[0]

    def check_start_time(self, table):
        sql = f"select min({self.part_col}) as start_time from {table} "
        self._dict_cursor.execute(sql)
        res = self._dict_cursor.fetchone().get("start_time")
        return res

    def check_end_time(self, table):
        sql = f"select max({self.part_col}) as end_time from {table} "
        self._dict_cursor.execute(sql)
        res = self._dict_cursor.fetchone().get("end_time")
        return res

    def get_create_table(self, table):
        sql = f"SHOW CREATE TABLE {table}"
        self._dict_cursor.execute(sql)
        res = self._dict_cursor.fetchone()
        return res.get("Create Table")

    def check_timestamp(self):
        # 检查是否有part_col这个字段，并且类型为timestamp
        sql = f"desc {self.table}"
        self._dict_cursor.execute(sql)
        res = self._dict_cursor.fetchall()
        res = [i for i in res if i.get("Field") == self.part_col]
        if len(res) > 0:
            res = res[0]
        else:
            self.logger.error(f"there is no column named {self.part_col} in {self.table} ")
            return False
        if res.get("Type").lower() == "timestamp":
            self.logger.info(f"the type of  {self.part_col} is already timestamp")
            return True
        else:
            self.logger.error(f"the type of {self.part_col} is {res.get('Type')}")
            return False
            # 注释部分为自动修改字段类型为TIMESTAMP 为了安全性 这部分由Mysql DBA手动执行
            # sql = f"alter table {self.table} modify create_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP;"
            # try:
            #     self._cursor.execute(sql)
            # except pymysql.err.OperationalError as e:
            #     print(e)
            #     self._conn.rollback()
            #     return False
            # return True

    def rename_table(self, table, tail="_old"):
        table_name = f"{table}{tail}"
        sql = f"RENAME TABLE {table} TO {table_name};"
        if self.sql_executor(sql):
            return True, table_name
        return False, table_name

    def get_create_partition_table_sql(self, source_table, num=24, htr=False):
        """
        :param source_table: 源表
        :param num: 往后面生成多少个分区
        :param htr: 生成的是热表还是冷表 False 为热表
        :return:建表语句（分区表）
        """
        create_table_sql = self.get_create_table(source_table)
        if htr:
            # 创建冷表
            s_time = self.check_start_time(source_table)
            # e_time = self.check_end_time(source_table)
            e_time = datetime.datetime.now()
            self.logger.info(f"Old Table Start Time:{s_time}")
            self.start_time = s_time

        else:
            # 创建热表 最多保留2个月数据，仅向前移动一个月
            s_time = sub_month(datetime.datetime.now(), self.hot_table_month - 1)
            e_time = datetime.datetime.now()

        months = MonthCreator(start=s_time, end=e_time, extend=num)  # 归档表
        if htr:
            self.logger.info(f"The Cold Table Use MonthCreator:{months}")
        else:
            self.logger.info(f"The Hot Table Use MonthCreator:{months}")
        partition_sql = create_table_sql + " " + f"PARTITION BY RANGE ( UNIX_TIMESTAMP({self.part_col}) ) ("
        # partitions_sql_do = ','.join(
        #     [f"PARTITION p{m.year}_{m.month} VALUES LESS THAN ( UNIX_TIMESTAMP('{m}') )" for m in months])
        mm = []
        for m in months:
            current_m = add_month(m, 1)
            s = "" if len(str(m.month)) == 2 else "0"
            mm.append(f"PARTITION P{m.year}_{s}{m.month} VALUES LESS THAN ( UNIX_TIMESTAMP('{current_m}') )")
        partitions_sql_do = ",".join(mm)
        partition_sql = partition_sql + partitions_sql_do + ");"
        partition_sql = sql_index_processing(partition_sql, self.part_col)
        if htr:
            partition_sql = sql_name_processing(partition_sql, self.table + self.new_bak_table_tail)
        else:
            partition_sql = sql_name_processing(partition_sql, self.table)
        return partition_sql

    def create_partition_table(self, num):
        if self.is_test:
            source_table = self.table
            bak_table = self.bak_table
        else:
            source_table = self.new_name
            bak_table = self.new_bak_name

        sql = self.get_create_partition_table_sql(source_table, num, False)
        if self.bak_table:
            sql2 = self.get_create_partition_table_sql(bak_table, num, True)
        else:
            sql2 = self.get_create_partition_table_sql(source_table, num, True)
        if not self.sql_executor(sql):
            self.logger.error("Create bak Table Error")
        if not self.sql_executor(sql2):
            self.logger.error("Create new Table Error")

    def sql_executor(self, sql):
        self.logger.info(sql)
        if self.is_test:
            return True
        try:
            self._cursor.execute(sql)
            return True
        except pymysql.err.OperationalError as e:
            self.logger.error(e)
            self._conn.rollback()
            return False

    def data_process(self):
        # 插入热表
        cut_time = sub_month(datetime.datetime.now(), self.hot_table_month - 1)  # 上个月的1日
        sql = f"insert into {self.table} select * from {self.new_name} where {self.part_col}>='{cut_time}'"
        self.sql_executor(sql)
        if self.bak_table:
            sql2 = f"insert into {self.table} select * from {self.new_bak_name} where {self.part_col}>='{cut_time}'"
            self.sql_executor(sql2)
        # 插入冷表
        sql = f"insert into {self.table}{self.new_bak_table_tail} select * from {self.new_name} where {self.part_col}<'{cut_time}'"
        self.sql_executor(sql)
        if self.bak_table:
            sql2 = f"insert into {self.table}{self.new_bak_table_tail} select * from {self.new_bak_name} where {self.part_col}<'{cut_time}'"
            self.sql_executor(sql2)

    def data_process_th(self):
        self.logger.info("start threading data process...")
        sqls = []
        tasks = []
        t = ThreadPoolExecutor(max_workers=4)
        cut_time = sub_month(datetime.datetime.now(), self.hot_table_month - 1)  # 上个月的1日
        # 插入热表
        sqls.append(f"insert into {self.table} select * from {self.new_name} where {self.part_col}>='{cut_time}'")
        if self.bak_table:
            sqls.append(
                f"insert into {self.table} select * from {self.new_bak_name} where {self.part_col}>='{cut_time}'")
        month_creator = MonthCreator(start=self.start_time, end=cut_time, extend=0)
        for m in month_creator:
            m2 = add_month(m, 1)
            sqls.append(
                f"insert into {self.table}{self.new_bak_table_tail} select * from {self.new_name} where {self.part_col}>='{m}'and {self.part_col}<'{m2}'")
            if self.bak_table:
                sqls.append(
                    f"insert into {self.table}{self.new_bak_table_tail} select * from {self.new_bak_name} where {self.part_col}>='{m}'and {self.part_col}<'{m2}'")

        self.conn_pool = PooledDB(
            creator=pymysql, maxconnections=15, mincached=0, maxcached=20, maxshared=0,
            blocking=True, ping=5, host=self.host, port=3306, user=self.user, password=self.passwd,
            database=self.db, autocommit=True
        )
        for sql in sqls:
            tasks.append(t.submit(self.data_process_th_worker, sql=sql))
        wait(tasks, return_when=ALL_COMPLETED)

    def data_process_th_worker(self, sql=""):
        self.logger.info(sql)
        if self.is_test:
            return
        conn = self.conn_pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql)
        except Exception as e:
            self.logger.error(f"Insert Error :{e}")
        finally:
            cursor.close()
            

    def before_run(self):
        self._cursor.execute(f"select count(1) from {self.table}")
        count1 = self._cursor.fetchone()[0]
        if self.bak_table is not None:
            self._cursor.execute(f"select count(1) from {self.bak_table}")
            count2 = self._cursor.fetchone()[0]
        else:
            count2 = 0
        self.data_count_before = count1 + count2
        self.logger.info(f"Before Partition:{self.table}:{count1}  "
                         f"{self.bak_table}:{count2} total:{self.data_count_before}")

    def check_partition(self, table):
        sql = f"SELECT PARTITION_NAME,PARTITION_DESCRIPTION FROM " \
              f"INFORMATION_SCHEMA.PARTITIONS WHERE TABLE_NAME = '{table}'; "
        self._dict_cursor.execute(sql)
        partitions = self._dict_cursor.fetchall()
        table_s = PrettyTable(['编号', '分区', '时间'])
        for i, partition in enumerate(partitions):
            table_s.add_row([i, partition['PARTITION_NAME'],
                             datetime.datetime.fromtimestamp(int(partition['PARTITION_DESCRIPTION']))])
        self.logger.info(f"{table} PARTITIONS:\n" + str(table_s))

    def after_run(self):
        if self.mul_thread:
            self.__enter__()
        self._cursor.execute(f"select count(1) from {self.table}")
        count1 = self._cursor.fetchone()[0]
        self._cursor.execute(f"select count(1) from {self.table + self.new_bak_table_tail}")
        count2 = self._cursor.fetchone()[0]
        self.data_count_after = count1 + count2
        self.check_partition(self.table)
        self.check_partition(self.table + self.new_bak_table_tail)
        self.logger.info(f"Before partition the num of data is {self.data_count_before}")
        self.logger.info(f"After partition the num of data is {self.data_count_after}")
        if self.data_count_before != self.data_count_after:
            raise Exception("Data Count Are Not Matched! orz ")
        else:
            self.logger.info("Data number check success! :-)")

    def run(self):
        self.logger.info("starting...")
        self.before_run()
        # 检查分区字段
        res = self.check_timestamp()
        if not res:
            raise NoCreateTimeError("")
        # 重命名并进行检查
        success, self.new_name = self.rename_table(self.table)
        if not success:
            self.logger.error(f"Rename table {self.table} to {self.new_name}")
            return
        if self.bak_table:
            success, self.new_bak_name = self.rename_table(self.bak_table)
            if not success:
                self.logger.error(f"Rename table {self.bak_table} to {self.new_bak_name}")
                return

        self.create_partition_table(self.extend_partition)
        s = time.time()
        if self.mul_thread:
            self.data_process_th()
        else:
            self.data_process()
        self.logger.info(f"Data process spend:{time.time() - s}s")
        if not self.is_test:
            self.after_run()
