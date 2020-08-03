#!/usr/bin/env python
import threading
import traceback

import pymysql
from pymongo import MongoClient, ReplaceOne
from DBUtils.PooledDB import PooledDB

from config import G_CONFIG
from utils.utls import logger


class IpToMongoDB(object):
    """
    数据设计结构:{"ip": str,  IP
                "type": ,IP类型，包括家庭宽带、数据中心、移动网络、企业专线、校园单位、未知
                "risk_tag": 风险标签，包括秒拨、代理、数据中心、无
                "risk_score": 风险分数，范围0-100，分数越高被黑产持有的概率也就越高
                "risk_level":风险等级，包括高、中、低、无
                "country": 国家
                "province": 省份
                "city": 城市
                "district": 区县
                 "owner": 运营商
                 "latitude": 纬度
                 "longitude": 经度
                 "adcode": 行政区划代码
                 "areacode": 国家编码
                 "continent": 大洲
                }
    """
    _instance = None

    _instance_lock = threading.Lock()

    @classmethod
    def get_instance(cls):
        with cls._instance_lock:  # 加锁
            if not cls._instance:
                logger.info("mongo has not instance")
                cls._instance = IpToMongoDB()
        return cls._instance

    def __init__(self):
        logger.info("mongo init start")
        self.conn = MongoClient(G_CONFIG.mongodb["uri"])
        self.conn.get_database(G_CONFIG.mongodb["db"]).get_collection(G_CONFIG.mongodb["collection"]). \
            create_index([("ip", 1)], unique=True)
        logger.info("mongo init finish")

    def get_conn(self):
        """
        返回collection的连接
        :return:
        """
        db_name = G_CONFIG.mongodb["db"]
        coll_name = G_CONFIG.mongodb["collection"]
        logger.info("get_conn")
        return self.conn[db_name][coll_name]

    def batch_insert(self, data_list):
        """
        批量插入信息
        :param data_list: [{"ip": str,  IP
                "type": ,IP类型，包括家庭宽带、数据中心、移动网络、企业专线、校园单位、未知
                "risk_tag": 风险标签，包括秒拨、代理、数据中心、无
                "risk_score": 风险分数，范围0-100，分数越高被黑产持有的概率也就越高
                "risk_level":风险等级，包括高、中、低、无
                "country": 国家
                "province": 省份
                "city": 城市
                "district": 区县
                 "owner": 运营商
                 "latitude": 纬度
                 "longitude": 经度
                 "adcode": 行政区划代码
                 "areacode": 国家编码
                 "continent": 大洲
                }...]
        :return: int(1: success, 0: failed)
        """
        if not data_list:
            return 0

        try:
            self.get_conn().insert_many(data_list, ordered=False, bypass_document_validation=True)
            return 1
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return 0

    def batch_update(self, data_list):
        """
        批量操作,存在则更新,不存在则insert
        :param data_list:
        :return:
        """
        if not data_list:
            return 0
        update_operations = list()
        try:
            for data in data_list:
                op = ReplaceOne({"ip": data["ip"]}, replacement=data, upsert=True)
                update_operations.append(op)
            self.get_conn().bulk_write(update_operations, ordered=False)
            return 1
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return 0


class IpToMysql(object):
    _instance = None

    @classmethod
    def get_instance(cls):
        if not cls._instance:
            cls._instance = IpToMysql(**G_CONFIG.mysql)
        return cls._instance

    def __init__(self, host, port, user, password, db, table, charset='utf8', **kwargs):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db
        self.charset = charset
        self.table = table

        self.pool = PooledDB(
            creator=pymysql, maxconnections=10, mincached=5, blocking=True,
            host=self.host, port=self.port, user=self.user, passwd=self.password, database=self.db,
            charset=self.charset, **kwargs
        )

    def create_table(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        sql = "create table if not exists `{table}` " \
              "(`ip` varchar(20) NOT NULL ," \
              "`type` varchar(10) COMMENT '类型', " \
              "`risk_tag` varchar(200) COMMENT '风险标签'," \
              "`risk_score` varchar(10) COMMENT '风险分数'," \
              "`risk_level` varchar(10) COMMENT '风险等级'," \
              "`country` varchar(50) COMMENT '国家', " \
              "`province` varchar(100) COMMENT '省份'," \
              "`city` varchar(100) COMMENT '城市'," \
              "`district` varchar(100) COMMENT '区县'," \
              "`owner` varchar(200) COMMENT '运营商'," \
              "`latitude` varchar(30) COMMENT '维度'," \
              "`longitude` varchar(30) COMMENT '经度'," \
              "`adcode` varchar(30) COMMENT '行政区划代码'," \
              "`areacode` varchar(30) COMMENT '国家编码'," \
              "`continent` varchar(30) COMMENT '大洲'," \
              " primary key (ip)) DEFAULT CHARSET=utf8;".format(table=self.table)
        cursor.execute(sql)
        conn.commit()
        cursor.close()
        conn.close()
        return 1

    def execute_many_sql_with_commit(self, param_list):
        """
        执行更新或插入语句，批量插入
        :param sql:
        :param param_list: [(), (), ...]
        :return:
        """
        if not param_list:
            return 0
        try:
            conn = self.pool.connection()
            cursor = conn.cursor()
            sql = "INSERT INTO `{table}` " \
                  "(`ip`, `type`, `risk_tag`, `risk_score`, `risk_level`,`country`, `province`, `city`, `district`, " \
                  "`owner`,`latitude`,`longitude`,`adcode`,`areacode`,`continent`) " \
                  "VALUES(%(ip)s, %(type)s, %(risk_tag)s, %(risk_score)s, %(risk_level)s,%(country)s, %(province)s, " \
                  "%(city)s,%(district)s, %(owner)s, %(latitude)s,%(longitude)s,%(adcode)s," \
                  "%(areacode)s,%(continent)s) ON DUPLICATE KEY UPDATE risk_score=VALUES(risk_score)," \
                  " risk_tag=VALUES(risk_tag),risk_level=VALUES(risk_level)"
            sql = sql.format(table=self.table)
            cursor.executemany(sql, param_list)
            conn.commit()
            cursor.close()
            conn.close()
            return 1
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return 0

    def execute_sql_find_wigh_ips(self, param_list):
        results = []
        if not param_list:
            return results
        try:
            conn = self.pool.connection()
            with conn.cursor() as cursor:
                query_sql = 'SELECT * from `{table}` where ip in (%s)' % ",".join(["%s"] * len(param_list))
                query_sql = query_sql.format(table=self.table)
                logger.info("query_sql")
                logger.info(query_sql)
                cursor.execute(query_sql, param_list)
                for row in cursor.fetchall():
                    data = {"ip": row[0],
                           "type": row[1],
                           "risk_tag": row[2],
                           "risk_score": row[3],
                           "risk_level": row[4],
                           "country": row[5],
                           "province": row[6],
                           "city": row[7],
                           "district": row[8],
                           "owner": row[9],
                           "latitude": row[10],
                           "longitude": row[11],
                           "adcode": row[12],
                           "areacode": row[13],
                           "continent": row[14],
                           }
                    results.append(data)

            return results
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return results

    def execute_update_sql_with_commit(self, param_list):
        """
        执行更新或插入语句,单条插入
        :param sql:
        :param param_list: [(), (), ...]
        :return:
        """
        if not param_list:
            return 0
        try:
            conn = self.pool.connection()
            with conn.cursor() as cursor:
                for data in param_list:
                    query_sql = "SELECT `risk_tag` from `{table}` where ip=%(ip)s"
                    query_sql = query_sql.format(table=self.table)
                    rows = cursor.execute(query_sql, data)
                    if rows == 0:
                        insert_sql = "INSERT INTO `{table}` " \
                                     "(`ip`, `type`, `risk_tag`, `risk_score`, `risk_level`,`country`, `province`, `city`, " \
                                     "`district`, `owner`,`latitude`,`longitude`,`adcode`,`areacode`,`continent`) " \
                                     "VALUES(%(ip)s, %(type)s, %(risk_tag)s, %(risk_score)s, %(risk_level)s,%(country)s, " \
                                     "%(province)s, %(city)s,%(district)s, %(owner)s, %(latitude)s,%(longitude)s,%(adcode)s," \
                                     "%(areacode)s,%(continent)s)"
                        insert_sql = insert_sql.format(table=self.table)
                        cursor.execute(insert_sql, data)
                    else:
                        result = cursor.fetchone()
                        new_tag = data["risk_tag"][:2]
                        index = result[0].find(new_tag)
                        # 风险标签是否存在，存在则更新时间，不存在则追加
                        if index == -1:
                            if data["type"] == "数据中心":
                                result[0] = result[0].replace("机房流量", "")
                            data["risk_tag"] = result[0] + "|" + data["risk_tag"]
                        else:
                            data["risk_tag"] = result[0].replace(result[0][index + 3:index + 22],
                                                                 data["risk_tag"][3:22])
                        update_sql = "UPDATE `{table}` set `risk_tag` =%(risk_tag)s,`risk_score` =%(risk_score)s," \
                                     "`risk_level`=%(risk_level)s where ip=%(ip)s"
                        update_sql = update_sql.format(table=self.table)
                        cursor.execute(update_sql, data)
            conn.commit()
            conn.close()
            return 1
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return 0
