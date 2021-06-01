import os
import time
import traceback

from utils.db import IpToMongoDB, IpToMysql
from utils.utls import logger, parse_data, remove_file, write_temp_file


class Consume(object):

    def run(self, database):
        if database == "mysql":
            IpToMysql.get_instance().create_table()
        path_to_task = "./task/"
        while True:
            files = os.listdir(path_to_task)
            if files:
                files.sort()
                self.deal_with_files(database, files)
            else:
                time.sleep(10)
        pass

    @classmethod
    def deal_with_files(cls, database, files):

        try:
            start_time = time.time()
            logger.info("consume run")
            lines = cls.load_file(files)
            datas, ips = cls.init_data(lines)

            if database == "mongodb":
                logger.debug("mongo")
                values = datas.values()
                logger.debug(values)
                cls.batch_update_into_mongo(values)
                write_temp_file("upgrade_version", files[len(files) - 1])
                logger.info("入库 耗时:{0}, 共插入数据{1}".format(time.time() - start_time, len(values)))

            elif database == "mysql":
                values = datas.values()
                cls.batch_update_into_mysql(values)
                write_temp_file("upgrade_version", files[len(files) - 1])
                logger.info("入库 耗时:{0}, 共插入数据{1}".format(time.time() - start_time, len(values)))
                pass
        except Exception as e:
            logger.error(e)
            logger.error("报错文件:{0}/n, {1}".format(files, traceback.format_exc()))
        finally:
            for file in files:
                remove_file(file)

    @classmethod
    def init_data(cls, lines):
        datas = {}
        ips = []
        for line in lines:
            p_data = parse_data(line)
            ips.append(p_data["ip"])
            datas[p_data["ip"]] = p_data
        return datas, ips

    @classmethod
    def load_file(cls, files):
        lines = []
        for file in files:
            if file == ".DS_Store":
                continue
            file_name = "./task/{}".format(file)
            with open(file_name, "r") as fp:
                file_content = fp.readlines()
                for line in file_content:
                    lines.append(line)
        return lines

    @classmethod
    def change_risk_tag_already_in_mongo(cls, datas, ips):
        start = time.time()
        logger.debug("ip length {}".format(len(ips)))
        results = IpToMongoDB.get_instance().get_conn().find({"ip": {"$in": ips}})
        logger.debug("change_risk_tag_already_in_mongo")
        logger.debug(results)
        cls.deal_with_results(datas, results)
        logger.debug("change time: {}".format((time.time() - start)))

    @classmethod
    def deal_with_results(cls, datas, results):
        for result in results:
            if result.get("ip") is None:
                logger.warn("result ip is null")
                continue
            if result.get("risk_tag") is None:
                logger.warn("result risk_tag is null")
                continue

            data = datas[result["ip"]]
            new_tag = data["risk_tag"][:2]
            index = result["risk_tag"].find(new_tag)

            if index == -1:
                if data["type"] == "数据中心":
                    result["risk_tag"] = result["risk_tag"].replace("机房流量", "")
                data["risk_tag"] = result["risk_tag"] + "|" + data["risk_tag"]
            else:
                data["risk_tag"] = result["risk_tag"].replace(result["risk_tag"][index + 3:index + 22],
                                                              data["risk_tag"][3:22])

    @classmethod
    def batch_update_into_mongo(cls, values):
        if IpToMongoDB.get_instance().batch_update(values):
            logger.error("尝试5次未下载成功")

        return values

    @classmethod
    def batch_update_into_mysql(cls, values):
        if IpToMysql.get_instance().execute_many_sql_with_commit(values):
            logger.error("尝试5次未下载成功")
        return values

    @classmethod
    def change_risk_tag_already_in_mysql(cls, datas, ips):
        results = IpToMysql.get_instance().execute_sql_find_wigh_ips(ips)
        cls.deal_with_results(datas, results)
