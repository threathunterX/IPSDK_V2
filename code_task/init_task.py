#!/usr/bin/env python
import json
import subprocess
import time
import traceback

import requests

from config import G_CONFIG
from utils.db import IpToMongoDB, IpToMysql, IpToRedis
from utils.utls import logger, aes_decrypt_seg, parse_data, write_temp_file


class InitialPackage(object):
    """
    process InitialPackage
    """

    @classmethod
    def download_initial(cls, url, file_name):
        """
        从oss下载数据重命名后保存
        :param url: oss下载链接
        :param file_name: 文件名
        :return: bool
        """
        try:

            file_path = "./download/" + file_name
            down_cmd = "wget '{0}' -O {1}".format(url, file_path)
            subprocess.call(down_cmd, shell=True)
            # status_upload(file_name.split(".")[0], "download")
            return True
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return False

    def get_initial_resp(self):
        url = "https://{host}/api/v6/initial/".format(host=G_CONFIG.host)
        payload = {
            "snuser": G_CONFIG.user["snuser"]
        }
        logger.info(payload)
        try:
            r = requests.post(url, data=json.dumps(payload))
            rjson = json.loads(r.text)
            if rjson["status"] == 200:
                result = json.loads(aes_decrypt_seg(rjson["data"]))
                if self.download_initial(url=result["file"]["link"], file_name=result["file"]["name"]):
                    # status_upload(result["file"]["name"].split(".")[0], "download")
                    return result["file"]["name"]
            else:
                logger.warn("error code is {}".format(rjson["status"]))
                return ""
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return ""

    @classmethod
    def unzip(cls, filename):
        file_new = filename.split('.')[0]
        subprocess.call("gunzip ./download/{file_old} && mv ./download/{file_new} ./task/ ".
                        format(file_old=filename, file_new=file_new),
                        shell=True)
        return file_new

    @classmethod
    def write_data(cls, database, filename):
        try:
            with open("./task/{}".format(filename), "r") as f:
                json_data_list = list()
                for lines in f:
                    parse_line = parse_data(lines)
                    json_data_list.append(parse_line)
                    if len(json_data_list) >= 100000:
                        cls.insert_to_db(database, json_data_list)
                        json_data_list = list()
                cls.insert_to_db(database, json_data_list)

                # logger.info("入库 耗时:{0}, 共插入数据{1}".format(time.time() - start_time, len(json_data_list)))
        except Exception as e:
            logger.error(e)
            logger.error("报错文件:{0}/n, {1}".format(filename, traceback.format_exc()))
        finally:
            cmd = "rm -rf ./task/{}".format(filename)
            subprocess.call(cmd, shell=True)
            return

    @classmethod
    def insert_to_db(cls, database, json_data_list):
        start_time = time.time()
        if database == "mongodb":
            IpToMongoDB.get_instance().batch_update(json_data_list)
        elif database == "mysql":
            IpToMysql.get_instance().execute_many_sql_with_commit(json_data_list)

        logger.info("入库 耗时:{0}, 共插入数据{1}".format(time.time() - start_time, len(json_data_list)))

    def process(self, database):
        if database == "mysql":
            IpToMysql.get_instance().create_table()
        filename = self.get_initial_resp()
        file_new = self.unzip(filename)
        self.write_data(database, file_new)
        write_temp_file("init_version", file_new)
