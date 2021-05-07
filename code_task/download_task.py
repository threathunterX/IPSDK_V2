import json
import subprocess
import time
import traceback

import requests

from config import G_CONFIG
from utils.utls import logger, aes_encrypt_seg, aes_decrypt_seg, exec_func_times_if_error, read_temp_file


class DownLoad(object):
    """
    download handle
    """

    @classmethod
    def download(cls, url, file_name):
        """
        从oss下载数据重命名后保存
        :param url: oss下载链接
        :param file_name: 文件名
        :return: bool
        """
        try:
            r = requests.get(url)
            file_path = "./download/" + file_name
            with open(file_path, "wb") as code:
                code.write(r.content)
            cmd = "gunzip {file}".format(file=file_path)
            logger.info(cmd)
            subprocess.call(cmd, shell=True)
            name_upzip = file_name.split(".")[0]
            mv_cmd = "mv ./download/{file} ./task/".format(file=name_upzip)
            logger.info(mv_cmd)
            subprocess.call(mv_cmd, shell=True)
            return True
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return False

    def get_upgrade_resp(self, data):
        """
        获取更新包下载链接信息
        :param data:
        :return: dict()
        """
        url = "https://{host}/api/v6/upgrade/".format(host=G_CONFIG.host)

        pstr = json.dumps(data)

        cstr = aes_encrypt_seg(pstr)

        payload = {
            "snuser": G_CONFIG.user["snuser"],
            "data": cstr
        }

        try:
            r = requests.post(url, data=json.dumps(payload))
            rjson = json.loads(r.text)
            if rjson["status"] == 200:
                return json.loads(aes_decrypt_seg(rjson["data"]))
            else:
                logger.warn("error code is {}".format(rjson["status"]))
                return {}
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
            return {}

    def run(self):
        """
        持续从服务端下载文件到本地
        :return:
        """
        # 先下载全量包
        # self.get_initial_resp()
        curver = G_CONFIG.curver
        upgrade_file = read_temp_file("upgrade_version")
        if upgrade_file is not "":
            curver = upgrade_file
        while True:
            result = self.get_upgrade_resp({"curver": curver, "limit": 100})
            count = result.get("count", 0)
            curver = result.get("nextver", curver)
            files = result.get("files", [])
            if count:
                for file in files:
                    if exec_func_times_if_error(self.download, url=file["link"], file_name=file["name"], times=10):
                        continue
                    else:
                        logger.error("尝试10次未下载成功")

            time.sleep(10)
