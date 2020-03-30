# -*- encoding: utf-8 -*-
"""
@File    : config.py.py
@Time    : 2019/10/15 11:54 AM
@Author  : slyu
@Email   : yusulong@threathunter.cn
@Software: PyCharm
"""
__all__ = ["G_CONFIG"]


class Config(object):
    """
    全局配置信息
    """
    @property
    def mongodb(self):
        """
        mongodb 配置
        :return: {"uri": "", "db": "", "collection": ""}
        """
        return {
            "uri": "mongodb://{user}:{password}@{host}:3717",
            "db": "blackip",
            "collection": "blackip"
        }

    @property
    def mysql(self):
        """
        mysql 配置
        :return: dict()
        """
        return {
            "host": "{host}",
            "port": 3306,
            "user": "{user}",
            "password": "{password}",
            "db": "blackip",
            "table": "blackip",
            "charset": "utf8"
        }

    @property
    def user(self):
        """
        th_user 配置
        :return: {"snuser": "", "snkey": ""}
        """
        return {
            "snuser": "XXX",
            "snkey": "XXX"
        }

    @property
    def curver(self):
        """
        更新时间配置 , 注： 可以更改为下面格式的时间，如果某个时间点入库失败，可以将此配置调为这个时间点重新更新，默认是当前时间
        :return: "%Y%m%d%H%M%S"
        """
        import datetime
        return datetime.datetime.now().strftime("%Y%m%d%H%M%S")

    @property
    def host(self):
        """
        host 配置
        :return: "ipdata.threathunter.cn"
        """
        return "ipdata.threathunter.cn"


G_CONFIG = Config()
