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
            "uri": "mongodb://{user}:{password}@{host}:{port}",
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
            "host": "******",
            "port": 3306,
            "user": "******",
            "password": "******",
            "db": "blackip",
            "table": "blackip",
            "charset": "utf8"
        }

    @property
    def user(self):
        """
        th_user 永安在线官网控制台查看配置 //https:www.yazx.com
        :return: {"snuser": "", "snkey": ""}
        """
        return {
            "snuser": "******",
            "snkey": "******"
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
        host 配置 永安在线IP画像服务端域名, 联系工作人员获取
        :return: "hostname"
        """
        return "******"


G_CONFIG = Config()
