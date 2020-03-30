# -*- encoding: utf-8 -*-
"""
@File    : demo_mysql.py
@Time    : 2019/10/15 2:40 PM
@Author  : slyu
@Email   : yusulong@threathunter.cn
@Software: PyCharm
"""
from multiprocessing import Process
from .utls import DownLoad, InitialPackage, produce, consume


if __name__ == "__main__":
    InitialPackage().process("mysql")
    # 获取更新包下载链接 并下载压缩包
    download_task = Process(target=DownLoad().run)
    download_task.daemon = True
    download_task.start()

    # 解压文件并转移
    produce_task = Process(target=produce)
    produce_task.daemon = True
    produce_task.start()

    # 顺序消费入库
    consume_task = Process(target=consume, args=("mysql",))
    consume_task.daemon = True
    consume_task.start()

    download_task.join()
