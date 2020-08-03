# -*- encoding: utf-8 -*-
"""
@File    : demo_mongo.py
@Time    : 2019/10/15 2:39 PM
@Author  : slyu
@Email   : yusulong@threathunter.cn
@Software: PyCharm
"""
import os
from multiprocessing import Process

from code_task.consume_task import Consume
from code_task.download_task import DownLoad
from code_task.init_task import InitialPackage


def create_init_dir():
    path_to_task = "./task/"
    if not os.path.isdir(path_to_task):
        os.mkdir(path_to_task)
    path_to_watch = "./download/"
    if not os.path.isdir(path_to_watch):
        os.mkdir(path_to_watch)


if __name__ == "__main__":
    create_init_dir()

    InitialPackage().process("mongodb")

    # 获取更新包下载链接 并下载压缩包
    download_task = Process(target=DownLoad().run)
    download_task.daemon = True
    download_task.start()

    # 消费入库
    consume_task = Process(target=Consume().run, args=("mongodb",))
    consume_task.daemon = True
    consume_task.start()

    download_task.join()
