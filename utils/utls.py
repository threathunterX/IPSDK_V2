#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2019/8/20 下午8:25
# @Author  : yu
# @Software: PyCharm
# @license : Copyright(C),威胁猎人

import base64
import json
import os
import subprocess
import traceback
from functools import wraps

import requests
from Crypto import Random
from Crypto.Cipher import AES

from config import G_CONFIG
from utils.thlog import ThLog

__all__ = ["logger", "aes_encrypt_seg", "aes_decrypt_seg", "parse_data", "remove_file", "exec_func_times_if_error",
           "read_temp_file", "write_temp_file"]

logger = ThLog("cli")


def aes_decrypt_seg(phoneno, snkey=G_CONFIG.user["snkey"]):
    data = base64.decodebytes(bytes(phoneno, encoding="utf8"))
    cihpertxt = data[AES.block_size:]
    remainder = len(cihpertxt) % AES.block_size
    if remainder:
        padded_value = cihpertxt + b'\0' * (AES.block_size - remainder)
    else:
        padded_value = cihpertxt
    cryptor = AES.new(bytes(snkey, encoding="utf8"), AES.MODE_CFB, data[0:AES.block_size], segment_size=128)
    plain_text = cryptor.decrypt(padded_value)
    return str(plain_text[0:len(cihpertxt)], encoding="utf8")


def aes_encrypt_seg(phoneno, snkey=G_CONFIG.user["snkey"]):
    remainder = len(phoneno) % AES.block_size
    if remainder:
        padded_value = phoneno + '\0' * (AES.block_size - remainder)
    else:
        padded_value = phoneno
    # a random 16 byte key
    iv = Random.new().read(AES.block_size)
    # CFB mode
    cipher = AES.new(bytes(snkey, encoding='utf8'), AES.MODE_CFB, iv, segment_size=128)
    # drop the padded value(phone number length is short the 16bytes)
    value = cipher.encrypt(bytes(padded_value, encoding="utf8")[:len(phoneno)])
    ciphertext = iv + value
    return str(base64.encodebytes(ciphertext).strip(), encoding="utf8")


def remove_file(file):
    cmd = "rm -rf ./task/{}".format(file)
    logger.info("remove: {}".format(cmd))
    subprocess.call(cmd, shell=True)


def parse_data(origin_data):
    """
    解析成 json 数据入mongo
    :param origin_data: 文件里的每一行的数据
    :return:
    """
    try:
        data_list = origin_data.split("\n")[0].split("\t")
        return {"ip": data_list[0],
                "type": data_list[1],
                "risk_tag": data_list[2],
                "risk_score": data_list[3],
                "risk_level": data_list[4],
                "country": data_list[5],
                "province": data_list[6],
                "city": data_list[7],
                "district": data_list[8],
                "owner": data_list[9],
                "latitude": data_list[10],
                "longitude": data_list[11],
                "adcode": data_list[12],
                "areacode": data_list[13],
                "continent": data_list[14],
                }
    except Exception as e:
        logger.error(e)
        logger.error(traceback.format_exc())
        return {}


def exec_times(func):
    '''
    传入一个返回值0，1的函数 如果错误就继续调用最大调用此时为times，如果调用成功返回1调用失败返回0
    :param func: 回调函数
    :param times: 最大调用次数
    :return: 如果调用函数func成功返回1调用失败返回0
    '''

    @wraps(func)
    def warpper(*args, **kwargs):
        n = 0
        while func(*args, **kwargs) != 1:
            n = n + 1
            if n < 5:
                continue
            if n == 5:
                return 0
        return 1

    return warpper


def exec_func_times_if_error(func, *func_args, times=5, **kwargs):
    '''
    传入一个返回值0，1的函数 如果错误就继续调用最大调用此时为times，如果调用成功返回1调用失败返回0
    :param func: 回调函数
    :param times: 最大调用次数
    :return: 如果调用函数func成功返回1调用失败返回0
    '''
    n = 0
    while func(*func_args, **kwargs) != 1:
        n = n + 1
        if n < times:
            continue
        if n == times:
            return 0
    return 1


def write_temp_file(file_name, version):
    with open(file_name, 'w') as f:
        f.seek(0)
        f.write(version)


def read_temp_file(file_name) -> str:
    if not os.path.exists(file_name):
        return ""
    with open(file_name, 'r') as fp:
        result = fp.readline()
    return result
