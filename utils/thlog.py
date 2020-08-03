#!/usr/bin/env python
import logging
import logging.handlers
import time
from logging import getLogger, INFO, WARN, DEBUG, ERROR, FATAL, WARNING, CRITICAL


class ThLog(object):
    LOG_LEVEL = logging.DEBUG
    DATE_FORMAT = time.strftime('%Y-%m-%d', time.localtime(time.time()))

    FORMAT = '[%(asctime)s]-%(levelname)-8s<%(name)s> - %(threadName)s {%(filename)s:%(lineno)s} -> %(message)s'
    formatter = logging.Formatter(FORMAT)

    def __init__(self, module_name):
        self._normal = None
        self._error = None
        self.name = module_name

    def get_normal_log(self):
        file_name = './{0}.log'.format(self.name)
        normal_handler = logging.handlers.TimedRotatingFileHandler(filename=file_name, backupCount=30, when="D")
        normal_handler.setFormatter(self.formatter)
        normal_log = logging.getLogger(self.name)
        normal_log.setLevel(self.LOG_LEVEL)
        normal_log.addHandler(normal_handler)
        return normal_log

    def get_error_log(self):
        file_name = './ERROR_{0}.log'.format(self.name)
        error_handler = logging.handlers.TimedRotatingFileHandler(filename=file_name, backupCount=7, when="D")
        error_handler.setFormatter(self.formatter)
        error_log = getLogger(self.name + '_error')
        error_log.setLevel(self.LOG_LEVEL)
        error_log.addHandler(error_handler)
        return error_log

    @property
    def normal_log(self):
        if not self._normal:
            self._normal = self.get_normal_log()
        return self._normal

    @property
    def error_log(self):
        if not self._error:
            self._error = self.get_error_log()
        return self._error

    def set_name(self, name):
        self.name = name

    def setLevel(self, level):
        self.normal_log.setLevel(level)

    def _backup_print(self, msg, *args, **kwargs):
        if args:
            msg = "{0}/{1}".format(msg, str(args))
        if kwargs:
            msg = "{0}/{1}".format(msg, str(kwargs))
        print(msg)

    def debug(self, msg, *args, **kwargs):
        if self.normal_log.isEnabledFor(DEBUG):
            self.normal_log._log(DEBUG, msg, args, **kwargs)
            self._backup_print(msg, args, kwargs)

    def info(self, msg, *args, **kwargs):
        if self.normal_log.isEnabledFor(INFO):
            self.normal_log._log(INFO, msg, args, **kwargs)
            self._backup_print(msg, args, kwargs)

    def warning(self, msg, *args, **kwargs):
        if self.normal_log.isEnabledFor(WARN):
            self.normal_log._log(WARNING, msg, args, **kwargs)

    def warn(self, msg, *args, **kwargs):
        if self.normal_log.isEnabledFor(WARN):
            self.normal_log._log(WARN, msg, args, **kwargs)

    def error(self, msg, *args, **kwargs):
        if self.error_log.isEnabledFor(ERROR):
            self.normal_log._log(ERROR, msg, args, **kwargs)
            self.error_log._log(ERROR, msg, args, **kwargs)

    def critical(self, msg, *args, **kwargs):
        if self.error_log.isEnabledFor(CRITICAL):
            self.normal_log._log(CRITICAL, msg, args, **kwargs)
            self.error_log._log(CRITICAL, msg, args, **kwargs)

    def fatal(self, msg, *args, **kwargs):
        if self.error_log.isEnabledFor(FATAL):
            self.normal_log._log(FATAL, msg, args, **kwargs)
            self.error_log._log(FATAL, msg, args, **kwargs)
