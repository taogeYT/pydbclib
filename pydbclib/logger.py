# -*- coding: utf-8 -*-
"""
mylog module
不要直接用logging.info 之类的直接打印日志，直接用logging模块会默认生成root log，其携带的roothandler会影响子log打印
日志重复打印问题，是因为对同一个log实例，多次添加handler造成的
并且日志存在继承关系，会对子log产生影响，子log不仅执行自身的handler，也会执行上层的handler
一直到找到root的handler
避免同一个模块将"instance_log"或"class_log"和module_log同时使用
"""
__all__ = ["module_log", "instance_log", "class_log"]

import logging
import os
# from vastio.config import config
# DEBUG = config.DEBUG

DEBUG = None
_debug = {
    True: logging.DEBUG,
    False: logging.INFO,
    None: logging.WARN
}
_format = {
    False: ('<%(asctime)s %(levelname)s %(name)s> %(message)s', '%Y-%m-%d %H:%M:%S'),
    True: ('<%(asctime)s %(levelname)s %(name)s %(message)s', '%Y-%m-%d %H:%M:%S'),
    None: ('<%(asctime)s %(levelname)s %(name)s%(message)s', '%Y-%m-%d %H:%M:%S')
}

def _add_handler(debug):
    handler = logging.StreamHandler()
    formatter = logging.Formatter(*_format[debug])
    handler.setFormatter(formatter)
    return handler

def class_log(debug=DEBUG):
    def wrapper(instance):
        name = "%s.%s%s" % (instance.__module__,
                            instance.__name__, "> "[:debug])
        log = logging.getLogger(name)
        if not log.handlers:
            log.addHandler(_add_handler(debug))
            log.setLevel(_debug[debug])
        instance.log = log
        return instance
    return wrapper


def instance_log(instance, debug=DEBUG):
    """
    Usage: instance_log(__name__)
    :param instance: a class instance
    :param debug: debug on
    """
    name = "%s.%s%s" % (instance.__module__,
                        instance.__class__.__name__, "> "[:debug])
    log = logging.getLogger(name)
    if not log.handlers:
        log.addHandler(_add_handler(debug))
        log.setLevel(_debug[debug])
    instance.log = log


def module_log(name, debug=DEBUG):
    """
    Usage: module_log(__name__)
    :param name: __name__ or __file__
    :param debug: debug on
    :return: a log instance
    """
    if name.endswith('.py'):
        PROJECT_PATH = os.path.dirname(os.path.abspath(__file__))
        PROJECT = os.path.basename(PROJECT_PATH)
        name = os.path.abspath(name)
        relative_path = name.replace(PROJECT_PATH, '')
        tmp_name = ''.join([PROJECT, relative_path])
        module_name = os.path.splitext(tmp_name)[0].replace(os.sep, '.')
    else:
        module_name = name
    name = "{}{}".format(module_name, "> "[:debug])
    log = logging.getLogger(name)
    if not log.handlers:
        log.addHandler(_add_handler(debug))
        log.setLevel(_debug[debug])
    return log


if __name__ == '__main__':
    log = module_log(__name__)
    log.info('Log module test')
