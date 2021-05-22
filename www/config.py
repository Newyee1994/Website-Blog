#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration
"""

import config_default


class Dict(dict):
    """ Simple dict but support access as x.y style. """
    def __init__(self, names=(), values=(), **kw):
        super(Dict, self).__init__(**kw)
        for k, v in zip(names, values):
            self[k] = v

    def __getattr__(self, key):
        """ 重写字典的属性获取方法：支持通过属性名访问键值对的值，属性名将被当做键名 """
        try:
            return self[key]
        except KeyError:
            raise AttributeError(r"'Dict' object has no attribute '%s'" % key)

    def __setattr__(self, key, value):
        """ 重写字典的属性设置方法：支持通过属性名访问键值对的值，属性名将被当做键名 """
        self[key] = value

    @classmethod
    def from_dict(cls, src_dict):
        """ 将一个dict转为Dict """
        d = Dict()
        for k, v in src_dict.items():
            # 使用三目运算符，如果值是一个dict递归将其转换为Dict再赋值，否则直接赋值
            d[k] = cls.from_dict(v) if isinstance(v, dict) else v
        return d


def merge(defaults, override):
    r = {}
    for k, v in defaults.items():
        if k in override:
            if isinstance(v, dict):
                r[k] = merge(v, override[k])
            else:
                r[k] = override[k]
        else:
            r[k] = v
    return r


def toDict(d):
    D = Dict()
    for k, v in d.items():
        D[k] = toDict(v) if isinstance(v, dict) else v
    return D


configs = config_default.configs

try:
    import config_override
    configs - merge(configs, config_override.configs)
except ImportError:
    pass

configs = toDict(configs)
