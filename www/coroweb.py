#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import asyncio
import functools
import inspect
import logging
import os
from urllib import parse
from aiohttp import web
from apis import APIError
# apis是处理分页的模块，APIError 是指API调用时发生逻辑错误


def get(path):
    """ Define decorator @get('/path') """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'GET'
        wrapper.__route__ = path
        return wrapper
    return decorator


def post(path):
    """ Define decorator @post('/path') """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kw):
            return func(*args, **kw)
        wrapper.__method__ = 'POST'
        wrapper.__route__ = path
        return wrapper
    return decorator


def get_required_kw_args(fn):
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        # 在 args 里加上仅包含关键字（keyword）的参数， 且不包括默认值， 然后返回 args
        if param.kind == inspect.Parameter.KEYWORD_ONLY and param.default == inspect.Parameter.empty:
            args.append(name)
    return tuple(args)


def get_named_kw_args(fn):
    args = []
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            args.append(name)
    return tuple(args)


def has_named_kw_args(fn):
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.KEYWORD_ONLY:
            return True


def has_var_kw_arg(fn):
    params = inspect.signature(fn).parameters
    for name, param in params.items():
        if param.kind == inspect.Parameter.VAR_KEYWORD:
            return True


def has_request_arg(fn):
    sig = inspect.signature(fn)
    params = sig.parameters
    found = False
    for name, param in params.items():
        if name == 'request':
            found = True
            continue
        if found and (param.kind != inspect.Parameter.VAR_POSITIONAL and param.kind != inspect.Parameter.KEYWORD_ONLY and param.kind != inspect.Parameter.VAR_KEYWORD):
            raise ValueError(f'Request\'s parameters must be the last named parameter in function: {fn.__name__}{str(sig)}')
    return found


class RequestHandler:
    """ 从URL函数中分析其需要接收的参数，从request中获取必要的参数，
        调用URL函数，然后把结果转换为web.Response对象
    """
    def __init__(self, app, fn):
        self._app = app
        self._func = fn
        self._has_request_arg = has_request_arg(fn)
        self._has_var_kw_arg = has_var_kw_arg(fn)
        self._has_named_kw_args = get_named_kw_args(fn)
        self._named_kw_args = get_named_kw_args(fn)
        self._required_kw_args = get_required_kw_args(fn)


    async def __call__(self, request):
        kw = None
        if self._has_var_kw_arg or self._has_named_kw_args or self._required_kw_args:
            if request.method == 'POST':
                return web.HTTPBadRequest(text='Missing Content-Type')
            ct = request.content_type.lower()
            if ct.startswith('application/json'):
                params = await request.json()
                if not isinstance(params, dict):
                    return web.HTTPBadRequest(text='JSON body must be object.')
                elif ct.startswith('application/x-www-form-urlencoded') or ct.startswith('multipart/form-data'):
                    params = await request.post()
                    kw = dict(**params)
                else:
                    return web.HTTPBadRequest(text=f'Unsupported Content-Type: {request.content_type}')
            if request.method == 'GET':
                qs = request.query_string
                if qs:
                    kw = {k: v[0] for k, v in parse.parse_qs(qs, True).items()}
        if kw is None:
            kw = dict(**request.match_info)
        else:
            if not self._has_var_kw_arg and self._named_kw_args:
                # remove all unamed kw:
                copy = dict()
                for name in self._named_kw_args:
                    if name in  kw:
                        copy[name] = kw[name]
                kw = copy
            # check named arg
            for k, v in request.match_info.items():
                if k in kw:
                    logging.warning(f'Duplicate arg name in named arg and kw args: {k}')
                kw[k] = v
        if self._has_request_arg:
            kw['request'] = request
        # check required kw
        if self._required_kw_args:
            for name in self._required_kw_args:
                if not name in kw:
                    return web.HTTPBadRequest(text=f'Missing argument: {name}')
        logging.info(f'Call with args: {str(kw)}')
        try:
            r = await self._func(**kw)
            return r
        except APIError as e:
            return dict(error=e.error, data=e.data, message=e.message)


def add_static(app):
    """ 用来注册static文件夹下的文件 """
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'static')
    app.router.add_static('/static/', path)
    logging.info(f"Add static {'/static/'} => {path}")


def add_route(app, fn):
    """ 用来注册一个URL处理函数 """
    method = getattr(fn, '__method__', None)
    path = getattr(fn, '__route__', None)
    if path is None or method is None:
        raise ValueError(f'@get or @post not defined in {str(fn)}.')
    if not asyncio.iscoroutinefunction(fn) and not inspect.isgeneratorfunction(fn):
        fn = asyncio.coroutine(fn)
    logging.info(f'Add route {method} {path} => {fn.__name__}({", ".join(inspect.signature(fn).parameters.keys())})')
    app.router.add_route(method, path, RequestHandler(app, fn))


def add_routes(app, module_name):
    """ 把很多次add_route()注册的调用变成自动扫描：自动把handler模块的所有符合条件的函数注册了 """
    n = module_name.rfind('.')
    if n == (-1):
        mod = __import__(module_name, globals(), locals())
    else:
        name = module_name[n+1:]
        mod = getattr(__import__(module_name[:n], globals(), locals(), [name]), name)
    for attr in dir(mod):
        if attr.startswith('_'):
            continue
        fn = getattr(mod, attr)
        if callable(fn):
            method = getattr(fn, '__method__', None)
            path = getattr(fn, '__route__', None)
            if method and path:
                add_route(app, fn)
