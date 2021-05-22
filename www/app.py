#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
async web application.
"""

import logging; logging.basicConfig(level=logging.INFO)
from aiohttp import web
import asyncio
import json
import time
import os
from datetime import datetime
from jinja2 import Environment, FileSystemLoader
import orm
from coroweb import add_routes, add_static


# async def index(request):
#     return web.Response(body=b'<h1>Awesome Website</h1>', content_type='text/html')
#
#
# def init():
#     app = web.Application()
#     app.router.add_get('/', index)
#     logging.info('Server started at http://127.0.0.1:9000...')
#     web.run_app(app, host='127.0.0.1', port=9000)


def init_jinja2(app, **kw):
    """ 初始化jinja2的函数 """
    logging.info('Init jinja2...')
    options = dict(
        autoescape=kw.get('autoescape', True),
        block_start_string=kw.get('block_start_string', '{%'),
        block_end_string=kw.get('block_end_string', '%}'),
        variable_start_string=kw.get('variable_end_string', '}}'),
        auto_reload=kw.get('auto_reload', True),
    )
    path = kw.get('path', None)
    if path is None:
        path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
    logging.info(f'Set jinja2 template path: {path}')
    env = Environment(loader=FileSystemLoader(path), **options)
    filters = kw.get('filters', None)
    if filters is not None:
        for name, f in filters.items():
            env.filters[name] = f
    app['__templating__'] = env


# 以下是middleware,可以把通用的功能从每个URL处理函数中拿出来集中放到一个地方
async def logger_factory(app, handler):
    """ URL处理日志工厂 """

    async def logger(request):
        logging.info(f'Request: {request.method} {request.path}')
        # await asyncio.sleep(0.3)
        return (await handler(request))

    return logger


async def response_factory(app, handler):
    """ 响应返回处理工厂 """

    async def response(request):
        logging.info('Resopnse handler...')
        r = await handler(request)
        if isinstance(r, web.StreamResponse):
            return r
        if isinstance(r, bytes):
            resp = web.Response(body=r)
            resp.content_type = 'application/octet-stream'
            return resp
        if isinstance(r, str):
            if r.startswith('redirect'):
                return web.HTTPFound(r[9:])
            resp = web.Response(body=r.encode('utf-8'))
            resp.content_type = 'text/html;charset=utf-8'
            return resp
        if isinstance(r, dict):
            template = r.get('__template__')
            if template is None:
                resp = web.Response(
                    body=json.dumps(r, ensure_ascii=False, default=lambda o: o.__dict__).encode('utf-8'))
                resp.content_type = 'application/json;charset=utf-8'
                return resp
            else:
                resp = web.Response(body=app['__templating__'].get_template(template).render(**r).encode('utf-8'))
                resp.content_type = 'text/html;charset=utf-8'
                return resp
        if isinstance(r, int) and r >= 100 and r < 600:
            return web.Response(r)
        if isinstance(r, tuple) and len(r) == 2:
            t, m = r
            if isinstance(t, int) and t >= 200 and t < 600:
                return web.Response(t, str(m))
        # default
        resp = web.Response(body=str(r).encode('utf-8'))
        resp.content_type = 'text/plain;charset=utf-8'
        return resp

    return response


def datetime_filter(t):
    """ 时间转换 """
    delta = int(time.time() - t)
    if delta < 60:
        return '1分钟前'
    if delta < 3600:
        return f'{delta // 60}分钟前'
    if delta < 86400:
        return f'{delta // 3600}小时前'
    if delta < 604800:
        return f'{delta // 86400}天前'
    dt = datetime.fromtimestamp(t)
    return f'{dt.year}年{dt.month}月{dt.day}日'


async def init(loop):
    await orm.create_pool(loop=loop, host='127.0.0.1', port=3306, user='root', password='root', db='awesome')
    app = web.Application(
        loop=loop,
        middlewares=[
            logger_factory, response_factory
        ]
    )
    init_jinja2(app, filters=dict(datetime=datetime_filter))
    add_routes(app, 'handlers')
    add_static(app)

    runner = web.AppRunner(app)
    await runner.setup()
    srv = await loop.create_server(runner.server, host='127.0.0.1', port=9000)
    logging.info('Server started at http://127.0.0.1:9000...')
    return srv


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()
