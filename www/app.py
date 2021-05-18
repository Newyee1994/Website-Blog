#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
async web application.
"""

import logging; logging.basicConfig(level=logging.INFO)
import asyncio
from aiohttp import web


def index(request):
    return web.Response(body=b'<h1>Awesome Website</h1>', content_type='text/html')


async def init(loop):
    app = web.Application()
    app.router.add_route('GET', '/', index)
    runner = web.AppRunner(app)
    await runner.setup()
    srv = await loop.create_server(runner.server, host='127.0.0.1', port=9000)
    logging.info('server started at http://127.0.0.1:9000...')
    return srv


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init(loop))
    loop.run_forever()
