#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
url handlers
"""

import re
import time
import json
import logging
import hashlib
import base64
import asyncio
from coroweb import get, post
from models import User, Comment, Blog, next_id
from apis import Page, APIValueError, APIResourceNotFoundError, APIPermissionError, APIError


def get_page_index(page_str):
    """ 获取页码信息 """
    p = 1
    try:
        p = int(page_str)
    except ValueError as e:
        pass
    if p < 1:
        p = 1
    return p


@get('/')
async def index(request):
    """ 处理首页URL """
    # users = await User.findAll()
    # return {
    #     '__template__': 'test.html',
    #     'users': users
    # }
    summary = 'Lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.'
    blogs = [
        Blog(id='1', name='Test Blog', summary=summary, created_at=time.time()-120),
        Blog(id='2', name='Something New', summary=summary, created_at=time.time()-3600),
        Blog(id='3', name='Learn Swift', summary=summary, created_at=time.time()-7200)
    ]
    return {
        '__template__': 'blogs.html',
        'blogs': blogs
    }


@get('/api/users')
async def api_get_users(*, page='1'):
    """ 获取用户信息API """
    # users = await User.findAll(orderBy='created_at desc')
    # for u in users:
    #     u.passwd = '*' * 6
    # return dict(users=users)
    page_index = get_page_index(page)
    num = await User.findNumber('count(id)')
    p = Page(num, page_index)
    if num == 0:
        return dict(page=p, users=())
    users = await User.findAll(orderBy='created_at desc', limit=(p.offset, p.limit))
    for u in users:
        u.passwd = '*' * 6
    return dict(page=p, users=users)
