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
from aiohttp import web

from markdown2 import markdown

from config import configs
from coroweb import get, post
from models import User, Comment, Blog, next_id
from apis import Page, APIValueError, APIResourceNotFoundError, APIPermissionError, APIError


COOKIE_NAME = 'awesession'
_COOKIE_KEY = configs.session.secret
_RE_EMAIL = re.compile(r'^[a-z0-9\.\-\_]+\@[a-z0-9\-\_]+(\.[a-z0-9\-\_]+){1,4}$')
_RE_SHA1 = re.compile(r'^[0-9a-f]{40}$')


def check_admin(request):
    """ 检查是否是管理员用户 """
    if request.__user__ is None or not request.__user__.admin:
        raise APIPermissionError()


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


def text2html(text):
    """ 文本转HTML """
    lines = map(lambda s: '<p>%s</p>' % s.replace('&', '&amp;').replace('<', '&lt;').replace('>', '&gt;'), filter(lambda s: s.strip() != '', text.split('\n')))
    return ''.join(lines)


def user2cookie(user, max_page):
    """ 计算加密cookie：Generate cookie str by user."""
    # build cookie string by: id-expires-sha1
    expires = str(int(time.time() + max_page))
    s = '-'.join((user.id, user.passwd, expires, _COOKIE_KEY))
    lst = [user.id, expires, hashlib.sha1(s.encode('utf-8')).hexdigest()]
    return '-'.join(lst)


async def cookie2user(cookie_str: str):
    """ 解密cookie：Parse cookie and load user if cookie is valid. """
    if not cookie_str:
        return None
    try:
        lst = cookie_str.split('-')
        if len(lst) != 3:
            return None
        uid, expires, sha1 = lst
        if int(expires) < time.time():
            return None
        user = await User.find(uid)
        if user is None:
            return None
        s = '-'.join((uid, user.passwd, expires, _COOKIE_KEY))
        if sha1 != hashlib.sha1(s.encode('utf-8')).hexdigest():
            logging.info('Invalid sha1.')
            return None
        user.passwd = '*' * 6
        return user
    except Exception as e:
        logging.exception(e)
        return None


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
        'blogs': blogs,
        # '__user__': request.__user__
    }


@get('/blog/{id}')
async def get_blog(id):
    """ 处理日志详情页面URL """
    blog = await Blog.find(id)
    comments = await Comment.findAll('blog_id=?', [id], orderBy='created_at desc')
    for c in comments:
        c.html_content = text2html(c.content)
    blog.html_content = markdown(blog.content)
    return {
        '__template__': 'blog.html',
        'blog': blog,
        'comments': comments
    }


@get('/manage/blogs/create')
def manage_create_blog():
    """ 创建日志页面 """
    return {
        '__template__': 'manage_blog_edit.html',
        'id': '',
        'action': '/api/blogs'
    }


@get('/register')
def register():
    """ 处理注册页面URL """
    return {
        '__template__': 'register.html'
    }


@get('/signin')
def signin():
    """ 处理登录页面URL """
    return {
        '__template__': 'signin.html'
    }


@get('/signout')
def signout(request):
    """ 用户注销 """
    referer = request.headers.get('Referer')
    r = web.HTTPFound(referer or '/')
    r.set_cookie(COOKIE_NAME, '-deleted-', max_age=0, httponly=True)
    logging.info('User signed out.')
    return r


@post('/api/authenticate')
async def authenticate(*, email, passwd):
    """ 用户登录验证 """
    if not email:
        raise APIValueError('email', 'Invalid email.')
    if not passwd:
        raise APIValueError('passwd', 'Invalid password.')
    users = await User.findAll('email=?', [email])
    if len(users) == 0:
        raise APIValueError('email', 'Email not exist.')
    user = users[0]
    # check passwd:
    sha1 = hashlib.sha1()
    sha1.update(user.id.encode('utf-8'))
    sha1.update(b':')
    sha1.update(passwd.encode('utf-8'))
    if user.passwd != sha1.hexdigest():
        raise APIValueError('passwd', 'Invalid password.')
    # authenticate ok, set cookie:
    r = web.Response()
    r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
    user.passwd = '*' * 6
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r


@post('/api/users')
async def api_register_user(*, email, name, passwd):
    """ 用户注册API """
    if not name or not name.strip():
        raise APIValueError('name')
    if not email or not _RE_EMAIL.match(email):
        raise APIValueError('email')
    if not passwd or not _RE_SHA1.match(passwd):
        raise APIValueError('passwd')
    users = await User.findAll('email=?', [email])
    if len(users) > 0:
        raise APIError('register:failed', 'email', 'Email is already in use')
    uid = next_id()
    sha1_passwd = f'{uid}:{passwd}'
    user = User(id=uid, name=name.strip(), email=email, passwd=hashlib.sha1(sha1_passwd.encode('utf-8')).hexdigest(),
                image=f'http://www.gravatar.com/avatar/{hashlib.md5(email.encode("utf-8")).hexdigest()}?d=mm&s=120')
    await user.save()
    # make session cookie:
    r = web.Response()
    r.set_cookie(COOKIE_NAME, user2cookie(user, 86400), max_age=86400, httponly=True)
    user.passwd = '*' * 6
    r.content_type = 'application/json'
    r.body = json.dumps(user, ensure_ascii=False).encode('utf-8')
    return r


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


@get('/api/blogs/{id}')
async def api_get_blog(*, id):
    """ 获取日志详情API """
    blog = await Blog.find(id)
    return blog


@post('/api/blogs')
async def api_create_blog(request, *, name, summary, content):
    """ 发表日志API """
    check_admin(request)
    if not name or not name.strip():
        raise APIValueError('name', 'Name cannot be empty.')
    if not summary or not summary.strip():
        raise APIValueError('summary', 'Summary cannot be empty.')
    if not content or not content.strip():
        raise APIValueError('content', 'Content cannot be empty.')
    blog = Blog(user_id=request.__user__.id, user_name=request.__user__.name, user_image=request.__user__.image, name=name.strip(), summary=summary.strip(), content=content.strip())
    await blog.save()
    return blog
