#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import logging
import aiomysql
# import asyncio


def log(sql, args=()):
    logging.info('SQL: %s' % sql)


async def create_pool(loop, **kw):
    logging.info('Create Database Connection Pool...')
    global __pool
    __pool = await aiomysql.create_pool(
        host=kw.get('host', 'localhost'),
        port=kw.get('port', 3306),
        user=kw['user'],
        password=kw['password'],
        db=kw['db'],
        charset=kw.get('charset', 'utf8'),
        autocommit=kw.get('autocommit', True),
        maxsize=kw.get('maxsize', 10),
        minsize=kw.get('minsize', 1),
        loop=loop
    )


async def select(sql, args, size=None):
    log(sql, args)
    global __pool
    async with __pool.get() as conn:
        async with conn.cursor(aiomysql.DictCursor) as cur:
            await cur.execute(sql.replace('?', '%s'), args or ())
            if size:
                rs = await cur.fetchmany(size)
            else:
                rs = await cur.fetchall()
        logging.info(f'Rows Returned: {len(rs)}')
        return rs


async def execute(sql, args, autocommit=True):
    log(sql)
    async with __pool.get() as conn:
        if not autocommit:
            await conn.begin()
        try:
            async with conn.cursor(aiomysql.DictCursor) as cur:
                await cur.execute(sql.replace('?', '%s'), args)
                affected = cur.rowcount
            if not autocommit:
                await conn.commit()
        except BaseException:
            raise
        return affected


# ====================================================================================================
class Field:

    def __init__(self, name, column_type, primary_key, default, nullable):
        self.name = name
        self.column_type = column_type
        self.primary_key = primary_key
        self.default = default
        self.nullable = nullable    # 用于确定它是否可以为空

    def __str__(self):
        return f'<{self.__class__.__name__}, {self.column_type}:{self.name}>'


class StringField(Field):

    def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)', nullable=True):
        super().__init__(name, ddl, primary_key, default, nullable)


class BooleanField(Field):

    def __init__(self, name=None, default=False):
        super().__init__(name, 'boolean', False, default, False)


class IntegerField(Field):

    def __init__(self, name=None, primary_key=False, default=0, nullable=True):
        super().__init__(name, 'bigint', primary_key, default, nullable)


class FloatField(Field):

    def __init__(self, name=None, primary_key=False, default=0.0, nullable=True):
        super().__init__(name, 'real', primary_key, default, nullable)


class TextField(Field):

    def __init__(self, name=None, default=None, nullable=True):
        super().__init__(name, 'text', False, default, nullable)


# ====================================================================================================
def create_args_string(num):
    return ', '.join(('?',) * num)


def get_column_string(mappings) -> str:
    """ 生成每个列连成的字符串，每一列的格式为 `列名` 类型 [not null]
    首先在每一列之间添加逗号，然后使用一个 map 函数，将 mappings.items() 中的每一个 item 都赋值给 s，这每个 item 都是一个 list，s[0] 是键，也就是变量名，s[1] 是值，也就是 Field，然后将它们格式化成字符串，形式就是上面说的 `列名` 类型 [not null] ，如果 Field 中指定了 name 则使用，没有则使用变量名。这里说一下，我的 Field 中增加了一个 nullable 属性，用于确定它是否可以为空，和教程的略有不同。
    :param mappings: 字典，键是变量名，值是 Field
    :return:
    """
    return ', '.join(map(lambda s: "`%s` %s %s" % (
        s[1].name or s[0],
        s[1].column_type,
        '' if s[1].nullable else 'not null'),
                         mappings.items()))


class ModelMetaclass(type):

    def __new__(cls, name, bases, attrs):
        """

        :param name: 类的名字 str
        :param bases: 类继承的父类集合 Tuple
        :param attrs: 类的方法集合
        """
        # 排除 Model 类本身
        if name == 'Model':
            return type.__new__(cls, name, bases, attrs)
        # 获取 table 名称
        tableName = attrs.get('__table__', None) or name
        logging.info(f'Found model: {name} (table: {tableName})')
        # 获取所有的 Field 和主键名
        mappings = dict()
        fields = []
        primaryKey = None
        for k, v in attrs.items():
            if isinstance(v, Field):
                logging.info(f'  Found mapping: {k} ==> {v}')
                mappings[k] = v
                if v.primary_key:
                    # 找到主键
                    if primaryKey:
                        raise RuntimeError(f'Duplicate primary key for field: {k}')
                    primaryKey = k
                else:
                    fields.append(k)
        if not primaryKey:
            raise RuntimeError('Primary key not found.')
        for k in mappings.keys():
            attrs.pop(k)
        escaped_fields = list(map(lambda f: f'`{f}`', fields))
        attrs['__mappings__'] = mappings            # 保存属性和列的映射关系
        attrs['__table__'] = tableName              # table 名称
        attrs['__primary_key__'] = primaryKey       # 主键属性名
        attrs['__fields__'] = fields                # 除主键外的属性名
        # 构造默认的 Select, Insert, Update, Delete 语句
        attrs['__select__'] = f"select `{primaryKey}`, {', '.join(escaped_fields)} from `{tableName}`"
        attrs[
            '__insert__'] = f"insert into `{tableName}` ({', '.join(escaped_fields)}, `{primaryKey}`) values ({create_args_string(len(escaped_fields) + 1)})"
        attrs[
            '__update__'] = f"update `{tableName}` set {', '.join(map(lambda f: f'`{mappings.get(f).name or f}`=?', fields))} where `{primaryKey}`=?"
        attrs['__delete__'] = f"delete from `{tableName}` where `{primaryKey}`=?"
        # 新增动态创建表
        attrs['__create__'] = "create table if not exists `%s` (%s, primary key (`%s`)) engine=InnoDB default charset=utf8mb4;" % (tableName, get_column_string(mappings), mappings.get(primaryKey).name or primaryKey)
        return type.__new__(cls, name, bases, attrs)


class Model(dict, metaclass=ModelMetaclass):
    def __init__(self, **kw):
        super(Model, self).__init__(**kw)

    def __getattr__(self, key):
        try:
            return self[key]
        except KeyError:
            raise AttributeError(f"'Model' object has no attribute '{key}'")

    def __setattr__(self, key, value):
        self[key] = value

    def getValue(self, key):
        return getattr(self, key, None)

    def getValueOrDefault(self, key):
        value = getattr(self, key, None)
        if value is None:
            field = self.__mappings__[key]
            if field.default is not None:
                value = field.default() if callable(field.default) else field.default
                logging.info(f'Using default value for {key}: {str(value)}')
                setattr(self, key, value)
        return value

    @classmethod
    async def findAll(cls, where=None, args=None, **kw):
        """ find objects by where clause. """
        sql = [cls.__select__]
        if where:
            sql.append('where')
            sql.append(where)
        if args is None:
            args = []
        orderBy = kw.get('orderBy', None)
        if orderBy:
            sql.append('order by')
            sql.append(orderBy)
        limit = kw.get('limit', None)
        if limit is not None:
            sql.append('limit')
            if isinstance(limit, int):
                sql.append('?')
                args.append(limit)
            elif isinstance(limit, tuple) and len(limit) == 2:
                sql.append('?, ?')
                args.extend(limit)
            else:
                raise ValueError(f'Invalid limit value: {str(limit)}')
        rs = await select(' '.join(sql), args)
        return [cls(**r) for r in rs]

    @classmethod
    async def findNumber(cls, selectField, where=None, args=None):
        """ find number by select and where. """
        sql = [f"select {selectField} _num_ from `{cls.__table__}`"]
        if where:
            sql.append('where')
            sql.append(where)
        rs = await select(' '.join(sql), args, 1)
        if len(rs) == 0:
            return None
        return rs[0]['_num_']

    @classmethod
    async def find(cls, pk):
        """ find object by primary key. """
        rs = await select(f"{cls.__select__} where `{cls.__primary_key__}`=?", [pk], 1)
        if len(rs) == 0:
            return None
        return cls(**rs[0])

    async def save(self):
        args = list(map(self.getValueOrDefault, self.__fields__))
        args.append(self.getValueOrDefault(self.__primary_key__))
        rows = await execute(self.__insert__, args)
        if rows != 1:
            logging.warning(f'Failed to insert record: affected rows: {rows}')

    async def update(self):
        args = list(map(self.getValue, self.__fields__))
        args.append(self.getValue(self.__primary_key__))
        rows = await execute(self.__update__, args)
        if rows != 1:
            logging.warning(f'Failed to update by primary key: affected rows: {rows}')

    async def remove(self):
        args = [self.getValue(self.__primary_key__)]
        rows = await execute(self.__delete__, args)
        if rows != 1:
            logging.warning(f'Failed to remove by primary key: affected rows: {rows}')

    @classmethod
    async def create(cls):
        """ Create table if table (with the same name) not exists. """
        await execute(cls.__create__, None)
