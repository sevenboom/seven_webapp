import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time

#mysql wrap
import aiomysql

@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info("create database connection pool...")
	global __pool
	__pool = yield from aiomysql.create_pool(
		host=kw.get('host', 'localhost'),
		port=kw.get('port', '3306'),
		user=kw['user'],
		db=kw['db'],
		charset=kw.get('charset', 'utf-8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),
		loop=loop
	)

@asyncio.coroutine
def select(sql, args, size=None):
	log(sql, args)
	global __pool
	with (yield from __pool) as conn:
		cur = yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?', '%s'),args or ())
		log(cur.description)
		res = []
		if size:
			res = yield from cur.fetchmany(size)
		else:
			res = yield from cur.fechall()
		logging.info("rows returned: %s"%len(res))
		return res

@asyncio.coroutine
def execute(sql, args):
	log(sql)
	with (yield from __pool) as conn:
		try:
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
		except BaseException as e:
			raise
		return affected

#define ORM

#define field type
class Field(object):

	def __init__(self, name, column_type):
		self.name = name
		self.column_type = column_type

	def __str__(self):
		return '<%s:%s>'%(self.__class__.__name__, self.name)


class StringField(Field):

	def __init__(self, name):
		super(StringField, self).__init__(name, 'varchar(100)')


class IntergerField(Field):

	def __init__(self, name):
		super(IntergerField, self).__init__(name, 'bigint')


# model class
class ModelMetaclass(type):

	def __new__(cls, name, bases, attrs):
		if name=='Model':
			return type.__new__(cls, name, bases, attrs)
		print('Found model: %s'%name)
		mappings = dict()
		for k, v in attrs.items():
			if isinstance(v, Field):
				print("Found mapping:%s => %s"%(k, v))
				mappings[k] = v
		for k in mappings:
			attrs.pop(k)
		attrs['__mappings__']=mappings
		attrs['__table__'] = name
		return type.__new__(cls, name, bases, attrs)



class Model(dict, metaclass=ModelMetaclass):

	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getitem__(self, key):
		try:
			return self[key]
		except KeyError as e:
			raise AttributeError(r"'Model' object has no attribute '%s'"%key)

	def __setitem__(self, key, value):
		try:
			self[key] = value
		except AttributeError as e:
			raise e  

	def save(self):
		fields=[]
		params = []
		args=[]
		for k,v in self.__mappings__.items():
			fields.append(v.name)
			params.append('?')
			args.append(getattr(self, k, None))
		sql="insert into %s (%s) values (%s)"%(self.__table__,','.join(fields), ','.join(params))
		print('SQL: %s' %sql)
		print('ARGS: %s' %str(args))




