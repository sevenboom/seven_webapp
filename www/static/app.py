#! python3
#!/usr/bin/env python3 (在windows上不能直接运行)
# -*- coding: utf-8 -*-

import logging; logging.basicConfig(level=logging.INFO)

import asyncio, os, json, time
from datetime import datetime


from aiohttp import web

def index(request):
	return web.Response(content_type='text/html', body=b'<h1>Seven<h1>')

@asyncio.coroutine
def init(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', index)
    srv = yield from loop.create_server(app.make_handler(), "127.0.0.1", 7778)
    logging.info("server start at http://127.0.0.1:7778...")
    return srv

loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()
    
	
