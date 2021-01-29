import redis
import hashlib
import random
import pymysql
import pandas as pd
import time

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db= 'test', charset='utf8')

def timer(func):
    def wrap(*args, **kwargs):
        s = time.time()
        rv = func(*args, **kwargs)
        e = time.time()
        print(func.__name__ + ' costs '+str(round(e-s, 2))+' secs')
        return rv
    return wrap

