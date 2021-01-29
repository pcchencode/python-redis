import redis
import hashlib
import random
import pymysql
import pandas as pd
import time

r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
conn = pymysql.connect(host='localhost', port=3306, user='root', passwd='', db= 'test', charset='utf8')
cursor = conn.cursor()

def timer(func):
    def wrap(*args, **kwargs):
        s = time.time()
        rv = func(*args, **kwargs)
        e = time.time()
        print(func.__name__ + ' costs '+str(round(e-s, 2))+' secs')
        return rv
    return wrap

@timer
def run_redis(users=100, query=10000, r=r, conn=conn):
    r.flushdb() # 實驗一開始先清除 redis cache
    trun_cmd = f"""TRUNCATE TABLE test.tb_user"""
    cursor.execute(trun_cmd) # 實驗一開始先清除 mysql database
    conn.commit()

    # 100 個使用者; 隨機進來登入 1000次
    for i in range(1, query):
        account = str(random.randint(1, users))
        m = hashlib.md5()
        m.update(account.encode("utf-8"))
        password = m.hexdigest()
        
        # 先去 redis 找
        pw = r.get(account)
        if not pw:
            # redis 找不到就先去 mysql 找
            sql = f"""
            SELECT password FROM test.tb_user WHERE account={account}
            """
            df = pd.read_sql(sql, conn)
            if len(df)==0:
                # 如果 mysql 找不到，就寫一筆進去 database
                cmd = f"""
                INSERT INTO test.tb_user(account, password) VALUES ("{account}", "{password}")
                """
                cursor.execute(cmd)
                conn.commit()
                # 再寫一筆進去 redis
                r.set(account, password, ex=10000)
            else:
                # mysql 有找到，一樣要寫到 redis
                r.set(account, password, ex=10000)
    return

@timer
def run_mysql(users=100, query=10000, r=r, conn=conn):
    r.flushdb() # 實驗一開始先清除 redis cache
    trun_cmd = f"""TRUNCATE TABLE test.tb_user"""
    cursor.execute(trun_cmd) # 實驗一開始先清除 mysql database
    conn.commit()

    # 100 個使用者; 隨機進來登入 1000次
    for i in range(1, query):
        account = str(random.randint(1, users))
        m = hashlib.md5()
        m.update(account.encode("utf-8"))
        password = m.hexdigest()
        
        # 每次都直接去 mysql 找
        sql = f"""
        SELECT password FROM test.tb_user WHERE account={account}
        """
        df = pd.read_sql(sql, conn)
        if len(df)==0:
            # 如果 mysql 找不到，就寫一筆進去 database
            cmd = f"""
            INSERT INTO test.tb_user(account, password) VALUES ("{account}", "{password}")
            """
            cursor.execute(cmd)
            conn.commit()
    return

run_redis()
run_mysql()
