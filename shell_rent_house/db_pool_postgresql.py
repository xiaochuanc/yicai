import pymysql
from DBUtils.PooledDB import PooledDB
import inspect
import psycopg2


class DB_POOL(object):
    # mysql_info:字典格式的数据库配置信息
    #
    def __init__(self, mysql_info, m, n):
        self.pool = PooledDB(creator=psycopg2,mincached=m,maxcached=m,maxconnections=n,blocking=True,
                             host=mysql_info['host'],
                             user=mysql_info['user'],
                             password=mysql_info['password'],
                             database=mysql_info['db'],
                             port=mysql_info['port']
                             )

    def sql_exe(self,sql):
        conn = self.pool.connection()
        cur = conn.cursor()
        try:
            res = cur.execute(sql)
            conn.commit()
            data = cur.fetchall()
            if data:
                return data
            else:
                return res
        except Exception as e:
            print(e)
            print('{}.{}错误'.format(self.__class__.__name__, inspect.stack()[1][3]))
            conn.rollback()
            return
        finally:
            cur.close()
            conn.close()

    def sql_exe_ex(self,sql, *args):
        conn = self.pool.connection()
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            res = cur.execute(sql, *args)
            conn.commit()
            data = cur.fetchall()
            if data:
                return data
            else:
                return res
        except Exception as e:
            print(e)
            print('{}.{}错误'.format(self.__class__.__name__, inspect.stack()[1][3]))
            conn.rollback()
            return
        finally:
            cur.close()
            conn.close()

    def sql_exe_getlastid(self, sql, *args):
        conn = self.pool.connection()
        cur = conn.cursor(cursor=pymysql.cursors.DictCursor)
        try:
            res = cur.execute(sql, *args)
            conn.commit()
            last_id = cur.lastrowid
            return res, last_id
        except Exception as e:
            print(e)
            print('{}.{}错误'.format(self.__class__.__name__, inspect.stack()[1][3]))
            conn.rollback()
            return
        finally:
            cur.close()
            conn.close()

    def sql_exe_many(self, sql, T):
        conn = self.pool.connection()
        cur = conn.cursor()
        try:
            res = cur.executemany(sql, T)
            conn.commit()
            return res
        except Exception as e:
            print(e)
            print('{}.{}错误'.format(self.__class__.__name__, inspect.stack()[1][3]))
            conn.rollback()
            return
        finally:
            cur.close()
            conn.close()