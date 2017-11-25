import pymssql
import time


class Meitu:
    def __init__(self):
        self._table = '[dbo].[mzitu]'
        self._conn = pymssql.connect(user='sa', password='1qaz2wsxE', host='localhost', database='PyDatabase')
        self._createtable()

    def _createtable(self):
        try:
            cur = self._conn.cursor()
            # 建表
            cur.execute('''
            if not exists (select OBJECT_ID from sys.tables where object_id=OBJECT_ID('{0}') AND type = 'U')
            create table {0}(
              id            int identity(1,1) primary key,
              url           nvarchar(100),
              title         nvarchar(100),
              inuse         bit default 0
            )
            '''.format(self._table))
        finally:
            self._conn.commit()

    def datacount(self, query):
        try:
            cur = self._conn.cursor()
            cur.execute('select count(*) from {} where {}'.format(self._table, query))
            result = cur.fetchone()
            return result[0]
        except SyntaxError as ex:
            print(ex.msg)
            return 0

    def pushrange(self, pages={}):
        try:
            cur = self._conn.cursor()
            insert = '''
            if not exists (select * from {0} where url = %s)
            insert into {0} (url,title) values (%s,%s)
            '''.format(self._table)
            rows = []
            for r in pages:
                rows.append((r, r, pages[r]))
            cur.executemany(insert, rows)
        finally:
            self._conn.commit()

    def _changeinuse(self, rid, inuse):
        try:
            cur = self._conn.cursor()
            update = 'update {} set inuse = %d where id = %d'.format(self._table)
            cur.execute(update, (inuse, rid))
        finally:
            self._conn.commit()

    def useurl(self, rid):
        self._changeinuse(rid, 1)

    def unuseurl(self, rid):
        self._changeinuse(rid, 0)

    def pop(self):
        cur = self._conn.cursor()
        seltop = 'select top 1 id,url,title from {} with (UPDLOCK) where inuse = 0'.format(self._table)
        cur.execute(seltop)
        top1 = cur.fetchone()
        if top1:
            rid = top1[0]
            result = [top1[1], top1[2]]
            time.sleep(10)
            self.useurl(rid)
            return result
        else:
            return None

    def close(self):
        self._conn.close()

'''def run():
    data = Meitu()
    result = data.pop()
    print(result)
    data.close()'''


if __name__ == '__main__':
    '''import sqlite3
    sqliteconn = sqlite3.connect('meitu.db')
    cur = sqliteconn.cursor()
    cur.execute('select url,title from webpages')
    dic = {}
    for r in cur.fetchall():
        dic[r[0]] = r[1]
    print(dic)
    data = Meitu()
    data.pushrange(dic)
    data.close()'''
    '''import threading
    ths = []
    for i in range(10):
        thread = threading.Thread(target=run)
        thread.start()
        print('start a new thread')
        ths.append(thread)
    for th in ths:
        th.join()'''





