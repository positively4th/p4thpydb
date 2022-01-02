from ..db import DB as DB0
from ..ts import Ts
from .util import Util


import psycopg3

from contrib.p4thpy.subprocesshelper import SubProcessHelper
from contrib.p4thpy.subprocesshelper import SubProcessError


class DB(DB0):


    def __init__(self, url=None, log=None, **kwargs):

        super().__init__(Util(), log=log);

        self._url = self.createURL(**kwargs) if url is None else url

        self.log.debug('url %s' % self.url)
        self.db = psycopg3.connect(self.url, autocommit=True)
        self._cursor = None

    @classmethod
    def createURL(cls, **kwargs):
        res = ['postgres://']
        res.append(kwargs['username'] if 'username' in kwargs else 'postgres')
        if 'password' in kwargs:
            res.append(':' + kwargs['password'])
        res.append('@')
        res.append(kwargs['host'] if 'host' in kwargs else 'localhost')
        res.append(':')
        res.append(kwargs['port'] if 'post' in kwargs else '5432')
        if 'db' in kwargs :
            res.append('/')
            res.append(kwargs['db'])
        return ''.join(res)
    
    def __del__(self):
        self.log.debug('Closing db!')
        if hasattr(self, 'db'):
            self.db.close()
            self.db = None
            
    def query(self, qp, transformer=None, stripParams=False, debug=None):

        def dictify(rows, description):
            names = [c.name for c in description]
            return [
                dict(zip(names, row)) for row in rows
            ]


        try:

            q,p, T = self.util.qpTSplit(qp)
            p = P.pStrip(q, p) if stripParams else p
            T = transformer if not transformer is None else Ts.transformerFactory(T, inverse=True)

            self.log.debug('q,p,T: %s, %s, %s' % (q, p, T))
                
            r = self.cursor.execute(q, p)


            description = self.cursor.description
            if (description is None):
                return []
        
            if T is None:
                return dictify(r.fetchall(), description)
        
            return [
                T(row) for row in dictify(r.fetchall(), description)
            ]
        except psycopg3.errors.InFailedSqlTransaction as e:
            self.log.error("query error: %s", e.diag.message_primary)
            #import sys
            #import traceback
            #traceback.print_stack()
            raise e
        except Exception as e:
            self.log.error("query error: %s", str(e))
            #import sys
            #import traceback
            #traceback.print_stack()
            raise e

    def tableExists(self, table, columnNames):
        schema, _table = self.util.schemaTableSplit(table);
        q = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE  table_name = %s AND (%s = 1 OR table_schema = %s)
        LIMIT 2
        """.format()
        p = [table, 0 if schema else 1, schema]
        rows = self.query((q,p), debug=False)
        if len(rows) != 1:
            return False

        q = """
        SELECT *
        FROM information_schema.columns
        WHERE  table_name = %s AND (%s = 1 OR table_schema = %s)
        """
        p = [table, 0 if schema else 1, schema]
        rows = self.query((q,p), debug=False)
        a = set([row['column_name'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)
    
    def exportToFile(self, path, invert=False):

        format = 't'

        returnCode = 1
        
        if (invert):
            args = [
                'pg_restore',
                '-d', self.url,
                '--clean',
                #'--format=' + format,
                '--single-transaction',
                '--if-exists',
                '--verbose',
                path,
            ]
        else:
            args = [
                'pg_dump',
                self.url,
                '-f', path,
                '--clean',
                '--format=' + format,
                '--verbose',
            ]

        self.log.info(str(args))
        returnCode = SubProcessHelper.run(self.log, args)
        return returnCode
    
    def startTransaction(self):
        if len(self.savepoints) == 0:
            self.query('BEGIN'.format(id), debug=None);

        return super().startTransaction()
        
    def rollback(self):
        res = super().rollback()
        if len(self.savepoints) == 0:
            self.query('ROLLBACK'.format(id), debug=None);
        return res
        
    def commit(self):
        res = super().commit()
        if len(self.savepoints) == 0:
            self.query('COMMIT'.format(id), debug=None);
        return res

        
    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.db.cursor()
        return self._cursor
    
    @property
    def connection(self):
        return self.db
        
    @property
    def url(self):
        return self._url
        
