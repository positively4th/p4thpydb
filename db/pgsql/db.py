import psycopg

from ..db import DB as DB0
from ..db import DBError as DBError0
from .util import Util
import re


from contrib.p4thpy.subprocesshelper import SubProcessHelper
from contrib.p4thpy.subprocesshelper import SubProcessError

parameterMatchers = [
    r'([(][{}].*?)'.format(Util.pNamePrefix) # <-- safe form = (:...)s
]
parameterMatchers = [
    '(?:{})'.format(p) for p in parameterMatchers
]
modulusReplacePattern = r'%(?!(?:{}))'.format('|'.join(parameterMatchers))


class DBError(DBError0):
    pass


class DB(DB0):

    @staticmethod
    def protectMod(q):
        return re.sub(modulusReplacePattern, '%%', q)

    @classmethod
    def createPipes(cls):
        from .pipes import Pipes
        return Pipes()

    @classmethod
    def createORM(cls, db):
        from .orm import ORM as _PGORM
        return _PGORM(db)

    @classmethod
    def createUtil(cls):
        from .util import Util as _PGUtil
        return _PGUtil()

    def __init__(self, url=None, log=None, **kwargs):
        #print('\n', url, '\n')
        super().__init__(Util(), log=log);

        self._url = self.createURL(**kwargs) if url is None else url

        self.log.debug('url %s' % self.url)
        self.db = psycopg.connect(self.url, autocommit=True)
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

    def _queryHelper(self, qpT, transformer=None, stripParams=False, fetch=True, debug=None):

        def createFetchOne(_cursor):

            def fetchEmpty():
                pass

            if not _cursor.description:
                return fetchEmpty

            names = [c.name for c in _cursor.description] \
                if _cursor.description is not None else []

            def fetchOne():
                if _cursor.closed:
                    return
                values = _cursor.fetchone()
                if values:
                    return dict(zip(names, values))

            return fetchOne

        try:
            util = self.createUtil()
            q, p, T = self.util.qpTSplit(qpT)
            q = DB.protectMod(q)
            p = util.pStrip(q, p) if stripParams else p

            self.log.debug('q,p,T: %s, %s, %s' % (q, p, T))

            cursor = self.cursor;
            r = cursor.execute(q, p)
            description = cursor.description

            #assert description is not None
            #if (description is None):
            #    return []
            return createFetchOne(cursor)

        except psycopg.errors.InFailedSqlTransaction as e:
            self.log.error("query error: %s", e.diag.message_primary)
            # import sys
            # import traceback
            # traceback.print_stack()
            raise e
        except Exception as e:
            print('')
            print('')
            print('')
            self.log.error("Unknown error: %s", str(e))
            for x in qpT: print(x)
            import sys
            import traceback
            traceback.print_stack()
            print('')
            print('')
            print('')
            raise e

    def tableExists(self, table, columnNames):
        schema, _table = self.util.schemaTableSplit(table);
        p = {}
        q = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE  table_name = {} AND ({} = 1 OR table_schema = {})
        LIMIT 2
        """.format(*self.util.ps(p, [table, 0 if schema else 1, schema]))
        rows = [r for r in self.query((q,p), debug=False)]
        if len(rows) != 1:
            return False

        p = {}
        q = """
        SELECT *
        FROM information_schema.columns
        WHERE  table_name = {} AND ({} = 1 OR table_schema = {})
        """.format(*self.util.ps(p, [table, 0 if schema else 1, schema]))
        rows = self.query((q,p), debug=False)
        a = set([row['column_name'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)
    
    def exportToFile(self, path, invert=False, explain=False, schemas=None, restoreTables=None):

        errs = []
        def explainSink(out, err):
            nonlocal errs

            if not out is None:
                print(SubProcessHelper.decode(out).rstrip())
            if not err is None:
                errs.append(SubProcessHelper.decode(err).rstrip())

        def runSink(out, err):
            if not out is None:
                self.log.debug(SubProcessHelper.decode(out).rstrip())
            if not err is None:
                self.log.info(SubProcessHelper.decode(err).rstrip())

        prettyAction = 'restore' if invert else 'dump'

        format = 't'

        returnCode = 1

        preQueries = []

        if (invert):
            args = [
                'pg_restore',
                '-d' if not explain else '-f', self.url if not explain else '-',
                '--data-only' if not restoreTables is None else None,
                '--clean' if restoreTables is None else None,
                '--if-exists' if restoreTables is None else None,
                #'--format=' + format,
                '--single-transaction',
                '--strict-names',
                '--verbose',
                path,
            ]
            if not restoreTables is None:
                if len(restoreTables) < 1:
                    raise DBError('Nothing to restore. No tables given.')
                for table in restoreTables:
                    for schema in schemas:
                        preQueries.append('TRUNCATE {}.{}'.format(schema, table))
                    args.append('--table={}'.format(table))
        else:
            args = [
                'pg_dump',
                self.url,
                '-f' if not explain else None, path if not explain else None,
                '--clean',
                '--format=' + format,
                '--verbose',
            ]

        if not schemas is None:
            if len(schemas) < 1:
                    raise DBError('Nothing to restore. No schenas given.')
            for schema in schemas:
                args.append('--schema={}'.format(schema))
                

        args = [arg for arg in args if not arg is None]
        self.log.info(str(args))

        if len(preQueries) > 0:
            self.log.warning('Running queries outside {} transaction'.format(prettyAction))
            preQueries = ['BEGIN'] + preQueries + ['COMMIT']
            for q in preQueries:
                self.log.info(q)
                if not explain:
                    self.query(q)

        returnCode = SubProcessHelper.run(args, outErrSink=explainSink if explain else runSink, log=self.log)

        if returnCode != 0:
            for err in errs:
                self.log.error(err)

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
        return self.db.cursor()

    @property
    def connection(self):
        return self.db
        
    @property
    def url(self):
        return self._url
        
