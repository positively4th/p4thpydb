import ramda as R
import asyncio
from os import path
from ..db import DB as DB0
from ..db import DBError as DBError0
from .util import Util
from .pipes import Pipes

import apsw
# print ("      Using APSW file",apsw.__file__)                # from the extension module
# print ("         APSW version",apsw.apswversion())           # from the extension module
# print ("   SQLite lib version",apsw.sqlitelibversion())      # from the sqlite library code
# print ("SQLite header version",apsw.SQLITE_VERSION_NUMBER)   # from the sqlite header file at compile time


class DBError(DBError0):
    pass


class DB(DB0):

    warn_async_ctr = 1

    @classmethod
    def createPipes(cls):
        from .pipes import Pipes as _SQLITEPipe
        return _SQLITEPipe()

    @classmethod
    def createORM(cls, db):
        from .orm import ORM as _SQLITEORM
        return _SQLITEORM(db)

    @classmethod
    def createUtil(cls):
        from .util import Util as _SQLiteUtil
        return _SQLiteUtil()

    def clone(self):
        return self.__class__(*self.args, **self.kwargs)

    def __init__(self, *args, fileName=':memory:', extensions=[],
                 pragmas=[], attaches={}, log=None,
                 busyRetries=None, busyTimeout=None):

        util = Util()
        super().__init__(util, log=log)

        self.args = args
        self.kwargs = {
            'fileName': fileName,
            'extensions': extensions,
            'pragmas': pragmas,
            'attaches': attaches,
            'log': log,
            'busyRetries': busyRetries,
            'busyTimeout': busyTimeout,
        }
        self.log.info('filename: %s' % fileName)
        self._filePath = fileName
        self.db = apsw.Connection(fileName, statementcachesize=20)
        if busyTimeout is not None:
            self.db.setbusytimeout(busyTimeout)
        if busyRetries is not None:
            self.db.setbusyhandler(lambda retries: retries <= busyRetries)
        self.db.setbusyhandler(lambda retries: True)

        self.extensions = []
        if len(extensions) > 0:
            self.db.enableloadextension(True)
            self.log.info('Loading extensions is enabled!')
            for extension in extensions:
                print(extension)
                self.db.loadextension(path.abspath(extension))

        # self.db.execute("pragma journal_mode=WAL")
        self.db.setrowtrace(DB.__rowFactory__)
        self._cursor = None

        # self.cursor.execute('PRAGMA page_size = 4096');
        self.pragmas = []
        for pragma in pragmas:
            print(pragma)
            self.cursor.execute(pragma)

        self.attaches = {}
        for schema, filePath in attaches.items():
            self.log.info('attaching {} as {}.'.format(filePath, schema))
            self.attach(filePath, schema)

    @staticmethod
    def __rowFactory__(cursor, row):
        columns = [t[0] for t in cursor.getdescription()]
        return dict(zip(columns, row))

    def __del__(self):
        # print('Closing db!')
        # if hasattr(self, 'db'):
        #    self.db.close()
        pass

    async def async_query(self, qpT, transformer=None, stripParams=False, debug=None):

        def helper():
            return self.query(qpT, transformer=transformer,
                              stripParams=stripParams, fetchAll=True, debug=debug)
        if self.warn_async_ctr > 0:
            self.log.warning(
                'sqlite DB version does not support async/await and the code runs executor.')
            self.warn_async_ctr -= 1
        res = await asyncio.get_event_loop().run_in_executor(None, helper)
        return res

    def queryTables(self, *args, schemaRE=None, tableRE=None, pathRE=None, **kwargs):
        schemas = R.map(lambda r: r['schema'])(self.querySchemas())
        tableQuery = self.tableQuery(
            schemas=schemas, schemaRE=schemaRE, tableRE=tableRE, pathRE=pathRE)
        return self.query(tableQuery, *args, **kwargs)

    def queryColumns(self, *args, schemaRE=None, tableRE=None, columnRE=None,
                     pathRE=None, **kwargs):
        schemas = R.map(lambda r: r['schema'])(self.querySchemas())
        columnsQuery = self.columnQuery(schemas=schemas, schemaRE=schemaRE, tableRE=tableRE, columnRE=columnRE,
                                        pathRE=pathRE)
        return self.query(columnsQuery, *args, **kwargs)

    def queryIndexes(self, *args, schemaRE=None, tableRE=None, indexRE=None,
                     pathRE=None, definitionRE=None, **kwargs):
        schemas = R.map(lambda r: r['schema'])(self.querySchemas())
        indexQuery = self.indexQuery(schemas=schemas, schemaRE=schemaRE, tableRE=tableRE, indexRE=indexRE,
                                     pathRE=pathRE, definitionRE=definitionRE)
        return self.query(indexQuery, *args, **kwargs)

    @classmethod
    def schemaQuery(cls, p={}, schemaRE=None):

        q = '''
        SELECT name AS schema 
        FROM  pragma_database_list
        '''
        qp = (q, p)
        pipes = Pipes()
        if schemaRE:
            qp = pipes.matches(qp, {
                'schema': schemaRE,
            }, quote=True)
        return qp

    @classmethod
    def indexQuery(cls, p={}, schemas=['main'], schemaRE=None, tableRE=None, indexRE=None, pathRE=None,
                   definitionRE=None):

        q = []
        for schema in schemas:
            q.append('''
                SELECT '{schema}' AS "schema", *
                , _ix.tbl_name AS "table" 
    	        , _ix.name AS "index" 
    	        , '{schema}' || '.' || _ix.tbl_name || '.' || _ix.name AS "path" 
    	        , _ix.sql AS "definition"
    	        , _ix.sql IS NULL AS primary_key 
                FROM "{schema}".sqlite_master _ix
                WHERE _ix."type" = 'index'
            '''.format(schema=schema))

        q = '\n\nUNION\n\n'.join(q)

        qp = (q, p)
        pipes = Pipes()
        if not schemaRE is None:
            qp = pipes.matches(qp, {
                'schema': schemaRE,
            }, quote=True)

        if not tableRE is None:
            qp = pipes.matches(qp, {
                'table': tableRE,
            }, quote=True)

        if not indexRE is None:
            qp = pipes.matches(qp, {
                'index': indexRE,
            }, quote=True)

        if not pathRE is None:
            qp = pipes.matches(qp, {
                'path': pathRE,
            }, quote=True)

        if definitionRE is not None:
            qp = pipes.matches(qp, {
                'definition': definitionRE,
            }, quote=True)

        return qp

    @classmethod
    def tableQuery(cls, p={}, schemas=['main'], schemaRE=None, tableRE=None, pathRE=None):

        q = []
        for schema in schemas:
            q.append('''
            SELECT '{schema}' AS "schema"
              , m.name AS "table", 
              '{schema}.' || m.name AS "path"
            FROM {schema}.sqlite_master AS m
            WHERE m.name NOT IN ('sqlite_sequence')
            '''.format(schema=schema))

        q = '\n\nUNION\n\n'.join(q)

        qp = (q, p)
        pipes = Pipes()

        if schemaRE:
            qp = pipes.matches(qp, {
                'schema': schemaRE,
            }, quote=True)

        if tableRE:
            qp = pipes.matches(qp, {
                'table': tableRE,
            }, quote=True)

        if pathRE:
            qp = pipes.matches(qp, {
                'path': pathRE,
            }, quote=True)

        return qp

    @classmethod
    def columnQuery(cls, p={}, schemas=['main'], schemaRE=None, tableRE=None, columnRE=None,
                    pathRE=None):

        q = []
        for schema in schemas:
            q.append('''
            SELECT '{schema}' AS "schema"
              , m.name AS "table" 
              , p.name AS "column", 
              '{schema}.' || m.name || '.' || p.name AS "path",
                p.pk AS "primary_key"
            FROM {schema}.sqlite_master AS m
            JOIN {schema}.pragma_table_info(m.name) AS p
            WHERE m.name NOT IN ('sqlite_sequence')
            '''.format(schema=schema))

        q = '\n\nUNION\n\n'.join(q)

        qp = (q, p)
        pipes = Pipes()

        if schemaRE:
            qp = pipes.matches(qp, {
                'schema': schemaRE,
            }, quote=True)

        if tableRE:
            qp = pipes.matches(qp, {
                'table': tableRE,
            }, quote=True)

        if columnRE:
            qp = pipes.matches(qp, {
                'column': columnRE,
            }, quote=True)

        if pathRE:
            qp = pipes.matches(qp, {
                'path': pathRE,
            }, quote=True)

        return qp

    def _queryHelper(self, qpT, transformer=None, stripParams=False, debug=None):

        def createFetchOne(_cursor):

            def fetchOne():
                return _cursor.fetchone()

            return fetchOne

        q, p, T = self.util.qpTSplit(qpT)
        p = P.pStrip(q, p) if stripParams else p
        # T = transformer if not transformer is None else Ts.transformerFactory(T, inverse=True)
        self.log.debug('q,p,T: %s, %s, %s' % (q, p, T))

        cursor = self.cursor
        r = cursor.execute(q, p)
        return createFetchOne(cursor)

    def attach(self, filePath, name=None):
        _name = name if name else filePath

        # self.db.attach(filePath, _name)
        cursor = self.db.cursor()
        q = 'ATTACH DATABASE "{}" AS "{}"'.format(filePath, _name)
        # print('q', q)
        cursor.execute(q)

    def tableExists(self, tableName, columnNames):
        sch, tbl = self.util.schemaTableSplit(tableName)
        sch = 'main' if sch is None else sch

        p = {}
        q = '''
        SELECT m.*, m.name as "table", p.name as "column", p.pk as "isPrimaryKey"
        FROM `{sch}`.sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE m.name = {tbl}
        ORDER BY m.name, p.name
        '''.format(sch=sch, tbl=self.util.p(p, tbl))
        rows = self.query((q, p))
        a = set([row['column'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)

    async def async_tableExists(self, tableName, columnNames):
        sch, tbl = self.util.schemaTableSplit(tableName)
        sch = 'main' if sch is None else sch

        p = {}
        q = '''
        SELECT m.*, m.name as "table", p.name as "column", p.pk as "isPrimaryKey"
        FROM `{sch}`.sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE m.name = {tbl}
        ORDER BY m.name, p.name
        '''.format(sch=sch, tbl=self.util.p(p, tbl))
        rows = await self.async_query((q, p))
        a = set([row['column'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)

    def exportToFile(self, path, invert=False, explain=False, schemas=['main'], restoreTables=None):

        if (invert):
            if not restoreTables is None:
                if len(restoreTables) < 1:
                    raise DBError('Nothing to restore. No tables given.')
                self.log.warning(
                    'Restore table subset is not implemented, restoring all tables.')
            for schema in schemas:
                try:
                    shell = apsw.Shell(db=self.connection)
                    # print(path, ' -> ', con.db_filename('main'))

                    if explain:
                        print('Restoring database {} from {}.'
                              .format(self.connection.db_filename(schema), path))
                        continue

                    shell.command_restore([schema, path])
                except apsw.Error as e:
                    self.log.error(e)
                    return -1
            return 0

        for schema in schemas:
            if explain:
                print('Dumping database {} to {}.'
                      .format(self.connection.db_filename(schema), path))
                continue

            newCon = apsw.Connection(path, statementcachesize=20)
            with newCon.backup(schemas[0], self.connection, schemas[0]) as b:
                while not b.done:
                    b.step(100)
                    # print(b.remaining, b.pagecount, "\r")
        return 0

    @property
    def cursor(self):
        return self.db.cursor()

    @property
    def filePath(self):
        return self._filePath

    @property
    def connection(self):
        return self.db
