from os import path

from ..db import DB as DB0
from ..ts import Ts
from .util import Util

import apsw
#print ("      Using APSW file",apsw.__file__)                # from the extension module
#print ("         APSW version",apsw.apswversion())           # from the extension module
#print ("   SQLite lib version",apsw.sqlitelibversion())      # from the sqlite library code
#print ("SQLite header version",apsw.SQLITE_VERSION_NUMBER)   # from the sqlite header file at compile time


class DB(DB0):

    def __init__(self, *args, fileName=':memory:', extensions=[],
                 pragmas=[], attaches={}, log=None):

        util = Util();
        super().__init__(util, log=log);

        self.log.info('filename: %s' % fileName)
        self._filePath = fileName
        self.db = apsw.Connection(fileName, statementcachesize=20)

        if len(extensions) > 0:
            self.db.enableloadextension(True)
            self.log.info('Loading extensions is enabled!')
            for extension in extensions:
                print(extension)
                self.db.loadextension(path.abspath(extension))

        self.db.setrowtrace(DB.__rowFactory__)
        self._cursor = None

        #self.cursor.execute('PRAGMA page_size = 4096');
        for pragma in pragmas:
            print(pragma)
            self.cursor.execute(pragma)

        for schema, filePath in attaches.items():
            self.log.info('attaching {} as {}.'.format(filePath, schema))
            self.attach(filePath, schema)

    
    @staticmethod
    def __rowFactory__(cursor, row):
        columns = [t[0] for t in cursor.getdescription()]
        #print('cols', columns)
        #print('row', row)
        return dict(zip(columns, row))

    def __del__(self):
        #print('Closing db!')
        #if hasattr(self, 'db'):
        #    self.db.close()
        pass
        
    def query(self, qp, transformer=None, stripParams=False, debug=None):
        q,p, T = self.util.qpTSplit(qp)
        p = P.pStrip(q, p) if stripParams else p
        T = transformer if not transformer is None else Ts.transformerFactory(T, inverse=True)
        
        self.log.debug('q,p,T: %s, %s, %s' % (q, p, T))

        r = self.cursor.execute(q, p)
        if T is None:
            return r.fetchall()

        return [
            T(row) for row in r.fetchall()
        ]
    
    def attach(self, filePath, name=None):
        _name = name if name else filePath

        #self.db.attach(filePath, _name)
        cursor = self.db.cursor()
        q = 'ATTACH DATABASE "{}" AS "{}"'.format(filePath, _name)
        #print('q', q)
        cursor.execute(q)

    def tableExists(self, tableName, columnNames):
        p = {}
        q = '''
        SELECT m.name as "table", p.name as "column", p.pk as "isPrimaryKey"
        FROM sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE m.name = {}
        ORDER BY m.name, p.name
        '''.format(self.util.p(p, tableName))
        rows = self.query((q,p))
        a = set([row['column'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)
                
    def exportToFile(self, path, invert=False):

        if (invert):
            shell = apsw.Shell(db=self.connection);
            #print(shell.process_command, path)
            shell.command_restore([path])
            #assert 1 == 0


        newCon = apsw.Connection(path, statementcachesize=20)
        with newCon.backup("main", self.connection, "main") as b:
            while not b.done:
                b.step(100)
                #print(b.remaining, b.pagecount, "\r")

    @property
    def cursor(self):
        if self._cursor is None:
            self._cursor = self.db.cursor()
        return self._cursor
    
    @property
    def filePath(self):
        return self._filePath

    @property
    def connection(self):
        return self.db
        
