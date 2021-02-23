
from ..db import DB as DB0
from ..ts import Ts

import apsw
print ("      Using APSW file",apsw.__file__)                # from the extension module
print ("         APSW version",apsw.apswversion())           # from the extension module
print ("   SQLite lib version",apsw.sqlitelibversion())      # from the sqlite library code
print ("SQLite header version",apsw.SQLITE_VERSION_NUMBER)   # from the sqlite header file at compile time


class DB(DB0):

    
    tableQuery = "SELECT name FROM sqlite_master WHERE type ='table' AND name NOT LIKE 'sqlite_%'"
    schemaQuery = "" \
                  + "SELECT m.name as tableName, p.name as columnName" \
                  + " FROM sqlite_master m" \
                  + ' LEFT OUTER JOIN pragma_table_info(m.name) p on m.name <> p.name'

    @staticmethod
    def __rowFactory__(cursor, row):
        columns = [t[0] for t in cursor.getdescription()]
        #print('cols', columns)
        #print('row', row)
        return dict(zip(columns, row))

    def __init__(self, fileName=':memory:', extensions=[], pragmas=[], attaches={}):

        super().__init__();

        self._filePath = fileName
        self.db = apsw.Connection(fileName, statementcachesize=20)

        if len(extensions) > 0:
            self.db.enableloadextension(True)
            print('\nLoading extensions is enabled!\n')
            for extension in extensions:
                print(extension)
                self.db.loadextension(abspath(extension))

        self.db.setrowtrace(DB.__rowFactory__)
        self._cursor = None

        #self.cursor.execute('PRAGMA page_size = 4096');
        for pragma in pragmas:
            print(pragma)
            self.cursor.execute(pragma)

        for schema, filePath in attaches.items():
            print('attaching {} as {}.'.format(filePath, schema))
            self.attach(filePath, schema)

        self.savepoints = []

    def __del__(self):
        #print('Closing db!')
        self.db.close()
        
    def query(self, qp, transformer=None, stripParams=False, debug=None):
        q,p, T = self.__qpTSplit__(qp)
        p = P.pStrip(q, p) if stripParams else p
        T = transformer if not transformer is None else Ts.transformerFactory(T, inverse=True)
        if debug or (debug is None and self.__class__.__DEBUG__):
            print('q,p,T:', q, p, T)

        r = self.cursor.execute(q, p)
        if T is None:
            return r.fetchall()

        return [
            T(row) for row in r.fetchall()
        ]
    
    def __del__(self):
        #print('Closing db!')
        self.db.close()
    
    def attach(self, filePath, name=None):
        _name = name if name else filePath

        #self.db.attach(filePath, _name)
        cursor = self.db.cursor()
        q = 'ATTACH DATABASE "{}" AS "{}"'.format(filePath, _name)
        #print('q', q)
        cursor.execute(q)

    def exportToFile(self, path, invert=False):

        if (invert):
            shell = apsw.Shell(db=self.db.connection);
            #print(shell.process_command, path)
            shell.command_restore([path])
            #assert 1 == 0


        newCon = apsw.Connection(path, statementcachesize=20)
        with newCon.backup("main", self.db.connection, "main") as b:
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
        
