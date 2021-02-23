#import re
#from uuid import uuid4
#import subprocess
#import sqlite3
import apsw

from .db import DB
from ..pipe import Pipe
from ..dbcompare import DBCompare as DBCompare0
from ..dbcompare import QueryFactory as QueryFactory0
from ..dbcompare import QueryRunner as QueryRunner0

        
class QueryFactory(QueryFactory0):

    def __init__(self, db=None, pipe=None):
        super().__init__(db=db)
        self.pipe = Pipe() if pipe is None else pipe
        
    def columnsQuery(self, p={}, tables=None):
        q = '''
        SELECT m.name as "table", p.name as "column", p.pk as "isPrimaryKey"
        FROM sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE m.name NOT IN ('sqlite_sequence')
        ORDER BY m.name, p.name
        '''
        qp = (q, p)
        qp = self.pipe.member(qp, 'table', tables)
        return qp
        

class QueryRunner(QueryRunner0):

    def __init__(self, db):
        super().__init__(DB(db) if isinstance(db, str) else db)
        
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
    
class DBCompare(DBCompare0):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        
if __name__ == '__main__':

    pass
