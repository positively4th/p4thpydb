#import re
#from uuid import uuid4
#import subprocess
#import sqlite3
import apsw

from .db import DB
from .pipes import Pipes
from .util import Util
from ..differ import Differ as Differ0
from ..differ import QueryFactory as QueryFactory0
from ..differ import QueryRunner as QueryRunner0

        
class QueryFactory(QueryFactory0):

    def __init__(self):
        super().__init__(Util())
        self.pipes = Pipes()
        
    def columnsQuery(self, p={}, tableRE=None, columnRE=None):

        q = '''
        SELECT m.name as "table", p.name as "column", 
          m.name || '.' || p.name as "tablecolumn",
          p.pk as "isPrimaryKey"
        FROM main.sqlite_master AS m
        JOIN main.pragma_table_info(m.name) AS p
        WHERE m.name NOT IN ('sqlite_sequence')
        ORDER BY m.name, p.name
        '''
        qp = (q, p)
        if tableRE:
            qp = self.pipes.matches(qp, {
                'table': tableRE,
            }, quote=True)
        if columnRE:
            qp = self.pipes.matches(qp, {
            'tablecolumn': columnRE,
            }, quote=True)
        qp = self.pipes.columns(qp, ['table', 'column', 'isPrimaryKey'], quote=True)
        return qp
        
    def logQueries(self, table):

        def createProc(logTable, op):
            row = 'OLD' if op == 'D' else 'NEW'
            return '''
            BEGIN
            INSERT INTO {logTable} 
            SELECT _r.*, '{op}', 
              strftime('YYYY-MM-DDTHH:MM:SS.SSS', 'now'), 
              COALESCE((SELECT MAX(_ordinal_) + 1 as _ordinal_ FROM {logTable}), 0)
             FROM {table} _r
             WHERE _r.rowid = {row}.rowid;
            END
            '''.format(logTable=_q(logTable), table=_q(table), row=row, op=op)
        
        _u = self.util
        _q = self.util.quote
        _schema, _table = self.util.schemaTableSplit(table)
        _triggerName = '{}_trigger_{}' \
            .format(_u.schemaTableSplit((_schema, _table), infix='_', invert=True),
                    self.uid)
        _changeTable = '{}_change_{}' \
            .format(_u.schemaTableSplit((_schema, _table), infix='_', invert=True),
                    self.uid)
        qsOpen = [
            '''
            drop table if exists {} 
            '''.format(_q(_changeTable))
            ,
            '''
            create temp table {} as select * from {} where 1 = 0
            '''.format(_q(_changeTable), _q(table))
            ,
            '''
            alter table {} add column "_op_" text
            '''.format(_q(_changeTable))
            ,
            '''                
            alter table {} add column "_time_" test                
            '''.format(_q(_changeTable))
            ,
            '''                
            alter table {} add column "_ordinal_" int;                 
            '''.format(_q(_changeTable))
            ,
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_U'), _q(table))
            ,
            '''
            create temp trigger {triggerName} after update on {table}
            for each row {proc}
            '''.format(triggerName=_q(_triggerName+'_U'), table=_q(table),
                       proc=createProc(_changeTable, 'U'))
            ,
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_I'), _q(table))
            ,
            '''
            create temp trigger {triggerName} after insert on {table}
            for each row {proc}
            '''.format(triggerName=_q(_triggerName+'_I'), table=_q(table),
                       proc=createProc(_changeTable, 'I'))
            ,
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_D'), _q(table))
            ,
            '''
            create temp trigger {triggerName} before delete on {table}
            for each row {proc}
            '''.format(triggerName=_q(_triggerName+'_D'), table=_q(table),
                       proc=createProc(_changeTable, 'D'))
        ]
        qsClose = [
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_U'), _q(table))
            ,
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_I'), _q(table))
            ,
            '''
            drop trigger if exists {}
            '''.format(_q(_triggerName+'_D'), _q(table))
            ,
            '''
            drop table if exists {}
            '''.format(_q(_changeTable))
            ,
        ]
        return _changeTable, qsOpen, qsClose

class QueryRunner(QueryRunner0):

    def __init__(self, db):
        super().__init__(db)
        
    
class Differ(Differ0):

    def __init__(self, db):
        super().__init__(Util(), QueryFactory(), QueryRunner(db))
        
if __name__ == '__main__':

    pass
