from uuid import uuid4

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

    def schemasQuery(self, p={}):
        q = '''
        SELECT schema_name AS schema, *
        FROM information_schema.schemata AS _s
        '''
        qp = (q, p)

        return qp
        

    def columnsQuery(self, p={}, schemaRE=None, tableRE=None, columnRE=None,
                     columnMask=['table', 'column', 'isPrimaryKey']):
        q = '''
        SELECT tc.table_schema || '.'  || c.table_name AS table, c.column_name AS column, 
          tc.table_schema || '.'  || c.table_name || '.' || c.column_name AS _fqn,
          tc.table_schema AS schema,
          CASE 
           WHEN ccu.column_name IS NOT NULL THEN 1
            ELSE 0
          END AS "isPrimaryKey"
        FROM information_schema.columns AS c
        LEFT JOIN information_schema.table_constraints tc ON (
          c.table_schema = tc.constraint_schema
          AND 
          tc.table_name = c.table_name AND constraint_type = 'PRIMARY KEY'
        )
        LEFT JOIN information_schema.constraint_column_usage AS ccu ON (
          ccu.constraint_schema = tc.constraint_schema
          AND
          ccu.constraint_name = tc.constraint_name
         AND 
          ccu.column_name = c.column_name
        ) 
        where c.table_schema not in ('pg_catalog', 'information_schema')
        '''
        qp = (q, p)

        if not schemaRE is None:
            qp = self.pipes.matches(qp, {
                'schema': schemaRE,
            }, quote=True)

        if not tableRE is None:
            qp = self.pipes.matches(qp, {
                'table': tableRE,
            }, quote=True)

        if not columnRE is None:
            qp = self.pipes.matches(qp, {
                '_fqn': columnRE,
            }, quote=True)
 
        qp = self.pipes.columns(qp, columnMask, quote=True)

        return qp

    
    def logQueries(self, table):
        _u = self.util
        _q = self.util.quote
        _schema, _table = self.util.schemaTableSplit(table)
        _procName = '{}_proc_{}' \
            .format(_u.schemaTableSplit((_schema, _table), infix='_', invert=True),
                    self.uid)

        _triggerName = '{}_trigger_{}' \
            .format(_u.schemaTableSplit((_schema, _table), infix='_', invert=True),
                    self.uid)
        _changeTable = '{}_change_{}'.format(_schema, _table, self.uid)

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
            alter table {} add column "_op_" char(1)
            '''.format(_q(_changeTable))
            ,
            '''                
            alter table {} add column "_time_" timestamp without time zone;                 
            '''.format(_q(_changeTable))
            ,
            '''                
            alter table {} add column "_ordinal_" serial;                 
            '''.format(_q(_changeTable))
            ,
            '''
            drop trigger if exists {} on {}
            '''.format(_q(_triggerName), _q(table))
            ,
            '''
            CREATE OR REPLACE FUNCTION {procName}() RETURNS TRIGGER AS $res$
            BEGIN
              IF (TG_OP = 'DELETE') THEN
                INSERT INTO {changes} SELECT OLD.*, 'D', now();
                RETURN OLD;
              ELSIF (TG_OP = 'UPDATE') THEN
                INSERT INTO {changes} SELECT NEW.*, 'U', now();
                RETURN NEW;
              ELSIF (TG_OP = 'INSERT') THEN
                INSERT INTO {changes} SELECT NEW.*, 'I', now();
                RETURN NEW;
              END IF;
              RETURN NULL;
            END;
            $res$ LANGUAGE plpgsql;
            '''.format(procName=_q(_procName), changes=_q(_changeTable))
            ,
            '''
            create trigger {triggerName} after insert or update or delete on {table}
            for each row execute procedure {procName}()
            '''.format(triggerName=_q(_triggerName), table=_q(table),
                       procName=_q(_procName))
        ]
        qsClose = [
            '''
            drop trigger if exists {} on {}
            '''.format(_q(_triggerName), _q(table))
            ,
            '''
            drop function if exists {}
            '''.format(_q(_procName))
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
        util = Util()
        super().__init__(Util(), QueryFactory(), QueryRunner(db))
        
if __name__ == '__main__':

    pass
