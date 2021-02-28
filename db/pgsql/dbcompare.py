from uuid import uuid4

from .db import DB
from .pipes import Pipes
from .util import Util
from ..dbcompare import DBCompare as DBCompare0
from ..dbcompare import QueryFactory as QueryFactory0
from ..dbcompare import QueryRunner as QueryRunner0

        
class QueryFactory(QueryFactory0):

    def __init__(self):
        super().__init__(Util())
        self.pipes = Pipes()
        
    def columnsQuery(self, p={}, tableRE='.*'):
        q = '''
        SELECT tc.table_schema || '.'  || c.table_name AS table, c.column_name AS column, 
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
        order by c.table_schema, c.table_name, c.column_name
        '''
        qp = (q, p)
        qp = self.pipes.matches(qp, {
            'table': tableRE,
        })
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
            alter table {} add column "_time_" timestamp;                 
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
        
class DBCompare(DBCompare0):

    def __init__(self, db):
        util = Util()
        super().__init__(Util(), QueryFactory(), QueryRunner(db))
        
if __name__ == '__main__':

    pass
