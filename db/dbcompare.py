import re
from uuid import uuid4
import subprocess
import sqlite3

from .db import DB
from contrib.p4thpy.tools import Tools

class QueryFactory:

    def __init__(self, db=None):
        self.db = DB() if db is None else DB()
        
    def _joinCondition(self, leftAlias, rightAlias, columns=None):
        _columns = columns \
            if Tools.isDict(columns) else dict(zip(columns, columns))

        cs = Tools.mapKeyVal(_columns,
                           lambda rc, lc: '{}.{} = {}.{}'
                           .format(self.db.quote(leftAlias),
                                   self.db.quote(lc),
                                   self.db.quote(rightAlias),
                                   self.db.quote(rc)))
        return ' AND '.join(cs.values())
        
    def _anyDiffCondition(self, leftAlias, rightAlias, columnNames):

        def isDistcintFrom(qL, qR):
            return '''(
             CASE 
              WHEN {qL} = {qR} OR ({qL} IS NULL AND {qR} IS NULL) THEN 0
              ELSE 1
             END = 1
            )'''.format(qL=qL, qR=qR)

        #print(columnNames)
        cs = Tools.map(columnNames,
                           lambda columnName: isDistcintFrom(self.db.quote(columnName, table=leftAlias),
                                                         self.db.quote(columnName, table=rightAlias))
                             )
        return ' OR '.join(cs)

    def deletedRowsQuery(self, primaryKeyNames, preTable, postTable, columnNames, p={}):
        joinClause = self._joinCondition('_pre', '_post', primaryKeyNames)
        selectClause = ','.join(self.db.quote(columnNames, table='_pre'))
        orderClause = ','.join(self.db.quote(primaryKeyNames, table='_pre'))
        pkNotNullClause = '_post.{} IS NULL'.format(self.db.quote(primaryKeyNames[0]))

        q = '''
        SELECT {selectClause}
        FROM {preTable} as _pre
        LEFT JOIN {postTable} as _post ON {joinClause}
        WHERE {pkNotNullClause}
        ORDER BY {orderClause}
        '''.format(selectClause=selectClause,
                   preTable=self.db.quote(preTable),
                   postTable=self.db.quote(postTable),
                   joinClause=joinClause,
                   pkNotNullClause=pkNotNullClause,
                   orderClause=orderClause)
        return (q,p)

    def changedRowsQuery(self, primaryKeyNames, preTable, postTable, columnNames, p={}):
        valueColumns = list(set(columnNames) - set(primaryKeyNames))
        joinClause = self._joinCondition('_pre', '_post', primaryKeyNames)
        selectClause = ','.join(self.db.quote(columnNames, table='_post'))
        orderClause = ','.join(self.db.quote(primaryKeyNames, table='_pre'))
        anyDiffClause = self._anyDiffCondition('_pre', '_post', valueColumns)

        q = '''
        SELECT {selectClause}
        FROM {preTable} as _pre
        INNER JOIN {postTable} as _post ON {joinClause}
        WHERE {anyDiffClause}
        ORDER BY {orderClause}
        '''.format(selectClause=selectClause,
                   preTable=self.db.quote(preTable),
                   postTable=self.db.quote(postTable),
                   joinClause=joinClause,
                   anyDiffClause=anyDiffClause,
                   orderClause=orderClause)
        return (q,p)

    def createdRowsQuery(self, primaryKeyNames, preTable, postTable, columnNames, p={}):
        joinClause = self._joinCondition('_post', '_pre', primaryKeyNames)
        selectClause = ','.join(self.db.quote(columnNames, table='_post'))
        orderClause = ','.join(self.db.quote(primaryKeyNames, table='_post'))
        pkNotNullClause = '_pre.{} IS NULL'.format(self.db.quote(primaryKeyNames[0]))

        q = '''
        SELECT {selectClause}
        FROM {postTable} as _post
        LEFT JOIN {preTable} as _pre ON {joinClause}
        WHERE {pkNotNullClause}
        ORDER BY {orderClause}
        '''.format(selectClause=selectClause,
                   preTable=self.db.quote(preTable),
                   postTable=self.db.quote(postTable),
                   joinClause=joinClause,
                   pkNotNullClause=pkNotNullClause,
                   orderClause=orderClause)
        return (q,p)

        
    def cloneTables(self, tables=[]):
        qps = {}
        for table in tables:
            name = '''{}_{}'''.format(str(uuid4()), table)
            q = '''
            CREATE TEMP TABLE "{}" AS SELECT * FROM "{}";
            '''.format(name, table)
            qps[name] = (q,{})
        return qps

    
#import apsw


class QueryRunner:

    def __init__(self, db):
        self.db = db
        
    def run(self, qps):
        #print(qps)
        _qps = (qps, {}) if Tools.isString(qps) else qps
        _qps = [_qps] if Tools.isTuple(_qps) else _qps
        res = []
        for _,qp in Tools.keyValIter(_qps, stringAsSingular=True):
            _qp = (qp,) if Tools.isString(qp) else qp
            #print(_qp)
            res.append(self.db.query(_qp))
        return res[0] if Tools.isTuple(qps) or Tools.isString(qps) else res
    
class DBCompare:

    def __init__(self, queryFactory, queryRunner):
        self.queryFactory = queryFactory
        self.queryRunner = queryRunner

    def queryColumns(self, tables=None):
        columnsQuery = self.queryFactory.columnsQuery(tables=tables)
        return self.queryRunner.run(columnsQuery)
 
    def queryTables(self):
        columns = self.queryColumns()
        tables = Tools.pipe(columns, [
            [Tools.pluck, [lambda r, i: r['table']], {}],
            [Tools.unique, [], {}]
        ])
        return tables

    def queryPrimaryKeys(self, tables):
        columnsQuery = self.queryFactory.columnsQuery(tables)
        r = self.queryRunner.run(columnsQuery)
        tables = Tools.pipe(flatTableColumnRows, [
            [Tools.pluck, [lambda r, i: r['table']], {}],
            [Tools.unique, [], {}]
        ])
        return tables

    def cloneTables(self, tables):
        qps = self.queryFactory.cloneTables(tables)
        nameMap = dict(zip(tables, qps.keys()))
        self.queryRunner.run(qps)
        return nameMap
        
    
    def prepare(self, spec={}):
        spec['tables'] = self.queryTables() \
            if not 'tables' in spec else spec['tables']

        spec['columns'] = self.queryColumns(spec['tables']) \
            if not 'columns' in spec else spec['columns']

        spec['tableCloneMap'] = self.cloneTables(spec['tables'])

        return spec

    def diff(self, spec={}):

        def columnsByTableEncoder (column, *args):
            return column['table']

        columnsByTable = Tools.group(spec['columns'], columnsByTableEncoder)
        tableDiffMap = {} if 'tableDiffMap' not in spec else spec['tableDiffMap']
        for table, columns in Tools.keyValIter(columnsByTable):
            assert table in spec['tableCloneMap']

            diff = tableDiffMap[table] if table in tableDiffMap else {}

            #primaryKeys = Tools.filter(columns, lambda column: column['isPrimaryKey'])
            primaryKeyNames = Tools.pipe(columns, [
                [Tools.filter, [lambda column: column['isPrimaryKey']]],
                [Tools.map, [lambda column: column['column']]]
            ])

            columnNames = Tools.map(columns, lambda column: column['column'])

            spec['primaryKeys'] = primaryKeyNames
            
            if not 'deleted' in diff:
                preTable = spec['tableCloneMap'][table]
                pqDeletedRows = self.queryFactory.deletedRowsQuery(primaryKeyNames, preTable, table, columnNames)
                deletedRows = self.queryRunner.run(pqDeletedRows)
                diff['deleted'] = deletedRows

            if not 'changed' in diff:
                preTable = spec['tableCloneMap'][table]
                pqChangedRows = self.queryFactory.changedRowsQuery(primaryKeyNames, preTable, table, columnNames)
                changedRows = self.queryRunner.run(pqChangedRows)
                diff['changed'] = changedRows

            if not 'created' in diff:
                preTable = spec['tableCloneMap'][table]
                pqCreatedRows = self.queryFactory.createdRowsQuery(primaryKeyNames, preTable, table, columnNames)
                createdRows = self.queryRunner.run(pqCreatedRows)
                diff['created'] = createdRows

            tableDiffMap[table] = diff
            
        spec['tableDiffMap'] = tableDiffMap 

        

        
if __name__ == '__main__':

    print('\n', 'No tests to run.')
