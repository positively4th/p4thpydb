from uuid import uuid4
import ramda as R

from .util import Util
from contrib.p4thpymisc.src.misc import items


class QueryFactoryError(Exception):
    pass


class QueryFactory:

    uid = str(uuid4())

    def __init__(self, util):
        self.util = util

    def _joinCondition(self, leftAlias, rightAlias, columns=None):
        _columns = columns \
            if isinstance(columns, dict) else dict(zip(columns, columns))

        cs = R.pipe(
            R.to_pairs,
            R.map(lambda rclc: (rclc[0],  '{}.{} = {}.{}'
                                .format(self.util.quote(leftAlias),
                                        self.util.quote(rclc[1]),
                                        self.util.quote(rightAlias),
                                        self.util.quote(rclc[0])))
                  ),
            R.from_pairs

        )(_columns)
        return ' AND '.join(cs.values())

    def _anyDiffCondition(self, leftAlias, rightAlias, columnNames):

        def isDistcintFrom(qL, qR):
            return '''(
             CASE 
              WHEN {qL} = {qR} OR ({qL} IS NULL AND {qR} IS NULL) THEN 0
              ELSE 1
             END = 1
            )'''.format(qL=qL, qR=qR)

        cs = R.map(lambda columnName: isDistcintFrom(self.util.quote(columnName, table=leftAlias),
                                                     self.util.quote(columnName, table=rightAlias))
                   )(columnNames)
        return ' OR '.join(cs)

    def latestChangeQuery(self, primaryKeyNames, changeTable, p={}):
        joinClause = self._joinCondition('_l', '_r', primaryKeyNames)
        orderClause = ','.join(self.util.quote(primaryKeyNames, table='_l'))
        pkNullClause = '_r.{} IS NULL'.format(
            self.util.quote(primaryKeyNames[0]))

        q = '''
        SELECT _l.*
        FROM {changeTable} as _l
        LEFT JOIN {changeTable} as _r ON (
          {joinClause} AND _r._ordinal_ > _l._ordinal_
        )
        WHERE {pkNullClause}
        ORDER BY {orderClause}
        '''.format(changeTable=self.util.quote(changeTable),
                   joinClause=joinClause,
                   pkNullClause=pkNullClause,
                   orderClause=orderClause)
        return (q, p)

    def deletedRowsQuery(self, primaryKeyNames, changeTable, columnNames, p={}):
        selectClause = ','.join(self.util.quote(columnNames, table='_lc'))
        orderClause = ','.join(self.util.quote(primaryKeyNames, table='_lc'))

        q, p = self.latestChangeQuery(primaryKeyNames, changeTable, p=p)
        q = '''
        WITH _lc AS (
          {_lc}
        )
        SELECT {selectClause}
        FROM _lc
        WHERE _lc._op_ = 'D'
        ORDER BY {orderClause}
        '''.format(
            _lc=q,
            selectClause=selectClause,
            preTable=self.util.quote(changeTable),
            orderClause=orderClause)
        return (q, p)

    def changedRowsQuery(self, primaryKeyNames, changeTable, columnNames, p={}):
        selectClause = ','.join(self.util.quote(columnNames, table='_lc'))
        orderClause = ','.join(self.util.quote(primaryKeyNames, table='_lc'))

        q, p = self.latestChangeQuery(primaryKeyNames, changeTable, p=p)
        q = '''
        WITH _lc AS (
          {_lc}
        )
        SELECT {selectClause}
        FROM _lc
        WHERE _lc._op_ = 'U'
        ORDER BY {orderClause}
        '''.format(
            _lc=q,
            selectClause=selectClause,
            preTable=self.util.quote(changeTable),
            orderClause=orderClause)
        return (q, p)

    def createdRowsQuery(self, primaryKeyNames, changeTable, columnNames, p={}):
        selectClause = ','.join(self.util.quote(columnNames, table='_lc'))
        orderClause = ','.join(self.util.quote(primaryKeyNames, table='_lc'))

        q, p = self.latestChangeQuery(primaryKeyNames, changeTable, p=p)
        q = '''
        WITH _lc AS (
          {_lc}
        )
        SELECT {selectClause}
        FROM _lc
        WHERE _lc._op_ = 'I'
        ORDER BY {orderClause}
        '''.format(
            _lc=q,
            selectClause=selectClause,
            preTable=self.util.quote(changeTable),
            orderClause=orderClause)
        return (q, p)

    def logQueries(self, table):
        raise QueryFactoryError('Not implemented')


class QueryRunner:

    uid = str(uuid4())

    def __init__(self, db):
        self.db = db

    def run(self, qps, *args, **kwargs):
        # print(qps)
        _qps = (qps, {}) if isinstance(qps, str) else qps
        _qps = [_qps] if isinstance(_qps, tuple) else _qps
        res = []
        for _, qp in items(_qps):
            _qp = (qp,) if isinstance(qp, str) else qp
            res.append(self.db.query(_qp, *args, fetchAll=True, **kwargs))
        return res[0] if isinstance(qps, tuple) or isinstance(qps, str) else res


class Differ:

    def __init__(self, util, queryFactory, queryRunner):
        self.util = util
        self._queryFactory = queryFactory
        self._queryRunner = queryRunner

    @property
    def queryFactory(self):
        return self._queryFactory

    @property
    def queryRunner(self):
        return self._queryRunner

    def queryColumns(self, schemaRE=None, tableRE=None, columnRE=None):
        columnsQuery = self.queryFactory.columnsQuery(
            schemaRE=schemaRE, tableRE=tableRE, columnRE=columnRE)
        return self.queryRunner.run(columnsQuery)

    def querySchemas(self):
        schemasQuery = self.queryFactory.schemasQuery()
        return self.queryRunner.run(schemasQuery)

    def queryTables(self, schemaRE=None, tableRE=None, columnRE=None):
        columns = self.queryColumns(
            schemaRE=schemaRE, tableRE=tableRE, columnRE=columnRE)
        tables = R.pipe(
            R.pluck('table'),
            R.uniq,
        )(columns)
        return tables

    def queryPrimaryKeys(self, tableRE=None):
        columnsQuery = self.queryFactory.columnsQuery(tableRE=tableRE)
        rows = self.queryRunner.run(columnsQuery)
        rows = R.pipe(
            R.filter(lambda r: bool(r['isPrimaryKey'])),
            R.map(lambda r: {'table': r['table'], 'column': r['column']}),
            R.uniq,
        )(rows)
        return rows

    def setUpLogging(self, tables):

        nameMap = {}
        restorQueries = []
        for table in tables:
            changeTableName, qsOpen, qsClose \
                = self.queryFactory.logQueries(table)
            self.queryRunner.run(qsOpen, debug=None)
            nameMap[table] = changeTableName
            restorQueries = restorQueries + qsClose
        return nameMap, restorQueries

    def tearDownLogging(self, removeQueries):

        nameMap = {}
        restorQueries = []
        for table in tables:
            changeTableName, qsOpen, qsClose \
                = self.queryFactory.logQueries(table)
            self.queryRunner.run(qsOpen)
            nameMap[table] = changeTableName
            restorQueries = restorQueries + qsClose
        return nameMap, restorQueries

    def prepare(self, spec={}):
        spec['tableRE'] = spec['tableRE'] if 'tableRE' in spec else None
        spec['columnRE'] = spec['columnRE'] if 'columnRE' in spec else None
        spec['tables'] = spec['tables'] if 'tables' in spec \
            else self.queryTables(tableRE=spec['tableRE'], columnRE=spec['columnRE'])

        spec['columns'] = self.queryColumns(tableRE=spec['tableRE'], columnRE=spec['columnRE']) \
            if not 'columns' in spec else spec['columns']

        nameMap, restorQueries = self.setUpLogging(spec['tables'])
        spec['tableCloneMap'] = nameMap
        spec['restoreQueries'] = restorQueries

        return spec

    def diff(self, spec={}):

        def columnsByTableEncoder(column, *args):
            return column['table']

        columnsByTable = R.group_by(columnsByTableEncoder)(spec['columns'])
        tableDiffMap = {
        } if 'tableDiffMap' not in spec else spec['tableDiffMap']
        for table, columns in items(columnsByTable):
            assert table in spec['tableCloneMap']

            diff = tableDiffMap[table] if table in tableDiffMap else {}
            primaryKeyNames = R.pipe(
                R.filter(lambda column: column['isPrimaryKey']),
                R.map(lambda column: column['column'])
            )(columns)

            columnNames = R.map(lambda column: column['column'])(columns)

            spec['primaryKeys'] = primaryKeyNames

            if not 'deleted' in diff:
                changeTable = spec['tableCloneMap'][table]
                pqDeletedRows = self.queryFactory.deletedRowsQuery(
                    primaryKeyNames, changeTable, columnNames)
                deletedRows = self.queryRunner.run(pqDeletedRows)
                diff['deleted'] = deletedRows

            if not 'changed' in diff:
                changeTable = spec['tableCloneMap'][table]
                pqChangedRows = self.queryFactory.changedRowsQuery(
                    primaryKeyNames, changeTable, columnNames)
                changedRows = self.queryRunner.run(pqChangedRows)
                diff['changed'] = changedRows

            if not 'created' in diff:
                changeTable = spec['tableCloneMap'][table]
                pqCreatedRows = self.queryFactory.createdRowsQuery(
                    primaryKeyNames, changeTable, columnNames)
                createdRows = self.queryRunner.run(pqCreatedRows)
                diff['created'] = createdRows

            tableDiffMap[table] = diff

        spec['tableDiffMap'] = tableDiffMap

    def finalize(self, spec):
        if spec and 'restoreQueries' in spec:
            self.queryRunner.run(spec['restoreQueries'], debug=None)
            del spec['restoreQueries']
