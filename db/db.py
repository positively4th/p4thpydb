from .ts import Ts
import re
from uuid import uuid4
import logging
log0 = logging.getLogger(__name__)


class DBError(Exception):
    pass


class DB:

    @staticmethod
    def escapeRE(text):
        res = re.escape(text)
        return res

    @classmethod
    def createPipes(cls):
        assert False, 'Not implemented'

    @classmethod
    def createORM(cls, db):
        assert False, 'Not implemented'

    @classmethod
    def createUtil(cls):
        assert False, 'Not implemented'

    def clone(self):
        assert False, 'Not implemented'

    def __init__(self, util, log=log0):
        self.util = util
        self._log = log
        self.savepoints = []

    @property
    def log(self):
        return log0 if self._log is None else self._log

    def exportToFile(self, path, invert=False, explain=False, schemas=None, restoreTables=None):
        raise DBError('Not implemented')

    def startTransaction(self):
        self.log.debug('startTransaction (%s)' % len(self.savepoints))
        id = str(uuid4())
        self.query('SAVEPOINT "{}"'.format(id), debug=None)
        self.savepoints.append(id)

    def rollback(self):
        self.log.debug('rollback (%s)' % len(self.savepoints))
        if len(self.savepoints) < 1:
            return
        id = self.savepoints.pop()
        self.query(('ROLLBACK TO "{}"'.format(id)), debug=None)
        self.query(('RELEASE "{}"'.format(id)), debug=None)

    def commit(self):
        self.log.debug('commit (%s)' % len(self.savepoints))
        id = self.savepoints.pop()
        self.query(('RELEASE "{}"'.format(id)), debug=None)

    def queryColumns(self, *args, schemaRE=None, tableRE=None, columnRE=None,
                     pathRE=None, **kwargs):
        columnsQuery = self.columnQuery(schemaRE=schemaRE, tableRE=tableRE, columnRE=columnRE,
                                        pathRE=pathRE)
        return self.query(columnsQuery, *args, **kwargs)

    def querySchemas(self, *args, **kwargs):
        schemaQuery = self.schemaQuery()
        return self.query(schemaQuery, *args, **kwargs)

    def queryTables(self, *args, schemaRE=None, tableRE=None, pathRE=None, **kwargs):
        tablesQuery = self.tableQuery(
            schemaRE=schemaRE, tableRE=tableRE, pathRE=pathRE)
        return self.query(tablesQuery, *args, **kwargs)

    def queryIndexes(self, *args, schemaRE=None, tableRE=None, indexRE=None,
                     pathRE=None, definitionRE=None, **kwargs):
        indexQuery = self.indexQuery(schemaRE=schemaRE, tableRE=tableRE, indexRE=indexRE,
                                     pathRE=pathRE, definitionRE=definitionRE)
        return self.query(indexQuery, *args, **kwargs)

    def _queryHelper(self, qpT, transformer=None, stripParams=False, debug=None):
        raise DBError('Not implemented.')

    def query(self, qpT, transformer=None, stripParams=False, fetchAll=False, debug=None):
        q, p, T = self.util.qpTSplit(qpT)
        T = transformer if transformer is not None else Ts.transformerFactory(
            T, inverse=True)
        fetchOne = self._queryHelper((q, p), transformer, stripParams, debug)

        gen = self.generateRow(fetchOne, T)
        if fetchAll:
            return [r for r in gen]

        return gen

    @classmethod
    def constantRows(cls, colTypeMap: dict, rows: tuple | list):

        util = cls.createUtil()
        qColumns = util.quote([col for col in colTypeMap.keys()])
        qColumns = ', '.join(qColumns)
        qRows = []
        for row in rows:
            qRow = []
            for name, type in colTypeMap.items():
                value = row[name] if name in row else 'null'
                if value != 'null':
                    value = 'cast({value} as {type})'.format(
                        value=value, type=type)
                qRow.append(value)
            qRows.append('({})'.format(', '.join(qRow)))
        qRows = ','.join(qRows)

        q = '''
        select *
        from (
        values {rows} 
        ) as q ({columns})
        '''.format(columns=qColumns, rows=qRows)

        return q


    @staticmethod
    def generateRow(fetchOne, T):
        while True:
            row = fetchOne()
            if row:
                yield T(row)
            else:
                break
