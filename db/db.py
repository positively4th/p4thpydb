from .ts import Ts
import re
from uuid import uuid4
from xxhash import xxh64
import ramda as R
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

    def __init__(self, util, idArgs, cloneArgs, log=log0):
        self.cloneArgs = cloneArgs
        self.idArgs = idArgs

        self.util = util
        self._log = log
        self.savepoints = []

    @classmethod
    def _makeArgs(cls, args, kwargs):
        return (args, kwargs)

    def clone(self):
        return self.__class__(*self.cloneArgs[0], **self.cloneArgs[1])

    def _makeId(self, idArgs):
        id = xxh64()
        id.update(self.__class__.__name__)
        id.update(self.__class__.__module__)

        for arg in sorted(idArgs[0]):
            try:
                id.update(str(arg))
            except Exception as e:
                self.log.warning(
                    f"'DB cannot be identidied with given init arguments: {str(e)}")
                id.update(str(uuid4()))

        for key in sorted(idArgs[1].keys()):
            try:
                id.update(str(key))
                id.update(str(idArgs[1][key]))
            except Exception as e:
                self.log.warning(
                    f"'DB cannot be identidied with given init arguments: {key}: {str(e)}")
                id.update(str(uuid4()))

        return str(id.hexdigest())

    @property
    def id(self):
        return self._makeId(self.idArgs)

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
        if isinstance(T, dict) and not callable(T):
            T = Ts.RowTransformer(T)
        pipes = []
        if callable(T):
            pipes.append(
                lambda row: T(row, inverse=True)
            )
        if callable(transformer):
            pipes.append(transformer)
        _T = R.pipe(*pipes) if len(pipes) > 0 else lambda r: r

        fetchOne = self._queryHelper((q, p), None, stripParams, debug)

        gen = self.generateRow(fetchOne, _T)
        if fetchAll:
            return [r for r in gen]

        return gen

    @classmethod
    def constantRows(cls, colTypeMap: dict, rows: tuple | list):

        def eval(type, value):
            return type(value) if callable(type) \
                else f'cast({value} as {type})'

        util = cls.createUtil()
        qColumns = util.quote([col for col in colTypeMap.keys()])
        qColumns = ', '.join(qColumns)
        qRows = []
        for row in rows:
            qRow = []
            for name, type in colTypeMap.items():
                value = row[name] if name in row and row[name] is not None else 'null'
                if value != 'null':
                    value = eval(type, value)
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
