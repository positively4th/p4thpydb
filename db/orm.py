import itertools
import ramda as R

from .ormqueries import ORMQueries
from .ormqueries import TableSpecModel


class ORM(ORMQueries):

    def __init__(self, db, util, pipe):
        super().__init__(util, pipe)
        self.db = db

    def query(self, qpT, *args, **kwargs):
        return self.db.query(qpT, *args, **kwargs)

    def queries(self, args):
        return [self.query(_[0], **_[1]) for _ in args]

    def createTable(self, *args, **kwargs):
        self.queries(self._createTable(*args, **kwargs))
        self.queries(self._createViews(args[0]))

    def ensureTable(self, tableSpec):
        res = not self.tableExists(tableSpec)
        qArgs = []
        if res:
            qArgs += self._createTable(tableSpec)
            qArgs += self._createViews(tableSpec)
            self.queries(qArgs)
        return res

    def dropTable(self, tableSpec):
        qArgs = []
        qArgs += self._dropViews(tableSpec)
        qArgs += self._dropTable(tableSpec)
        self.queries(qArgs)

    def dropView(self, viewName):
        self.queries(self._dropView(viewName))

    def dropViews(self, tableSpec):
        self.queries(self._dropViews(tableSpec))

    def tableExists(self, tableSpec):
        model = TableSpecModel(tableSpec)
        # allColumns = ', '.join(self.util.quote(model.allColumns()))
        return self.db.tableExists(tableSpec['name'], model.allColumns())

    def insert(self, tableSpec, rows, fetchAll=False, returning=None, batchSize=None, debug=False):
        _batchSize = self.defBatchSize if batchSize is None else batchSize
        qArgs = self._insert(
            tableSpec, rows, returning=returning, batchSize=_batchSize)
        res = self.queries(qArgs)
        res = itertools.chain(*res)
        if fetchAll:
            res = list(res)
        return res

    def update(self, tableSpec, rows, debug=False, returning=None):

        qArgs = self._update(tableSpec, rows, debug, returning)
        updRows = self.queries(qArgs)
        res = self.ensureSingleRows(updRows)
        assert res is None or len(res) == len(rows)
        return res

    def upsert(self, tableSpec, rows, fetchAll=False, batchSize=None, returning=None, debug=False):
        _batchSize = self.defBatchSize if batchSize is None else batchSize
        _returning = self.prepareReturning(tableSpec, returning)
        updates, omits = self._upsertUpdate(
            tableSpec, rows, returning=_returning)
        res = self.queries(updates)
        ups, ins = self._upsertInsert(
            tableSpec, rows, res, batchSize=_batchSize, returning=returning)

        assert ins is not None
        for i in ins:
            if ins is None:
                pass
            assert i is not None

        ins = self.queries(ins)

        ups = itertools.chain(*ups)
        ups = map(R.omit(omits), ups)

        res = itertools.chain(*ins, ups)
        if fetchAll:
            res = [r for r in res]
        return res

    def delete(self, tableSpec, keyMaps, fetchAll=False):
        qArgs = self._deleteSelectQuery(tableSpec, keyMaps)
        res = itertools.chain(*self.queries(qArgs))
        qqArgs = []
        for qpT, _ in qArgs:
            qqArgs += self._deleteDeleteQuery(tableSpec, qpT[0], qpT[1])
        self.queries(qqArgs)

        if fetchAll:
            res = [r for r in res]
        return res
