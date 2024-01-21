import ramda as R
import asyncio
import itertools

from .ormqueries import ORMQueries
from .ormqueries import TableSpecModel


class ORM(ORMQueries):

    def __init__(self, db, util, pipe):
        super().__init__(util, pipe)
        self.db = db

    def query(self, qpT, *args, **kwargs):
        q, p, T = self.util.qpTSplit(qpT)
        return self.db.query((q, p, T), *args, **kwargs)

    def queries(self, args):
        return asyncio.gather(*[self.query(_[0], **_[1]) for _ in args])

    async def tableExists(self, tableSpec):
        model = TableSpecModel(tableSpec)
        return await self.db.tableExists(tableSpec['name'], model.allColumns())

    async def createTable(self, tableSpec):
        await self.queries(self._createTable(tableSpec))
        await self.queries(self._createViews(tableSpec))

    async def ensureTable(self, tableSpec):
        res = not await self.tableExists(tableSpec)
        if res:
            await self.queries(self._createTable(tableSpec))
            await self.queries(self._createViews(tableSpec))
        return res

    def dropTable(self, tableSpec):
        qArgs = []
        qArgs += self._dropViews(tableSpec)
        qArgs += self._dropTable(tableSpec)
        return self.queries(qArgs)

    async def insert(self, tableSpec, rows, returning=None, batchSize=None):
        _batchSize = self.defBatchSize if batchSize is None else batchSize
        qArgs = self._insert(
            tableSpec, rows, returning=returning, batchSize=_batchSize)
        return R.unnest(await self.queries(qArgs))

    async def update(self, tableSpec, rows, debug=False, returning=None):
        qArgs = self._update(tableSpec, rows, debug, returning)
        updRows = await self.queries(qArgs)
        res = self.ensureSingleRows(updRows)
        assert res is None or len(res) == len(rows)
        return res

    async def upsert(self, tableSpec, rows, returning=None, batchSize=None):
        _returning = self.prepareReturning(tableSpec, returning)

        _batchSize = self.defBatchSize if batchSize is None else batchSize
        updates, omits = self._upsertUpdate(
            tableSpec, rows, returning=_returning)
        res = await self.queries(updates)
        res = (
            (
                r for r in rows
            ) for rows in res
        )
        ups, ins = self._upsertInsert(
            tableSpec, rows, res, batchSize=_batchSize, returning=returning)
        ups = itertools.chain(*ups)
        # ups = list(ups) #for debugging
        ins = await self.queries(ins)
        ups = map(R.omit(omits), ups)
        # ups = list(ups) #for debugging
        return itertools.chain(*ins, ups)

    async def delete(self, tableSpec, keyMaps,):
        qArgs = self._deleteSelectQuery(tableSpec, keyMaps)
        res = R.unnest(await self.queries(qArgs))
        qqArgs = []
        for qpT, _ in qArgs:
            qqArgs += self._deleteDeleteQuery(tableSpec, qpT[0], qpT[1])
        await self.queries(qqArgs)
        return res
