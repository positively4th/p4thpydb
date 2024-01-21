import asyncio
from .db import DB as DB_sqlite

from uuid import uuid4


class DB(DB_sqlite):

    @property
    def sync(self):
        return super()

    @classmethod
    def createORM(cls, db):
        from .orm_async import ORM as _SQLITEORM
        return _SQLITEORM(db)

    warn_async_ctr = 1

    async def query(self, qpT, transformer=None, stripParams=False, debug=None):

        def helper():
            return self.sync.query(qpT, transformer=transformer,
                                   stripParams=stripParams, fetchAll=True, debug=debug)
        if self.warn_async_ctr > 0:
            self.log.warning(
                'sqlite DB does not support async/await and the code runs in executor.')
            self.warn_async_ctr -= 1
        return await asyncio.get_event_loop().run_in_executor(None, helper)

    async def startTransaction(self):
        self.log.debug('startTransaction (%s)' % len(self.savepoints))
        id = str(uuid4())
        await self.query('SAVEPOINT "{}"'.format(id), debug=None)
        self.savepoints.append(id)

    async def rollback(self):
        self.log.debug('rollback (%s)' % len(self.savepoints))
        if len(self.savepoints) < 1:
            return
        id = self.savepoints.pop()
        await self.query(('ROLLBACK TO "{}"'.format(id)), debug=None)
        await self.query(('RELEASE "{}"'.format(id)), debug=None)

    async def commit(self):
        self.log.debug('commit (%s)' % len(self.savepoints))
        id = self.savepoints.pop()
        await self.query(('RELEASE "{}"'.format(id)), debug=None)

    async def tableExists(self, tableName, columnNames):
        sch, tbl = self.util.schemaTableSplit(tableName)
        sch = 'main' if sch is None else sch

        p = {}
        q = '''
        SELECT m.*, m.name as "table", p.name as "column", p.pk as "isPrimaryKey"
        FROM `{sch}`.sqlite_master AS m
        JOIN pragma_table_info(m.name) AS p
        WHERE m.name = {tbl}
        ORDER BY m.name, p.name
        '''.format(sch=sch, tbl=self.util.p(p, tbl))
        rows = await self.query((q, p))
        a = set([row['column'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)
