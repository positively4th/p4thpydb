import psycopg

from .db import DB as DB_pgsql
from ..ts import Ts


class DB(DB_pgsql):

    @property
    def sync(self):
        return super()

    @classmethod
    def createORM(cls, db):
        from .orm_async import ORM as _PGORM
        return _PGORM(db)

    async def query(self, qpT, debug=None):
        q, p, T = self.util.qpTSplit(qpT)
        q = DB.protectMod(q)
        T = Ts.transformerFactory(T, inverse=True)

        async with await self.async_cursor as cursor:
            await cursor.execute(q, p)
            if cursor.rownumber is not None:
                rows = await cursor.fetchall()
            else:
                return None

        return [T(row) for row in rows]

    def __init__(self, url=None, log=None, aSync=False, **kwargs):
        super().__init__(url, log, **kwargs)

        self._cursor = None
        self._async_db = None

    def __del__(self):
        if self._async_db is not None:
            self.async_db.close()
            self.async_db = None

    def connect(self):
        return psycopg.connect(self.url, autocommit=True)

    async def tableExists(self, table, columnNames):
        schema, _table = self.util.schemaTableSplit(table)
        p = {}
        q = """
        SELECT table_name 
        FROM information_schema.tables 
        WHERE  table_name = {} AND ({} = 1 OR table_schema = {})
        LIMIT 2
        """.format(*self.util.ps(p, [table, 0 if schema else 1, schema]))
        rows = [r for r in await self.query((q, p), debug=False)]
        if len(rows) != 1:
            return False

        p = {}
        q = """
        SELECT *
        FROM information_schema.columns
        WHERE  table_name = {} AND ({} = 1 OR table_schema = {})
        """.format(*self.util.ps(p, [table, 0 if schema else 1, schema]))
        rows = await self.query((q, p), debug=False)
        a = set([row['column_name'] for row in rows])
        b = set(columnNames)
        return a.issubset(b) and b.issubset(a)

    @property
    async def async_db(self):
        if self._async_db is None:
            self._async_db = await psycopg.AsyncConnection.connect(self.url, autocommit=True, row_factory=psycopg.rows.dict_row)
        return self._async_db

    @property
    async def async_cursor(self):
        return (await self.async_db).cursor()
