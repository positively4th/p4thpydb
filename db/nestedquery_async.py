from .nestedquery import NestedQuery as NestedQuery_sync


class NestedQuery(NestedQuery_sync):

    @classmethod
    async def query(cls, executer, qs, qSep=None, *args, **kwargs):
        qOpen, qLoad, qClose = cls.splitQueries(qs)

        for q in qOpen:
            await executer(q, *args, **kwargs)

        r = await executer(qLoad, *args, **kwargs)

        for q in qClose:
            await executer(q, *args, **kwargs)

        return r
