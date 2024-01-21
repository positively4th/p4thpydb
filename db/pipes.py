

class PipesError(Exception):
    pass


class Pipes():

    def __init__(self, util, *args, **kwargs):
        self.util = util

    def quote(self, expr, quote=True, table=None):
        return self.util.quote(expr, quote, table)

    def __applyPipes__(self, f, *args, **kwargs):
        if callable(f):
            return f(*args, **kwargs)
        if isinstance(f, (tuple, list)) and len(f) == 3:
            return f
        if isinstance(f, (str)) and getattr(self, f):
            f = getattr(self, f)
            return self.__applyPipes__(f, *args, **kwargs)

        raise PipesError('Cannot apply pipe.')

    def concat(self, qpT, pipes=[], *args, **kwargs):

        for pipe in pipes:
            assert len(pipe) >= 1
            assert len(pipe) <= 2

            f = pipe[0]
            _kwargs = {}
            _kwargs.update(pipe[1] if len(pipe) > 1 else {})

            # _args = [qp] + list(args)
            # print(qp, _args, _kwargs)
            qpT = self.__applyPipes__(f, qpT, **_kwargs)
        return qpT

    def member(self, qpT, expr, values=None, op='IN', quote=True):
        q, p, T = self.util.qpTSplit(qpT)
        vs = values
        # print('pipeValues', values)
        if vs == None:
            return q, p, T
        vs = [vs] if isinstance(vs, (float, int, str)) else list(vs)
        if T and expr in T:
            vs = [T[expr](v) for v in vs]
        pp = dict(p)
        q = 'SELECT * FROM ({}) AS _q WHERE {} {} ({})'.format(q,
                                                               self.quote(expr, quote), op, self.util.ps(pp, vs, sep=','))
        return q, pp, T

    def equals(self, qpT, map=None, quote=True, op='=', *args, **kwargs):
        q, p, T = self.util.qpTSplit(qpT)
        if map == None:
            return q, p, T

        if isinstance(map, (list, tuple)):
            q, p, T = self.any((q, p, T), pipes=[
                [self.equals, {'map': m, 'quote': quote, 'op': op}] for m in map
            ]
            )
            return q, p, T

        Tm = {k: T[k](v) if T and k in T else v for k, v in map.items()}

        pp = dict(p)
        wheres = ['{} {} {}'.format(self.quote(
            l, quote), op, self.util.p(pp, r, l)) for l, r in Tm.items()]
        q = 'SELECT * FROM ({}) AS _q WHERE {}'.format(q, ' AND '.join(wheres))
        return q, pp, T

    def matches(self, qpT, map=None, quote=True, *args, **kwargs):
        raise PipesError('Not implemented')

    def like(self, qpT, expr, pattern='%', op='LIKE'):
        q, p, T = self.util.qpTSplit(qpT)

        pp = dict(p)
        q = 'SELECT * FROM ({}) AS _q WHERE {} {} ({})'.format(q,
                                                               expr, op, self.util.p(pp, pattern))
        return q, pp, T

    def any(self, qpT, pipes, cteName=None, op='UNION'):
        cteName = cteName if cteName is not None else '_q_' + self.util.idCtr()
        q, p, T = self.util.qpTSplit(qpT)

        if len(pipes) < 1:
            return q, p, T

        qPipeed = 'SELECT * FROM {}'.format(cteName)
        qs = []
        for pipe in pipes:
            qq, p, T = self.util.qpTSplit(
                self.concat((qPipeed, p), pipes=[pipe]))
            qs.append('SELECT * FROM ({}) AS _q_{}'.format(qq, self.util.idCtr()))

        q = ['\nWITH {} AS ({})'.format(cteName, q)]
        q.append('\n{}\n'.format(op).join(qs))
        q = '\n'.join(q)
        return q, p, T

    def all(self, qp, pipes, cteName=None):
        return self.any(qp, pipes, cteName, op='INTERSECT')

    def order(self, qpT, exprs, orders=None, quote=False):
        q, p, T = self.util.qpTSplit(qpT)
        _orders = orders if orders != None else ['ASC']
        _orders = [_orders] if isinstance(_orders, str) else _orders
        _exprs = [exprs] if isinstance(exprs, str) else exprs

        pairs = [' '.join([self.quote(expr, quote), _orders[i % len(_orders)]])
                 for (i, expr) in enumerate(_exprs)]
        q = 'SELECT * FROM ({}) AS _q ORDER BY {}'.format(q, ', '.join(pairs))
        return q, p, T

    def limit(self, qpT, limit, offset=None):
        q, p, T = self.util.qpTSplit(qpT)
        if limit == None:
            return q, p, T

        o = ''
        pp = dict(p)
        if not offset is None and offset > 0:
            o = 'OFFSET {}'.format(self.util.p(pp, offset))
        q = r'{} LIMIT {} {}'.format(q, self.util.p(pp, limit), o)
        return q, pp, T

    def distinct(self, qpT):
        q, p, T = self.util.qpTSplit(qpT)
        q = 'SELECT DISTINCT * FROM ({}) AS _q'.format(q)
        return q, p, T

    def columns(self, qpT, columns=[], quote=True):
        # Todo: Remove unused transforms
        q, p, T = self.util.qpTSplit(qpT)
        # print('pipeValues', values0)
        if len(columns) < 1:
            return q, p
        q = 'SELECT {} FROM ({}) AS _q'.format(
            ', '.join([self.quote(c, quote) for c in columns]), q)
        T = None if T is None else {name: T[name]
                                    for name in columns if name in T}
        return q, p, T

    def aliases(self, qpT, aliasExprMap={}, quote=False):
        # Todo: Remove unused transforms
        q, p, T = self.util.qpTSplit(qpT)
        # print('pipeValues', values0)
        if len(aliasExprMap) < 1:
            return q, p

        pairs = ['{} AS {}'.format(self.quote(expr, quote), self.quote(
            alias, quote)) for alias, expr in aliasExprMap.items() if alias and expr]
        q = 'SELECT {} FROM ({}) AS _q'.format(', '.join(pairs), q)
        if not T is None:
            TT = {}
            TT.update({
                expr: T[expr] for alias, expr in aliasExprMap.items() if expr in T
            })
            TT.update({
                alias: T[alias] for alias, expr in aliasExprMap.items() if alias in T
            })
            TT = TT if len(TT) > 0 else None
        else:
            TT = None
        return q, p, TT

    def aggregate(self, qpT, aggregates={}, keys={}, quote=True, op='=', *args, **kwargs):
        q, p, T = self.util.qpTSplit(qpT)

        if map == None:
            return q, p, T

        if isinstance(keys, (list, tuple)):
            _keys = {
                key: self.quote(key, quote) for key in keys
            }
        else:
            _keys = keys

        columns = [
            '{} as {}'.format(expr, self.quote(alias, quote)) for alias, expr in _keys.items()
        ] + [
            '{} as {}'.format(expr, self.quote(alias, quote)) for alias, expr in aggregates.items()
        ]
        groupColumns = [
            '{}'.format(self.quote(expr, quote)) for expr, alias in _keys.items()
        ]

        q = 'SELECT {} FROM ({}) AS _q'.format(', '.join(columns), q)
        if len(groupColumns) > 0:
            q = '{} GROUP BY {}'.format(q, ', '.join(groupColumns))
        return q, p, T
