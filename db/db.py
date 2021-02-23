
try:
    pass
except ImportError:
    pass

class DB:

    __DEBUG__ = False

    _idCtr = 0

    @classmethod
    def idCtr(cls):
        cls._idCtr = cls._idCtr + 1
        return str(cls._idCtr)
    
    @classmethod
    def __qpSplit__(cls, qp0):
        print('__qpSplit__ is deprecated, use __qpTSplit__ instead!')
        assert 1 == 0
        
    @staticmethod
    def __qpTSplit__(qp0):
        qp = (qp0,) if isinstance(qp0, str) else qp0 
        q = qp[0]
        p = qp[1] if len(qp) > 1 else []
        T = qp[2] if len(qp) > 2 else None
        return q, p, T
        

    def quote(self, expr, quote=True, table=None, quoteChar='`'):
        prefix = '' if table is None else self.quote(table, quote) + '.'
        if not isinstance(expr, str):
            try:
                if not quote:
                    return [prefix + e for e in expr]
                return [self.quote(e, True, table) for e in expr]
            except Exception as e:
                print(e)
                pass

        if not quote:
            return prefix + expr
        _expr = expr.split('.')
        _expr = ['{prefix}{qc}{e}{qc}'.format(prefix=prefix, e=e, qc=quoteChar) for e in _expr]

        return '.'.join(_expr)

    def __init__(self):
        pass
    
    def startTransaction(self):
        print('startTransaction', self.savepoints)
        id = str(uuid4())
        self.query('SAVEPOINT "{}"'.format(id), debug=True);
        self.savepoints.append(id)
        
    def rollback(self):
        print('rollback', self.savepoints)
        if len(self.savepoints) < 1:
            return
        id = self.savepoints.pop()
        self.query(('ROLLBACK TO "{}"'.format(id)),debug=True);
        self.query(('RELEASE "{}"'.format(id)), debug=True);
        
    def commit(self):
        print('commit', self.savepoints)
        id = self.savepoints.pop()
        self.query(('RELEASE "{}"'.format(id)), debug=True);



    
