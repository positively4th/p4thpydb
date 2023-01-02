from uuid import uuid4
import re
from contrib.p4thpy.uniqify import Uniqify
from contrib.p4thpy.tools import Tools

class UtilError(Exception):
    pass

class Util():

    def __init__(self, prefix, suffix, quoteChar, placeholder, pNamePrefix=''):
        self.prefix = prefix
        self.suffix = suffix
        self.quoteChar = quoteChar
        self.placeholder = placeholder
        self.pNamePrefix = pNamePrefix
        self._pRe = None

    _idCtr = 0
    _pId = str(uuid4()).replace('-','_')


    @classmethod
    def idCtr(cls):
        cls._idCtr = cls._idCtr + 1
        return str(cls._idCtr)
    
    @classmethod
    def schemaTableSplit(cls, dottedPair, invert=False, infix='.'):

        if invert:
            if isinstance(dottedPair, str):
                return dottedPair
            if len(dottedPair) < 2:
                return dottedPair[0]
            if dottedPair[0]:
                return '{}{}{}'.format(dottedPair[0], infix, dottedPair[1])
            return dottedPair[1]
            
        pair = dottedPair.split(infix)
        return (pair[0] if len(pair) > 1 else None, pair[1] if len(pair) > 1 else pair[0])


    @classmethod
    def qpTSplit(cls, qp0):
        qp = (qp0,) if isinstance(qp0, str) else qp0 
        q = qp[0]
        p = qp[1] if len(qp) > 1 else []
        T = qp[2] if len(qp) > 2 else None
        return q, p, T

    def quote(self, expr, quote=True, table=None):
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
        _expr = ['{prefix}{qc}{e}{qc}'.format(prefix=prefix, e=e, qc=self.quoteChar) for e in _expr]

        return '.'.join(_expr)

    def p(self, p, val, name='', prefix='', suffix=''):
        name = str(self.pNamePrefix) + Uniqify.next(prefix=name, suffix=self._pId, sep='_', caster=str)
        p[name] = val
        return self.prefix + prefix + name + suffix + self.suffix

    def ps(self, p, values, sep=None):
        names = []
        for key, val in Tools.keyValIter(values):
            names.append(self.p(p, val, name=str(key)))
        return names if sep == None else sep.join(names)

    def pStrip(self, q,p):

        pNames = self.pRe.findall(q)
        #print('>',pNames)
        pStripped = {}
        for pName in pNames:
            if not pName in p:
                raise UtilError('{} from {} notis not a known parameter in {}.'.format(pName, q, p))
            pStripped[pName] = p[pName]
        return pStripped

    @property
    def pRe(self):
        if self._pRe is None:
            self._pRe = re.compile(r'{prefix}([\w]*?){suffix}[^\w]'.format(prefix=re.escape(self.prefix), suffix=re.escape(self.suffix)))
        return self._pRe 
