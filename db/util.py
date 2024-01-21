from uuid import uuid4
import re

from contrib.p4thpymisc.src.misc import items


class UtilError(Exception):
    pass


class Util():

    uniqifyCtr = 0

    quoteRe = re.compile(r'[:][|](.*?)[|][:]')

    def __init__(self, prefix, suffix, quoteChar, placeholder, pNamePrefix=''):
        self.prefix = prefix
        self.suffix = suffix
        self.quoteChar = quoteChar
        self.placeholder = placeholder
        self.pNamePrefix = pNamePrefix
        self._pRe = None

    _idCtr = 0
    _pId = str(uuid4()).replace('-', '_')

    @classmethod
    def nextUniq(cls, suffix=None, prefix=None, sep='', caster=str):
        cls.uniqifyCtr = cls.uniqifyCtr + 1

        res = caster(cls.uniqifyCtr)

        if prefix:
            res = caster(prefix) + sep + res

        if suffix:
            res = res + sep + caster(suffix)

        return res

    @classmethod
    def parseIndexName(cls, fqn):
        return fqn

    @classmethod
    def parseIndexTableName(cls, fqn):
        return fqn

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

        def quotePath(path):
            path = path.split('.')
            path = ['{prefix}{qc}{e}{qc}'.format(
                prefix=prefix, e=e, qc=self.quoteChar) for e in path]
            return '.'.join(path)

        prefix = '' if table is None else self.quote(table, quote) + '.'
        if not isinstance(expr, str):
            try:
                if quote is False:
                    return [prefix + e for e in expr]
                return [self.quote(e, quote=quote, table=table) for e in expr]
            except Exception as e:
                print(e)
                pass

        if quote is False:
            return prefix + expr
        elif quote is True:
            return quotePath(expr)

        def replace(match):
            return quotePath(match.group(1))

        return self.quoteRe.sub(replace, expr)

    def p(self, p, val, name='', prefix='', suffix=''):
        name = str(self.pNamePrefix) + self.nextUniq(name, self._pId, str)
        p[name] = val
        return self.prefix + prefix + name + suffix + self.suffix

    def ps(self, p, values, sep=None):
        names = []
        for key, val in items(values):
            names.append(self.p(p, val, name=str(key)))
        return names if sep == None else sep.join(names)

    def pStrip(self, q, p):

        pNames = self.pRe.findall(q)
        # print('>',pNames)
        pStripped = {}
        for pName in pNames:
            if not pName in p:
                raise UtilError(
                    '{} from {} notis not a known parameter in {}.'.format(pName, q, p))
            pStripped[pName] = p[pName]
        return pStripped

    @property
    def pRe(self):
        if self._pRe is None:
            self._pRe = re.compile(r'{prefix}([\w]*?){suffix}[^\w]'.format(
                prefix=re.escape(self.prefix), suffix=re.escape(self.suffix)))
        return self._pRe
