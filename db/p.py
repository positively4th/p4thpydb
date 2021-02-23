from uuid import uuid4
import re
from contrib.p4thpy.uniqify import Uniqify
from contrib.p4thpy.tools import Tools

class PError(Exception):
    pass

class P():

    _pId = str(uuid4()).replace('-','_')
    pRe = re.compile(r'[:]([\w]*?)[^\w]')


    @classmethod
    def p(cls, p, val, name='', prefix=':'):
        name = Uniqify.next(prefix=name, suffix=P._pId, sep='_', caster=str)
        p[name] = val
        return prefix + name

    @classmethod
    def ps(cls, p, values, sep=None):
        names = []
        for key, val in Tools.keyValIter(values):
            names.append(P.p(p, val, name=str(key)))
        #print(values, names, p)
        return names if sep == None else sep.join(names)

    @classmethod
    def pStrip(cls, q,p):

        pNames = P.pRe.findall(q)
        #print('>',pNames)
        pStripped = {}
        for pName in pNames:
            if not pName in p:
                raise PError('{} from {} notis not a known parameter in {}.'.format(pName, q, p))
            pStripped[pName] = p[pName]
        return pStripped


