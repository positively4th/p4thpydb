import re
from uuid import uuid4

from .util import Util

class NestedQueryDepthError(Exception):
    pass

class NestedQueryMissingQueryError(Exception):
    pass



class NestedQuery():
    replaceRe0 = r'[:][<]([+-=]?)({{name}})[>][:](:? ([A-Za-z0-9_]*?)){0,1}'

    uuid = str(uuid4())
    quoteChar = '"'
    replaceRe = re.compile(replaceRe0.replace('{{name}}', '[A-Za-z0-9_]*?'))
    quoteRe = re.compile(r'[:][|](.*?)[|][:]')
    qSep = ';\n\n-- next query\n\n'

    @staticmethod
    def query(executer, qs, qSep=None, *args, **kwargs):
        qSep = NestedQuery.qSep if qSep is None else qSep
        
        assert isinstance(qs, str) or len(qs) == 3
        
        if isinstance(qs, str):
            qOpen = []
            qLoad = qs
            qClose = []
        elif len(qs) == 3:
            qOpen = qs[0].split(qSep)
            qLoad = qs[1]
            qClose = qs[2].split(qSep)
        
        for q in qOpen:
            executer(q, *args, **kwargs)

        for r in executer(qLoad, *args, **kwargs):
            yield r

        for q in qClose:
            executer(q, *args, **kwargs)

    @staticmethod
    def buildCTE(query, queriesMap, uuid=None, escape=r'"'):

        orderMap, _ = NestedQuery._order(query, queriesMap)
        uuid = NestedQuery.uuid if uuid is None else uuid

        def whileChanging(val, f, iterMax=100):
            for tmp in range(iterMax):
                tmp = f(val)
                if tmp == val:
                    return val
                val = tmp
            return None

        def createSubQueryReplacer(qName, qSub):

            def replaceSubQuery(q):
                def onMatch(matches):
                    # r'[:][<]([+-=]?)([A-Za-z0-9_]*?)[>][:](:? ([A-Za-z0-9_]*?)){0,1}'

                    alias = ' _' + qName if matches[3] is None else ' ' + matches[3]
                    res = '({}) {}'.format(qSub, alias)
                    return res

                return pattern.sub(onMatch, q)

            pattern = re.compile(NestedQuery.replaceRe0.replace('{{name}}', qName))

            return replaceSubQuery

        def createCTEReplacer(fr, to):

            def replaceCTE(q):
                return q.replace(fr, to)

            return replaceCTE

        def applyReplacements(subject, replacements):
            for replacement in replacements:
                subject = replacement(subject)
            return subject

        def createCTEQuery(qName0):
            materialized = ''
            if qName0[0] == '=':
                qName = qName0[1:]
                replacements.append(createSubQueryReplacer(qName, queriesMap[qName]))
                return []
            elif qName0[0] in ('+', '-'):
                materialized = qName0[0]
                qName = qName0[1:]
                materialized = ' MATERIALIZED ' if materialized == '+' else ' NOT MATERIALIZED '
            else:
                qName = qName0
            q = queriesMap[qName]
            replacement = escape + Util.nextUniq(prefix=qName, suffix=uuid, sep='_') + escape
            replacements.append(createCTEReplacer(':<'+qName0+'>:', replacement))
            return ['{} AS{}({})'.format(replacement, materialized, q)]

        qs = []
        replacements = []
        for qName in sorted(orderMap, key=orderMap.get, reverse=True):
            qs += createCTEQuery(qName)
        res = 'WITH {}\n{}'.format(',\n'.join(qs), query) \
            if len(qs) > 0 else query

        res = whileChanging(res, lambda res: applyReplacements(res, replacements))

        res = NestedQuery._quote(res)
        return res
    
    @staticmethod
    def buildTempQueries(query, queriesMap, uuid=None, escape=r'"', qSep=None):
        qSep = NestedQuery.qSep if qSep is None else qSep

        def stripMat(qName):
            if qName is None:
                return None
            return qName[1:] if qName[0] in ('+', '-') else qName

        _orderMap, _parentChildMap = NestedQuery._order(query, queriesMap)

        orderMap = {}
        for qName, order in _orderMap.items():
            qName = stripMat(qName)
            orderMap[qName] = max(order, orderMap[qName]) if qName in orderMap else order
        parentChildMap = {}
        for qName, children in _parentChildMap.items():
            qName = stripMat(qName)
            cs = [c for c in children]
            parentChildMap[qName] = set(cs+ parentChildMap[qName]) if qName in parentChildMap else cs

        uuid = NestedQuery.uuid if uuid is None else uuid

        def createTempQuery(qName):

            q = queriesMap[qName]
            uqName = escape + Util.nextUniq(prefix=qName, suffix=uuid, sep='_') + escape

            replacements[qName] = uqName
            replacements['-'+qName] = uqName
            replacements['+'+qName] = uqName
            return r'CREATE TEMP TABLE {} AS {}'.format(uqName, q)

        def dropTempQuery(uqName):
            return r'DROP TABLE {}'.format(uqName)

        def childExists(child, parentChildMap):
            for p,cs in parentChildMap.items():
                if len({child, '-' + child, '+' + child}.intersection(set(cs))):
                    return True
            return False
        
        def findDiscardees(qName):
            children = parentChildMap[qName] if qName in parentChildMap else []
            obsolete = []
            while len(children) > 0:
                child = children.pop()
                if not childExists(stripMat(child), parentChildMap):
                    obsolete.append(stripMat(child))
            return obsolete
        
        qOpen = []
        qClose = {}
        replacements = {}
        for qName in sorted(orderMap, key=orderMap.get, reverse=True):
            qOpen.append(createTempQuery(qName))
            qClose[qName] = dropTempQuery(replacements[qName])
            for qn in findDiscardees(qName):
                qOpen.append(qClose.pop(stripMat(qn)))


        qOpen = qSep.join(qOpen)
        qClose = qSep.join(qClose.values())
        qLoad = query

        for fr, to in replacements.items():
            qOpen = qOpen.replace(':<'+fr+'>:', to)
            qLoad = qLoad.replace(':<'+fr+'>:', to)
            qClose = qClose.replace(':<'+fr+'>:', to)

        qOpen = NestedQuery._quote(qOpen)
        qLoad = NestedQuery._quote(qLoad)
        qClose = NestedQuery._quote(qClose)
        return qOpen, qLoad, qClose
    
    
    @staticmethod
    def _quote(query, quoteChar=None):

        _quoteChar = NestedQuery.quoteChar if quoteChar is None else quoteChar
        def replace(match):

            matchee = match.group(1)
            while len(matchee) > 0 and matchee[0] == _quoteChar:
                matchee = matchee[1:]
            while len(matchee) > 0 and matchee[-1] == _quoteChar:
                matchee = matchee[0:-1]

            parts = matchee.split('.')
            return '.'.join([
                _quoteChar + part + _quoteChar for part in parts
            ])

        res = NestedQuery.quoteRe.sub(replace,query)
        return res
        
    @staticmethod
    def _order(query, queriesMap, maxDepth=100):

        def probe(orderMap, parentChildMap, q, qParent, depth):

            def recurse(match):

                nonlocal orderMap
                nonlocal parentChildMap
                nonlocal q
                
                sign = match.group(1) if match.group(1) else ''
                qName = match.group(2)
                if not qName in queriesMap:
                    raise NestedQueryMissingQueryError('{} is not a known query in {}.'.format(qName, q))

                qKey = sign + qName

                parentChildMap[qParent] = parentChildMap[qParent] if qParent in parentChildMap else set([])
                parentChildMap[qParent].add(qKey)

                orderMap[qKey] = max(depth, orderMap[qKey] if qKey in orderMap else depth)
                orderMap, parentChildMap = probe(orderMap, parentChildMap, queriesMap[qName], qKey, depth+1)

            if depth > maxDepth:
                raise NestedQueryDepthError('{} seems to be too deep.'.format(query))

            NestedQuery.replaceRe.sub(recurse, q)

            return orderMap, parentChildMap
        
        return probe({}, {}, query, None, 0) 
            

